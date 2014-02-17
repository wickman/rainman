from binascii import hexlify
import errno
import os
import socket

from .handshake import PeerHandshake
from .fs import DISK
from .peer import Peer
from .peer_tracker import PeerTracker
from .piece_broker import PieceBroker
from .session import Session

from tornado import gen
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_mkdtemp
from twitter.common.quantity import Amount, Time


class Client(TCPServer):
  class Error(Exception): pass
  class BindError(Error): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self,
               peer_id,
               chroot=None,
               io_loop=None,
               session_impl=Session,
               fs=DISK):
    self.peer_id = peer_id
    self._ip = socket.gethostbyname(socket.gethostname())
    self._chroot = chroot or safe_mkdtemp()
    safe_mkdir(self._chroot)
    self._torrents = {}  # map from handshake prefix => Torrent
    self._trackers = {}  # map from handshake prefix => PeerTracker
    self._sessions = {}  # map from handshake prefix => Session
    self._piece_brokers = {}  # map from handshake prefix => PieceBroker
    self._failed_handshakes = 0
    self._port = None
    self._session_impl = session_impl
    self._fs = fs  # this should probably be broker_impl
    self._peer_callback = self.default_peer_callback
    super(Client, self).__init__(io_loop=io_loop)

  def log(self, msg):
    log.debug('[%s]: %s' % (self.peer_id, msg))

  @property
  def port(self):
    return self._port

  @property
  def sessions(self):
    return self._sessions.values()

  def iter_sessions(self):
    for hash, session in self._sessions.items():
      yield (self._torrents[hash], session)

  def get_session(self, torrent):
    return self._sessions.get(torrent.handshake_prefix)

  def get_tracker(self, torrent):
    return self._trackers.get(torrent.handshake_prefix)

  def get_broker(self, torrent):
    return self._piece_brokers.get(torrent.handshake_prefix)

  def get_torrent(self, handshake_prefix):
    return self._torrents.get(handshake_prefix)

  # Override the default listener to find a port in the port range
  def listen(self):
    for port in self.PORT_RANGE:
      try:
        self.log('Peer listener attempting to bind @ %s' % port)
        super(Client, self).listen(port)
        self._port = port
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise self.BindError('Could not bind to any port in range %s' % repr(self.PORT_RANGE))
    self.log('Client bound to port %d' % self._port)

  # TODO(wickman) implement unregister_torrent
  def register_torrent(self, torrent, root=None, callback=None):
    """Register a torrent with this client.

       :param torrent: :class:`Torrent`
       :param root: The path that may or may not exist for this torrent.  If None, create one.
    """
    if torrent.handshake_prefix in self._torrents:
      # skip dual register.
      return
    target_dir = root or os.path.join(self._chroot, hexlify(torrent.handshake_prefix))
    self._piece_brokers[torrent.handshake_prefix] = piece_broker = PieceBroker.from_metainfo(
        torrent.info,
        chroot=target_dir,
        io_loop=self.io_loop,
        fs=self._fs)
    # TODO(wickman) this needs to be asynchronized via iopool
    piece_broker.initialize()
    self._torrents[torrent.handshake_prefix] = torrent
    self._sessions[torrent.handshake_prefix] = session = self._session_impl(
        piece_broker,
        io_loop=self.io_loop)
    # by default do not start the tracker
    tracker = self._trackers[torrent.handshake_prefix] = PeerTracker.get(torrent, self)
    tracker.start()
    session.start()
    if callback:
      callback(session)

  def set_peer_callback(self, callback):
    """Set the callback to be invoked when new peer connections are requested.  By default
       the callback is set to one that always adds incoming peers to a torrent session.

       :param callback: A callback that will be invoked with two arguments: the
           :class:`Torrent` the peer is registering with, the peer_id, and the
           :class:`tornado.iostream.IOStream` established with the peer.
    """
    self._peer_callback = callback

  def default_peer_callback(self, torrent, peer_id, iostream):
    _, session = self.establish_session(torrent.handshake_prefix)
    peer = Peer(peer_id, iostream, self.get_broker(torrent))
    session.add_peer(peer)

  # --- handshake logic
  @gen.engine
  def handshake(self, torrent, iostream, session, prefix='', callback=None):
    """Complete the handshake given a session.

       Uses:
         session.torrent
         session.peer_id
    """
    write_handshake = PeerHandshake.make(torrent.info, self.peer_id)
    read_handshake, _ = yield [
        gen.Task(iostream.read_bytes, PeerHandshake.LENGTH - len(prefix)),
        gen.Task(iostream.write, write_handshake)]
    read_handshake = prefix + read_handshake

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == torrent.hash:
        succeeded = True
      else:
        self.log('Mismatched torrent hashes with %s' % handshake.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      self.log('Got bad handshake: %s' % e)
      iostream.close()

    if callback:
      callback(handshake.peer_id if succeeded else None)

  def establish_session(self, handshake_prefix):
    torrent = self._torrents.get(handshake_prefix)
    if not torrent:
      raise self.Error('Cannot initiate a connection for an unknown torrent.')
    session = self._sessions.get(handshake_prefix)
    if not session:
      raise self.Error('No session associated with torrent!')
    return torrent, session

  @gen.coroutine
  def maybe_add_peer(self, handshake_task, torrent, iostream, session):
    peer_id = yield handshake_task
    if peer_id in session.peer_ids:
      iostream.close()
      raise gen.Return(None)
    if peer_id:
      self._peer_callback(torrent, peer_id, iostream)
      raise gen.Return(peer_id)
    else:
      self._failed_handshakes += 1
      iostream.close()
      raise gen.Return(None)

  @gen.coroutine
  def initiate_connection(self, torrent, address):
    torrent, session = self.establish_session(torrent.handshake_prefix)
    iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
    yield gen.Task(iostream.connect, address)
    handshake_task = gen.Task(self.handshake, torrent, iostream, session)
    raise gen.Return((yield gen.Task(
        self.maybe_add_peer, handshake_task, torrent, iostream, session)))

  @gen.coroutine
  def handle_stream(self, iostream, address):
    self.log('Client got incoming connection from %s' % (address,))
    handshake_prefix = yield gen.Task(iostream.read_bytes, PeerHandshake.PREFIX_LENGTH)
    try:
      torrent, session = self.establish_session(handshake_prefix)
    except self.Error:
      self._failed_handshakes += 1
      iostream.close()
      return
    handshake_task = gen.Task(self.handshake, torrent, iostream, session, prefix=handshake_prefix)
    yield gen.Task(self.maybe_add_peer, handshake_task, torrent, iostream, session)
