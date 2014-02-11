from collections import namedtuple
import errno
import hashlib
import os
import socket

from .handshake import PeerHandshake
from .piece_broker import PieceBroker
from .peer_tracker import PeerTracker
from .session import Session

from tornado import gen
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_mkdtemp
from twitter.common.quantity import Amount, Data, Time


class Client(TCPServer):
  class Error(Exception): pass
  class BindError(Error): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, peer_id, chroot=None, io_loop=None, session_impl=Session):
    self.peer_id = peer_id
    self._ip = socket.gethostbyname(socket.gethostname())
    self._chroot = chroot or safe_mkdtemp()
    safe_mkdir(self._chroot)
    self._torrents = {}  # map from handshake prefix => TorrentInfo
    self._trackers = {}  # map from handshake prefix => PeerTracker
    self._sessions = {}  # map from handshake prefix => Session
    self._piece_brokers = {}  # map from handshake prefix => PieceBroker
    self._failed_handshakes = 0
    self._port = None
    self._session_impl = session_impl
    self._peer_callback = self.default_peer_callback
    super(Client, self).__init__(io_loop=io_loop)

  @property
  def port(self):
    return self._port

  @property
  def sessions(self):
    return self._sessions.values()

  def get_session(self, torrent):
    return self._sessions.get(PeerHandshake.make(torrent.info))

  def get_tracker(self, torrent):
    return self._trackers.get(PeerHandshake.make(torrent.info))

  def get_broker(self, torrent):
    return self._piece_brokers.get(PeerHandshake.make(torrent.info))

  def get_torrent(self, handshake_prefix):
    return self._torrents.get(handshake_prefix)

  # Override the default listener to find a port in the port range
  def listen(self):
    for port in self.PORT_RANGE:
      try:
        log.debug('Peer listener attempting to bind @ %s' % port)
        super(Client, self).listen(port)
        self._port = port
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise self.BindError('Could not bind to any port in range %s' % repr(port_range))
    log.debug('Client bound to port %d' % self._port)

  def register_torrent(self, torrent, session=None, callback=None):
    """Register a torrent with this client.

       :param torrent: :class:`Torrent`
       :param session: A :class:`Session` to pair to this torrent.  If None provided, a new
           session will be created.
    """
    torrent_hash = hashlib.sha1(torrent.info.raw()).hexdigest()
    handshake_prefix = PeerHandshake.make(torrent.info)
    if handshake_prefix in self._torrents:
      # skip dual register.
      return
    target_dir = os.path.join(self._chroot, torrent_hash)
    self._piece_brokers[handshake_prefix] = piece_broker = PieceBroker.from_torrent(
        torrent, chroot=target_dir, io_loop=self.io_loop)
    # TODO(wickman) this needs to be asynchronized via iopool
    piece_broker.initialize()
    self._torrents[handshake_prefix] = torrent
    if not session:
      self._sessions[handshake_prefix] = session = self._session_impl(
          piece_broker, io_loop=self.io_loop)
    # by default do not start the tracker
    self._trackers[handshake_prefix] = PeerTracker.get(torrent, self.peer_id, session)
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
    _, session = self.establish_session(PeerHandshake.make(torrent.info))
    session.add_peer(peer_id, iostream)

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
        log.debug('Mismatched torrent hashes with %s' % handshake.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Got bad handshake: %s' % e)
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

  @gen.engine
  def maybe_add_peer(self, handshake_task, iostream, session, callback=None):
    peer_id = yield handshake_task
    if peer_id:
      session.add_peer(peer_id, iostream)
    else:
      self._failed_handshakes += 1
      iostream.close()
    callback(peer_id)

  @gen.engine
  def initiate_connection(self, torrent, address, callback=None):
    handshake_prefix = PeerHandshake.make(torrent.info)
    torrent, session = self.establish_session(handshake_prefix)
    iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
    yield gen.Task(iostream.connect, address)
    handshake_task = gen.Task(self.handshake, torrent, iostream, session)
    callback((yield gen.Task(self.maybe_add_peer, handshake_task, iostream, session)))

  @gen.engine
  def handle_stream(self, iostream, address):
    log.debug('Client got incoming connection from %s' % (address,))
    handshake_prefix = yield gen.Task(iostream.read_bytes, PeerHandshake.PREFIX_LENGTH)
    try:
      torrent, session = self.establish_session(handshake_prefix)
    except self.Error as e:
      self._failed_handshakes += 1
      iostream.close()
      return
    handshake_task = gen.Task(self.handshake, torrent, iostream, session, prefix=handshake_prefix)
    yield gen.Task(self.maybe_add_peer, handshake_task, iostream, session)


class ClientFairScheduler(object):
  """A fair scheduler that tries to maintain even ingress/egress bandwidth across all
     torrents.
  """
  MAX_CONNECTIONS = 20
  MAX_REQUESTS = 100
  REQUEST_SIZE = Amount(64, Data.KB)
  DEFAULT_BANDWIDTH_PER_SECOND = Amount(20, Data.MB)

  def __init__(self,
               client,
               request_size=REQUEST_SIZE,
               max_requsts=MAX_REQUESTS,
               max_connections=MAX_CONNECTIONS,
               target_bandwidth=DEFAULT_BANDWIDTH_PER_SECOND):
    self.client = client
    self.client.set_peer_callback(self.peer_callback)

  @property
  def num_connections(self):
    return sum(len(session.peers) for session in self.client.sessions)

  def peer_callback(self, torrent, peer_id, iostream):
    pass
