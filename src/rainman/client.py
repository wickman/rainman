from collections import namedtuple
import errno
import socket

from .handshake import PeerHandshake
from .session import Session

from tornado import gen
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
from twitter.common import log
from twitter.common.quantity import Amount, Time


TorrentInfo = namedtuple('TorrentInfo', 'torrent session session_provider')


class Client(TCPServer):
  class Error(Exception): pass
  class BindError(Error): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, peer_id, io_loop=None, port=None):
    self.peer_id = peer_id
    self._ip = socket.gethostbyname(socket.gethostname())
    self._torrents = {}  # map from handshake prefix => TorrentInfo
    self._failed_handshakes = 0
    super(Client, self).__init__(io_loop=io_loop)
    self._port = self._do_bind(port)
    log.debug('Bound at port %s' % self._port)

  def _do_bind(self, port=None):
    port_range = [port] if port else self.PORT_RANGE
    for port in port_range:
      try:
        log.debug('Peer listener attempting to bind @ %s' % port)
        self.listen(port)
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise self.BindError('Could not bind to any port in range %s' % repr(port_range))

  @property
  def port(self):
    return self._port

  def default_session_provider(torrent):
    return Session(torrent, io_loop=self.io_loop)

  def register_torrent(self, torrent, session=None, session_provider=None):
    handshake_prefix = PeerHandshake.make(torrent.info)
    self._torrents[handshake_prefix] = TorrentInfo(
        torrent,
        session,
        session_provider or self.default_session_provider)

  @gen.engine
  def handshake(self, iostream, session, prefix='', callback=None):
    """Complete the handshake given a session.

       Uses:
         session.torrent
         session.peer_id
    """
    write_handshake = PeerHandshake.make(session.torrent.info, self.peer_id)
    read_handshake, _ = yield [
        gen.Task(iostream.read_bytes, PeerHandshake.LENGTH - len(prefix)),
        gen.Task(iostream.write, write_handshake)]
    read_handshake = prefix + read_handshake

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == session.torrent.hash:
        succeeded = True
      else:
        log.debug('Mismatched torrent hashes with %s' % handshake.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Got bad handshake: %s' % e)
      iostream.close()

    if callback:
      callback(handshake.peer_id if succeeded else None)

  def establish_session(self, handshake_prefix):
    torrent, session, session_provider = self._torrents.get(handshake_prefix, (None, None, None))
    if not torrent:
      raise self.Error('Cannot initiate a connection for an unknown torrent.')
    if not session:
      if not session_provider:
        raise self.Error('Cannot initiate a connection without a session or session provider.')
      session = session_provider(torrent)
      self._torrents[handshake_prefix] = TorrentInfo(torrent, session, session_provider)
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
    handshake_task = gen.Task(self.handshake, iostream, session)
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
    handshake_task = gen.Task(self.handshake, iostream, session, prefix=handshake_prefix)
    yield gen.Task(self.maybe_add_peer, handshake_task, iostream, session)
