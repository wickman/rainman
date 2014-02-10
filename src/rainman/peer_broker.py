import errno
import socket

from .handshake import PeerHandshake
from .session import Session

from tornado import gen
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
from twitter.common import log
from twitter.common.quantity import Amount, Time


class PeerBroker(TCPServer):
  class Error(Exception): pass
  class BindError(Error): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, io_loop=None, port=None):
    self._torrents = {}  # map from handshake prefix => torrent
    self._sessions = {}  # map from handshake prefix => session
    self._failed_handshakes = 0
    super(PeerBroker, self).__init__(io_loop=io_loop)
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

  def register_torrent(self, torrent, session_provider=None):
    if session_provider is None:
      def default_session_provider(port):
        return Session(torrent, port=port, io_loop=self.io_loop)
      session_provider = default_session_provider
    self._torrents[PeerHandshake.make(torrent.info)] = session_provider

  @gen.engine
  def handshake(self, address, iostream, session, prefix='', callback=None):
    """Complete the handshake given a session.

       Uses:
         session.torrent
         session.peer_id

    """
    log.debug('Session [%s] starting handshake with %s:%s' % (
        session.peer_id, address[0], address[1]))

    handshake_full = PeerHandshake.make(session.torrent.info, session.peer_id)
    read_handshake, _ = yield [
        gen.Task(iostream.read_bytes, PeerHandshake.LENGTH - len(prefix)),
        gen.Task(iostream.write, handshake_full)]
    read_handshake = prefix + read_handshake

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == session.torrent.hash:
        succeeded = True
      else:
        log.debug('Session [%s] got mismatched torrent hashes.' % session.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Session [%s] got bad handshake with %s:%s: %s' % (session.peer_id,
        address[0], address[1], e))
      iostream.close()

    log.debug('Session [%s] finishing handshake with %s:%s' % (session.peer_id,
        address[0], address[1]))

    if callback:
      callback(succeeded)

  @gen.engine
  def initiate_connection(self, torrent, address):
    session = self._sessions.get(torrent)
    if not session:
      raise ValueError('Torrent not yet registered.')
    iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
    yield gen.Task(iostream.connect, address)
    success = yield gen.Task(self.handshake, iostream, session)
    if success:
      session.add_peer(address, iostream)

  @gen.engine
  def handle_stream(self, iostream, address):
    log.debug('PeerBroker got incoming connection from %s' % (address,))
    handshake_prefix = yield gen.Task(iostream.read_bytes, PeerHandshake.PREFIX_LENGTH)
    session = self._sessions.get(handshake_prefix)
    if not session:
      session_provider = self._torrents.get(handshake_prefix)
      if not session_provider:
        self._failed_handshakes += 1
        iostream.close()
        return
      self._sessions[handshake_prefix] = session = session_provider(self.port)
    success = yield gen.Task(self.handshake, address, iostream, session, prefix=handshake_prefix)
    if success:
      session.add_peer(address, iostream)
    else:
      self._failed_handshakes += 1

