import errno
import socket

from .handshake import PeerHandshake
from .session import Session

from tornado import gen
from tornado.iostream import IOStream
from tornado.netutil import TCPServer
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
    super(PeerBroker, self).__init__(io_loop=io_loop)
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
    log.debug('Bound at port %s' % port)
    self._port = port

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
  def handshake(self, iostream, session, prefix='', callback=None):
    """
      Supply prefix if we've already snooped the start of the handshake.
    """
    log.debug('Session [%s] starting handshake with %s:%s' % (
        self._session.peer_id,
        self._address[0],
        self._address[1]))

    handshake_full = PeerHandshake.make(self._session.torrent.info, self._session.peer_id)
    read_handshake, _ = yield [
        gen.Task(iostream.read_bytes, PeerHandshake.LENGTH - len(prefix)),
        gen.Task(iostream.write, handshake_full)]
    read_handshake = prefix + read_handshake

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == self._hash:
        self._id = handshake.peer_id
        self._iostream = iostream
        succeeded = True
      else:
        log.debug('Session [%s] got mismatched torrent hashes.' % self._session.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Session [%s] got bad handshake with %s:%s: %s' % (self._session.peer_id,
        self._address[0], self._address[1], e))
      iostream.close()

    log.debug('Session [%s] finishing handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

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
    handshake_prefix = yield gen.Task(iostream.read_bytes, PeerHandshake.PREFIX_LENGTH)
    session = self._sessions.get(handshake_prefix)
    if not session:
      session_provider = self._torrents.get(handshake_prefix)
      if not session_provider:
        iostream.close()
        return
      self._sessions[handshake_prefix] = session = session_provider(self.port)
    success = yield gen.Task(self.handshake, iostream, session, prefix=handshake_prefix)
    if success:
      session.add_peer(address, iostream)
