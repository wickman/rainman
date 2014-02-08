# TODO(wickman) Flesh this out
import hashlib

from tornado import gen
from tornado.netutil import TCPServer
from twitter.common import log


class PeerHandshake(object):
  class InvalidHandshake(Exception): pass

  PREFIX_LENGTH = 48
  LENGTH = 68
  PROTOCOL_STR = 'BitTorrent protocol'
  EXTENSION_BITS = {
    44: 'Extension protocol',
    62: 'Fast peers',
    64: 'DHT',
  }

  SPANS = [
    slice( 0,  1),  # First byte = 19
    slice( 1, 20),  # "BitTorrent protocol"
    slice(20, 28),  # Reserved bits
    slice(28, 48),  # Metainfo hash
    slice(48, 68)   # Peer id
  ]

  @staticmethod
  def make(info, peer_id=None):
    """
      Make a Peer-Peer handshake.  If peer_id is omitted, make only the handshake prefix.
    """
    if isinstance(info, MetaInfo):
      hash = hashlib.sha1(info.raw()).digest()
    elif isinstance(info, str) and len(str) == 20:
      hash = info
    else:
      raise ValueError('Expected MetaInfo or hash, got %s' % repr(info))
    handshake = [chr(len(PeerHandshake.PROTOCOL_STR)), PeerHandshake.PROTOCOL_STR,
                 chr(0) * 8, hash, peer_id if peer_id else '']
    return ''.join(handshake)

  @classmethod
  def error(cls, msg):
    raise cls.InvalidHandshake('Invalid handshake: %s' % msg)

  def __init__(self, handshake_bytes):
    (handshake_byte, protocol_str, self._reserved_bits, self._hash, self._peer_id) = map(
        handshake_bytes.__getitem__, PeerHandshake.SPANS)
    if handshake_byte != chr(len(PeerHandshake.PROTOCOL_STR)):
      self.error('Initial byte of handshake should be 0x%x' % len(PeerHandshake.PROTOCOL_STR))
    if protocol_str != PeerHandshake.PROTOCOL_STR:
      self.error('This is not the BitTorrent protocol!')
    self._reserved_bits = struct.unpack('>Q', self._reserved_bits)[0]

  @property
  def hash(self):
    return self._hash

  @property
  def peer_id(self):
    return self._peer_id

  @staticmethod
  def is_set(ull, bit):
    return bool(ull & (1 << (64 - bit)))

  @property
  def dht(self):
    return self.is_set(self._reserved_bits, self.EXTENSION_BITS['DHT'])

  @property
  def fast_peers(self):
    return self.is_set(self._reserved_bits, self.EXTENSION_BITS['Fast peers'])

  @property
  def extended(self):
    return self.is_set(self._reserved_bits, self.EXTENSION_BITS['Extension protocol'])


class PeerBroker(TCPServer):
  class BindError(Exception): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, io_loop=None, port=None):
    self._torrents = {}  # map from handshake prefix => torrent
    self._sessions = {}  # map from handshake prefix => session
    super(PeerBroker, self).__init__(io_loop=io_loop)
    port_range = [port] if port else PeerBroker.PORT_RANGE
    for port in port_range:
      try:
        log.debug('Peer listener attempting to bind @ %s' % port)
        self.listen(port)
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise PeerBroker.BindError('Could not bind to any port in range %s' % repr(port_range))
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
  def handle_stream(self, iostream, address):
    handshake_prefix = yield gen.Task(iostream.read_bytes, PeerHandshaker.PREFIX_LENGTH)
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

  @gen.engine
  def handshake(self, iostream, session, prefix='', callback=None):
    """
      Supply prefix if we've already snooped the start of the handshake.
    """
    log.debug('Session [%s] starting handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

    handshake_full = PeerHandshake.make(self._session.torrent.info, self._session.peer_id)
    read_handshake, _ = yield [gen.Task(iostream.read_bytes, PeerHandshake.LENGTH - len(prefix)),
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
      self.io_stream.add_callback(callback, succeeded)
