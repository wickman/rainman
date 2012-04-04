import hashlib
import random
import struct

from twitter.common import log

from tornado import gen
from .metainfo import MetaInfo


class PeerId(object):
  PREFIX = '-TW7712-'  # TWTTR
  LENGTH = 20
  @classmethod
  def generate(cls):
    return cls.PREFIX + ''.join(random.sample('0123456789abcdef', cls.LENGTH - len(cls.PREFIX)))


class PeerHandshake(object):
  class InvalidHandshake(Exception): pass

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
    return self.is_set(self.EXTENSION_BITS['DHT'])

  @property
  def fast_peers(self):
    return self.is_set(self.EXTENSION_BITS['Fast peers'])

  @property
  def extended(self):
    return self.is_set(self.EXTENSION_BITS['Extension protocol'])


class Peer(object):
  def __init__(self, address, session, handshake_cb=None):
    self._id = None
    self._address = address
    self._session = session
    self._hash = hashlib.sha1(session.torrent.info.raw()).digest()
    self._handshake_cb = handshake_cb or (lambda *x: x)
    self._iostream = None

  @property
  def id(self):
    return self._id

  @property
  def address(self):
    return self._address

  @gen.engine
  def handshake(self, iostream):
    log.debug('Session [%s] starting handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))
    handshake_full = PeerHandshake.make(self._session.torrent.info, self._session.peer_id)
    read_handshake, _ = yield [gen.Task(iostream.read_bytes, PeerHandshake.LENGTH),
                                gen.Task(iostream.write, handshake_full)]

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
      log.debug('Session [%s] got bad handshake with %s:%s: ' % (self._session.peer_id,
        self._address[0], self._address[1], e))
      iostream.close()

    self._handshake_cb(self, succeeded)
    log.debug('Session [%s] finishing handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

  def disconnect(self):
    if self._iostream:
      self._iostream.close()
