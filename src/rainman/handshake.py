import hashlib
import struct

from .metainfo import MetaInfo


class PeerHandshake(object):
  class InvalidHandshake(Exception): pass

  PREFIX_LENGTH = 48
  LENGTH = 68
  PROTOCOL_STR = b'BitTorrent protocol'
  EXTENSION_BITS = {
    44: 'Extension protocol',
    62: 'Fast peers',
    64: 'DHT',
  }

  SPANS = (
    slice(0, 1),  # First byte = 19
    slice(1, 20),  # "BitTorrent protocol"
    slice(20, 28),  # Reserved bits
    slice(28, 48),  # Metainfo hash
    slice(48, 68),   # Peer id
  )

  @classmethod
  def make(cls, info, peer_id=None):
    """
      Make a Peer-Peer handshake.  If peer_id is omitted, make only the handshake prefix.
    """
    if isinstance(info, MetaInfo):
      hash = hashlib.sha1(info.raw()).digest()
    elif isinstance(info, bytes) and len(info) == 20:
      hash = info
    else:
      raise ValueError('Expected MetaInfo or hash, got %s' % repr(info))
    handshake = [
        struct.pack('B', len(cls.PROTOCOL_STR)),
        cls.PROTOCOL_STR,
        b'\x00' * 8,
        hash,
        peer_id if peer_id else b'',
    ]
    return b''.join(handshake)

  @classmethod
  def error(cls, msg):
    raise cls.InvalidHandshake('Invalid handshake: %s' % msg)

  def __init__(self, handshake_bytes):
    (handshake_byte, protocol_str, self._reserved_bits, self._hash, self._peer_id) = map(
        handshake_bytes.__getitem__, self.SPANS)
    if handshake_byte != chr(len(self.PROTOCOL_STR)):
      self.error('Initial byte of handshake should be 0x%x' % len(self.PROTOCOL_STR))
    if protocol_str != self.PROTOCOL_STR:
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
