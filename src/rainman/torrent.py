import hashlib

from .codec import BDecoder, BEncoder
from .metainfo import MetaInfo
from .handshake import PeerHandshake


class Torrent(object):
  @classmethod
  def from_file(cls, filename):
    with open(filename, 'rb') as fp:
      mi, _ = BDecoder.decode(fp.read())
    return cls(mi)

  def __init__(self, d=None):
    self._d = d or {}
    self._info = MetaInfo(self._d.get('info')) if 'info' in self._d else None
    self._invalidate()

  def _invalidate(self):
    self._hash = hashlib.sha1(self.info.raw()).digest() if self.info else None
    self._prefix = PeerHandshake.make(self._hash) if self._hash else None

  @property
  def hash(self):
    return self._hash

  @property
  def handshake_prefix(self):
    return self._prefix

  def handshake(self, peer_id=None):
    return PeerHandshake.make(self._hash, peer_id)

  def to_file(self, filename):
    assert 'announce' in self._d and 'info' in self._d
    with open(filename, 'wb') as fp:
      fp.write(BEncoder.encode(self._d))

  @property
  def announce(self):
    return self._d.get('announce')

  @announce.setter
  def announce(self, value):
    assert isinstance(value, str)
    self._d['announce'] = value

  @property
  def info(self):
    return self._info

  @info.setter
  def info(self, value):
    if isinstance(value, dict):
      self._d['info'] = value
      self._info = MetaInfo(value)
    elif isinstance(value, MetaInfo):
      self._info = value
      self._d['info'] = value.as_dict()
    else:
      raise ValueError(value)
    self._invalidate()
