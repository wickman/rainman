import hashlib

from .codec import BDecoder, BEncoder
from .metainfo import MetaInfo
from .handshake import PeerHandshake


class Torrent(dict):
  @classmethod
  def from_file(cls, filename):
    with open(filename, 'rb') as fp:
      torrent, _ = BDecoder.decode(fp.read())
    if 'info' not in torrent or 'announce' not in torrent:
      raise ValueError('Invalid .torrent file!')
    return cls.from_metainfo(torrent['info'], torrent['announce'])

  @classmethod
  def from_metainfo(cls, metainfo, announce):
    return cls(info=MetaInfo(metainfo), announce=announce)

  def __init__(self, *args, **kw):
    super(Torrent, self).__init__(*args, **kw)
    self._invalidate()

  def to_file(self, filename):
    assert 'announce' in self and 'info' in self
    with open(filename, 'wb') as fp:
      fp.write(BEncoder.encode(self))

  def _invalidate(self):
    self._hash = hashlib.sha1(BEncoder.encode(self.get('info', {}))).digest()
    self._prefix = PeerHandshake.make(self._hash)

  def raw(self):
    return BEncoder.encode(self)

  @property
  def hash(self):
    return self._hash

  @property
  def handshake_prefix(self):
    return self._prefix

  def handshake(self, peer_id=None):
    return PeerHandshake.make(self._hash, peer_id)

  @property
  def announce(self):
    return self.get('announce')

  @announce.setter
  def announce(self, value):
    assert isinstance(value, str)
    self['announce'] = value

  @property
  def info(self):
    return self['info']

  @info.setter
  def info(self, value):
    if not isinstance(value, dict):
      raise TypeError('Info must be a dictionary or MetaInfo.')
    self['info'] = MetaInfo(value)
    self._invalidate()
