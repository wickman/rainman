import errno
import hashlib
import math
import os

from twitter.common.collections import OrderedSet
from twitter.common.quantity import Amount, Data

from .codec import BEncoder, BDecoder

class Torrent(object):
  @staticmethod
  def from_file(filename):
    with open(filename, 'rb') as fp:
      mi, _ = BDecoder.decode(fp.read())
    return Torrent(mi)

  def __init__(self, d=None):
    self._d = d or {}
    self._info = MetaInfo(self._d.get('info')) if 'info' in self._d else None

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


class MetaInfoFile(object):
  def __init__(self, name, start, end):
    self._name = name
    self._start = start
    self._end = end

  @property
  def name(self):
    return self._name

  @property
  def start(self):
    return self._start

  @property
  def end(self):
    return self._end

  @property
  def length(self):
    return self.end - self.start

  def __repr__(self):
    return 'MetaInfoFile(%r, %r, %r)' % (self._name, self._start, self._end)


# TODO(wickman)  Call the appropriate parts of Fileset here instead of
# cargo-culting iter_chunks.
class MetaInfoBuilder(object):
  """
    Helper class for constructing MetaInfo objects.

    builder = MetaInfoBuilder('my_package')
    for fn in os.listdir('.'):
      builder.add(fn)
    metainfo = builder.build()
  """

  class FileNotFound(Exception): pass
  class EmptyInfo(Exception): pass

  MIN_CHUNK_SIZE = Amount(64, Data.KB)
  MAX_CHUNK_SIZE = Amount(1, Data.MB)
  DEFAULT_CHUNKS = 256

  def __init__(self, name=None):
    self._name = name
    self._files = OrderedSet()
    self._stats = {}

  def add(self, filename):
    try:
      stat = os.stat(filename)
    except OSError as e:
      if e.errno == errno.ENOENT:
        raise MetaInfoBuilder.FileNotFound("Could not find %s" % filename)
      raise
    self._files.add(filename)
    self._stats[filename] = stat.st_size

  def remove(self, filename):
    self._files.discard(filename)
    self._stats.pop(filename)

  @staticmethod
  def choose_size(total_size):
    chunksize = 2**int(round(math.log(1. * total_size / MetaInfoBuilder.DEFAULT_CHUNKS, 2)))
    if chunksize < MetaInfoBuilder.MIN_CHUNK_SIZE.as_(Data.BYTES):
      return int(MetaInfoBuilder.MIN_CHUNK_SIZE.as_(Data.BYTES))
    elif chunksize > MetaInfoBuilder.MAX_CHUNK_SIZE.as_(Data.BYTES):
      return int(MetaInfoBuilder.MAX_CHUNK_SIZE.as_(Data.BYTES))
    return chunksize

  def build(self):
    if len(self._files) == 0:
      raise MetaInfoBuilder.EmptyInfo("No files in metainfo!")
    total_size = sum(self._stats.values())
    piece_size = MetaInfoBuilder.choose_size(total_size)
    d = {
      'pieces': ''.join(hashlib.sha1(chunk).digest() for chunk in self.iter_chunks(piece_size)),
      'piece length': piece_size
    }
    if self._name:
      d.update(name = self._name)
    if len(self._files) == 1:
      d.update(length = sum(self._stats[fn] for fn in self._files),
               name = os.path.basename(iter(self._files).next()))
    else:
      d.update(files = [
        {'length': self._stats[fn],
         'path': [sp.encode(MetaInfo.ENCODING) for sp in fn.split(os.path.sep)]}
        for fn in self._files])
    return MetaInfo(d)

  def iter_chunks(self, chunksize):
    chunk = ''
    for fn in self._files:
      with open(fn, 'rb') as fp:
        while True:
          addendum = fp.read(chunksize - len(chunk))
          chunk += addendum
          if len(chunk) == chunksize:
            yield chunk
            chunk = ''
          if len(addendum) == 0:
            break
    if len(chunk) > 0:
      yield chunk


class MetaInfo(object):
  ENCODING = 'utf8'

  def __init__(self, info):
    self._info = info
    self._assert_sanity()

  def _assert_sanity(self):
    expected_keys = ('pieces', 'piece length',)
    for key in expected_keys:
      assert key in self._info, 'Missing key: %s' % key
    assert ('length' in self._info) + ('files' in self._info) == 1
    if 'length' in self._info:
      pieces, leftover = divmod(self._info['length'], self._info['piece length'])
      pieces += leftover > 0
      assert len(self._info['pieces']) == pieces * 20

  @property
  def name(self):
    return self._info.get('name')

  @property
  def length(self):
    if 'length' in self._info:
      return self._info['length']
    else:
      return sum(file['length'] for file in self._info.get('files', []))

  @property
  def piece_size(self):
    return self._info.get('piece length')

  @property
  def pieces(self):
    pieces = self._info.get('pieces')
    for k in range(0, len(pieces), 20):
      yield pieces[k : k+20]

  @property
  def num_pieces(self):
    return len(self._info.get('pieces')) / 20

  @property
  def files(self):
    if 'length' in self._info:
      yield MetaInfoFile(self.name, 0, self._info['length'])
    else:
      offset = 0
      for fd in self._info.get('files'):
        path = self._info.get('name', []) + fd['path']
        yield MetaInfoFile(os.path.join(*path), offset, offset + fd['length'])
        offset += fd['length']

  def raw(self):
    return BEncoder.encode(self._info)

  def as_dict(self):
    return self._info
