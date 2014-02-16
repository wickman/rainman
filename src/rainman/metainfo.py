import binascii
import hashlib
import math
import os

from .codec import BEncoder

from twitter.common import log
from twitter.common.quantity import Amount, Data


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


class MetaInfoBuilder(object):
  """Helper class for constructing MetaInfo objects."""
  class Error(Exception): pass
  class FileNotFound(Error): pass
  class EmptyInfo(Error): pass

  ENCODING = 'utf8'
  MIN_CHUNK_SIZE = Amount(64, Data.KB)
  MAX_CHUNK_SIZE = Amount(1, Data.MB)
  DEFAULT_CHUNKS = 256

  @classmethod
  def choose_size(cls, total_size):
    chunksize = 2 ** int(round(math.log(1. * total_size / cls.DEFAULT_CHUNKS, 2)))
    if chunksize < cls.MIN_CHUNK_SIZE.as_(Data.BYTES):
      return int(cls.MIN_CHUNK_SIZE.as_(Data.BYTES))
    elif chunksize > cls.MAX_CHUNK_SIZE.as_(Data.BYTES):
      return int(cls.MAX_CHUNK_SIZE.as_(Data.BYTES))
    return chunksize

  def __init__(self, fileset, relpath=None, name=None):
    self._name = name
    self._fileset = fileset
    self._relpath = relpath

  def _iter_hashes(self):
    for index, chunk in enumerate(self._fileset.iter_chunks()):
      digest = hashlib.sha1(chunk).digest()
      log.debug('MetaInfoBuilder %d = %s' % (index, binascii.hexlify(digest)))
      yield digest

  def build(self, piece_size=None):
    if len(self._fileset) == 0:
      raise self.EmptyInfo("No files in metainfo!")
    piece_size = piece_size or self.choose_size(self._fileset.size)
    d = {
      'pieces': b''.join(self._iter_hashes()),
      'piece length': piece_size
    }
    if self._name:
      d['name'] = self._name

    def relpath(filename):
      if self._relpath is None:
        return filename
      return os.path.relpath(filename, self._relpath)

    if len(self._fileset) == 1:
      # special-case for single-file
      fileset_name, _ = iter(self._fileset).next()
      d.update(
          length=self._fileset.size,
          name=relpath(os.path.basename(fileset_name)).encode(self.ENCODING))
    else:
      def encode_filename(filename):
        return [sp.encode(self.ENCODING) for sp in relpath(filename).split(os.path.sep)]
      d['files'] = [{'length': filesize, 'path': encode_filename(filename)}
                    for filename, filesize in self._fileset]
    return MetaInfo(d)


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
      yield pieces[k:k + 20]

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
