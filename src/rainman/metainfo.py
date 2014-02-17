import binascii
from collections import namedtuple
import hashlib
import math
import os

from .codec import BEncoder
from .fileset import FileSet
from .fs import DISK

from twitter.common import log
from twitter.common.lang import Compatibility
from twitter.common.quantity import Amount, Data


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

  @classmethod
  def from_dir(cls, basedir, piece_size=None):
    def iter_files():
      for root, _, files in os.walk(basedir):
        for filename in files:
          yield os.path.join(root, filename)
    return cls(iter_files(), relpath=basedir, name=os.path.basename(basedir), piece_size=piece_size)

  @classmethod
  def from_file(cls, filename, piece_size=None):
    return cls(
        [filename],
        relpath=os.path.dirname(filename),
        name=os.path.basename(basedir),
        piece_size=piece_size)

  def __init__(self, filelist, piece_size=None, relpath=None, name=None):
    value_error = ValueError(
        'filelist should be a list of files or list of (filename, filesize) pairs.')
    def expand_filelist():
      for element in filelist:
        if isinstance(element, Compatibility.string):
          yield (element, os.path.getsize(element))
        elif isinstance(element, (list, tuple)):
          if len(element) != 2:
            raise value_error
          if not isinstance(element[0], Compatibility.string):
            raise value_error
          if not isinstance(element[1], Compatibility.integer):
            raise value_error
          yield element
        else:
          raise value_error
    self._filelist = list(expand_filelist())
    self._piece_size = self.choose_size(sum(filesize for _, filesize in self._filelist))
    self._fileset = FileSet(self._filelist, self._piece_size)
    self._relpath = relpath
    self._name = name

  def _iter_hashes(self, fs=DISK):
    for index, chunk in enumerate(self._fileset.iter_chunks(fs=fs)):
      digest = hashlib.sha1(chunk).digest()
      log.debug('MetaInfoBuilder %d = %s' % (index, binascii.hexlify(digest)))
      yield digest

  def build(self, fs=DISK):
    if len(self._fileset) == 0:
      raise self.EmptyInfo("No files in metainfo fileset!")

    d = {
      'pieces': b''.join(self._iter_hashes(fs=fs)),
      'piece length': self._piece_size,
    }

    def relpath(filename):
      if self._relpath is None:
        log.info('Metainfo: %s' % filename)
        return filename
      log.info('Metainfo: %s' % os.path.relpath(filename, self._relpath))
      return os.path.relpath(filename, self._relpath)

    if len(self._fileset) == 1:
      # special-case for single-file
      fileset_name, _ = iter(self._fileset).next()
      d['name'] = self._name or os.path.basename(fileset_name).encode(self.ENCODING)
      d['length'] = self._fileset.size
    else:
      d['name'] = self._name or 'none'
      def encode_filename(filename):
        return [sp.encode(self.ENCODING) for sp in relpath(filename).split(os.path.sep)]
      d['files'] = [{'length': filesize, 'path': encode_filename(filename)}
                    for filename, filesize in self._fileset]

    return MetaInfo(d)


class MetaInfoFile(namedtuple('MetaInfoFile', 'name start end')):
  @property
  def length(self):
    return self.end - self.start


class MetaInfo(dict):
  ENCODING = 'utf8'

  def __init__(self, *args, **kw):
    super(MetaInfo, self).__init__(*args, **kw)
    self._assert_sanity()

  def _assert_sanity(self):
    expected_keys = ('pieces', 'piece length',)
    for key in expected_keys:
      assert key in self, 'Missing key: %s' % key
    assert ('length' in self) + ('files' in self) == 1
    if 'length' in self:
      pieces, leftover = divmod(self['length'], self['piece length'])
      pieces += leftover > 0
      assert len(self['pieces']) == pieces * 20

  @property
  def name(self):
    return self['name']

  @property
  def length(self):
    if 'length' in self:
      return self['length']
    else:
      return sum(pair['length'] for pair in self.get('files', []))

  @property
  def piece_size(self):
    return self['piece length']

  @property
  def piece_hashes(self):
    pieces = self['pieces']
    for k in range(0, len(pieces), 20):
      yield pieces[k:k + 20]

  @property
  def num_pieces(self):
    return len(self['pieces']) // 20

  def files(self, rooted_at=None):
    if 'length' in self:
      rooted_at = rooted_at if rooted_at is not None else ''
      yield MetaInfoFile(os.path.join(rooted_at, self.name), 0, self['length'])
    else:
      offset = 0
      name = rooted_at if rooted_at is not None else ''  # ignore 'name'
      for fd in self['files']:
        path = [name] + fd['path']
        yield MetaInfoFile(os.path.join(*path), offset, offset + fd['length'])
        offset += fd['length']

  def raw(self):
    return BEncoder.encode(self)
