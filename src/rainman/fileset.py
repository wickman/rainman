import hashlib
import math
import os
import struct

from twitter.common.lang import Compatibility


class fileslice(object):
  """A :class:`slice` over a file."""
  class Error(Exception): pass
  class ReadError(Error): pass
  class WriteError(Error): pass

  def __init__(self, filename, slice_):
    self._filename = filename
    self._slice = slice_
    assert self.length >= 0

  def rooted_at(self, path):
    return fileslice(os.path.join(path, self._filename), self._slice)

  @property
  def length(self):
    return self.stop - self.start

  @property
  def start(self):
    return self._slice.start

  @property
  def stop(self):
    return self._slice.stop

  # Force fileslice objects to be contiguous
  @property
  def step(self):
    return None

  def read(self):
    with open(self._filename, 'rb') as fp:
      fp.seek(self.start)
      data = fp.read(self.length)
      if len(data) != self.length:
        raise self.ReadError('File is truncated at this slice!')
      return data

  def write(self, data):
    if len(data) != (self.stop - self.start):
      raise self.WriteError('Block must be of appropriate size!')
    with open(self._filename, 'r+b') as fp:
      fp.seek(self.start)
      fp.write(data)

  def __repr__(self):
    return 'fileslice(%r[%r,%r])' % (self._filename, self.start, self.stop)


class Request(object):
  __slots__ = ('index', 'offset', 'length')

  def __init__(self, index, offset, length):
    self.index = index
    self.offset = offset
    self.length = length

  def __hash__(self):
    return hash((self.index, self.offset, self.length))

  def __str__(self):
    return 'Request(%s[%s:%s])' % (self.index, self.offset, self.offset + self.length)

  def __eq__(self, other):
    return (self.index == other.index and
            self.offset == other.offset and
            self.length == other.length)


class Piece(Request):
  __slots__ = ('block',)

  def __init__(self, index, offset, length, block):
    super(Piece, self).__init__(index, offset, length)
    self.block = block

  def __str__(self):
    return 'Piece(%s[%s:%s]*)' % (self.index, self.offset, self.offset + self.length)


class FileSet(object):
  """A logical concatenation of files, chunked into chunk sizes."""

  @classmethod
  def from_torrent(cls, torrent):
    return cls([(mif.name, mif.length) for mif in torrent.info.files], torrent.info.piece_size)

  def __init__(self, files, piece_size):
    """:param files: Ordered list of (filename, filesize) tuples.
       :param piece_size: Size of pieces in bytes.
    """
    # Assert sanity of input and compute filename of on-disk piece cache.
    if not (isinstance(piece_size, int) and piece_size > 0):
      raise ValueError('Expected piece size to be a positive integer.')
    sha = hashlib.sha1()
    for file_pair in files:
      try:
        fn, fs = file_pair
      except (ValueError, TypeError):
        raise ValueError('Expected files to be a list of file, size pairs.')
      if not isinstance(fn, Compatibility.string):
        raise ValueError('Expect filenames to be strings.')
      if not isinstance(fs, Compatibility.integer) or fs <= 0:
        raise ValueError('Expected filesize to be a non-negative integer.')
      sha.update(fn)
      sha.update(struct.pack('>q', fs))
    self._hash = sha.hexdigest()
    self._files = files
    self._piece_size = piece_size
    self._size = sum(pr[1] for pr in self._files)

  @property
  def size(self):
    return self._size

  @property
  def piece_size(self):
    return self._piece_size

  @property
  def num_pieces(self):
    return int(math.ceil(1. * self.size / self._piece_size))

  @property
  def hash(self):
    """a sha hash uniquely representing the filename,filesize pair list."""
    return self._hash

  def iter_slices(self, index, begin, length):
    """
      Given (piece index, begin, length), return an iterator over fileslice objects
      that cover the interval.
    """
    piece = slice(index * self._piece_size + begin,
                  index * self._piece_size + begin + length)
    offset = 0
    for (fn, fs) in self._files:
      if offset + fs <= piece.start:
        offset += fs
        continue
      if offset >= piece.stop:
        break
      file = slice(offset, offset + fs)
      overlap = slice(max(file.start, piece.start), min(file.stop, piece.stop))
      if overlap.start < overlap.stop:
        yield fileslice(fn, slice(overlap.start - offset, overlap.stop - offset))
      offset += fs

  def __iter__(self):
    return iter(self._files)
