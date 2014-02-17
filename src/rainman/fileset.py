import hashlib
import math
import os
import struct

from .fs import DISK, Fileslice
from .request import Request

from twitter.common.lang import Compatibility


class FileSet(object):
  """A logical concatenation of files, chunked into chunk sizes."""

  @classmethod
  def from_metainfo(cls, metainfo, **kw):
    return cls(
        [(mif.name, mif.length) for mif in metainfo.files()],
        metainfo.piece_size,
        **kw)

  def __init__(self, files, piece_size):
    """:param files: Ordered list of (filename, filesize) tuples.
       :param piece_size: Size of pieces in bytes.
    """
    # Assert sanity of input and compute filename of on-disk piece cache.
    if not (isinstance(piece_size, int) and piece_size > 0):
      raise ValueError('Expected piece size to be a positive integer.')
    sha = hashlib.sha1()
    self._files = list(files)
    for file_pair in self._files:
      try:
        fn, fs = file_pair
      except (ValueError, TypeError):
        raise ValueError('Expected files to be a list of file, size pairs.')
      if not isinstance(fn, Compatibility.string):
        raise ValueError('Expect filenames to be strings.')
      if not isinstance(fs, Compatibility.integer) or fs < 0:
        raise ValueError('Expected filesize to be a non-negative integer, got %r' % (fs,))
      sha.update(fn)
      sha.update(struct.pack('>q', fs))
    self._hash = sha.hexdigest()
    self._piece_size = piece_size
    self._size = sum(pr[1] for pr in self._files)

  def rooted_at(self, root):
    return self.__class__(
        [(os.path.join(root, fn), fs) for (fn, fs) in self._files],
        self._piece_size)

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

  def iter_files(self):
    for filename, filesize in self._files:
      yield Fileslice(filename, slice(0, filesize))

  def iter_slices(self, request):
    """
      Given a Request, return an iterator over fileslice objects that cover
      the Request interval.
    """
    piece = slice(request.index * self._piece_size + request.offset,
                  request.index * self._piece_size + request.offset + request.length)
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
        yield Fileslice(fn, slice(overlap.start - offset, overlap.stop - offset))
      offset += fs

  def iter_chunks(self, fs=DISK):
    total_size = self.size
    for k in range(self.num_pieces):
      request = Request(k, 0, min(self.piece_size, total_size))
      yield b''.join(fs.read(slice_) for slice_ in self.iter_slices(request))
      total_size -= request.length

  def __iter__(self):
    return iter(self._files)

  def __len__(self):
    return len(self._files)
