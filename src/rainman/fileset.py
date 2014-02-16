import hashlib
import math
import os
import struct

from .request import Request

from twitter.common.lang import Compatibility
from twitter.common import log
from twitter.common.dirutil import touch


class SliceBase(object):
  """A :class:`slice` over a file."""
  class Error(Exception): pass
  class ReadError(Error): pass
  class WriteError(Error): pass

  def __init__(self, filename, slice_):
    self._filename = filename
    self._slice = slice_
    assert self.length >= 0

  def rooted_at(self, chroot):
    return self.__class__(os.path.join(chroot, self.filename), self._slice)

  @property
  def filename(self):
    return self._filename

  @property
  def start(self):
    return self._slice.start

  @property
  def stop(self):
    return self._slice.stop

  @property
  def length(self):
    return self.stop - self.start

  # Force fileslice objects to be contiguous
  @property
  def step(self):
    return None

  def read(self):
    """Read the data composing this slice."""
    raise NotImplemented

  def write(self, data):
    """Read write the data in `data` to this slice.  `data` must be the correct size."""
    raise NotImplemented

  def fill(self):
    """Fill the slice with sentinel data if the slice is empty. Returns truthy if anything
       was filled, falsy if nothing was touched."""
    raise NotImplemented

  def __repr__(self):
    return '%s(%r[%r,%r])' % (self.__class__.__name__, self._filename, self.start, self.stop)


class fileslice(SliceBase):
  """A :class:`slice` over a real file."""
  def read(self):
    """
    with open(self._filename, 'rb') as fp:
      fp.seek(self.start)
      log.debug('%s reading.' % self)
      data = fp.read(self.length)
      if len(data) != self.length:
        raise self.ReadError('File is truncated at this slice!')
      return data
    """
    with open(self._filename, 'rb') as fp:
      fp.seek(self.start)
      log.debug('%s reading.' % self)
      data = fp.read(self.length)
      if len(data) != self.length:
        raise self.ReadError('File is truncated at this slice!')
      return data

  def write(self, data):
    if len(data) != (self.stop - self.start):
      raise self.WriteError('Block must be of appropriate size!')
    with open(self._filename, 'r+b') as fp:
      log.debug('%s writing.' % self)
      fp.seek(self.start)
      fp.write(data)
    assert self.read() == data

  def fill(self):
    if self.length == 0:
      return False
    touch(self._filename)
    with open(self._filename, 'r+b') as fp:
      if os.path.getsize(self._filename) < self.stop:
        log.debug('%s filling.' % self)
        fp.seek(self.stop - 1, 0)
        # write a sentinel byte which will fill the file at least up to stop.
        fp.write(b'\x00')
        return True
    return False


class memslice(SliceBase):
  _FAKE_FS = {}

  def read(self):
    if self._filename not in self._FAKE_FS:
      raise self.ReadError('File does not exist!')
    data = self._FAKE_FS[self._filename]
    if len(data) < self.stop:
      raise self.ReadError('File not long enough!')
    return bytes(data[self.start:self.stop])

  def write(self, blob):
    if len(blob) != (self.stop - self.start):
      raise self.WriteError('Block must be of appropriate size!')
    if self._filename not in self._FAKE_FS:
      raise self.WriteError('File does not exist!')
    data = self._FAKE_FS[self._filename]
    if len(data) < self.start:
      data.extend(bytearray(self.start - len(data)))
    data[self.start:self.stop] = blob

  def fill(self):
    if self._filename not in self._FAKE_FS:
      self._FAKE_FS[self._filename] = bytearray(self.stop)
      return True
    else:
      data = self._FAKE_FS[self._filename]
      if self.stop < len(data):
        return False
      data.extend(bytearray(self.stop - len(data)))
      return True


class FileSet(object):
  """A logical concatenation of files, chunked into chunk sizes."""

  @classmethod
  def from_metainfo(cls, metainfo, **kw):
    return cls(
        [(mif.name, mif.length) for mif in metainfo.files],
        metainfo.piece_size,
        **kw)

  def __init__(self, files, piece_size, slice_impl=fileslice):
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
    self._slice_impl = slice_impl

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
      yield self._slice_impl(filename, slice(0, filesize))

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
        yield self._slice_impl(fn, slice(overlap.start - offset, overlap.stop - offset))
      offset += fs

  def iter_chunks(self):
    total_size = self.size
    for k in range(self.num_pieces):
      request = Request(k, 0, min(self.piece_size, total_size))
      yield b''.join(slice_.read() for slice_ in self.iter_slices(request))
      total_size -= request.length

  def __iter__(self):
    return iter(self._files)

  def __len__(self):
    return len(self._files)
