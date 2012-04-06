import errno
import hashlib
import os
import struct
import sys
import tempfile

import tornado.gen
from twitter.common.dirutil import safe_mkdir, safe_rmtree

from .iopool import IOPool
from .sliceset import SliceSet

__all__ = ('FileSet', 'FileIOPool')


class fileslice(object):
  """
    file-annotated slice with read/write methods.
    unfortunately slice is not subclassable, so we just duck-type as much as possible.
  """
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
        raise fileslice.ReadError('File is truncated at this slice!')
      return data

  def write(self, data):
    if len(data) != (self.stop - self.start):
      raise fileslice.WriteError('Block must be of appropriate size!')
    with open(self._filename, 'r+b') as fp:
      fp.seek(self.start)
      fp.write(data)

  def __repr__(self):
    return 'fileslice(%r[%r,%r])' % (self._filename, self.start, self.stop)


class Piece(object):
  def __init__(self, index, offset, length, block=None):
    self.index = index
    self.offset = offset
    self.length = length
    self.block = block

  @property
  def is_request(self):
    return self.block is None

  def __eq__(self, other):
    return (self.index == other.index and
            self.offset == other.offset and
            self.length == other.length and
            self.block == other.block)


class FileSet(object):
  def __init__(self, files, piece_size):
    """
      files: ordered list of (filename, filesize) pairs
      chroot (optional): location to store (or resume) this fileset.
                         if not supplied, create a new one.
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
      if not isinstance(fn, str):
        raise ValueError('Expect filenames to be strings.')
      if not (isinstance(fs, int) and fs >= 0):
        raise ValueError('Expected filesize to be a non-negative integer.')
      sha.update(fn)
      sha.update(struct.pack('>q', fs))
    self._hash = sha.hexdigest()
    self._files = files
    self._piece_size = piece_size
    self._size = sum((pr[1] for pr in self._files), 0)

  @property
  def size(self):
    return self._size

  @property
  def piece_size(self):
    return self._piece_size

  @property
  def num_pieces(self):
    return self.size / self._piece_size

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


class NullCallbackDispatcher(object):
  def __init__(self, callback=None):
    self._callback = callback

  def __enter__(self):
    pass

  def __exit__(self, type, value, traceback):
    if self._callback:
      self._callback()
    return False


class FileManager(object):
  """
    Translate Fileset read/write operations to IOLoop operations via a ThreadPool.
  """
  DEFAULT_SPLAT_BYTES = 64*1024

  @staticmethod
  def from_session(session):
    fs = FileSet([(mif.name, mif.length) for mif in session.info.files], session.piece_size)
    return FileManager(fs, list(session.torrent.info.pieces), chroot=session.chroot,
        io_loop=session.io_loop)

  def __init__(self, fileset, piece_hashes=None, chroot=None, io_loop=None):
    self._fileset = fileset
    self._pieces = piece_hashes or [chr(0) * 20] * self._fileset.num_pieces
    self._actual_pieces = []
    self._fileset = fileset
    self._sliceset = SliceSet()
    self._chroot = chroot or tempfile.mkdtemp()
    self._iopool = IOPool(io_loop=io_loop)
    self._initialize()

  #  --- piece initialization

  @staticmethod
  def safe_size(filename):
    try:
      return os.path.getsize(filename)
    except OSError as e:
      if e.errno == errno.ENOENT:
        return 0
      else:
        raise

  @staticmethod
  def fill(filename, size, splat=None):
    splat = splat or chr(0) * FileManager.DEFAULT_SPLAT_BYTES
    splat_size = len(splat)
    current_size = FileManager.safe_size(filename)
    assert current_size <= size
    if current_size != size:
      diff = size - current_size
      with open(filename, 'a') as fp:
        while diff > 0:
          if diff > splat_size:
            fp.write(splat)
            diff -= splat_size
          else:
            fp.write(chr(0) * diff)
            diff = 0
    return size - current_size

  @property
  def hashfile(self):
    return os.path.join(self._chroot, '.%s.pieces' % self._fileset.hash)

  def _initialize(self):
    touched = 0
    total_size = 0
    # zero out files
    for filename, filesize in self._fileset:
      fullpath = os.path.join(self._chroot, filename)
      touched += self.fill(fullpath, filesize)
      total_size += filesize
    # load up cached pieces file if it's there
    if touched == 0 and os.path.exists(self.hashfile):
      with open(self.hashfile) as fp:
        self._actual_pieces = fp.read()
    # or compute it on the fly
    if len(self._actual_pieces) != self._fileset.num_pieces:
      self._actual_pieces = list(self.iter_hashes())
      with open(self.hashfile, 'wb') as fp:
        fp.write(''.join(self._actual_pieces))
    # cover the spans that we've succeeded in the sliceset
    for index in range(self._fileset.num_pieces):
      if self._actual_pieces[index] == self._pieces[index]:
        self._sliceset.add(self._to_slice(index, 0, self._fileset.piece_size))

  # ---- helpers

  def to_slice(self, index, begin, length):
    start = index * self._fileset.piece_size + begin
    stop  = min(start + length, self._fileset.size)
    return slice(start, stop)

  def update_cache(self, index):
    with open(self._hashfile, 'r+b') as fp:
      fp.seek(index * 20)
      fp.write(self._actual_pieces[index])

  def have(self, index):
    return self._pieces[index] == self._expected_pieces[index]

  def iter_slices(self, index, begin, length):
    """
      Given (piece index, begin, length), return an iterator over fileslice objects
      that cover the interval.
    """
    piece = slice(index * self._piece_size + begin,
                  index * self._piece_size + begin + length)
    offset = 0
    for (fn, fs) in self._fileset.files:
      if offset + fs <= piece.start:
        offset += fs
        continue
      if offset >= piece.stop:
        break
      file = slice(offset, offset + fs)
      overlap = slice(max(file.start, piece.start), min(file.stop, piece.stop))
      if overlap.start < overlap.stop:
        yield fileslice(os.path.join(self._chroot, fn),
                        slice(overlap.start - offset, overlap.stop - offset))
      offset += fs

  def iter_hashes(self):
    """iterate over the sha1 hashes, blocking."""
    for chunk in self.iter_pieces():
      yield hashlib.sha1(chunk).digest()

  def iter_pieces(self):
    """iterate over the pieces backed by this fileset.  blocking."""
    # TODO(wickman)  Port this over to use the fileslice interface.
    chunk = ''
    for fn, _ in self._fileset:
      with open(os.path.join(self._chroot, fn), 'rb') as fp:
        while True:
          addendum = fp.read(self._fileset.piece_size - len(chunk))
          chunk += addendum
          if len(chunk) == self._fileset.piece_size:
            yield chunk
            chunk = ''
          if len(addendum) == 0:
            break
    if len(chunk) > 0:
      yield chunk

  # ---- io_loop interface

  @tornado.gen.engine
  def read(self, piece, callback):
    """
      Read a Piece (piece) asynchronously.
      Returns immediately, calls callback(data) when the read is complete.
    """
    slices = list(self._fileset.iter_slices(piece.index, piece.offset, piece.length))
    read_slices = yield [
        tornado.gen.Task(self._iopool.add, fileslice.read, slice_.rooted_at(self._chroot))
        for slice_ in slices]
    callback(''.join(read_slices))

  @tornado.gen.engine
  def write(self, piece, callback=None):
    """
      Write a Piece (piece) asynchronously.
      Returns immediately, calls callback with no parameters when the write
      (and hash cache flush) finishes.
    """
    slices = []
    offset = 0
    for slice_ in self._fileset.iter_slices(piece.index, piece.offset, piece.length):
      slices.append(
          tornado.gen.Task(self._iopool.add, fileslice.write, slice_.rooted_at(self._chroot),
                           piece.block[offset : offset+slice_.length]))
      offset += slice_.length
    with NullCallbackDispatcher(callback):
      yield slices
      yield tornado.gen.Task(self.touch, piece.index, piece.offset, piece.length)

  @tornado.gen.engine
  def touch(self, index, begin, length, callback=None):
    callback = callback or (lambda x: x)
    self._sliceset.add(self.to_slice(index, begin, length))
    piece_slice = self.to_slice(index, 0, self._fileset.piece_size)
    with NullCallbackDispatcher(callback):
      if piece_slice not in self._sliceset:
        # the piece isn't complete so don't bother with an expensive calculation
        return
      self._actual_pieces[index] = hashlib.sha1(
          (yield tornado.gen.Task(self.read, Piece(index, begin, length)))).digest()
      if self._pieces[index] == self._actual_pieces[index]:
        self.update_cache(index)
      else:
        # the hash was incorrect.  none of this data is good.
        self._sliceset.erase(piece_slice)

  def destroy(self):
    safe_rmtree(self._chroot)
    self._iopool.stop()
