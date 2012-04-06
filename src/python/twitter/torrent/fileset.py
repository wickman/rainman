import errno
import hashlib
import os
import struct
import sys
import tempfile

import tornado.gen
from twitter.common.dirutil import safe_mkdir, safe_rmtree
from .iopool import IOPool

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


class FileSet(object):
  def __init__(self, files, piece_size, chroot=None):
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
    self._chroot = chroot or tempfile.mkdtemp()
    safe_mkdir(self._chroot)
    self._files = files
    self._pieces = ''
    self._piece_size = piece_size
    self._initialized = False
    self._splat = chr(0) * piece_size
    self.initialize()

  @staticmethod
  def safe_size(filename):
    try:
      return os.path.getsize(filename)
    except OSError as e:
      if e.errno == errno.ENOENT:
        return 0
      else:
        raise

  def fill(self, filename, size):
    current_size = self.safe_size(filename)
    assert current_size <= size
    if current_size != size:
      diff = size - current_size
      with open(filename, 'a') as fp:
        while diff > 0:
          if diff > self._piece_size:
            fp.write(self._splat)
            diff -= self._piece_size
          else:
            fp.write(chr(0) * diff)
            diff = 0
    return size - current_size

  def initialize(self):
    touched = 0
    total_size = 0
    for filename, filesize in self._files:
      fullpath = os.path.join(self._chroot, filename)
      touched += self.fill(fullpath, filesize)
      total_size += filesize
    pieces_file = os.path.join(self._chroot, '.%s.pieces' % self._hash)
    if touched == 0 and os.path.exists(pieces_file) and (
        os.path.getsize(pieces_file) == filesize / self._piece_size * 20):
      with open(pieces_file) as fp:
        self._pieces = fp.read()
    else:
      self._pieces = ''.join(self.iter_hashes())
      with open(pieces_file, 'wb') as fp:
        fp.write(self._pieces)
    self._initialized = True

  def initialized(self):
    return self._initialized

  def read(self, index, begin, length):
    return ''.join(fileslice.read() for fileslice in self.iter_slices(index, begin, length))

  def write(self, index, begin, block):
    offset = 0
    for fileslice in self.iter_slices(index, begin, len(block)):
      fileslice.write(block[offset:offset+fileslice.length])
      offset += fileslice.length

  def hash(self, index):
    """
      Returns the computed (not cached) sha1 of the piece at index.
    """
    piece = ''.join(fileslice.read() for fileslice in self.iter_slices(index, 0, self._piece_size))
    return hashlib.sha1(piece).digest()

  # TODO(wickman) Index files by aggregate slice, then do a binary search here, if
  # this seems to be slow for huge torrents.
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
        yield fileslice(os.path.join(self._chroot, fn),
                        slice(overlap.start - offset, overlap.stop - offset))
      offset += fs

  def iter_hashes(self):
    for chunk in self.iter_pieces():
      yield hashlib.sha1(chunk).digest()

  def iter_pieces(self):
    chunk = ''
    for fn, _ in self._files:
      with open(os.path.join(self._chroot, fn), 'rb') as fp:
        while True:
          addendum = fp.read(self._piece_size - len(chunk))
          chunk += addendum
          if len(chunk) == self._piece_size:
            yield chunk
            chunk = ''
          if len(addendum) == 0:
            break
    if len(chunk) > 0:
      yield chunk

  def destroy(self):
    safe_rmtree(self._chroot)


class FileIOPool(IOPool):
  """
    Translate Fileset read/write operations to IOLoop operations via a ThreadPool.

    Example:

    import time
    import tornado.ioloop
    from twitter.torrent.fileset import FileSet
    from twitter.torrent.iopool import FileIOPool
    from twitter.torrent.peer import Piece

    fs = FileSet([('a.txt', 1000000),
                  ('b.txt', 2000000),
                  ('c.txt', 500000)],
                 piece_size=512*1024)
    pc = Piece(1, 30000, 512*1024*2)
    fiop = FileIOPool(fs)

    global_ioloop = tornado.ioloop.IOLoop.instance()

    def done(*args, **kw):
      print 'done received %s bytes.' % len(args[0])
      print '%.6f' % time.time()
      global_ioloop.stop()

    print '%.6f' % time.time()
    fiop.read(pc, done)
    global_ioloop.start()
    fs.destroy()
  """

  def __init__(self, fileset, io_loop=None):
    self._fileset = fileset
    super(FileIOPool, self).__init__(io_loop=io_loop)

  @tornado.gen.engine
  def read(self, piece, callback):
    slices = list(self._fileset.iter_slices(piece.index, piece.offset, piece.length))
    read_slices = yield [tornado.gen.Task(self.add, fileslice.read, slice_) for slice_ in slices]
    callback(''.join(read_slices))

  @tornado.gen.engine
  def write(self, piece, callback=None):
    slices = []
    offset = 0
    for slice_ in self._fileset.iter_slices(piece.index, piece.offset, piece.length):
      slices.append(tornado.gen.Task(self.add, fileslice.write,
          piece.data[offset : offset+slice_.length]))
      offset += slice_.length
    yield slices
