import hashlib
import os
import tempfile

from .bitfield import Bitfield
from .fileset import (
    FileSet,
    fileslice,
    Piece,
    Request,
)
from .iopool import IOPool
from .sliceset import SliceSet

from tornado import gen, ioloop
from twitter.common.dirutil import (
    safe_mkdir,
    safe_open,
    safe_rmtree,
    safe_size,
)
from twitter.common import log


class PieceManager(object):
  """A Piece overlay on a FileSet.

     Translates Requests to operations over a FileSet and keeps track of
     live shas.
  """

  DEFAULT_SPLAT_BYTES = 64 * 1024

  @classmethod
  def from_torrent(cls, torrent, **kw):
    return cls(FileSet.from_torrent(torrent), list(torrent.info.pieces), **kw)

  @classmethod
  def fill(cls, filename, size, splat=None):
    "Fills file `filename` with `size` bytes from `splat` or 0s if splat is None."
    splat = splat or b'\x00' * cls.DEFAULT_SPLAT_BYTES
    splat_size = len(splat)
    current_size = safe_size(filename)
    assert current_size <= size
    if current_size != size:
      diff = size - current_size
      with safe_open(filename, 'a+b') as fp:
        while diff > 0:
          if diff > splat_size:
            fp.write(splat)
            diff -= splat_size
          else:
            fp.write(splat[:diff])
            diff = 0
    return size - current_size

  def __init__(self, fileset, piece_hashes=None, chroot=None):
    self._fileset = fileset
    self._pieces = piece_hashes or [b'\x00' * 20] * self._fileset.num_pieces
    self._actual_pieces = []
    self._fileset = fileset
    self._sliceset = SliceSet()
    self._chroot = chroot or tempfile.mkdtemp()
    safe_mkdir(self._chroot)
    self._initialize()

  def __contains__(self, block):
    slice_ = self.to_slice(block)
    return slice_ in self._sliceset

  #  --- piece initialization
  @property
  def slices(self):
    return self._sliceset

  @property
  def total_size(self):
    return sum((fp[1] for fp in self._fileset), 0)

  @property
  def assembled_size(self):
    # test this
    assembled = 0
    for index in range(len(self._pieces) - 1):
      if self.have(index):
        assembled += self._fileset.piece_size
    if self.have(len(self._pieces) - 1):
      _, leftover = divmod(self.total_size, self._fileset.piece_size)
      assembled += self._fileset.piece_size if not leftover else leftover
    return assembled

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
        fp.write(b''.join(self._actual_pieces))
    # cover the spans that we've succeeded in the sliceset
    for index in range(self._fileset.num_pieces):
      if self._actual_pieces[index] == self._pieces[index]:
        # XXX this is buggy for tail pieces -- but it's probably ok?
        self._sliceset.add(self.to_slice(index, 0, self._fileset.piece_size))

  # ---- helpers
  def to_slice(self, block):
    if not isinstance(block, Request):
      raise TypeError('to_slice expects Request, got %s' % block)
    start = block.index * self._fileset.piece_size + block.begin
    stop = min(start + block.length, self._fileset.size)
    return slice(start, stop)

  def update_cache(self, index):
    # TODO(wickman)  Cache this filehandle and do seek/write/flush.  It current takes
    # 1ms per call.
    with open(self.hashfile, 'r+b') as fp:
      fp.seek(index * 20)
      fp.write(self._actual_pieces[index])

  def have(self, index):
    return self._pieces[index] == self._actual_pieces[index]

  def covers(self, piece):
    "Does this PieceManager cover :class:`Request` piece?"
    piece_slice = self.to_slice(piece)
    piece_start_index = piece_slice.start / self._fileset.piece_size
    piece_stop_index = piece_slice.stop / self._fileset.piece_size
    return all(self.have(index) for index in range(piece_start_index, piece_stop_index))

  def iter_slices(self, piece):
    """
      Given (piece index, begin, length), return an iterator over fileslice objects
      that cover the interval.
    """
    piece = self.to_slice(piece)
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

  def piece_size(self, index):
    num_pieces, leftover = divmod(self._fileset.size, self._fileset.piece_size)
    num_pieces += leftover > 0
    assert index < num_pieces, 'Got index (%s) but num_pieces is %s' % (index, num_pieces)
    if not leftover:
      return self._fileset.piece_size
    if index == num_pieces - 1:
      return leftover
    return self._fileset.piece_size

  def iter_blocks(self, index, block_size):
    """yield :class:`Request` objects for the piece at index with a given block size"""
    piece_size = self.piece_size(index)
    for start_offset in range(0, piece_size, block_size):
      yield Request(
          index,
          start_offset,
          block_size if start_offset + block_size <= piece_size else piece_size % block_size)

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

  def iter_hashes(self):
    """iterate over the sha1 hashes, blocking."""
    for chunk in self.iter_pieces():
      yield hashlib.sha1(chunk).digest()

  def destroy(self):
    safe_rmtree(self._chroot)


# TODO(wickman) Probably this should have a bitfield on it?
class PieceBroker(PieceManager):
  """Translates FileSet read/write operations to IOLoop operations via a ThreadPool."""

  def __init__(self, fileset, piece_hashes=None, chroot=None, io_loop=None):
    super(PieceBroker, self).__init__(fileset, piece_hashes, chroot)
    self._bitfield = Bitfield(len(self._pieces))
    for index, (piece, actual_piece) in enumerate(zip(self._pieces, self._actual_pieces)):
      self._bitfield[index] = piece == actual_piece
    self._io_loop = io_loop or ioloop.IOLoop.instance()
    self._iopool = IOPool(io_loop=self._io_loop)

  @property
  def bitfield(self):
    return self._bitfield

  # ---- io_loop interface
  def whole_piece(self, index):
    num_pieces, leftover = divmod(self._fileset.size, self._fileset.piece_size)
    num_pieces += leftover > 0
    assert index < num_pieces
    if not leftover or index < num_pieces - 1:
      return Request(index, 0, self._fileset.piece_size)
    return Request(index, 0, leftover)

  @gen.engine
  def read(self, request, callback):
    """Read a :class:`Request` asynchronously.

       Returns immediately, calls callback(data) when the read is complete.
    """
    if not isinstance(request, Request):
      raise TypeError('PieceManager.read expects request of type request, got %s' % type(request))

    slices = list(self._fileset.iter_slices(request))
    read_slices = yield [
        gen.Task(self._iopool.add, fileslice.read, slice_.rooted_at(self._chroot))
        for slice_ in slices]
    callback(b''.join(read_slices))

  @gen.engine
  def write(self, piece, callback):
    """Write a Piece (piece) asynchronously.

      Returns immediately, calls callback with no parameters when the write
      (and hash cache flush) finishes.
    """
    if not isinstance(piece, Piece):
      raise TypeError('PieceManager.write expects piece of type Piece, got %s' % type(piece))

    if self.to_slice(piece.index, piece.offset, piece.length) in self._sliceset:
      log.debug('Dropping dupe write(%s)' % piece)
      callback()
      return

    slices = []
    offset = 0
    for slice_ in self._fileset.iter_slices(piece):
      slices.append(
          gen.Task(self._iopool.add, fileslice.write, slice_.rooted_at(self._chroot),
                   piece.block[offset:offset + slice_.length]))
      offset += slice_.length
    yield slices
    yield gen.Task(self.validate, piece.index, piece.offset, piece.length)
    callback()

  @gen.engine
  def validate(self, index, begin, length, callback):
    log.debug('FileIOPool.sliceset.add(%s)' % self.to_slice(index, begin, length))
    self._sliceset.add(self.to_slice(index, begin, length))
    piece_slice = self.to_slice(index, 0, self._fileset.piece_size)
    if piece_slice not in self._sliceset:
      # the piece isn't complete so don't bother with an expensive calculation
      callback()
      return
    log.debug('FileIOPool.touch: Performing SHA1 on %s' % index)
    # Consider pushing this computation onto an IOPool
    self._actual_pieces[index] = hashlib.sha1(
        (yield gen.Task(self.read, self.whole_piece(index)))).digest()
    if self._pieces[index] == self._actual_pieces[index]:
      log.debug('FileIOPool.touch: Finished piece %s!' % index)
      self._bitfield[index] = True
      self.update_cache(index)
    else:
      # the hash was incorrect.  none of this data is good.
      log.debug('FileIOPool.touch: Corrupt piece %s, erasing extent %s' % (index, piece_slice))
      self._sliceset.erase(piece_slice)
    callback()

  def stop(self):
    self._iopool.stop()

  def destroy(self):
    super(PieceManager, self).destroy()
    self.stop()
