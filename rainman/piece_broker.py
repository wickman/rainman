import hashlib

from .bitfield import Bitfield
from .request import Piece, Request
from .iopool import IOPool
from .piece_manager import PieceManager

from tornado import gen, ioloop
from twitter.common import log


class PieceBroker(PieceManager):
  """Translates FileSet read/write operations to IOLoop operations via a ThreadPool."""

  def __init__(self, fileset, piece_hashes=None, chroot=None, io_loop=None, **kw):
    super(PieceBroker, self).__init__(fileset, piece_hashes, chroot, **kw)
    self._bitfield = Bitfield(len(self._pieces))
    self._io_loop = io_loop or ioloop.IOLoop.instance()
    self._iopool = IOPool(io_loop=self._io_loop)

  # TODO(wickman) This code should validate that blocks are not larger than pieces throughout.
  @property
  def bitfield(self):
    return self._bitfield

  def initialize(self):
    super(PieceBroker, self).initialize()
    for index, (piece, actual_piece) in enumerate(zip(self._pieces, self._actual_pieces)):
      self._bitfield[index] = piece == actual_piece

  # ---- io_loop interface
  @gen.coroutine
  def read(self, request):
    """Read a :class:`Request` asynchronously.

       Returns immediately, calls callback(data) when the read is complete.
    """
    if not isinstance(request, Request):
      raise TypeError('PieceManager.read expects request of type request, got %s' % type(request))

    slices = list(self._fileset.iter_slices(request))
    read_slices = yield [
        gen.Task(self._iopool.add, self._fs.read, slice_.rooted_at(self._chroot))
        for slice_ in slices]
    raise gen.Return(b''.join(read_slices))

  @gen.coroutine
  def write(self, piece):
    """Write a Piece (piece) asynchronously.

      Returns immediately, calls callback with no parameters when the write
      (and hash cache flush) finishes.
    """
    if not isinstance(piece, Piece):
      raise TypeError('PieceManager.write expects piece of type Piece, got %s' % type(piece))

    if self.to_slice(piece) in self._sliceset:
      log.debug('Dropping dupe write(%s)' % piece)
      raise gen.Return(None)

    slices = []
    offset = 0
    for slice_ in self._fileset.iter_slices(piece):
      iopool_task = gen.Task(
          self._iopool.add,
          self._fs.write,
          slice_.rooted_at(self._chroot),
          piece.block[offset:offset + slice_.length])
      slices.append(iopool_task)
      offset += slice_.length
    yield slices
    raise gen.Return((yield self.validate(piece)))

  # XXX(wickman) This logic is actually incorrect when the block is bigger than a piece.
  # Consider correcting that, though it isn't a big priority.
  @gen.coroutine
  def validate(self, piece):
    whole_piece = self.whole_piece(piece.index)
    piece_slice = self.to_slice(piece)
    full_slice = self.to_slice(whole_piece)

    log.debug('FileIOPool.sliceset.add(%s)' % piece_slice)

    self._sliceset.add(piece_slice)
    if full_slice not in self._sliceset:
      # the piece isn't complete so don't bother with an expensive calculation
      raise gen.Return(False)

    log.debug('FileIOPool.touch: Performing SHA1 on piece %s' % piece.index)
    # Consider pushing this computation onto an IOPool
    self._actual_pieces[piece.index] = hashlib.sha1((yield self.read(whole_piece))).digest()
    if self._pieces[piece.index] == self._actual_pieces[piece.index]:
      log.debug('FileIOPool.touch: Finished piece %s!' % piece.index)
      self._bitfield[piece.index] = True
      self.update_cache(piece.index)
      raise gen.Return(True)
    else:
      # the hash was incorrect.  none of this data is good.
      log.debug('FileIOPool.touch: Corrupt piece %s, erasing extent %s' % (
          piece.index, full_slice))
      self._sliceset.erase(full_slice)
      raise gen.Return(False)

  def stop(self):
    self._iopool.stop()

  def destroy(self):
    super(PieceBroker, self).destroy()
    self.stop()
