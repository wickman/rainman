import binascii
import hashlib
import os

from .fileset import FileSet
from .fs import DISK
from .request import Request
from .sliceset import SliceSet

from twitter.common.dirutil import (
    safe_mkdir,
    safe_mkdtemp,
    safe_rmtree,
)
from twitter.common import log
from twitter.common.lang import Compatibility


class PieceManager(object):
  """A Piece overlay on a FileSet.

     Translates Requests to operations over a FileSet and keeps track of
     live shas.
  """

  DEFAULT_SPLAT_BYTES = 64 * 1024

  @classmethod
  def from_metainfo(cls, metainfo, **kw):
    return cls(
        FileSet.from_metainfo(metainfo),
        list(metainfo.piece_hashes),
        **kw)

  def __init__(self, fileset, piece_hashes=None, chroot=None, fs=DISK):
    self._fileset = fileset
    self._pieces = piece_hashes or [b'\x00' * 20] * self._fileset.num_pieces
    self._actual_pieces = []
    self._fileset = fileset
    self._sliceset = SliceSet()
    self._chroot = chroot or safe_mkdtemp()
    self._fs = fs
    safe_mkdir(self._chroot)

  def __contains__(self, block):
    return self.covers(block)

  @property
  def slices(self):
    return self._sliceset

  @property
  def total_size(self):
    return sum((fp[1] for fp in self._fileset), 0)

  @property
  def num_pieces(self):
    return self._fileset.num_pieces

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
  def left(self):
    return self.total_size - self.assembled_size

  @property
  def hashfile(self):
    return os.path.join(self._chroot, '.%s.pieces' % self._fileset.hash)

  def whole_piece(self, index):
    num_pieces, leftover = divmod(self._fileset.size, self._fileset.piece_size)
    num_pieces += leftover > 0
    assert index < num_pieces
    if not leftover or index < num_pieces - 1:
      return Request(index, 0, self._fileset.piece_size)
    return Request(index, 0, leftover)

  def to_slice(self, block):
    if not isinstance(block, Request):
      raise TypeError('to_slice expects Request, got %s' % block)
    start = block.index * self._fileset.piece_size + block.offset
    stop = min(start + block.length, self._fileset.size)
    return slice(start, stop)

  # TODO(wickman) make this async via IOPool -- this means pulling
  # initialization out of __init__
  def initialize(self):
    touched = False
    # fill out files
    for slice_ in self._fileset.iter_files():
      touched |= self._fs.fill(slice_.rooted_at(self._chroot))
    log.debug('%s initialize touched files: %s' % (self, touched))
    # load up cached pieces file if it's there
    if not touched and os.path.exists(self.hashfile):
      log.debug('%s loaded existing hashfile.' % self)
      with open(self.hashfile) as fp:
        self._actual_pieces = fp.read()
        # block it into usable indices
        self._actual_pieces = [
          self._actual_pieces[offset:offset + 20]
          for offset in range(0, len(self._actual_pieces), 20)]
    # or compute it on the fly
    if len(self._actual_pieces) != self._fileset.num_pieces * 20:
      log.debug('%s writing new hashfile.' % self)
      self._actual_pieces = list(self.iter_hashes())
      with open(self.hashfile, 'wb') as fp:
        fp.write(b''.join(self._actual_pieces))
    # cover the spans that we've succeeded in the sliceset
    for index in range(self._fileset.num_pieces):
      if self._actual_pieces[index] == self._pieces[index]:
        self._sliceset.add(self.to_slice(self.whole_piece(index)))
      log.debug('%s has %s hash index %d' % (
           self,
           'incorrect' if self._actual_pieces[index] != self._pieces[index] else 'correct',
           index))

  # ---- helpers
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
    piece_start_index = piece_slice.start // self._fileset.piece_size
    piece_stop_index = (piece_slice.stop - 1) // self._fileset.piece_size
    return all(self.have(index) for index in range(piece_start_index, piece_stop_index + 1))

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
    """yield :class:`Request` objects for the piece at index with a given block size
       that are not yet part of the sliceset."""
    if not isinstance(index, Compatibility.integer):
      raise TypeError('iter_blocks requires index to be an integer.')
    if not isinstance(block_size, Compatibility.integer):
      raise TypeError('iter_blocks requires block_size to be an integer.')
    piece_size = self.piece_size(index)
    for start_offset in range(0, piece_size, block_size):
      request = Request(
          index,
          start_offset,
          block_size if start_offset + block_size <= piece_size else piece_size % block_size)
      if self.to_slice(request) not in self._sliceset:
        yield request

  def iter_pieces(self):
    """iterate over the piece blobs backed by this fileset.  blocking."""
    for index in range(self.num_pieces):
      request = self.whole_piece(index)
      yield b''.join(self._fs.read(slice_.rooted_at(self._chroot))
                     for slice_ in self._fileset.iter_slices(request))

  def iter_hashes(self):
    """iterate over the sha1 hashes, blocking."""
    for index, chunk in enumerate(self.iter_pieces()):
      digest = hashlib.sha1(chunk).digest()
      log.debug('PieceManager %d = %s' % (index, binascii.hexlify(digest)))
      yield digest

  def destroy(self):
    safe_rmtree(self._chroot)
