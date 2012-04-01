import errno
import hashlib
import os
import struct
import tempfile

from twitter.common.dirutil import safe_mkdir, safe_rmtree

# First size all the pieces if necessary.
# If no sizing took place:
#   If .{{sha}}.hashes exists and is the expected size:
#     populate pieces with the data in the above
# else:
#   compute hashes
#   write .{{sha}}.hashes
#
# TODO(wickman) Consider optimizing this if it becomes a bottleneck.

class Fileset(object):
  def __init__(self, files, piece_size, chroot=None):
    """
      files: ordered list of (filename, filesize) pairs
      chroot (optional): location to store (or resume) this fileset.
                         if not supplied, create a new one.
    """
    # Assert sanity of input and compute filename of on-disk piece cache.
    sha = hashlib.sha1()
    for fn, fs in files:
      assert isinstance(fn, str)
      assert isinstance(fs, int) and fs >= 0
      sha.update(fn)
      sha.update(struct.pack('>q', fs))
    self._hash = sha.digest()
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
    current_size = Fileset.safe_size(filename)
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
      with open(pieces_file, 'w') as fp:
        fp.write(self._pieces)
    self._initialized = True

  def initialized(self):
    return self._initialized

  def read(self, index, begin, length):
    pass

  def write(self, index, begin, block):
    pass

  def hash(self, index):
    """
      Returns the computed (not cached) sha1 of the piece at index.
    """
    pass

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
