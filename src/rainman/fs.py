import os

from twitter.common.dirutil import touch
from twitter.common.lang import Interface
from twitter.common import log


class Fileslice(object):
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

  def __repr__(self):
    return '%s(%r[%r,%r])' % (self.__class__.__name__, self._filename, self.start, self.stop)


class Filesystem(Interface):
  def read(self, slice_):
    pass

  def write(self, slice_, data):
    pass

  def fill(self, slice_):
    """Fill the slice with zero bytes.  Returns true if bytes were written."""
    pass


class DiskFilesystem(Filesystem):
  """A filesystem implementing reads/writes over real files."""

  def read(self, slice_):
    log.debug('Disk reading %s' % slice_)
    with open(slice_.filename, 'rb') as fp:
      fp.seek(slice_.start)
      data = fp.read(slice_.length)
      if len(data) != slice_.length:
        raise self.ReadError('File is truncated at this slice!')
      return data

  def write(self, slice_, data):
    log.debug('Disk writing %s' % slice_)
    if len(data) != slice_.length:
      raise self.WriteError('Block must be of appropriate size!')
    with open(slice_._filename, 'r+b') as fp:
      fp.seek(slice_.start)
      fp.write(data)

  def fill(self, slice_):
    log.debug('Disk filling %s' % slice_)
    if slice_.length == 0:
      return False
    touch(slice_.filename)
    with open(slice_.filename, 'r+b') as fp:
      if os.path.getsize(slice_.filename) < slice_.stop:
        fp.seek(slice_.stop - 1, 0)
        # write a sentinel byte which will fill the file at least up to stop.
        fp.write(b'\x00')
        return True
    return False


class MemoryFilesystem(Filesystem):
  def __init__(self):
    self._contents = {}

  def clear(self):
    self._contents = {}

  def read(self, slice_):
    log.debug('Memory reading %s.' % slice_)
    if slice_.filename not in self._contents:
      raise self.ReadError('File does not exist!')
    data = self._contents[slice_.filename]
    if len(data) < slice_.stop:
      raise self.ReadError('File not long enough!')
    return bytes(data[slice_.start:slice_.stop])

  def write(self, slice_, blob):
    log.debug('Memory writing %s.' % slice_)
    if len(blob) != slice_.length:
      raise self.WriteError('Block must be of appropriate size!')
    if slice_.filename not in self._contents:
      raise self.WriteError('File does not exist!')
    data = self._contents[slice_.filename]
    if len(data) < slice_.start:
      data.extend(bytearray(slice_.start - len(data)))
    data[slice_.start:slice_.stop] = blob

  def fill(self, slice_):
    log.debug('Memory filling %s.' % slice_)
    if slice_.filename not in self._contents:
      self._contents[slice_.filename] = bytearray(slice_.stop)
      return True
    else:
      data = self._contents[slice_.filename]
      if slice_.stop < len(data):
        return False
      data.extend(bytearray(slice_.stop - len(data)))
      return True


DISK = DiskFilesystem()
