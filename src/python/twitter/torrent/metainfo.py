import hashlib
import os
import tempfile

from twitter.common.collections import OrderedSet

from .bencode import BEncoder, BDecoder

class MetaInfo(object):
  def __init__(self, d=None):
    self._d = d or {}
    self._d['info'] = MetaInfoInfo(self._d.get('info'))

  @property
  def announce(self):
    return self._d.get('announce')

  @announce.setter
  def announce(self, value):
    assert isinstance(value, str)
    self._d['announce'] = value

  @property
  def info(self):
    return self._d.get('info')

  @info.setter
  def info(self, value):
    if isinstance(value, dict):
      self._d['info'] = MetaInfoInfo(value)
    elif isinstance(value, MetaInfoInfo):
      self._d['info'] = value
    else:
      raise ValueError(value)


class MetaInfoFile(object):
  def __init__(self, name, start, end):
    self._name = name
    self._start = start
    self._end = end

  @property
  def name(self):
    return self._name

  @property
  def start(self):
    return self._start

  @property
  def end(self):
    return self._end

  @property
  def length(self):
    return self.end - self.start

  def to_dict(self):
    return {
      'length': self.length,
      'path': [element.encode(MetaInfoInfo.ENCODING) for element in self.name.split(os.path.sep)],
    }


class MetaInfoInfo(object):
  """
    MetaInfoInfo write API
      mii = MetaInfoInfo(chroot='/tmp')
      mii.add('foo.zip')
      mii.add('bar.zip')
      mii.remove('foo.zip')

    mii = MetaInfoInfo.from_dict(...)  # bencoded, read API
    mii = MetaInfoInfo.from_torrent(...)
    mii = MetaInfoInfo.from_filename(...)
    mii = MetaInfoInfo.from_dir(...)

    mii.files => list of MetaInfoInfoFile
      MetaInfoInfoFile:
         .start, .end
         .length
         .name
  """

  class FileNotFound(Exception): pass
  class EmptyInfo(Exception): pass

  DEFAULT_PIECE_LENGTH = 2**16
  ENCODING = 'utf-8'

  def __init__(self):
    self._chroot = None
    self._files = OrderedSet()

  def add(self, filename):
    if not os.path.exists(filename):
      raise MetaInfoInfo.FileNotFound("Could not find %s" % filename)
    self._files.add(filename)

  def remove(self, filename):
    self._files.discard(filename)

  def to_dict(self):
    if len(self._files) == 0:
      raise MetaInfoInfo.EmptyInfo("No files in metainfo!")
    d = {
      'pieces': ''.join(hashlib.sha1(chunk).digest() for chunk in iter(self)),
      'piece length': MetaInfoInfo.DEFAULT_PIECE_LENGTH
    }
    if len(self._files) == 1:
      d.update(length = sum(os.path.getsize(fn) for fn in self._files),
               name = os.path.basename(self._files[0]))
    else:
      d.update(files = [
        {'length': os.path.getsize(fn),
         'path': [sp.encode(MetaInfoInfo.ENCODING) for sp in fn.split(os.path.sep)]}
        for fn in self._files])
    return d

  def __iter__(self):
    chunk = ''
    for fn in self._files:
      fp = open(fn)
      while True:
        addendum = fp.read(MetaInfoInfo.DEFAULT_PIECE_LENGTH - len(chunk))
        chunk += addendum
        if len(chunk) == MetaInfoInfo.DEFAULT_PIECE_LENGTH:
          yield chunk
          chunk = ''
        if len(addendum) == 0:
          fp.close()
          break
    if len(chunk) > 0:
      yield chunk
