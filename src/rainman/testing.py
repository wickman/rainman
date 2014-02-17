import os

from .fileset import FileSet, Fileslice
from .fs import DISK
from .metainfo import MetaInfoBuilder
from .torrent import Torrent

from twitter.common.dirutil import safe_mkdtemp


def make_fileset(filelist, piece_size, fs=DISK):
  "Given (filename, contents) list, return dir, FileSet pair."
  td = safe_mkdtemp()
  for filename, contents in filelist:
    sl = Fileslice(os.path.join(td, filename), slice(0, len(contents)))
    fs.fill(sl)
    fs.write(sl, contents)
  filelist = [(filename, len(contents)) for (filename, contents) in filelist]
  return td, FileSet(filelist, piece_size)


def make_metainfo(filelist, piece_size, fs=DISK):
  td, fileset = make_fileset(filelist, piece_size, fs=fs)
  mib = MetaInfoBuilder(fileset.rooted_at(td), relpath=td)
  return td, fileset, mib.build(fs)


def make_torrent(filelist, piece_size, tracker, fs=DISK):
  td, fileset, metainfo = make_metainfo(filelist, piece_size, fs=fs)
  torrent = Torrent()
  torrent.info = metainfo
  torrent.announce = tracker
  return td, fileset, torrent
