from contextlib import contextmanager
import os

from rainman.fileset import FileSet
from rainman.metainfo import MetaInfoBuilder, Torrent
from rainman.piece_manager import PieceBroker, PieceManager

from twitter.common.contextutil import temporary_dir


@contextmanager
def make_fileset(filelist, piece_size):
  "Given (filename, contents) list, return dir, FileSet pair."
  with temporary_dir() as td:
    for filename, contents in filelist:
      with open(os.path.join(td, filename), 'wb') as fp:
        fp.write(contents)
    filelist = [(filename, len(contents)) for (filename, contents) in filelist]
    yield td, FileSet(filelist, piece_size)


@contextmanager
def make_piece_broker(filelist, piece_size, complete=False, **kw):
  "Given (filename, contents) list, return dir, FileSet pair."
  with fileset(filelist, piece_size) as (td, fs):
    hashes = None
    if complete:
      pm = PieceManager(fs, chroot=td)
      hashes = list(pm.iter_hashes())
    yield PieceBroker(fs, chroot=td, piece_hashes=hashes, **kw)


@contextmanager
def make_metainfo(filelist, piece_size):
  with make_fileset(filelist, piece_size) as (td, fs):
    mib = MetaInfoBuilder()
    for filename, _ in filelist:
      mib.add(os.path.join(td, filename), filename)
    yield mib.build()


@contextmanager
def make_torrent(filelist, piece_size, tracker):
  with make_metainfo(filelist, piece_size) as metainfo:
    torrent = Torrent()
    torrent.info = metainfo
    torrent.announce = tracker
    yield torrent
