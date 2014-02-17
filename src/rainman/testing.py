from __future__ import print_function

import os
import random

from .client import Client
from .fileset import FileSet, Fileslice
from .fs import DISK
from .metainfo import MetaInfoBuilder
from .peer_id import PeerId
from .torrent import Torrent
from .scheduler import Scheduler

from tornado.testing import bind_unused_port
from twitter.common.dirutil import safe_mkdtemp, safe_mkdir
from twitter.common.quantity import Amount, Data, Time


class SocketClient(Client):
  def __init__(self, sock, port, io_loop, peer_id=None, **kw):
    self.__sock = sock
    self.__port = port
    super(SocketClient, self).__init__(peer_id or PeerId.generate(), io_loop=io_loop, **kw)

  def listen(self):
    self._port = self.__port
    self.add_sockets([self.__sock])


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


def random_stream(N):
  return os.urandom(N)


def make_ensemble(
    io_loop,
    num_seeders=1,
    num_leechers=1,
    piece_size=16384,
    max_filesize=32768,
    total_filesize=1048576,
    seed=31337,
    scheduler_impl=Scheduler,
    fs=DISK):

  root = safe_mkdtemp()

  seeder_sockets = [(PeerId.generate(), bind_unused_port()) for _ in range(num_seeders)]
  leecher_sockets = [(PeerId.generate(), bind_unused_port()) for _ in range(num_leechers)]
  tracker_info = os.path.join(root, 'tracker_info.txt')
  with open(tracker_info, 'w') as fp:
    for peer_id, (_, port) in seeder_sockets + leecher_sockets:
      print('%s 127.0.0.1 %d' % (peer_id, port), file=fp)
  tracker_info = 'file://' + tracker_info

  random.seed(seed)
  filelist = []
  files = 0
  while total_filesize > 0:
    filesize = min(total_filesize, random.randrange(0, max_filesize))
    total_filesize -= filesize
    filename = '%x.txt' % files
    filelist.append((filename, filesize))
    content = random_stream(filesize)
    for replica in ['dataset'] + ['seeder%d' % k for k in range(num_seeders)]:
      safe_mkdir(os.path.join(root, replica))
      real_path = os.path.join(root, replica, filename)
      slice_ = Fileslice(real_path, slice(0, filesize))
      fs.fill(slice_)
      fs.write(slice_, content)
    files += 1

  fileset = FileSet(filelist, piece_size)
  mib = MetaInfoBuilder(
      fileset.rooted_at(os.path.join(root, 'dataset')),
      relpath=os.path.join(root, 'dataset'))

  torrent = Torrent()
  torrent.info = mib.build(fs)
  torrent.announce = tracker_info

  seeder_clients = []
  leecher_clients = []

  def make_peer(peer_id, listener, port, chroot):
    client = SocketClient(listener, port, io_loop, peer_id, fs=fs)
    scheduler = scheduler_impl(client, request_size=Amount(piece_size / 4, Data.BYTES))
    client.listen()
    client.register_torrent(torrent, root=chroot)
    return scheduler

  for index, (peer_id, (listener, port)) in enumerate(seeder_sockets):
    seeder_clients.append(
        make_peer(peer_id, listener, port, os.path.join(root, 'seeder%d' % index)))
  for index, (peer_id, (listener, port)) in enumerate(leecher_sockets):
    leecher_clients.append(
        make_peer(peer_id, listener, port, os.path.join(root, 'leecher%d' % index)))

  return torrent, seeder_clients, leecher_clients
