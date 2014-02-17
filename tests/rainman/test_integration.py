from __future__ import print_function

import os
import random

from rainman.client import Client
from rainman.fileset import FileSet, Fileslice
from rainman.fs import MemoryFilesystem, DISK
from rainman.metainfo import MetaInfoBuilder
from rainman.peer_id import PeerId
from rainman.scheduler import Scheduler
from rainman.torrent import Torrent

from tornado import gen
from tornado.testing import (
    AsyncTestCase,
    bind_unused_port,
    gen_test,
)
from twitter.common.dirutil import safe_mkdtemp, safe_open, safe_mkdir
from twitter.common.quantity import Amount, Data, Time


from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')



class SocketClient(Client):
  def __init__(self, sock, port, io_loop, peer_id=None, **kw):
    self.__sock = sock
    self.__port = port
    super(SocketClient, self).__init__(peer_id or PeerId.generate(), io_loop=io_loop, **kw)

  def listen(self):
    self._port = self.__port
    self.add_sockets([self.__sock])


class FastScheduler(Scheduler):
  CONNECTION_REFRESH_INTERVAL = Amount(100, Time.MILLISECONDS)
  CHOKE_INTERVAL = Amount(250, Time.MILLISECONDS)


def random_stream(N):
  # return bytearray((random.getrandbits(8) for _ in range(N)))
  return os.urandom(N)


def make_ensemble(io_loop,
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
    scheduler = scheduler_impl(client, request_size=Amount(piece_size/4, Data.BYTES))
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


class TestIntegration(AsyncTestCase):
  @gen_test
  def test_single_seeder_single_leecher(self):
    torrent, seeders, leechers = make_ensemble(
        self.io_loop, num_seeders=1, num_leechers=1, fs=MemoryFilesystem())
    seeder = seeders[0].client
    leecher = leechers[0].client

    # check connection initiation
    assert seeder.peer_id not in leecher.get_session(torrent).peer_ids
    assert leecher.peer_id not in seeder.get_session(torrent).peer_ids
    yield gen.Task(leecher.initiate_connection, torrent, ('127.0.0.1', seeder.port))
    assert seeder.peer_id in leecher.get_session(torrent).peer_ids
    assert leecher.peer_id in seeder.get_session(torrent).peer_ids

  @gen_test
  def test_allocate_connections(self):
    torrent, seeders, leechers = make_ensemble(
        self.io_loop, num_seeders=1, num_leechers=1, fs=MemoryFilesystem())
    seeder_scheduler = seeders[0]
    leecher_scheduler = leechers[0]

    # check connection alocation
    connections = seeder_scheduler._allocate_connections()
    assert connections == []
    connections = leecher_scheduler._allocate_connections()
    assert connections == [(torrent, ('127.0.0.1', seeder_scheduler.client.port))]

  @gen_test
  def test_integrate(self):
    torrent, seeders, leechers = make_ensemble(
        self.io_loop,
        num_seeders=1,
        num_leechers=1,
        scheduler_impl=FastScheduler,
        fs=MemoryFilesystem())
    seeder_scheduler, seeder = seeders[0], seeders[0].client
    leecher_scheduler, leecher = leechers[0], leechers[0].client

    # run the torrent!
    leecher.get_session(torrent).register_done_callback(self.stop)
    leecher_scheduler.start()
    seeder_scheduler.start()

    # This test can take a while
    self.wait(timeout=20)
