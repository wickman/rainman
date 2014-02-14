from __future__ import print_function

import os
import random

from rainman.client import Client, Scheduler
from rainman.fileset import FileSet
from rainman.metainfo import MetaInfoBuilder
from rainman.peer_id import PeerId
from rainman.torrent import Torrent

from tornado import gen
from tornado.testing import (
    AsyncTestCase,
    bind_unused_port,
    gen_test,
)
from twitter.common.dirutil import safe_mkdtemp, safe_open
from twitter.common.quantity import Amount, Data


from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')



class SocketClient(Client):
  def __init__(self, sock, port, io_loop, peer_id=None):
    self.__sock = sock
    self.__port = port
    super(SocketClient, self).__init__(peer_id or PeerId.generate(), io_loop=io_loop)

  def listen(self):
    self._port = self.__port
    self.add_sockets([self.__sock])


def random_stream(N):
  return bytearray((random.getrandbits(8) for _ in range(N)))


def make_ensemble(io_loop, num_seeders=1, num_leechers=1, piece_size=64, seed=31337):
  root = safe_mkdtemp()

  seeder_sockets = [(PeerId.generate(), bind_unused_port()) for _ in range(num_seeders)]
  leecher_sockets = [(PeerId.generate(), bind_unused_port()) for _ in range(num_leechers)]
  tracker_info = os.path.join(root, 'tracker_info.txt')
  with open(tracker_info, 'w') as fp:
    for peer_id, (_, port) in seeder_sockets + leecher_sockets:
      print('Writing %s 127.0.0.1 %d to tracker_info.txt' % (peer_id, port))
      print('%s 127.0.0.1 %d' % (peer_id, port), file=fp)
  tracker_info = 'file://' + tracker_info

  random.seed(seed)
  filelist = []
  mib = MetaInfoBuilder()
  for name in ('a', 'b', 'c', 'd'):
    content = random_stream(random.randrange(0, 4096))
    filelist.append((name, len(content)))
    for replica in ['dataset'] + ['seeder%d' % k for k in range(num_seeders)]:
      real_path = os.path.join(root, replica, name)
      with safe_open(real_path, 'wb') as fp:
        fp.write(content)
    mib.add(real_path, name)  # do this once

  torrent = Torrent()
  torrent.info = mib.build(piece_size)
  torrent.announce = tracker_info

  fs = FileSet(filelist, piece_size)

  seeder_clients = []
  leecher_clients = []

  for peer_id, (listener, port) in seeder_sockets:
    client = SocketClient(listener, port, io_loop, peer_id)
    client.listen()
    scheduler = Scheduler(client, request_size=Amount(piece_size/4, Data.BYTES))
    client.register_torrent(torrent, root=os.path.join(root, 'seeder%d' % k))
    seeder_clients.append(scheduler)

  for peer_id, (listener, port) in leecher_sockets:
    client = SocketClient(listener, port, io_loop, peer_id)
    client.listen()
    scheduler = Scheduler(client, request_size=Amount(piece_size/4, Data.BYTES))
    client.register_torrent(torrent, root=os.path.join(root, 'leecher%d' % k))
    leecher_clients.append(scheduler)

  return torrent, seeder_clients, leecher_clients


class TestIntegration(AsyncTestCase):
  @gen_test
  def test_single_seeder_single_leecher(self):
    torrent, seeders, leechers = make_ensemble(self.io_loop, num_seeders=1, num_leechers=1)
    seeder = seeders[0]
    leecher = leechers[0]

    # check connection initiation
    assert torrent.handshake_prefix not in seeder.pieces
    assert torrent.handshake_prefix not in leecher.pieces
    yield gen.Task(leecher.client.initiate_connection,
        torrent, ('127.0.0.1', seeder.client.port))
    assert torrent.handshake_prefix in seeder.pieces
    assert torrent.handshake_prefix in leecher.pieces

    # check client peer tracker is populated
    peer_tracker = seeder.client.get_tracker(torrent)
    assert peer_tracker

  @gen_test
  def test_allocate_connections(self):
    torrent, seeders, leechers = make_ensemble(self.io_loop, num_seeders=1, num_leechers=1)
    seeder = seeders[0]
    leecher = leechers[0]

    # check connection alocation
    connections = yield seeder._allocate_connections()
    assert connections == []
    connections = yield leecher._allocate_connections()
    assert connections == [(torrent, ('127.0.0.1', seeder.client.port))]
