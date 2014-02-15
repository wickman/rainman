from __future__ import print_function

import os
import random

from rainman.client import Client, Scheduler
from rainman.fileset import FileSet
from rainman.metainfo import MetaInfoBuilder
from rainman.peer_id import PeerId
from rainman.torrent import Torrent

from tornado import gen
from tornado.ioloop import IOLoop
from tornado.testing import bind_unused_port
from twitter.common.dirutil import safe_mkdtemp, safe_open
from twitter.common.quantity import Amount, Data, Time
from twitter.common import log, app
from twitter.common.log.options import LogOptions


app.add_option('-p', dest='piece_size', type=int, default=65536)
app.add_option('-m', dest='max_filesize', type=int, default=1048576)
app.add_option('-t', dest='total_filesize', type=int, default=4*1048576)


class SocketClient(Client):
  def __init__(self, sock, port, io_loop, peer_id=None):
    self.__sock = sock
    self.__port = port
    super(SocketClient, self).__init__(peer_id or PeerId.generate(), io_loop=io_loop)

  def listen(self):
    self._port = self.__port
    self.add_sockets([self.__sock])


def random_stream(N):
  # return bytearray((random.getrandbits(8) for _ in range(N)))
  return os.urandom(N)


def make_ensemble(io_loop,
                  num_seeders=1,
                  num_leechers=1,
                  piece_size=64,
                  max_filesize=32768,
                  total_filesize=1048576,
                  seed=31337,
                  scheduler_impl=Scheduler):
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
  mib = MetaInfoBuilder()

  files = 0
  while total_filesize > 0:
    filesize = min(total_filesize, random.randrange(0, max_filesize))
    total_filesize -= filesize
    filename = '%x.txt' % files
    filelist.append((filename, filesize))
    content = random_stream(filesize)
    for replica in ['dataset'] + ['seeder%d' % k for k in range(num_seeders)]:
      real_path = os.path.join(root, replica, filename)
      with safe_open(real_path, 'wb') as fp:
        fp.write(content)
    mib.add(real_path, filename)  # do this once
    files += 1

  torrent = Torrent()
  torrent.info = mib.build(piece_size)
  torrent.announce = tracker_info

  fs = FileSet(filelist, piece_size)

  seeder_clients = []
  leecher_clients = []

  def make_peer(peer_id, listener, port, chroot):
    client = SocketClient(listener, port, io_loop, peer_id)
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


def run(io_loop, options):
  torrent, seeders, leechers = make_ensemble(
      io_loop,
      num_seeders=1,
      num_leechers=1,
      piece_size=options.piece_size,
      max_filesize=options.max_filesize,
      total_filesize=options.total_filesize)
  seeder_scheduler, seeder = seeders[0], seeders[0].client
  leecher_scheduler, leecher = leechers[0], leechers[0].client

  # run the torrent!
  leecher.get_session(torrent).register_done_callback(io_loop.stop)
  leecher_scheduler.start()
  seeder_scheduler.start()


def main(args, options):
  io_loop = IOLoop.instance()
  run(io_loop, options)
  io_loop.start()


LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
app.main()
