from __future__ import print_function

from rainman.fs import MemoryFilesystem
from rainman.scheduler import Scheduler
from rainman.testing import make_ensemble

import mock
from tornado import gen
from tornado.testing import (
    AsyncTestCase,
    gen_test,
)
from twitter.common.quantity import Amount, Time


from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')


class FastScheduler(Scheduler):
  CONNECTION_REFRESH_INTERVAL = Amount(100, Time.MILLISECONDS)
  CHOKE_INTERVAL = Amount(250, Time.MILLISECONDS)


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

  @gen_test(timeout=20)
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
    self.wait()

