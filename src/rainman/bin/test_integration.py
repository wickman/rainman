from __future__ import print_function

import functools

from rainman.fs import DiskFilesystem, MemoryFilesystem
from rainman.testing import make_ensemble

from tornado import gen
from tornado.ioloop import IOLoop
from twitter.common import app, log
from twitter.common.log.options import LogOptions


app.add_option('-p', dest='piece_size', type=int, default=65536)
app.add_option('-m', dest='max_filesize', type=int, default=1048576)
app.add_option('-t', dest='total_filesize', type=int, default=4*1048576)
app.add_option('-s', dest='num_seeders', type=int, default=1)
app.add_option('-l', dest='num_leechers', type=int, default=1)
app.add_option('--inmemory', dest='inmemory', default=False, action='store_true')


@gen.coroutine
def run(io_loop, options):
  torrent, seeders, leechers = make_ensemble(
      io_loop,
      num_seeders=options.num_seeders,
      num_leechers=options.num_leechers,
      piece_size=options.piece_size,
      max_filesize=options.max_filesize,
      total_filesize=options.total_filesize,
      fs=MemoryFilesystem() if options.inmemory else DiskFilesystem())

  finishes = []

  for leecher in leechers:
    leecher.client.get_session(torrent).register_done_callback(
       functools.partial(finishes.append, leecher.client.peer_id))
    leecher.start()

  for seeder in seeders:
    seeder.start()

  while len(finishes) != options.num_leechers:
    yield gen.Task(io_loop.add_timeout, io_loop.time() + 0.1)


def main(args, options):
  io_loop = IOLoop.instance()
  io_loop.run_sync(functools.partial(run, io_loop, options))


LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
app.main()
