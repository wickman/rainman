from __future__ import print_function

from rainman.fs import DiskFilesystem, MemoryFilesystem
from rainman.testing import make_ensemble

from tornado.ioloop import IOLoop
from twitter.common import app
from twitter.common.log.options import LogOptions


app.add_option('-p', dest='piece_size', type=int, default=65536)
app.add_option('-m', dest='max_filesize', type=int, default=1048576)
app.add_option('-t', dest='total_filesize', type=int, default=4*1048576)
app.add_option('--inmemory', dest='inmemory', default=False, action='store_true')


def run(io_loop, options):
  torrent, seeders, leechers = make_ensemble(
      io_loop,
      num_seeders=1,
      num_leechers=1,
      piece_size=options.piece_size,
      max_filesize=options.max_filesize,
      total_filesize=options.total_filesize,
      fs=MemoryFilesystem() if options.inmemory else DiskFilesystem())
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
