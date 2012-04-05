import functools
import threading

try:
  from Queue import Queue, Empty as QueueEmpty
except ImportError:
  from queue import Queue, Empty as QueueEmpty

import tornado.ioloop
from tornado import stack_context
from tornado import gen

from twitter.common.quantity import Amount, Time

from .fileset import fileslice


__all__ = ('IOPool',)

class IOPool(object):
  """
    Threadpool for deferred io_loop tasks that aren't necessarily socket controlled, e.g.
    file I/O or computationally bound.  Basically a bridge between threadpool operations and
    ioloop operations.
  """

  DEFAULT_WORKERS = 2
  MAXIMUM_WAIT = Amount(100, Time.MILLISECONDS)

  class Worker(threading.Thread):
    def __init__(self, inqueue, io_loop):
      self._inqueue = inqueue
      self._io_loop = io_loop
      self._stop = threading.Event()
      threading.Thread.__init__(self)
      self.daemon = True
      self.start()

    def run(self):
      while not self._stop.is_set():
        try:
          callable, callback = self._inqueue.get(timeout=IOPool.MAXIMUM_WAIT.as_(Time.SECONDS))
        except QueueEmpty:
          continue
        value = callable()
        self._io_loop.add_callback(functools.partial(callback, value))
        self._inqueue.task_done()

    def stop(self):
      self._stop.set()

  def __init__(self, io_loop=None, workers=DEFAULT_WORKERS):
    self._in_queue = Queue()
    self._io_loop = io_loop or tornado.ioloop.IOLoop.instance()
    self._workers = [IOPool.Worker(self._in_queue, self._io_loop) for k in range(workers)]

  def add(self, function, *args, **kw):
    callback = kw.pop('callback', (lambda *a,**k: True))
    callback = stack_context.wrap(callback)
    self._in_queue.put((functools.partial(function, *args, **kw), callback))

  def stop(self):
    for worker in self._workers:
      worker.stop()
      worker.join()

"""

Ex.

import time
import tornado.ioloop
from twitter.torrent.fileset import FileSet
from twitter.torrent.iopool import FileIOPool
from twitter.torrent.peer import Piece

fs = FileSet([('a.txt', 1000000),
              ('b.txt', 2000000),
              ('c.txt', 500000)],
             piece_size=512*1024)
pc = Piece(1, 30000, 512*1024*2)
fiop = FileIOPool(fs)

global_ioloop = tornado.ioloop.IOLoop.instance()

def done(*args, **kw):
  print 'done received %s bytes.' % len(args[0])
  print '%.6f' % time.time()
  global_ioloop.stop()

print '%.6f' % time.time()
fiop.read(pc, done)
global_ioloop.start()
fs.destroy()

"""


class FileIOPool(IOPool):
  """
    Translate Fileset read/write operations to IOLoop operations via a ThreadPool.
  """

  def __init__(self, fileset, io_loop=None):
    self._fileset = fileset
    super(FileIOPool, self).__init__(io_loop=io_loop)

  @gen.engine
  def read(self, piece, callback):
    slices = list(self._fileset.iter_slices(piece.index, piece.offset, piece.length))
    read_slices = yield [gen.Task(self.add, fileslice.read, slice_) for slice_ in slices]
    callback(''.join(read_slices))

  @gen.engine
  def write(self, piece, callback=None):
    slices = []
    offset = 0
    for slice_ in self._fileset.iter_slices(piece.index, piece.offset, piece.length):
      slices.append(gen.Task(self.add, fileslice.write, piece.data[offset:offset+slice_.length]))
      offset += slice_.length
    yield slices
