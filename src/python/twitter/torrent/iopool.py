import functools
import threading

try:
  from Queue import Queue, Empty as QueueEmpty
except ImportError:
  from queue import Queue, Empty as QueueEmpty

import tornado.ioloop
from tornado import stack_context

from twitter.common.quantity import Amount, Time

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
      self._stop_event = threading.Event()
      threading.Thread.__init__(self)
      self.daemon = True
      self.start()

    def run(self):
      while not self._stop_event.is_set():
        try:
          callable, callback = self._inqueue.get(timeout=IOPool.MAXIMUM_WAIT.as_(Time.SECONDS))
        except QueueEmpty:
          continue
        value = callable()
        self._io_loop.add_callback(functools.partial(callback, value))
        self._inqueue.task_done()

    def stop(self):
      self._stop_event.set()

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
