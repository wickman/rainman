from collections import defaultdict
import random

from toro import Condition
from tornado import gen
from twitter.common import log
from twitter.common.quantity import Amount, Time


class BoundedDecayingMap(object):
  """A bounded time-decaying map from any key to a list of assigned peers."""

  class Error(Exception): pass
  class Empty(Error): pass

  DEFAULT_WINDOW = Amount(15, Time.SECONDS)
  DEFAULT_CONCURRENCY = 32

  def __init__(self, io_loop, concurrency=DEFAULT_CONCURRENCY, window=DEFAULT_WINDOW):
    self.io_loop = io_loop
    self._concurrency = concurrency
    self._concurrent = 0
    self._condition = Condition()
    self._window = window.as_(Time.SECONDS)
    self._elements = defaultdict(list)

  @gen.coroutine
  def add(self, key, peer_id):
    """Indicate that we have requested key from peer_id"""
    try:
      while self._concurrent >= self._concurrency:
        yield self._condition.wait()
      self._concurrent += 1
      sentinel = self.io_loop.add_timeout(
         self.io_loop.time() + self._window,
         lambda: self.__remove_internal(key, peer_id))
      self._elements[key].append((peer_id, sentinel))
    except Exception as e:
      import traceback
      log.error(traceback.format_exc())

  def __remove_internal(self, key, peer_id):
    # sanity checks
    assert key in self._elements
    current_size = len(self._elements[key])
    self._elements[key] = [(pid, sentinel) for (pid, sentinel) in self._elements[key]
                           if pid == peer_id]
    assert len(self._elements[key]) == current_size - 1
    self._concurrent -= 1
    self._condition.notify(1)

  def remove(self, key):
    """Remove all peers associated with this key."""
    if key not in self._elements:
      return []
    for peer_id, sentinel in self._elements[key]:
      self._concurrent -= 1
      self._condition.notify(1)
      self.io_loop.remove_timeout(sentinel)
    return [peer_id for (peer_id, sentinel) in self._elements.pop(key)]

  def contains(self, key, peer_id):
    if key not in self._elements:
      return False
    return any(pid == peer_id for pid, _ in self._elements[key])

  def pop_random(self):
    # this is not random b/c of distribution
    if not self._elements:
      raise self.Empty()
    key, peer_ids = random.choice(list(self._elements.items()))
    (peer_id, sentinel) = peer_ids.pop(0)
    if not peer_ids:
      self.remove(key)
    self.io_loop.remove_timeout(sentinel)
    self._concurrent -= 1
    self._condition.notify(1)
    return (key, peer_id)

  def __len__(self):
    return self._concurrency - self._sem.counter
