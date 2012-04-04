from collections import deque
import time

from twitter.common.quantity import Amount, Time


class Bandwidth(object):
  TIME, VALUE = 0, 1

  def __init__(self, window=Amount(1, Time.MINUTES)):
    self._window_seconds = window.as_(Time.SECONDS)
    self._samples = deque()
    self._aggregate = 0

  def _filter(self, now=None):
    now = now or time.time()
    while len(self._samples) > 0 and now - self._samples[0][Bandwidth.TIME] > self._window_seconds:
      evict = self._samples.popleft()
      self._aggregate -= evict[Bandwidth.VALUE]

  def add(self, sample):
    now = time.time()
    self._filter(now)
    self._samples.append((now, sample))
    self._aggregate += sample

  @property
  def bandwidth(self):
    """Bandwidth in values per second."""
    self._filter()
    return 1. * self._aggregate / self._window_seconds
