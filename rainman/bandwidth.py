from collections import deque, namedtuple
import time

from twitter.common.quantity import Amount, Time


BandwidthSample = namedtuple('BandwidthSample', 'timestamp value')


class Bandwidth(object):
  """Naive windowing bandwidth calculator."""

  def __init__(self, window=Amount(1, Time.MINUTES), denominator=Time.SECONDS, clock=time):
    """
      Initialize a bandwidth calculator that smooths over 'window' and provides rates
      in units of unit per 'denominator'.
    """
    if not isinstance(window, Amount) or not isinstance(window.unit(), Time):
      raise ValueError('Expect bandwidth window to be an Amount of Time!')
    if not isinstance(denominator, Time):
      raise ValueError('Expect bandwidth rate denominator to be a Time unit!')
    self._window = window.as_(denominator)
    self._samples = deque()
    self._aggregate = 0
    self._clock = clock

  def _filter(self, now=None):
    now = now or self._clock.time()
    while len(self._samples) > 0 and now - self._samples[0].timestamp >= self._window:
      evict = self._samples.popleft()
      self._aggregate -= evict.value

  def add(self, sample):
    now = self._clock.time()
    self._filter(now)
    self._samples.append(BandwidthSample(now, sample))
    self._aggregate += sample

  @property
  def bandwidth(self):
    """Bandwidth in values per denominator (see constructor)."""
    self._filter()
    return 1. * self._aggregate / self._window
