from rainman.bandwidth import Bandwidth

import pytest
from twitter.common.quantity import Amount, Time, Data


class TestClock(object):
  def __init__(self, initial=0):
    self._time = initial

  def advance(self, ticks):
    self._time += ticks

  def time(self):
    return float(self._time)


def test_construction():
  with pytest.raises(ValueError):
    Bandwidth(window=20)
  with pytest.raises(ValueError):
    Bandwidth(window=Amount(20, Data.GB))
  with pytest.raises(ValueError):
    Bandwidth(denominator=Data.GB)
  # Defaults / Time values should work
  Bandwidth()
  Bandwidth(window=Amount(1, Time.SECONDS))
  Bandwidth(denominator=Time.SECONDS)


def test_add():
  Bandwidth().add(23)
  clock = TestClock(0)
  bw = Bandwidth(window=Amount(1, Time.MINUTES), denominator=Time.SECONDS, clock=clock)
  bw.add(60)
  assert bw.bandwidth == 1  # 60 units in one minute = 1 units per second
  clock.advance(30)
  assert bw.bandwidth == 1
  clock.advance(30)
  assert bw.bandwidth == 0
  bw.add(60)
  assert bw.bandwidth == 1
  bw.add(60)
  assert bw.bandwidth == 2


def test_fucked_clock():
  # clock going backwards loses data but doesn't except or anything.
  clock = TestClock()
  bw = Bandwidth(clock=clock)
  bw.add(100)
  assert bw.bandwidth == 1. * 100 / 60
  clock.advance(-1)
  assert bw.bandwidth == 1. * 100 / 60
  clock.advance(60)
  assert bw.bandwidth == 1. * 100 / 60
  clock.advance(1)
  assert bw.bandwidth == 0
  clock.advance(-2)
  assert bw.bandwidth == 0
