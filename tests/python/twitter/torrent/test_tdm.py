from twitter.common.quantity import Amount, Time
from twitter.torrent.scheduler import TimeDecayMap

class TestClock(object):
  def __init__(self, initial=0):
    self._time = initial

  def advance(self, ticks):
    self._time += ticks

  def time(self):
    return float(self._time)


def test_basic():
  clock = TestClock()
  tdm = TimeDecayMap(window=Amount(10, Time.SECONDS), clock=clock)

  assert tdm.outstanding == 0

  tdm.add("hello", "hello peer")
  assert "hello" in tdm
  assert "goodbye" not in tdm
  assert tdm["hello"] == ["hello peer"]
  assert tdm.outstanding == 1
  assert tdm.remove("hello") == ["hello peer"]
  assert tdm.outstanding == 0

  tdm.add("hello", "hello peer")
  assert tdm.outstanding == 1
  clock.advance(5)
  assert tdm.outstanding == 1
  tdm.add("hello", "other hello peer")
  assert tdm.outstanding == 2
  assert tdm["hello"] == ["hello peer", "other hello peer"]
  assert "hello" in tdm
  assert "goodbye" not in tdm

  clock.advance(5)
  assert tdm["hello"] == ["other hello peer"]
  assert tdm.outstanding == 1

  clock.advance(5)
  assert tdm["hello"] == []
  assert tdm.outstanding == 0


def test_odd():
  tdm = TimeDecayMap()
  assert tdm.outstanding == 0
  tdm.remove('not in there')
  assert tdm.outstanding == 0
