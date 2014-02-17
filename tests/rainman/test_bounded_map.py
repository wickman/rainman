from rainman.bounded_map import BoundedDecayingMap

import pytest
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.testing import AsyncTestCase, gen_test
from twitter.common.quantity import Amount, Time


from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')



class TestClock(object):
  def __init__(self, initial=0):
    self._time = initial

  def advance(self, ticks):
    self._time += ticks

  def time(self):
    return float(self._time)


CLOCK = TestClock()

io_loop_impl = IOLoop.current().__class__


class MockedTimeIOLoop(io_loop_impl):
  def time(self):
    return CLOCK.time()


class MockedTimeAsyncTestCase(AsyncTestCase):
  def get_new_ioloop(self):
    return MockedTimeIOLoop()


class TestBoundedDecayingMap(MockedTimeAsyncTestCase):

  def test_basic(self):
    bounded_map = BoundedDecayingMap(self.io_loop, concurrency=4, window=Amount(5, Time.SECONDS))

    @gen.coroutine
    def inner_loop():
      # safely handle peers not existing as they are ephemeral
      assert not bounded_map.contains('foo', 'peer1')
      assert bounded_map.remove('foo') == []

      # test adding peer and having it decay out of the map
      assert len(bounded_map) == 0
      yield bounded_map.add('foo', 'peer1')
      assert len(bounded_map) == 1
      assert bounded_map.remove('foo') == ['peer1']

      yield bounded_map.add('foo', 'peer1')
      CLOCK.advance(1)
      assert bounded_map.contains('foo', 'peer1')

      yield bounded_map.add('foo', 'peer2')
      CLOCK.advance(1)
      assert bounded_map.contains('foo', 'peer1')
      assert bounded_map.contains('foo', 'peer2')
      assert len(bounded_map) == 2

      CLOCK.advance(3)

    self.io_loop.run_sync(inner_loop)

    assert not bounded_map.contains('foo', 'peer1')
    assert bounded_map.contains('foo', 'peer2')
    assert len(bounded_map) == 1

  @gen_test
  def test_pop_random(self):
    bounded_map = BoundedDecayingMap(self.io_loop, concurrency=4, window=Amount(5, Time.SECONDS))

    with pytest.raises(BoundedDecayingMap.Empty):
      bounded_map.pop_random()

    yield bounded_map.add('foo1', 'peer1')
    yield bounded_map.add('foo2', 'peer2')
    yield bounded_map.add('foo2', 'peer3')
    assert len(bounded_map) == 3

    random_element = bounded_map.pop_random()
    assert random_element in frozenset([
        ('foo1', 'peer1'),
        ('foo2', 'peer2'),
        ('foo2', 'peer3'),
    ])
    assert len(bounded_map) == 2

    bounded_map = BoundedDecayingMap(self.io_loop, concurrency=4, window=Amount(5, Time.SECONDS))
    yield bounded_map.add('foo2', 'peer1')
    yield bounded_map.add('foo2', 'peer2')
    assert len(bounded_map) == 2
    bounded_map.pop_random()
    assert len(bounded_map) == 1
    bounded_map.pop_random()
    assert len(bounded_map) == 0
    with pytest.raises(BoundedDecayingMap.Empty):
      bounded_map.pop_random()
    assert len(bounded_map) == 0

  @gen_test
  def test_concurrency(self):
    bounded_map = BoundedDecayingMap(self.io_loop, concurrency=1, window=Amount(5, Time.SECONDS))

    yield bounded_map.add('foo1', 'peer1')
    assert bounded_map.contains('foo1', 'peer1')

    self.io_loop.add_future(bounded_map.add('foo1', 'peer2'), self.stop)
    self.io_loop.add_callback(bounded_map.remove, 'foo1')
    self.wait()

    assert not bounded_map.contains('foo1', 'peer1')
    assert bounded_map.contains('foo1', 'peer2')

