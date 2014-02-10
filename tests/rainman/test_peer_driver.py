from rainman.bitfield import Bitfield
from rainman.fileset import Piece, Request
from rainman.peer_driver import Command, PeerDriver

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test


"""
from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')
"""


class TestIOStream(object):
  def __init__(self):
    self._queue = ''

  def read_bytes(self, number, callback):
    print('Current queue len: %s' % len(self._queue))
    print('Asking to read %s bytes' % number)
    assert len(self._queue) >= number
    left, self._queue = self._queue[0:number], self._queue[number:]
    print('Read: %r' % left)
    callback(left)

  def write(self, buf, callback=None):
    print('Writing: %r' % buf)
    self._queue += buf
    if callback:
      callback()


# TODO(wickman) Just use mock?
class MockPeer(PeerDriver):
  def __init__(self, *args, **kw):
    super(MockPeer, self).__init__(*args, **kw)
    self._iostream = TestIOStream()
    self.keepalives = 0
    self.chokes = 0
    self.interests = 0
    self.haves = []
    self.bitfields = []
    self.requests = []
    self.cancels = []
    self.pieces = []

  @property
  def iostream(self):
    return self._iostream

  @gen.coroutine
  def keepalive(self):
    self.keepalives += 1

  @gen.coroutine
  def choke(self):
    self.chokes += 1

  @gen.coroutine
  def unchoke(self):
    self.chokes -= 1

  @gen.coroutine
  def interested(self):
    self.interests += 1

  @gen.coroutine
  def not_interested(self):
    self.interests -= 1

  @gen.coroutine
  def have(self, index):
    self.haves.append(index)

  @gen.coroutine
  def bitfield(self, bitfield):
    self.bitfields.append(bitfield)

  @gen.coroutine
  def request(self, piece):
    self.requests.append(piece)

  @gen.coroutine
  def cancel(self, piece):
    self.cancels.append(piece)

  @gen.coroutine
  def piece(self, piece):
    self.pieces.append(piece)


def test_basic_commands():
  wire = MockPeer()

  wire.send_choke()
  wire.recv()
  assert wire.chokes == 1

  wire.send_unchoke()
  wire.recv()
  assert wire.chokes == 0

  wire.send_interested()
  wire.recv()
  assert wire.interests == 1

  wire.send_not_interested()
  wire.recv()
  assert wire.interests == 0

  wire.send_keepalive()
  wire.recv()
  assert wire.keepalives == 1

  # multiple sends OK
  wire.send_keepalive()
  wire.send_choke()
  wire.send_interested()
  wire.recv()
  assert wire.keepalives == 2
  wire.recv()
  assert wire.chokes == 1
  wire.recv()
  assert wire.interests == 1


class DriverTest(AsyncTestCase):
  @gen_test
  def test_argument_commands(self):
    wire = MockPeer()

    # have
    yield wire.send_have(23)
    yield wire.recv()
    assert wire.haves == [23]

    # bitfield -- with expected quirky behavior [set to ceil(length/8)]
    bf1 = Bitfield(length=23, default=True)
    bf2 = Bitfield(length=24, default=True)
    yield wire.send_bitfield(bf1)
    yield wire.send_bitfield(bf2)
    yield wire.recv()
    yield wire.recv()
    assert wire.bitfields[0] != bf1
    assert wire.bitfields[1] == bf2
    assert len(wire.bitfields[0]) == len(wire.bitfields[1])

    # request
    yield wire.send_request(Request(0, 1, 2))
    yield wire.recv()
    assert wire.requests == [Request(0, 1, 2)]

    # cancel
    yield wire.send_cancel(Request(0, 1, 2))
    yield wire.recv()
    assert wire.cancels == [Request(0, 1, 2)]

    # piece
    piece = Piece(0, 1, len(b'hello world'), b'hello world')
    yield wire.send_piece(piece)
    yield wire.recv()
    assert wire.pieces == [piece]


# TODO(wickman) Test me!
def test_exception_in_stream():
  pass
