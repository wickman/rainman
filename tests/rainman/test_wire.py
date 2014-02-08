from rainman.bitfield import Bitfield
from rainman.fileset import Piece, Request
from rainman.wire import Command, PeerChannel


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

# TODO(wickman) Just use mock
class MockReceiver(PeerChannel):
  def __init__(self, *args, **kw):
    PeerChannel.__init__(self, *args, **kw)
    self.keepalives = 0
    self.chokes = 0
    self.interests = 0
    self.haves = []
    self.bitfields = []
    self.requests = []
    self.cancels = []
    self.pieces = []

  def keepalive(self):
    self.keepalives += 1

  def choke(self):
    self.chokes += 1

  def unchoke(self):
    self.chokes -= 1

  def interested(self):
    self.interests += 1

  def not_interested(self):
    self.interests -= 1

  def have(self, index):
    self.haves.append(index)

  def bitfield(self, bitfield):
    self.bitfields.append(bitfield)

  def request(self, piece):
    self.requests.append(piece)

  def cancel(self, piece):
    self.cancels.append(piece)

  def piece(self, piece):
    self.pieces.append(piece)


def test_basic_commands():
  tio = TestIOStream()
  sender = PeerChannel(tio)
  receiver = MockReceiver(tio)

  sender.send_choke()
  receiver.recv()
  assert receiver.chokes == 1

  sender.send_unchoke()
  receiver.recv()
  assert receiver.chokes == 0

  sender.send_interested()
  receiver.recv()
  assert receiver.interests == 1

  sender.send_not_interested()
  receiver.recv()
  assert receiver.interests == 0

  sender.send_keepalive()
  receiver.recv()
  assert receiver.keepalives == 1

  # multiple sends OK
  sender.send_keepalive()
  sender.send_choke()
  sender.send_interested()
  receiver.recv()
  assert receiver.keepalives == 2
  receiver.recv()
  assert receiver.chokes == 1
  receiver.recv()
  assert receiver.interests == 1


def test_argument_commands():
  tio = TestIOStream()
  sender = PeerChannel(tio)
  receiver = MockReceiver(tio)

  # have
  sender.send_have(23)
  receiver.recv()
  assert receiver.haves == [23]

  # bitfield -- with expected quirky behavior [set to ceil(length/8)]
  bf1 = Bitfield(length=23, default=True)
  bf2 = Bitfield(length=24, default=True)
  sender.send_bitfield(bf1)
  sender.send_bitfield(bf2)
  receiver.recv()
  receiver.recv()
  assert receiver.bitfields[0] != bf1
  assert receiver.bitfields[1] == bf2
  assert len(receiver.bitfields[0]) == len(receiver.bitfields[1])

  # request
  sender.send_request(Request(0, 1, 2))
  receiver.recv()
  assert receiver.requests == [Request(0, 1, 2)]

  # cancel
  sender.send_cancel(Request(0, 1, 2))
  receiver.recv()
  assert receiver.cancels == [Request(0, 1, 2)]

  # piece
  piece = Piece(0, 1, len(b'hello world'), b'hello world')
  sender.send_piece(piece)
  receiver.recv()
  assert receiver.pieces == [piece]


# TODO(wickman) Test me!
def test_exception_in_stream():
  pass
