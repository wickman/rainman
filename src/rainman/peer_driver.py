"""
coroutines take functions w/o callbacks and turn them into futures
they are also allowed to yield other futures (or gen.Task(engine_fn) or gen.Task(fn w/ callback))

engines take functions w/ or w/o callbacks and allow them to pipeline and yield tasks
to 'return' a value, must use callback()

iopool takes something that is synchronous (an ordinary function) and runs it in a separate
thread.  once that function has completed, the callback is called.  this could be turned
into a Future using

  @gen.coroutine
  def wrapper(func, *args, **kw):
    raise gen.Return(func(*args, **kw))

but this does not thread.

you can always call iostream.write(...) from outside an engine/coroutine and it will not
block.  if you need to sequence certain operations (e.g. read data to populate a buffer,
then send elsewhere), then it will need to be done within an engine/coroutine and yielded
appropriately as part of a task.

one danger here of course is doing something like:
  iostream.write('bytes1', callback1)
  iostream.write('bytes2', callback2)

in sequence.  first, not only is this dangerous, but second, callback1 may never be
called as only one outstanding write callback can be live on a stream.  therefore all i/o
on a single stream should likely be synchronized to a certain degree.
"""

from abc import abstractmethod, abstractproperty
import struct

from .bitfield import Bitfield
from .fileset import Piece, Request

from tornado import gen
from twitter.common import log
from twitter.common.lang import Interface


class Command(object):
  KEEPALIVE = -1
  CHOKE = 0
  UNCHOKE = 1
  INTERESTED = 2
  NOT_INTERESTED = 3
  HAVE = 4
  BITFIELD = 5
  REQUEST = 6
  PIECE = 7
  CANCEL = 8

  NAMES = {
    KEEPALIVE: 'KEEPALIVE',
    CHOKE: 'CHOKE',
    UNCHOKE: 'UNCHOKE',
    INTERESTED: 'INTERESTED',
    NOT_INTERESTED: 'NOT_INTERESTED',
    HAVE: 'HAVE',
    BITFIELD: 'BITFIELD',
    REQUEST: 'REQUEST',
    PIECE: 'PIECE',
    CANCEL: 'CANCEL',
  }

  @classmethod
  def to_string(cls, command):
    return cls.NAMES.get(command, 'UNKNOWN')


def encode_keepalive(command):
  return struct.pack('>I', 0)


def encode_bit(command):
  return b''.join([struct.pack('>I', 1),
                   struct.pack('B', command)])


def encode_have(command, index):
  return b''.join([struct.pack('>I', 5),
                   struct.pack('B', command),
                   struct.pack('>I', index)])


def encode_bitfield(command, bitfield):
  assert isinstance(bitfield, Bitfield)
  return b''.join([struct.pack('>I', 1 + bitfield.num_bytes),
                   struct.pack('B', command),
                   bitfield.as_bytes()])


def encode_request(command, request):
  if not isinstance(request, Request):
    raise TypeError('Expected request of type Request, got %s' % type(request))
  return b''.join([struct.pack('>I', 13),
                   struct.pack('B', command),
                   struct.pack('>III', request.index, request.offset, request.length)])


def encode_piece(command, piece):
  if not isinstance(piece, Piece):
    raise TypeError('Expected piece of type Piece, got %s' % type(piece))
  return b''.join([struct.pack('>I', 9 + piece.length),
                   struct.pack('B', command),
                   struct.pack('>II', piece.index, piece.offset),
                   piece.block])


def decode_bit(channel, command, _):
  if command == Command.CHOKE:
    return channel.choke()
  elif command == Command.UNCHOKE:
    return channel.unchoke()
  elif command == Command.INTERESTED:
    return channel.interested()
  elif command == Command.NOT_INTERESTED:
    return channel.not_interested()
  else:
    raise ValueError('Could not decode command: %s (type: %s)' % (command, type(command)))


def decode_have(channel, command, index):
  index, = struct.unpack('>I', index)
  return channel.have(index)


def decode_bitfield(channel, command, bitfield):
  # TODO(wickman) -- There is no easy way to know the intended length of
  # the bitfield here.  So we have to cheat and take the ceil().  Make
  # sure to fix the code in session.py:PieceSet.add/remove
  return channel.bitfield(Bitfield.from_bytes(bitfield))


def decode_request(channel, command, request):
  index, offset, length = struct.unpack('>III', request)
  request = Request(index, offset, length)
  if command == Command.REQUEST:
    return channel.request(request)
  elif command == Command.CANCEL:
    return channel.cancel(request)
  else:
    raise ValueError('Could not decode command: %s' % command)


def decode_piece(channel, command, body):
  index, offset = struct.unpack('>II', body[0:8])
  piece = Piece(index, offset, len(body[8:]), body[8:])
  return channel.piece(piece)


class PeerDriver(Interface):
  class CodingError(Exception): pass

  ENCODERS = {
    Command.KEEPALIVE: encode_keepalive,
    Command.CHOKE: encode_bit,
    Command.UNCHOKE: encode_bit,
    Command.INTERESTED: encode_bit,
    Command.NOT_INTERESTED: encode_bit,
    Command.HAVE: encode_have,
    Command.BITFIELD: encode_bitfield,
    Command.REQUEST: encode_request,
    Command.CANCEL: encode_request,
    Command.PIECE: encode_piece
  }

  DECODERS = {
    Command.CHOKE: decode_bit,
    Command.UNCHOKE: decode_bit,
    Command.INTERESTED: decode_bit,
    Command.NOT_INTERESTED: decode_bit,
    Command.HAVE: decode_have,
    Command.BITFIELD: decode_bitfield,
    Command.REQUEST: decode_request,
    Command.CANCEL: decode_request,
    Command.PIECE: decode_piece
  }

  @classmethod
  def _encode(cls, command, *args):
    """Encode a command and return the bytes associated with it."""
    return cls.ENCODERS[command](command, *args)

  @classmethod
  def _dispatch(cls, interface, command, body):
    """Decode a command and dispatch a call to the :class:`PeerDriver` interface."""
    return cls.DECODERS[command](interface, command, body)

  # ---- Iostream interface
  @abstractproperty
  def iostream(self):
    pass

  # ---- Receive methods.  These must all return futures (@gen.coroutines.)
  @abstractmethod
  def keepalive(self):
    pass

  @abstractmethod
  def choke(self):
    pass

  @abstractmethod
  def unchoke(self):
    pass

  @abstractmethod
  def interested(self):
    pass

  @abstractmethod
  def not_interested(self):
    pass

  @abstractmethod
  def have(self, index):
    pass

  @abstractmethod
  def bitfield(self, bitfield):
    pass

  @abstractmethod
  def request(self, request):
    pass

  @abstractmethod
  def cancel(self, request):
    pass

  @abstractmethod
  def piece(self, piece):
    pass

  # ---- Receive interface.
  @gen.coroutine
  def recv(self):
    """Receive a message on the iostream, and dispatch to the provided interface."""
    message_length = struct.unpack('>I', (yield gen.Task(self.iostream.read_bytes, 4)))[0]
    log.debug('message length: %d' % message_length)
    if message_length == 0:
      yield self.keepalive()
    else:
      message_body = yield gen.Task(self.iostream.read_bytes, message_length)
      message_id, message_body = ord(message_body[0]), message_body[1:]
      log.debug('message_id: %s' % message_id)
      yield self._dispatch(self, message_id, message_body)

  # ---- Send interface.
  @gen.coroutine
  def send(self, command, *args):
    log.debug('[%s] sending %s(%s)' % (
        self, Command.to_string(command), ', '.join(map(str, args))))
    yield gen.Task(self.iostream.write, self._encode(command, *args))

  def send_keepalive(self):
    return self.send(Command.KEEPALIVE)

  def send_choke(self):
    return self.send(Command.CHOKE)

  def send_unchoke(self):
    return self.send(Command.UNCHOKE)

  def send_interested(self):
    return self.send(Command.INTERESTED)

  def send_not_interested(self):
    return self.send(Command.NOT_INTERESTED)

  def send_have(self, index):
    return self.send(Command.HAVE, index)

  def send_bitfield(self, bitfield):
    return self.send(Command.BITFIELD, bitfield)

  def send_request(self, request):
    log.debug('wat?')
    lol = self.send(Command.REQUEST, request)
    log.debug('lol is %s' % lol)
    return lol

  def send_cancel(self, request):
    return self.send(Command.CANCEL, request)

  def send_piece(self, piece):
    return self.send(Command.PIECE, piece)
