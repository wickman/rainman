import struct

from tornado import gen

from .bitfield import Bitfield
from .fileset import Piece, Request


class Command(object):
  KEEPALIVE      = -1
  CHOKE          = 0
  UNCHOKE        = 1
  INTERESTED     = 2
  NOT_INTERESTED = 3
  HAVE           = 4
  BITFIELD       = 5
  REQUEST        = 6
  PIECE          = 7
  CANCEL         = 8


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
    channel.choke()
  elif command == Command.UNCHOKE:
    channel.unchoke()
  elif command == Command.INTERESTED:
    channel.interested()
  elif command == Command.NOT_INTERESTED:
    channel.not_interested()
  else:
    raise ValueError('Could not decode command: %s (type: %s)' % (command, type(command)))


def decode_have(channel, command, index):
  index, = struct.unpack('>I', index)
  channel.have(index)


def decode_bitfield(channel, command, bitfield):
  # TODO(wickman) -- There is no easy way to know the intended length of
  # the bitfield here.  So we have to cheat and take the ceil().  Make
  # sure to fix the code in session.py:PieceSet.add/remove
  channel.bitfield(Bitfield.from_bytes(bitfield))


def decode_request(channel, command, request):
  index, offset, length = struct.unpack('>III', request)
  request = Request(index, offset, length)
  if command == Command.REQUEST:
    channel.request(request)
  elif command == Command.CANCEL:
    channel.cancel(request)
  else:
    raise ValueError('Could not decode command: %s' % command)


def decode_piece(channel, command, body):
  index, offset = struct.unpack('>II', body[0:8])
  piece = Piece(index, offset, len(body[8:]), body[8:])
  channel.piece(piece)


class PeerChannel(object):
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
  def encode(cls, command, *args):
    """Encode a command and return the bytes associated with it."""
    return cls.ENCODERS[command](command, *args)

  def __init__(self, iostream):
    self._iostream = iostream

  # XXX(async)
  def decode(self, command, *args):
    """Decode a command an invoke its callback."""
    self.DECODERS[command](self, command, *args)

  @gen.engine
  def recv(self):
    message_length = struct.unpack('>I', (yield gen.Task(self._iostream.read_bytes, 4)))[0]
    if message_length == 0:
      self.keepalive()
      return
    message_body = yield gen.Task(self._iostream.read_bytes, message_length)
    message_id, message_body = ord(message_body[0]), message_body[1:]
    self.decode(message_id, message_body)

  # ---- Subclasses override should they want notifications on these events
  def keepalive(self):
    pass

  def choke(self):
    pass

  def unchoke(self):
    pass

  def interested(self):
    pass

  def not_interested(self):
    pass

  def have(self, index):
    pass

  def bitfield(self, bitfield):
    pass

  def request(self, request):
    pass

  def cancel(self, request):
    pass

  def piece(self, piece):
    pass

  # ---- Wrappers around self.send(Command, ...)
  def send(self, command, *args):
    # XXX(async) -- need an io_pool to enqueue or make this a gen.engine
    self._iostream.write(self.encode(command, *args))

  def send_keepalive(self):
    self.send(Command.KEEPALIVE)

  def send_choke(self):
    self.send(Command.CHOKE)

  def send_unchoke(self):
    self.send(Command.UNCHOKE)

  def send_interested(self):
    self.send(Command.INTERESTED)

  def send_not_interested(self):
    self.send(Command.NOT_INTERESTED)

  def send_have(self, index):
    self.send(Command.HAVE, index)

  def send_bitfield(self, bitfield):
    self.send(Command.BITFIELD, bitfield)

  def send_request(self, request):
    self.send(Command.REQUEST, request)

  def send_cancel(self, request):
    self.send(Command.CANCEL, request)

  def send_piece(self, piece):
    self.send(Command.PIECE, piece)
