import struct
from types import MethodType

from tornado import gen

from .bitfield import Bitfield
from .fileset import Piece

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


class PeerChannel(object):
  class CodingError(Exception): pass

  def encode_keepalive(command):
    return struct.pack('>I', 0)

  def encode_bit(command):
    return ''.join([struct.pack('>I', 1), struct.pack('B', command)])

  def encode_have(command, index):
    return ''.join([struct.pack('>I', 5), struct.pack('B', command), struct.pack('>I', index)])

  def encode_bitfield(command, bitfield):
    assert isinstance(bitfield, Bitfield)
    return ''.join([struct.pack('>I', 1 + bitfield.num_bytes), struct.pack('B', command),
                    bitfield.as_bytes()])

  def encode_request(command, piece):
    assert isinstance(piece, Piece)
    assert piece.is_request
    return ''.join([struct.pack('>I', 13), struct.pack('B', command),
                    struct.pack('>III', piece.index, piece.offset, piece.length)])

  def encode_piece(command, piece):
    assert isinstance(piece, Piece)
    assert not piece.is_request
    return ''.join([struct.pack('>I', 9 + piece.length), struct.pack('B', command),
                    struct.pack('>II', piece.index, piece.offset), piece.block])


  @staticmethod
  def encode(command, *args):
    return PeerChannel.ENCODERS[command](command, *args)

  def __init__(self, iostream):
    self._iostream = iostream

  def send(self, command, *args):
    self._iostream.write(PeerChannel.encode(command, *args))

  @gen.engine
  def recv(self):
    message_length = struct.unpack('>I', (yield gen.Task(self._iostream.read_bytes, 4)))[0]
    if message_length == 0:
      self.keepalive()
      return
    message_body = yield gen.Task(self._iostream.read_bytes, message_length)
    message_id = ord(message_body[0])
    # close your eyes and turn away
    MethodType(PeerChannel.DECODERS[message_id], self, self.__class__)(message_id, message_body[1:])

  def decode_bit(self, command, _):
    if command == Command.CHOKE:
      self.choke()
    elif command == Command.UNCHOKE:
      self.unchoke()
    elif command == Command.INTERESTED:
      self.interested()
    elif command == Command.NOT_INTERESTED:
      self.not_interested()
    else:
      raise PeerChannel.CodingError('Could not decode command: %s (type: %s)' % (command, type(command)))

  def decode_have(self, command, index):
    index, = struct.unpack('>I', index)
    self.have(index)

  def decode_bitfield(self, command, bitfield):
    # TODO(wickman) -- There is no easy way to know the intended length of
    # the bitfield here.  So we have to cheat and take the ceil().  Make
    # sure to fix the code in session.py:PieceSet.add/remove
    self.bitfield(Bitfield.from_bytes(bitfield))

  def decode_request(self, command, piece):
    piece = Piece(*struct.unpack('>III', piece))
    if command == Command.REQUEST:
      self.request(piece)
    elif command == Command.CANCEL:
      self.cancel(piece)
    else:
      raise PeerChannel.CodingError('Could not decode command: %s' % command)

  def decode_piece(self, command, body):
    index, offset = struct.unpack('>II', body[0:8])
    piece = Piece(index, offset, len(body[8:]), body[8:])
    self.piece(piece)

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

  def request(self, piece):
    pass

  def cancel(self, piece):
    pass

  def piece(self, piece):
    pass

  # ---- Wrappers around self.send(Command, ...)

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

  def send_request(self, piece):
    self.send(Command.REQUEST, piece)

  def send_cancel(self, piece):
    self.send(Command.CANCEL, piece)

  def send_piece(self, piece):
    self.send(Command.PIECE, piece)
