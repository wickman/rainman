import hashlib
import random
import struct
import time

from twitter.common import log
from twitter.common.quantity import Time, Amount

from tornado import gen

from .bandwidth import Bandwidth
from .bitfield import Bitfield
from .metainfo import MetaInfo



class PeerId(object):
  PREFIX = '-TW7712-'  # TWTTR
  LENGTH = 20
  @classmethod
  def generate(cls):
    return cls.PREFIX + ''.join(random.sample('0123456789abcdef', cls.LENGTH - len(cls.PREFIX)))


class PeerHandshake(object):
  class InvalidHandshake(Exception): pass

  LENGTH = 68
  PROTOCOL_STR = 'BitTorrent protocol'
  EXTENSION_BITS = {
    44: 'Extension protocol',
    62: 'Fast peers',
    64: 'DHT',
  }

  SPANS = [
    slice( 0,  1),  # First byte = 19
    slice( 1, 20),  # "BitTorrent protocol"
    slice(20, 28),  # Reserved bits
    slice(28, 48),  # Metainfo hash
    slice(48, 68)   # Peer id
  ]

  @staticmethod
  def make(info, peer_id=None):
    """
      Make a Peer-Peer handshake.  If peer_id is omitted, make only the handshake prefix.
    """
    if isinstance(info, MetaInfo):
      hash = hashlib.sha1(info.raw()).digest()
    elif isinstance(info, str) and len(str) == 20:
      hash = info
    else:
      raise ValueError('Expected MetaInfo or hash, got %s' % repr(info))
    handshake = [chr(len(PeerHandshake.PROTOCOL_STR)), PeerHandshake.PROTOCOL_STR,
                 chr(0) * 8, hash, peer_id if peer_id else '']
    return ''.join(handshake)

  @classmethod
  def error(cls, msg):
    raise cls.InvalidHandshake('Invalid handshake: %s' % msg)

  def __init__(self, handshake_bytes):
    (handshake_byte, protocol_str, self._reserved_bits, self._hash, self._peer_id) = map(
        handshake_bytes.__getitem__, PeerHandshake.SPANS)
    if handshake_byte != chr(len(PeerHandshake.PROTOCOL_STR)):
      self.error('Initial byte of handshake should be 0x%x' % len(PeerHandshake.PROTOCOL_STR))
    if protocol_str != PeerHandshake.PROTOCOL_STR:
      self.error('This is not the BitTorrent protocol!')
    self._reserved_bits = struct.unpack('>Q', self._reserved_bits)[0]

  @property
  def hash(self):
    return self._hash

  @property
  def peer_id(self):
    return self._peer_id

  @staticmethod
  def is_set(ull, bit):
    return bool(ull & (1 << (64 - bit)))

  @property
  def dht(self):
    return self.is_set(self.EXTENSION_BITS['DHT'])

  @property
  def fast_peers(self):
    return self.is_set(self.EXTENSION_BITS['Fast peers'])

  @property
  def extended(self):
    return self.is_set(self.EXTENSION_BITS['Extension protocol'])


class Command(object):
  CHOKE          = 0
  UNCHOKE        = 1
  INTERESTED     = 2
  NOT_INTERESTED = 3
  HAVE           = 4
  BITFIELD       = 5
  REQUEST        = 6
  PIECE          = 7
  CANCEL         = 8

  @staticmethod
  def wire(command, *args):
    if command in (Command.CHOKE, Command.UNCHOKE, Command.INTERESTED, Command.NOT_INTERESTED):
      return ''.join([struct.pack('>I', 1), struct.pack('B', command)])
    elif command == Command.HAVE:
      assert len(args) == 1
      return ''.join([struct.pack('>I', 5), struct.pack('B', command), struct.pack('>I', args[0])])
    elif command == Command.BITFIELD:
      assert len(args) == 1
      bitfield = args[0]
      return ''.join([struct.pack('>I', 1 + bitfield.num_bytes), struct.pack('B', command),
          bitfield.as_bytes()])
    elif command in (Command.REQUEST, Command.CANCEL):
      assert len(args) == 1 and isinstance(args[0], Piece)
      piece = args[0]
      assert piece.is_request
      return ''.join([struct.pack('>I', 13), struct.pack('B', command),
                      struct.pack('>III', piece.index, piece.begin, piece.length)])
    elif command == Command.PIECE:
      assert len(args) == 1 and isinstance(args[0], Piece)
      piece = args[0]
      assert not piece.is_request
      return ''.join([struct.pack('>I', 9 + piece.length), struct.pack('B', command),
                      struct.pack('>II', piece.index, piece.begin),
                      piece.block])
    else:
      raise Peer.BadMessage('Unknown message id: %s' % command)
    callback()


class Piece(object):
  def __init__(self, index, offset, length, block=None):
    self.index = index
    self.offset = offset
    self.length = length
    self.block = block

  @property
  def is_request(self):
    return self.block is None

  def __eq__(self, other):
    return (self.index == other.index and
            self.offset == other.offset and
            self.length == other.length and
            self.block == other.block)


class ConnectionState(object):
  BW_COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  MIN_KEEPALIVE_WINDOW = Amount(2, Time.MINUTES)
  MAX_KEEPALIVE_WINDOW = Amount(4, Time.MINUTES)

  def __init__(self):
    self._last_alive = time.time()
    self._interested = False
    self._choked = True
    self._queue = []
    self._sent = 0
    self._bandwidth = Bandwidth(window=ConnectionState.BW_COLLECTION_INTERVAL)

  @property
  def queue(self):
    return self._queue

  @property
  def healthy(self):
    now = time.time()
    return now - self._last_alive < ConnectionState.MAX_KEEPALIVE_WINDOW.as_(Time.SECONDS)

  @property
  def needs_ping(self):
    now = time.time()
    return now - self._last_alive >= ConnectionState.MIN_KEEPALIVE_WINDOW.as_(Time.SECONDS)

  def updates_keepalive(fn):
    def wrapper(self, *args, **kw):
      self._last_alive = time.time()
      return fn(self, *args, **kw)
    return wrapper

  @updates_keepalive
  def ping(self):
    pass

  @updates_keepalive
  def choke(self):
    self._choked = True

  @updates_keepalive
  def unchoke(self):
    self._choked = False

  @updates_keepalive
  def interested(self):
    self._interested = True

  @updates_keepalive
  def uninterested(self):
    self._interested = False

  @updates_keepalive
  def cancel_request(self, piece):
    self._queue = [pc for pc in self._queue if pc != piece]

  @updates_keepalive
  def sent(self, num_bytes):
    self._sent += num_bytes
    self._bandwidth.sample(num_bytes)

  del updates_keepalive


class Peer(object):
  class BadMessage(Exception): pass

  def __init__(self, address, session, handshake_cb=None):
    self._id = None
    self._active = True
    self._address = address
    self._session = session
    self._hash = hashlib.sha1(session.torrent.info.raw()).digest()
    self._handshake_cb = handshake_cb or (lambda *x: x)
    self._iostream = None
    self._outstanding_requests = 0
    self._bitfield = Bitfield(session.torrent.info.num_pieces)
    self._in = ConnectionState()
    self._out = ConnectionState()

  @property
  def egress_bytes(self):
    return self._out._sent

  @property
  def ingress_bytes(self):
    return self._in._sent

  @property
  def id(self):
    return self._id

  @property
  def address(self):
    return self._address

  @gen.engine
  def handshake(self, iostream):
    log.debug('Session [%s] starting handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))
    handshake_full = PeerHandshake.make(self._session.torrent.info, self._session.peer_id)
    read_handshake, _ = yield [gen.Task(iostream.read_bytes, PeerHandshake.LENGTH),
                                gen.Task(iostream.write, handshake_full)]

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == self._hash:
        self._id = handshake.peer_id
        self._iostream = iostream
        succeeded = True
      else:
        log.debug('Session [%s] got mismatched torrent hashes.' % self._session.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Session [%s] got bad handshake with %s:%s: ' % (self._session.peer_id,
        self._address[0], self._address[1], e))
      iostream.close()

    self._handshake_cb(self, succeeded)
    log.debug('Session [%s] finishing handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

  @gen.engine
  def run(self):
    while self._active:
      message_length = struct.unpack('>I', (yield gen.Task(self._iostream.read_bytes, 4)))[0]
      if message_length == 0:
        log.debug('Received keepalive from [%s]' % self._id)
        self._in.ping()
        return
      message_body = yield gen.Task(self._iostream.read_bytes, message_length)
      message_id = ord(message_body[0])
      self.dispatch(message_id, message_body[1:])

  def choke(self):
    self._out.choke()
    log.debug('Sending choke to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.CHOKE))

  def unchoke(self):
    self._out.unchoke()
    log.debug('Sending unchoke to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.UNCHOKE))

  def interested(self):
    self._out.interested()
    log.debug('Sending interested to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.INTERESTED))

  def uninterested(self):
    self._out.uninterested()
    log.debug('Sending uninterested to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.NOT_INTERESTED))

  @property
  def healthy(self):
    return self._in.healthy

  def send_keepalive(self):
    if self._out.needs_ping:
      self._out.ping()
      log.debug('Sending keepalive to [%s]' % self._id)
      self._iostream.write(struct.pack('>I', 0))

  def send_have(self, index):
    log.debug('Sending have(%s) to [%s]' % (index, self._id))
    self._iostream.write(Command.wire(Command.HAVE, index))

  def send_bitfield(self, bitfield):
    log.debug('Sending bitfield to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.BITFIELD, bitfield))

  def send_request(self, index, begin, length):
    def completed():
      self._outstanding_requests += 1
    self._iostream.write(Command.wire(Command.REQUEST, Piece(index, begin, length)),
      callback=decrement)

  @gen.engine
  def send(self, index, begin, length, callback=None):
    piece = Piece(index, begin, length)
    piece.block = yield gen.Task(self._session.filemanager.read, piece)
    yield gen.Task(self._iostream.write, Command.wire(Command.PIECE, piece))
    self._out.sent(length)
    if callback:
      self._io_loop.add_callback(callback)

  @gen.engine
  def dispatch(self, message_id, message_body, callback=None):
    # TODO(wickman): Split these into recv_ messages
    if message_id == Command.CHOKE:
      log.debug('Peer [%s] choking us.' % self._id)
      self._in.choke()
    elif message_id == Command.UNCHOKE:
      log.debug('Peer [%s] unchoking us.' % self._id)
      self._in.unchoke()
    elif message_id == Command.INTERESTED:
      log.debug('Peer [%s] interested.' % self._id)
      self._in.interested()
    elif message_id == Command.NOT_INTERESTED:
      log.debug('Peer [%s] uninterested.' % self._id)
      self._in.uninterested()
    elif message_id == Command.HAVE:
      piece, = struct.unpack('>I', message_body)
      self._bitfield[piece] = True
      log.debug('Peer [%s] has piece %s.' % (self._id, piece))
      self._session.pieces.have(piece)
    elif message_id == Command.BITFIELD:
      self._session.pieces.remove(self._bitfield)
      self._bitfield.fill(message_body)
      log.debug('Peer [%s] has %d/%d pieces.' % (
          self._id, sum((self._bitfield[k] for k in range(len(self._bitfield))), 0),
          len(self._bitfield)))
      self._session.pieces.add(self._bitfield)
    elif message_id == Command.REQUEST:
      index, begin, length = struct.unpack('>III', message_body)
      piece = Piece(index, begin, length)
      log.debug('Peer [%s] requested %s' % (self._id, piece))
      if piece in self._session.filemanager:
        log.debug('   => we have %s, initiating send.' % piece)
        yield gen.Task(self.send, index, begin, length)
      else:
        log.debug('   => do not have %s, ignoring.' % piece)
    elif message_id == Command.PIECE:
      index, begin = struct.unpack('>II', message[0:8])
      piece = Piece(index, begin, len(message[8:]), message[8:])
      log.debug('Received %s from [%s]' % (piece, self._id))
      yield gen.Task(self._session.filemanager.write, piece)
      self._in.sent(len(message) - 8)
    elif message_id == Command.CANCEL:
      index, begin, length = struct.unpack('>III', message_body)
      log.debug('Peer [%s] canceling %s' % (self._id, Piece(index, begin, length)))
      self._in.cancel_request(Piece(index, begin, length))
    else:
      raise Peer.BadMessage('Unknown message id: %s' % message_id)
    if callback:
      self._io_loop.add(callback)

  def disconnect(self):
    log.debug('Disconnecting from [%s]' % self._id)
    if self._iostream:
      self._iostream.close()
    self._active = False
