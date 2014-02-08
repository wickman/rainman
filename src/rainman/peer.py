import hashlib
import struct
import time

from .bandwidth import Bandwidth
from .bitfield import Bitfield
from .fileset import Piece, Request
from .metainfo import MetaInfo

from tornado import gen
from twitter.common import log
from twitter.common.quantity import Time, Amount


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
    # XXX(PY3)
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
    return self.is_set(self._reserved_bits, self.EXTENSION_BITS['DHT'])

  @property
  def fast_peers(self):
    return self.is_set(self._reserved_bits, self.EXTENSION_BITS['Fast peers'])

  @property
  def extended(self):
    return self.is_set(self._reserved_bits, self.EXTENSION_BITS['Extension protocol'])


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

  def updates_keepalive(fn):
    def wrapper(self, *args, **kw):
      self._last_alive = time.time()
      return fn(self, *args, **kw)
    return wrapper

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

  @property
  def choked(self):
    return self._choked

  @choked.setter
  @updates_keepalive
  def choked(self, value):
    self._choked = bool(value)

  @updates_keepalive
  def ping(self):
    pass

  @property
  def interested(self):
    return self._interested

  @interested.setter
  @updates_keepalive
  def interested(self, value):
    self._interested = bool(value)

  @updates_keepalive
  def cancel_request(self, piece):
    self._queue = [pc for pc in self._queue if pc != piece]

  @updates_keepalive
  def sent(self, num_bytes):
    self._sent += num_bytes
    self._bandwidth.add(num_bytes)

  del updates_keepalive


# TODO(wickman) Integrate with wire.py
class Peer(object):
  class Error(Exception): pass
  class BadMessage(Exception): pass

  def __init__(self, address, session):
    self._id = None
    self._active = True
    self._address = address
    self._session = session
    self._hash = hashlib.sha1(session.torrent.info.raw()).digest()
    self._iostream = None
    self._bitfield = Bitfield(session.torrent.info.num_pieces)
    self._in = ConnectionState()
    self._out = ConnectionState()

  def __str__(self):
    return 'Peer(%s iostream:%s)' % (self._id, self._iostream)

  @property
  def bitfield(self):
    return self._bitfield

  @property
  def egress_bytes(self):
    return self._out._sent

  @property
  def ingress_bytes(self):
    return self._in._sent

  @property
  def ingress_bandwidth(self):
    return self._in._bandwidth

  @property
  def egress_bandwidth(self):
    return self._out._bandwidth

  @property
  def id(self):
    return self._id

  @property
  def address(self):
    return self._address

  @gen.coroutine
  def handshake(self, iostream):
    log.debug('Session [%s] starting handshake with %s:%s' % (
        self._session.peer_id,
        self._address[0],
        self._address[1]))
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
      log.debug('Session [%s] got bad handshake with %s:%s: %s' % (self._session.peer_id,
        self._address[0], self._address[1], e))
      iostream.close()

    log.debug('Session [%s] finishing handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

    yield gen.Return(succeeded)

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
      self._recv_dispatch(message_id, message_body[1:])

  @property
  def choked(self):
    return self._out.choked

  @property
  def interested(self):
    return self._out.interested

  @interested.setter
  def interested(self, value):
    value = bool(value)
    if value == self._out.interested:
      return
    self._out.interested = value
    log.debug('Sending %sinterested to [%s]' % ('' if value else 'un', self._id))
    # XXX(sync)
    self._iostream.write(Command.wire(Command.INTERESTED) if self._out.interested
                    else Command.wire(Command.NOT_INTERESTED))

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

  def send_cancel(self, request):
    log.debug('Sending cancel %s to %s' % (request, self))
    self._iostream.write(Command.wire(Command.CANCEL, request))

  def send_request(self, request):
    log.debug('Sending request %s to %s' % (request, self))
    self._iostream.write(Command.wire(Command.REQUEST, request))

  def choke(self):
    self._in.choked = True
    log.debug('Sending choke to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.CHOKE))

  def unchoke(self):
    self._in.choked = False
    log.debug('Sending unchoke to [%s]' % self._id)
    self._iostream.write(Command.wire(Command.UNCHOKE))

  @gen.engine
  def send_piece(self, piece, callback=None):
    if not self._out._interested or self._out._choked:
      log.debug('Skipping send of %s to [%s] (interested:%s, choked:%s)' % (
          piece, self._id, self._out._interested, self._out._choked))
      return

    # In case the piece is actually an unpopulated request, populate.
    if isinstance(piece, request):
      piece = Piece(request.index, request.offset, request.length,
                    (yield gen.Task(self._session.filemanager.read, request)))

    yield gen.Task(self._iostream.write, Command.wire(Command.PIECE, piece))
    self._out.sent(length)
    if callback:
      self._session.io_loop.add_callback(callback)

  # --- peer channel impls ---
  def choke(self):
    self._out.choked = True

  def unchoke(self):
    self._out.choked = False

  def interested(self):
    self._in.interested = True

  def not_interested(self):
    self._in.interested = False

  def have(self, index):
    self._bitfield[index] = True
    self._session.pieces.have(piece)

  def bitfield(self, bitfield):
    # ???
    self._session.pieces.remove(self._bitfield)
    self._bitfield.fill(message_body)
    log.debug('Peer [%s] has %d/%d pieces.' % (
        self._id, sum((self._bitfield[k] for k in range(len(self._bitfield))), 0),
        len(self._bitfield)))
    self._session.pieces.add(self._bitfield)

  def request(self, request):
    # ???
    log.debug('Peer [%s] requested %s' % (self._id, request))
    if self._session.filemanager.covers(request):
      log.debug('   => we have %s, initiating send.' % request)
      yield gen.Task(self.send_piece, request)
    else:
      log.debug('   => do not have %s, ignoring.' % request)

  def cancel(self, request):
    self._in.cancel_request(request)

  @gen.engine
  def piece(self, piece):
    self._session.scheduler.received(piece, from_peer=self)
    yield gen.Task(self._session.filemanager.write, piece)
    self._in.sent(len(message_body) - 8)

  @gen.engine
  def _recv_dispatch(self, message_id, message_body, callback=None):
    # TODO(wickman): Split these into recv_ messages
    if message_id == Command.CHOKE:
      log.debug('Peer [%s] choking us.' % self._id)
      self._out.choked = True
    elif message_id == Command.UNCHOKE:
      log.debug('Peer [%s] unchoking us.' % self._id)
      self._out.choked = False
    elif message_id == Command.INTERESTED:
      log.debug('Peer [%s] interested.' % self._id)
      self._in.interested = True
    elif message_id == Command.NOT_INTERESTED:
      log.debug('Peer [%s] uninterested.' % self._id)
      self._in.interested = False
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
      request = Request(index, begin, length)
      log.debug('Peer [%s] requested %s' % (self._id, request))
      if self._session.filemanager.covers(request):
        log.debug('   => we have %s, initiating send.' % request)
        yield gen.Task(self.send_piece, request)
      else:
        log.debug('   => do not have %s, ignoring.' % request)
    elif message_id == Command.PIECE:
      index, begin = struct.unpack('>II', message_body[0:8])
      piece = Piece(index, begin, len(message_body[8:]), message_body[8:])
      log.debug('Received %s from [%s]' % (piece, self._id))
      self._session.scheduler.received(piece, from_peer=self)
      yield gen.Task(self._session.filemanager.write, piece)
      self._in.sent(len(message_body) - 8)
    elif message_id == Command.CANCEL:
      index, begin, length = struct.unpack('>III', message_body)
      request = Request(index, begin, length)
      log.debug('Peer [%s] canceling %s' % (self._id, request))
      self._in.cancel_request(request)
    else:
      raise Peer.BadMessage('Unknown message id: %s' % message_id)
    if callback:
      self._session.io_loop.add(callback)

  def disconnect(self):
    log.debug('Disconnecting from [%s]' % self._id)
    if self._iostream:
      self._iostream.close()
    self._active = False
