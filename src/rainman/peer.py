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
    return now - self._last_alive < self.MAX_KEEPALIVE_WINDOW.as_(Time.SECONDS)

  @property
  def needs_ping(self):
    now = time.time()
    return now - self._last_alive >= self.MIN_KEEPALIVE_WINDOW.as_(Time.SECONDS)

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


class Peer(Wire):
  """Peer state
  
  Requires:
    session.torrent
    session.peer_id
    session.filemanager
    session.pieces
    session.scheduler
  """

  class Error(Exception): pass
  class BadMessage(Error): pass
  class PeerInactive(Error): pass

  def __init__(self, address, session):
    self._id = None
    self._active = True
    self._address = address
    self._session = session
    self._hash = hashlib.sha1(session.torrent.info.raw()).digest()
    self._bitfield = Bitfield(session.torrent.info.num_pieces)
    self._in = ConnectionState()
    self._out = ConnectionState()
    self._iostream = None
    super(Peer, self).__init__()
  
  @property
  def iostream(self):
    if self._iostream is None:
      raise self.PeerInactive('Trying to send/recv on inactive peer %s' % self)
    return self._iostream

  def __str__(self):
    return 'Peer(%s)' % self._id

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

  @property
  def is_choked(self):
    return self._out.choked

  @property
  def is_interested(self):
    return self._out.interested

  @property
  def is_healthy(self):
    return self._in.healthy

  @gen.coroutine
  def handshake(self, iostream):
    log.debug('Session [%s] starting handshake with %s:%s' % (
        self._session.peer_id,
        self._address[0],
        self._address[1]))

    handshake_full = PeerHandshake.make(self._session.torrent.info, self._session.peer_id)
    read_handshake, _ = yield [
        gen.Task(iostream.read_bytes, PeerHandshake.LENGTH),
        gen.Task(iostream.write, handshake_full)]

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == self._hash:
        self._id = handshake.peer_id
        self._wire = Wire(self, iostream)
        succeeded = True
      else:
        log.debug('Session [%s] got mismatched torrent hashes.' % self._session.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Session [%s] got bad handshake with %s:%s: %s' % (self._session.peer_id,
        self._address[0], self._address[1], e))
      iostream.close()

    log.debug('Session [%s] finishing handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

    raise gen.Return(succeeded)

  # -- runner
  @gen.engine
  def run(self):
    while self._active:
      yield self._wire.recv()
  
  #---- sends
  @gen.coroutine
  def send_keepalive(self):
    if self._out.needs_ping:
      self._out.ping()
      yield super(Peer, self).send_keepalive()

  def send_choke(self):
    self._in.choked = True
    return super(Peer, self).send_choke()

  def send_unchoke(self):
    self._in.choked = False
    return super(Peer, self).send_unchoke()

  @gen.coroutine
  def send_interested(self):
    if self._out.interested:  # already interested
      return
    self._out.interested = True
    yield super(Peer, self).send_interested()

  @gen.coroutine
  def send_not_interested(self):
    if not self._out.interested:  # already interested
      return
    self._out.interested = False
    yield super(Peer, self).send_interested()

  @gen.coroutine
  def send_piece(self, piece, callback=None):
    if not self._out.interested or self._out.choked:
      log.debug('Skipping send of %s to [%s] (interested:%s, choked:%s)' % (
          piece, self._id, self._out.interested, self._out.choked))
      return

    # In case the piece is actually an unpopulated request, populate.
    if isinstance(piece, request):
      piece = Piece(request.index, request.offset, request.length,
                    (yield gen.Task(self._session.filemanager.read, request)))

    yield gen.Task(self._iostream.write, Command.wire(Command.PIECE, piece))
    self._out.sent(piece.length)

  # --- Wire impls
  @gen.coroutine
  def keepalive(self):
    self._in.ping()

  @gen.coroutine
  def choke(self):
    log.debug('Peer [%s] choking us.' % self._id)
    self._out.choked = True

  @gen.coroutine
  def unchoke(self):
    log.debug('Peer [%s] unchoking us.' % self._id)
    self._out.choked = False

  @gen.coroutine
  def interested(self):
    log.debug('Peer [%s] interested.' % self._id)
    self._in.interested = True

  @gen.coroutine
  def not_interested(self):
    log.debug('Peer [%s] uninterested.' % self._id)
    self._in.interested = False

  @gen.coroutine
  def have(self, index):
    self._bitfield[index] = True
    log.debug('Peer [%s] has piece %s.' % (self._id, index))
    self._session.pieces.have(index)

  @gen.coroutine
  def bitfield(self, bitfield):
    self._session.pieces.remove(self._bitfield)
    self._bitfield = bitfield
    log.debug('Peer [%s] has %d/%d pieces.' % (
        self._id, sum((self._bitfield[k] for k in range(len(self._bitfield))), 0),
        len(self._bitfield)))
    self._session.pieces.add(self._bitfield)

  @gen.coroutine
  def request(self, request):
    log.debug('Peer [%s] requested %s' % (self._id, request))
    if self._session.filemanager.covers(request):
      log.debug('   => we have %s, initiating send.' % request)
      yield self.send_piece(request)
    else:
      log.debug('   => do not have %s, ignoring.' % request)

  @gen.coroutine
  def cancel(self, request):
    index, begin, length = struct.unpack('>III', message_body)
    request = Request(index, begin, length)
    log.debug('Peer [%s] canceling %s' % (self._id, request))
    self._in.cancel_request(request)

  @gen.coroutine
  def piece(self, piece):
    log.debug('Received %s from [%s]' % (piece, self._id))
    self._session.scheduler.received(piece, from_peer=self)
    yield gen.Task(self._session.filemanager.write, piece)
    self._in.sent(len(message_body) - 8)

  def disconnect(self):
    log.debug('Disconnecting from [%s]' % self._id)
    if self._iostream:
      self._iostream.close()
      self._iostream = None
    self._active = False
