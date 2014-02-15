import hashlib
import time

from .bandwidth import Bandwidth
from .bitfield import Bitfield
from .fileset import Piece, Request
from .peer_driver import PeerDriver

from tornado import gen
from twitter.common.quantity import Time, Amount


class ConnectionState(object):
  BW_COLLECTION_INTERVAL = Amount(30, Time.SECONDS)
  MIN_KEEPALIVE_WINDOW = Amount(2, Time.MINUTES)
  MAX_KEEPALIVE_WINDOW = Amount(4, Time.MINUTES)

  def __init__(self):
    self._last_alive = time.time()
    self._interested = False
    self._choked = True
    self._sent = 0
    self._bandwidth = Bandwidth(window=self.BW_COLLECTION_INTERVAL)

  def updates_keepalive(fn):
    def wrapper(self, *args, **kw):
      self._last_alive = time.time()
      return fn(self, *args, **kw)
    return wrapper

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
  def sent(self, num_bytes):
    self._sent += num_bytes
    self._bandwidth.add(num_bytes)

  del updates_keepalive


class Peer(PeerDriver):
  """Peer."""

  class Error(Exception): pass
  class BadMessage(Error): pass
  class PeerInactive(Error): pass

  def __init__(self, peer_id, iostream, piece_broker, logger=lambda msg: False):
    self._id = peer_id
    self._active = False
    self._piece_broker = piece_broker
    self._bitfield = Bitfield(piece_broker.num_pieces)  # remote bitfield
    self._in = ConnectionState()
    self._out = ConnectionState()
    self._iostream = iostream
    self._request_queue = []
    self._bitfield_callbacks = []
    self._receive_callbacks = []
    self._logger = logger
    super(Peer, self).__init__()

  @property
  def active(self):
    return self._active

  @property
  def iostream(self):
    if self._iostream is None:
      raise self.PeerInactive('Trying to send/recv on inactive peer %s' % self)
    return self._iostream

  def __str__(self):
    return 'Peer(%s)' % self._id

  @property
  def remote_bitfield(self):
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
  def am_choking(self):
    return self._in.choked

  @property
  def peer_choking(self):
    return self._out.choked

  @property
  def am_interested(self):
    return self._in.interested

  @property
  def peer_interested(self):
    return self._out.interested

  @property
  def is_healthy(self):
    return self._in.healthy

  # -- register callbacks
  def register_bitfield_change(self, callback):
    self._bitfield_callbacks.append(callback)

  def register_piece_receipt(self, callback):
    self._receive_callbacks.append(callback)

  def _invoke_bitfield_change(self, haves, have_nots):
    for callback in self._bitfield_callbacks:
      callback(haves, have_nots)

  def _invoke_piece_receipt(self, piece, full):
    for callback in self._receive_callbacks:
      callback(piece, full)

  # -- runner
  @gen.coroutine
  def start(self):
    self._active = True
    self._logger('Sending bitfield to [%s]' % self._id)
    yield self.send_bitfield(self._piece_broker.bitfield)
    while self._active:
      self._logger('Running recv from [%s]' % self._id)
      yield self.recv()
      self._logger('Ran recv from [%s]' % self._id)

  def stop(self):
    self._logger('Disconnecting from [%s]' % self._id)
    self._active = False
    if self._iostream:
      self._iostream.close()
      self._iostream = None

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
    if self._in.interested:  # already interested
      return
    self._in.interested = True
    yield super(Peer, self).send_interested()

  @gen.coroutine
  def send_not_interested(self):
    if not self._in.interested:  # already not interested
      return
    self._in.interested = False
    yield super(Peer, self).send_interested()

  @gen.coroutine
  def send_next_piece(self):
    if not self._request_queue:
      raise gen.Return(None)
    if self.am_choking or not self.peer_interested:
      self._logger('Skipping send to [%s] (am_choking:%s, peer_interested:%s)' % (
          self._id, self.am_choking, self.peer_interested))
      raise gen.Return(None)
    piece = self._request_queue.pop(0)
    self._logger('Sending piece %s' % piece)
    yield self.send_piece(piece)
    raise gen.Return(piece)

  @gen.coroutine
  def send_all_pieces(self):
    if self.am_choking or not self.peer_interested or not self._request_queue:
      return
    all_requests, self._request_queue = self._request_queue, []
    yield [self.send_piece(request) for request in all_requests]

  @gen.coroutine
  def send_piece(self, piece):
    if self.am_choking or not self.peer_interested:
      self._logger('Skipping send of %s to [%s] (am_choking:%s, peer_interested:%s)' % (
          piece, self._id, self.am_choking, self.peer_interested))
      return

    # In case the piece is actually an unpopulated request, populate.
    if isinstance(piece, Request):
      piece = Piece(piece.index, piece.offset, piece.length,
                    (yield gen.Task(self._piece_broker.read, piece)))

    yield super(Peer, self).send_piece(piece)
    self._out.sent(piece.length)

  # --- PeerDriver impls
  # TODO(wickman) update_keepalive should be implemented on this end, not on ConnectionState?
  @gen.coroutine
  def keepalive(self):
    self._in.ping()

  @gen.coroutine
  def choke(self):
    self._logger('Peer [%s] choking us.' % self._id)
    self._in.ping()
    self._out.choked = True

  @gen.coroutine
  def unchoke(self):
    self._logger('Peer [%s] unchoking us.' % self._id)
    self._in.ping()
    self._out.choked = False

  @gen.coroutine
  def interested(self):
    self._logger('Peer [%s] interested.' % self._id)
    self._out.interested = True

  @gen.coroutine
  def not_interested(self):
    self._logger('Peer [%s] uninterested.' % self._id)
    self._out.interested = False

  @gen.coroutine
  def have(self, index):
    self._bitfield[index] = True
    self._in.ping()
    self._logger('Peer [%s] has piece %s.' % (self._id, index))
    self._invoke_bitfield_change([index], [])

  @gen.coroutine
  def bitfield(self, bitfield):
    # Technically we should never encounter have_nots -- the bitfield ought
    # to be the first message that we get before any haves.
    self._in.ping()
    haves, have_nots = [], []
    for index, (old, new) in enumerate(zip(self._bitfield, bitfield)):
      if old and not new:
        have_nots.append(index)
      if new and not old:
        haves.append(index)
    self._bitfield = bitfield
    self._logger('Peer [%s] now has %d/%d pieces.' % (
        self._id,
        sum((self._bitfield[k] for k in range(len(self._bitfield))), 0),
        len(self._bitfield)))
    self._invoke_bitfield_change(haves, have_nots)

  @gen.coroutine
  def request(self, request):
    self._logger('Peer [%s] requested %s' % (self._id, request))
    self._in.ping()
    if self._piece_broker.covers(request):
      self._logger('   => we have %s, queueing request.' % request)
      self._request_queue.append(request)
    else:
      self._logger('   => do not have %s, ignoring.' % request)

  @gen.coroutine
  def cancel(self, request):
    self._logger('Peer [%s] canceling %s' % (self._id, request))
    self._request_queue = [r for r in self._request_queue if r != request]
    self._in.ping()

  @gen.coroutine
  def piece(self, piece):
    self._logger('Received %s from [%s]' % (piece, self._id))
    self._in.sent(piece.length)
    self._invoke_piece_receipt(piece, (yield gen.Task(self._piece_broker.write, piece)))
