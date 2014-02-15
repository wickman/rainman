import datetime
import functools
import random

from .peer import Peer
from .piece_set import PieceSet

from tornado import gen
import tornado.ioloop
from twitter.common import log
from twitter.common.quantity import Amount, Data, Time


class Session(object):
  MAINTENANCE_INTERVAL = Amount(200, Time.MILLISECONDS)
  LOGGING_INTERVAL = Amount(1, Time.SECONDS)

  def __init__(self, piece_broker, io_loop):
    self.io_loop = io_loop
    self._peers = {}  # peer_id => Peer
    self._dead = []
    self._logging_timer = None
    self._queued_receipts = []
    self._done_callbacks = []
    self.piece_broker = piece_broker
    self.piece_set = PieceSet(piece_broker.num_pieces)

  # ---- properties
  @property
  def peer_ids(self):
    return self._peers.keys()

  @property
  def peers(self):
    return self._peers.values()

  def get_peer(self, peer_id):
    return self._peers[peer_id]

  @property
  def bitfield(self):
    return self.piece_broker.bitfield

  @property
  def remaining_bytes(self):
    return self.piece_broker.left

  @property
  def downloaded_bytes(self):
    return sum(peer.ingress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._peers.values()))

  @property
  def uploaded_bytes(self):
    return sum(peer.egress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._peers.values()))

  # ----- incoming peers from client
  def register_done_callback(self, done_callback):
    log.debug('Setting done callback.')
    if self.remaining_bytes == 0:
      log.debug('Invoking done callback.')
      self.io_loop.append(done_callback)
      return
    log.debug('Appending done callback.')
    self._done_callbacks.append(done_callback)

  def _on_piece_receipt(self, piece, finished):
    self._queued_receipts.append((piece, finished))
    if self.remaining_bytes == 0:
      callbacks, self._done_callbacks = self._done_callbacks, []
      for callback in callbacks:
        self.io_loop.add_callback(callback)

  def add_peer(self, peer):
    log.debug('Session adding new peer %s' % peer)
    if peer.id in self._peers:
      log.error('Session cannot add peer %s!' % peer)
      raise ValueError('Peer already a member of this session.')
    self._peers[peer.id] = peer
    peer.register_piece_receipt(self._on_piece_receipt)
    peer.register_bitfield_change(functools.partial(self._update_piece_set, peer.id))
    self.io_loop.add_callback(peer.start)
    log.debug('Session added new peer %s' % peer)

  # ----- maintenance
  def _update_piece_set(self, peer_id, haves, have_nots):
    log.debug('Peer %s updating piece_set.' % peer_id)
    for have in haves:
      self.piece_set.add_one(have)
    for have_not in have_nots:
      self.piece_set.remove_one(have_nots)

  def _filter_dead_peers(self):
    dead = set()
    for peer_id, peer in self._peers.items():
      if peer and (peer.iostream.closed() or not peer.is_healthy):
        log.debug('Garbage collecting peer [%s]' % peer_id)
        dead.add(peer_id)
        self._piece_set.remove(peer.remote_bitfield)
    for peer_id in dead:
      log.debug('Popping peer %s in filter_dead_connections' % repr(peer_id))
      peer = self._peers.pop(peer_id)
      peer.disconnect()
      self._dead.append(peer_id)

  def _broadcast_new_pieces(self):
    receipts = []
    for piece, finished in self._queued_receipts:
      if finished:
        for peer in self._peers.values():
          self.io_loop.add_callback(peer.send_have, piece.index)
    self._queued_receipts = []

  def _ping_peers_if_necessary(self):
    for peer in self._peers.values():
      self.io_loop.add_callback(peer.send_keepalive)

  def maintenance(self):
    log.debug('Running maintenance.')
    self._filter_dead_peers()
    self._broadcast_new_pieces()
    self._ping_peers_if_necessary()
    self.io_loop.add_timeout(
        self.io_loop.time() + self.MAINTENANCE_INTERVAL.as_(Time.SECONDS),
        self.maintenance)

  def periodic_logging(self):
    for peer in self.peers:
      log.debug('   = Peer [%s] in: %.1f Mbps  out: %.1f Mbps' % (
         peer.id,
         peer.ingress_bandwidth.bandwidth * 8. / 1048576,
         peer.egress_bandwidth.bandwidth * 8. / 1048576))

  def start(self):
    log.debug('Starting session.')
    self.io_loop.add_callback(self.maintenance)
    self._logging_timer = tornado.ioloop.PeriodicCallback(
        self.periodic_logging,
        self.LOGGING_INTERVAL.as_(Time.MILLISECONDS),
        self.io_loop)
    self._logging_timer.start()

  def stop(self):
    self._logging_timer.stop()
    self._active = False
