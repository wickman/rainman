import datetime
import random

from .peer import Peer
from .piece_set import PieceSet
from .time_decay_map import TimeDecayMap

from tornado import gen
import tornado.ioloop
from twitter.common import log
from twitter.common.quantity import Amount, Data, Time


class Session(object):
  MAINTENANCE_INTERVAL = Amount(200, Time.MILLISECONDS)
  LOGGING_INTERVAL = Amount(1, Time.SECONDS)

  def __init__(self, piece_broker, io_loop=None):
    self.io_loop = io_loop or tornado.ioloop.IOLoop()
    self._peers = {}  # peer_id => Peer
    self._dead = []
    self._piece_broker = piece_broker
    self._logging_timer = None
    self._queued_receipts = []

  # ---- properties
  @property
  def peer_ids(self):
    return self._peers.keys()

  @property
  def peers(self):
    return self._peers.values()

  @property
  def scheduler(self):
    return self._scheduler

  @property
  def piece_broker(self):
    return self._piece_broker

  @property
  def bitfield(self):
    return self.piece_broker.bitfield

  @property
  def downloaded_bytes(self):
    return sum(peer.ingress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._peers.values()))

  @property
  def uploaded_bytes(self):
    return sum(peer.egress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._peers.values()))

  @property
  def remaining_bytes(self):
    return self._piece_broker.left

  # ----- incoming peers from client
  def add_peer(self, peer):
    log.info('Adding peer: %s' % peer.id)
    if peer.id in self._peers:
      raise ValueError('Peer already a member of this session.')
    # add the peer but let the activation be controlled externally
    self._peers[peer.id] = peer
    peer.register_piece_receipt(
        lambda piece, complete: self._queued_receipts.append((piece, complete)))

  # ----- maintenance
  def _activate_new_peers(self):
    for peer_id, peer in self._peers.items():
      if not peer.active:
        peer.activate()
        self.io_loop.add_callback(peer.start)

  def _filter_dead_peers(self):
    dead = set()
    for peer_id, peer in self._peers.items():
      if peer and (peer.iostream.closed() or not peer.healthy):
        log.debug('Garbage collecting peer [%s]' % peer_id)
        dead.add(peer_id)
    for peer_id in dead:
      log.debug('Popping peer %s in filter_dead_connections' % repr(peer_id))
      peer = self._peers.pop(peer_id)
      peer.disconnect()
      self._dead.append(peer_id)

  @gen.engine
  def _broadcast_new_pieces(self):
    receipts = []
    for receipt in self._queued_receipts:
      for peer in self._peers.values():
        receipts.append(gen.Task(peer.send_have, receipt))
    self._queued_receipts = []
    yield receipts

  @gen.engine
  def _ping_peers_if_necessary(self):
    yield [gen.Task(peer.send_keepalive) for peer in self._peers.values()]

  def maintenance(self):
    self._activate_new_peers()
    self._filter_dead_peers()
    self._broadcast_new_pieces()
    self._ping_peers_if_necessary()
    self._io_loop.add_timeout(
        self.MAINTENANCE_INTERVAL.as_(Time.MILLISECONDS),
        self.maintenance)

  def periodic_logging(self):
    for peer in filter(None, self._connections.values()):
      log.info('   = Peer [%s] in: %.1f Mbps  out: %.1f Mbps' % (peer.id,
         peer.ingress_bandwidth.bandwidth * 8. / 1048576,
         peer.egress_bandwidth.bandwidth * 8. / 1048576))

  def start(self):
    self._io_loop.add_callback(self.maintenance)
    self._logging_timer = tornado.ioloop.PeriodicCallback(self.periodic_logging,
        self.LOGGING_INTERVAL.as_(Time.MILLISECONDS), self._io_loop)
    self._logging_timer.start()
    self._io_loop.add_callback(self.schedule)

  def stop(self):
    self._logging_timer.stop()
    self._active = False
