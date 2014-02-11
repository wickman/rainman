import tempfile

from .bitfield import Bitfield
from .piece_set import PieceSet
from .peer import Peer
from .peer_id import PeerId
from .peer_listener import PeerListener
from .peer_set import PeerSet
from .piece_manager import PieceManager
from .scheduler import Scheduler

from tornado import gen
import tornado.ioloop
from twitter.common import log
from twitter.common.quantity import Amount, Time


class Session(object):
  PEER_RETRY_INTERVAL = Amount(10, Time.SECONDS)
  MAINTENANCE_INTERVAL = Amount(200, Time.MILLISECONDS)
  LOGGING_INTERVAL = Amount(1, Time.SECONDS)

  def __init__(self, piece_broker, io_loop=None):
    self._connections = {}  # peer_id => Peer
    self._dead = []
    self._piece_broker = piece_broker
    self._io_loop = io_loop or tornado.ioloop.IOLoop()
    self._logging_timer = None
    self._scheduler = Scheduler(self)
    self._queued_receipts = []

  # ---- properties
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
  def io_loop(self):
    return self._io_loop

  @property
  def downloaded_bytes(self):
    return sum(peer.ingress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._connections.values()))

  @property
  def uploaded_bytes(self):
    return sum(peer.egress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._connections.values()))

  @property
  def assembled_bytes(self):
    return self._filemanager.assembled_size

  # ----- mutations
  def add_peer(self, peer_id, iostream):
    log.info('Adding peer: %s' % peer_id)
    if peer_id in self._connections:
      log.debug('  - already seen peer %s, skipping.' % peer_id)
      iostream.close()
      return
    # add the peer but let the activation be controlled externally
    self._connections[peer_id] = new_peer = Peer(peer_id, iostream, self._piece_broker)
    new_peer.register_piece_receipt(self._queued_receipts.append)

  @gen.engine
  def maintenance(self):
    def add_new_connections():
      # XXX need to cap the number of connections.  push peer set logic into client, and have it
      # decide which peers to add.
      for peer_id, peer in self._connections.items():
        if not peer.active:
          peer.activate()
          self.io_loop.add_callback(peer.start)

    def filter_dead_connections():
      dead = set()
      for peer_id, peer in self._peers.items():
        if peer and (peer.iostream.closed() or not peer.healthy):
          log.debug('Garbage collecting peer [%s]' % peer_id)
          dead.add(peer_id)
      for peer_id in dead:
        log.debug('Popping peer %s in filter_dead_connections' % repr(peer_id))
        peer = self._peers.pop(address)
        peer.disconnect()
        self._dead.append(peer_id)

    def broadcast_new_pieces():
      for k in range(len(self._bitfield)):
        if self._filemanager.have(k) and not self._bitfield[k]:
          # we completed a new piece
          log.debug('Completed piece %s' % k)
          for peer in filter(None, self._connections.values()):
            # XXX sync
            peer.send_have(k)
          self._bitfield[k] = True

    def ping_peers_if_necessary():
      for peer in filter(None, self._connections.values()):
        # XXX sync
        peer.send_keepalive()  # only sends if necessary

    add_new_connections()
    filter_dead_connections()
    broadcast_new_pieces()
    ping_peers_if_necessary()

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

  def stop(self):
    self._logging_timer.stop()
