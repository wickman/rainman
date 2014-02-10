import tempfile

from .bitfield import Bitfield
from .filemanager import FileManager
from .piece_set import PieceSet
from .peer import Peer
from .peer_id import PeerId
from .peer_listener import PeerListener
from .peer_set import PeerSet
from .scheduler import Scheduler

from tornado import gen
import tornado.ioloop
from twitter.common import log
from twitter.common.quantity import Amount, Time


class Session(object):
  PEER_RETRY_INTERVAL = Amount(10, Time.SECONDS)
  MAINTENANCE_INTERVAL = Amount(200, Time.MILLISECONDS)
  LOGGING_INTERVAL = Amount(1, Time.SECONDS)

  def __init__(self, torrent, chroot=None, port=None, io_loop=None):
    self._torrent = torrent
    self._port = port
    self._peers = None  # PeerSet: candidate peers
    self._listener = None  # PeerListener
    self._connections = {}  # address => Peer
    self._dead = []
    self._peer_id = None
    self._io_loop = io_loop or tornado.ioloop.IOLoop()  # singleton by default instead?
    self._logging_timer = None
    self._chroot = chroot or tempfile.mkdtemp()
    self._filemanager = FileManager.from_torrent(
        self._torrent, chroot=self._chroot, io_loop=self._io_loop)
    self._bitfield = Bitfield(torrent.info.num_pieces)  # this should be on the PieceManager
    for k in range(torrent.info.num_pieces):
      self._bitfield[k] = self._filemanager.have(k)
    self._pieces = PieceSet(self._torrent.info.num_pieces)
    self._scheduler = Scheduler(self)

  # ---- properties
  @property
  def scheduler(self):
    return self._scheduler

  @property
  def port(self):
    return self._port

  @property
  def chroot(self):
    return self._chroot

  @property
  def peer_id(self):
    # XXX
    if self._peer_id is None:
      self._peer_id = PeerId.generate()
      log.info('Started session %s' % self._peer_id)
    return self._peer_id

  @property
  def torrent(self):
    return self._torrent

  @property
  def io_loop(self):
    return self._io_loop

  # This is getting ridiculous
  @property
  def filemanager(self):
    return self._filemanager

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

  @property
  def pieces(self):
    return self._pieces

  def has(self, index):
    return self._bitfield[index]

  @property
  def bitfield(self):
    return self._bitfield

  def owners(self, index):
    return [peer for peer in filter(None, self._connections.values()) if peer.bitfield[index]]

  # ----- mutations
  @gen.coroutine
  def add_peer(self, address, iostream):
    log.info('Adding peer: %s' % address)
    if address in self._connections:
      log.debug('  - already seen peer, skipping.')
      iostream.close()
      return
    new_peer = Peer(address, iostream, self._torrent, self._filemanager)
    yield new_peer.send_handshake(self.peer_id)
    self._connections[address] = new_peer
    self._scheduler.add_peer(new_peer)
    self.io_loop.add_callback(new_peer.run)

  def maintenance(self):
    def add_new_connections():
      # XXX need to cap the number of connections.  push peer set logic into client, and have it
      # decide which peers to add.
      for address in self._peers:
        if address not in self._connections:
          self.add_peer(address)

    def filter_dead_connections():
      dead = set()
      for address, peer in self._connections.items():
        if peer and (peer._iostream.closed() or not peer.healthy):
          log.debug('Garbage collecting peer [%s]' % peer.id)
          dead.add(address)
      for address in dead:
        log.debug('Popping self._connections[%s] in filter_dead_connections' % repr(address))
        peer = self._connections.pop(address)
        peer.disconnect()
        self._dead.append(peer)

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
    self._listener = PeerListener(self.add_peer, io_loop=self._io_loop, port=self._port)
    self._port = self._listener.port
    self._peers = PeerSet(self)
    self._io_loop.add_callback(self.maintenance)
    self._logging_timer = tornado.ioloop.PeriodicCallback(self.periodic_logging,
        self.LOGGING_INTERVAL.as_(Time.MILLISECONDS), self._io_loop)
    self._logging_timer.start()
    self._scheduler.schedule()
    self._io_loop.start()

  def stop(self):
    self._listener.stop()
    self._logging_timer.start()
    self._filemanager.stop()
    self._scheduler.stop()
    self._io_loop.stop()
