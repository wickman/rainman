from .peer import Peer
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
    self._peers = {}  # peer_id => Peer
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
        peer.ingress_bytes for peer in filter(None, self._peers.values()))

  @property
  def uploaded_bytes(self):
    return sum(peer.egress_bytes for peer in self._dead) + sum(
        peer.ingress_bytes for peer in filter(None, self._peers.values()))

  @property
  def assembled_bytes(self):
    return self._filemanager.assembled_size

  # ----- mutations
  def add_peer(self, peer_id, iostream):
    log.info('Adding peer: %s' % peer_id)
    if peer_id in self._peers:
      log.debug('  - already seen peer %s, skipping.' % peer_id)
      iostream.close()
      return
    # add the peer but let the activation be controlled externally
    self._peers[peer_id] = new_peer = Peer(peer_id, iostream, self._piece_broker)
    new_peer.register_piece_receipt(self._queued_receipts.append)

  @gen.engine
  def maintenance(self):
    def add_new_peers():
      # TODO(wickman) need to cap the number of connections.  push peer set
      # logic into client/scheduler, and have it decide which peers to add.
      for peer_id, peer in self._peers.items():
        if not peer.active:
          peer.activate()
          self.io_loop.add_callback(peer.start)

    def filter_dead_peers():
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
          for peer in filter(None, self._peers.values()):
            # XXX sync
            peer.send_have(k)
          self._bitfield[k] = True

    def ping_peers_if_necessary():
      for peer in filter(None, self._peers.values()):
        # XXX sync
        peer.send_keepalive()  # only sends if necessary

    add_new_peers()
    filter_dead_peers()
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

  @gen.engine
  def schedule(self):
    while self._active:
      yield self.inner_schedule()
  
  # scheduling constants
  MAX_UNCHOKED_PEERS = 5
  MAX_PEERS = 50
  MAX_REQUESTS = 100
  CONSIDERED_PIECES = 20
  REQUEST_SIZE = Amount(16, Data.KB)
  OUTER_YIELD = datetime.timedelta(0, 0, Amount(10, Time.MILLISECONDS).as_(Time.MICROSECONDS))
  INNER_YIELD = datetime.timedelta(0, 0, Amount(1, Time.MILLISECONDS).as_(Time.MICROSECONDS))

  @gen.coroutine
  def inner_schedule(self):
    """Given a piece broker and set of peers, schedule ingress/egress appropriately.

      From BEP-003:

      The currently deployed choking algorithm avoids fibrillation by only
      changing who's choked once every ten seconds.  It does reciprocation and
      number of uploads capping by unchoking the four peers which it has the
      best download rates from and are interested.  Peers which have a better
      upload rate but aren't interested get unchoked and if they become
      interested the worst uploader gets choked.  If a downloader has a complete
      file, it uses its upload rate rather than its download rate to decide who
      to unchoke.

      For optimistic unchoking, at any one time there is a single peer which is
      unchoked regardless of it's upload rate (if interested, it counts as one
      of the four allowed downloaders.) Which peer is optimistically unchoked
      rotates every 30 seconds.  To give them a decent chance of getting a
      complete piece to upload, new connections are three times as likely to
      start as the current optimistic unchoke as anywhere else in the rotation.

      Uses:
        session.pieces        (TODO: pull this into scheduler and use callbacks)
        session.filemanager
        session.io_loop
        session.owners
    """
    session_bitfield = self._session.bitfield  # filemanager.bitfield
    # this should own pieces
    rarest = [index for index in self._session.pieces.rarest() if not session_bitfield[index]]
    rarest = rarest[:self.CONSIDERED_PIECES]
    random.shuffle(rarest)

    for piece_index in rarest:
      # find owners of this piece that are not choking us or for whom we've not yet registered
      # intent to download
      owners = [peer for peer in self._session.owners(piece_index)]
      if not owners:
        continue

      # don't bother scheduling unless there are unchoked peers or peers
      # we have not told we are interested
      if len([peer for peer in owners if not peer.choked or not peer.interested]) == 0:
        continue
      if self._requests.outstanding > self.MAX_REQUESTS:
        log.debug('Hit max requests, waiting.')
        yield gen.Task(self._session.io_loop.add_timeout, self.INNER_YIELD)

      # subpiece nomenclature is "block"
      request_size = self.REQUEST_SIZE.as_(Data.BYTES)
      for block in self._session.piece_broker.iter_blocks(piece_index, request_size):
        if block not in self._session.piece_broker and block not in self._requests:
          random_peer = random.choice(owners)
          if random_peer.choked:
            if not random_peer.interested:
              log.debug('Want to request %s from %s but we are choked, setting interested.' % (
                block, random_peer))
            random_peer.interested = True
            continue
          log.debug('Scheduler requesting %s from peer [%s].' % (block, random_peer))
          # XXX sync
          random_peer.send_request(block)
          self._requests.add(block, random_peer)

    yield gen.Task(self._session.io_loop.add_timeout, self.OUTER_YIELD)

  def received(self, piece, from_peer):
    if piece not in self._requests:
      log.error('Expected piece in request set, not there!')
    for peer in self._requests.remove(piece):
      if peer != from_peer:
        log.debug('Sending cancellation to peer [%s]' % peer.id)
        # XXX sync
        peer.send_cancel(piece)
