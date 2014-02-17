import functools
import math
import random

from .bounded_map import BoundedDecayingMap
from .peer import Peer
from .peer_tracker import PeerTracker

from tornado import gen
from tornado.ioloop import PeriodicCallback
import toro
from twitter.common import log
from twitter.common.quantity import Amount, Data, Time


class Scheduler(object):
  """Piece/connection scheduler for clients.

     BEP-003

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

     4 steps of the scheduler:

     allocate_connections
     manage_chokes
     queue_requests
     flush_queues
  """

  MAX_CONNECTIONS = 20
  MAX_REQUESTS = 64
  MIN_REQUESTS = 32
  REQUEST_SIZE = Amount(64, Data.KB)
  DEFAULT_BANDWIDTH_PER_SECOND = Amount(20, Data.MB)
  CONNECTION_REFRESH_INTERVAL = Amount(5, Time.SECONDS)
  CHOKE_INTERVAL = Amount(10, Time.SECONDS)

  def __init__(self,
               client,
               request_size=REQUEST_SIZE,
               min_requests=MIN_REQUESTS,
               max_requests=MAX_REQUESTS,
               max_connections=MAX_CONNECTIONS,
               target_bandwidth=DEFAULT_BANDWIDTH_PER_SECOND):
    self.io_loop = client.io_loop
    self.client = client
    self.client.set_peer_callback(self.peer_callback)
    self.active = False
    self.requests = BoundedDecayingMap(self.io_loop, concurrency=max_requests)
    self.sent_requests = BoundedDecayingMap(self.io_loop, concurrency=min_requests)
    self._pieces_available = toro.Condition(self.io_loop)

    # scheduling constants
    self._request_size = int(request_size.as_(Data.BYTES))

  @property
  def num_connections(self):
    return sum(len(session.peer_ids) for session in self.client.sessions)

  def piece_receipt_callback(self, torrent, piece, finished):
    # TODO(wickman) if finished, remove all pieces with this index
    self.sent_requests.remove((torrent.handshake_prefix, piece.to_request()))

  def notify_new_pieces(self, torrent, haves, _):
    # If we've received any new pieces, wake up consumers.
    session = self.client.get_session(torrent)
    new_pieces = sum(not session.bitfield[index] for index in haves)
    if new_pieces:
      log.debug('[%s] %d pieces have become available for torrent.' % (
          self.client.peer_id, new_pieces))
      self._pieces_available.notify_all()

  def rate_limit_torrent(self, torrent, peer_id):
    return False

  def peer_callback(self, torrent, peer_id, iostream):
    session = self.client.get_session(torrent)
    if peer_id in session.peer_ids or self.rate_limit_torrent(torrent, peer_id):
      log.debug('[%s] dropping incoming peer %s' % (self.client.peer_id, peer_id))
      iostream.close()
      return
    peer = Peer(peer_id, iostream, self.client.get_broker(torrent), logger=self.client.log)
    peer.register_piece_receipt(functools.partial(self.piece_receipt_callback, torrent))
    log.debug('[%s] adding peer %s' % (self.client.peer_id, peer_id))
    session.add_peer(peer)
    # register bitfield change callback after session's
    peer.register_bitfield_change(functools.partial(self.notify_new_pieces, torrent))

  def _allocate_connections(self):
    connections = []
    total_connections = sum(len(session.peer_ids) for session in self.client.sessions)
    if total_connections >= self.MAX_CONNECTIONS:
      return connections
    quota = self.MAX_CONNECTIONS - total_connections
    sessions = [(torrent, session, session.remaining_bytes)
                for (torrent, session) in self.client.iter_sessions()
                if session.remaining_bytes]
    sessions.sort(key=lambda val: val[2], reverse=True)  # sort sessions by remaining
    aggregate_bytes = sum(val[2] for val in sessions)  # aggregate total
    sessions = [(torrent, session, int(math.ceil(1. * quota * remaining / aggregate_bytes)))
                for (torrent, session, remaining) in sessions]  # normalize
    for torrent, session, allocated in sessions:
      for k in range(allocated):
        if total_connections >= self.MAX_CONNECTIONS:
          return connections
        try:
          peer_id, address = self.client.get_tracker(torrent).get_random(
              exclude=[self.client.peer_id])
        except PeerTracker.EmptySet:
          break  # continue onto next torrent
        if peer_id == self.client.peer_id:  # do not connection to self
          continue
        if peer_id not in session.peer_ids and (torrent, address) not in connections:
          connections.append((torrent, address))
        else:
          # TODO(wickman) Does it make sense to find another?
          continue
    return connections

  def allocate_connections(self):
    """Allocate the connections for this torrent client.

       Algorithm:
         Find the registered torrents with the largest remaining number of bytes
         and normalize by the total number of outstanding bytes.  Allocate connections
         proportionally until we've hit our connection limit.

       Does not disconnect any leechers, also does not provisionally connect to peers
       of finished torrents.
    """
    log.debug('[%s] allocating connections' % self.client.peer_id)
    connections = self._allocate_connections()
    log.debug('[%s] allocated %d connections.' % (self.client.peer_id, len(connections)))
    for torrent, address in connections:
      self.io_loop.add_callback(self.client.initiate_connection, torrent, address)

  def manage_chokes(self):
    log.debug('[%s] managing chokes.' % self.client.peer_id)
    # TODO(wickman) Make this smarter -- for now just unchoke everybody.
    for session in self.client.sessions:
      for peer in session.peers:
        log.debug('[%s] peer [%s] status: am_choking:%s am_interested:%s peer_choking:%s peer_interested:%s' % (
              self.client.peer_id,
              peer,
              peer.am_choking,
              peer.am_interested,
              peer.peer_choking,
              peer.peer_interested))
        if peer.am_choking and peer.peer_interested:
          log.debug('[%s] unchoking interested peer %s' % (self.client.peer_id, peer.id))
          self.io_loop.add_callback(peer.send_unchoke)

  @gen.coroutine
  def flush_requests(self):
    """
      Inspect the current aggregate ingress and egress bandwidth.

      Schedule ingress proportionally to egress performance and vice versa.
    """
    log.debug('[%s] flushing queues' % self.client.peer_id)

    # TODO(wickman) Make this smarter, for now just always flush queues but only if
    # we're not choked.
    while self.active:
      try:
        key, peer = self.requests.pop_random()
      except BoundedDecayingMap.Empty:
        log.debug('[%s] No requests available, deferring.' % self.client.peer_id)
        yield gen.Task(self.io_loop.add_timeout, self.io_loop.time() + 0.1)
        continue

      handshake_prefix, block = key
      yield self.sent_requests.add(key, peer)

      log.debug('[%s] sending request %s to peer [%s].' % (
          self.client.peer_id, block, peer))
      yield peer.send_request(block)

  @gen.coroutine
  def flush_receipts(self):
    """
      Inspect the current aggregate ingress and egress bandwidth.

      Schedule ingress proportionally to egress performance and vice versa.
    """
    log.debug('[%s] flushing receipts' % self.client.peer_id)

    # TODO(wickman) Make this smarter, for now just always flush queues but only if
    # we're not choked.
    while self.active:
      for session in self.client.sessions:
        for peer in session.peers:
          while True:
            log.debug('[%s] trying to send to %s.' % (self.client.peer_id, peer))
            piece = (yield peer.send_next_piece())
            if not piece:
              break
            log.debug('[%s] sent piece %s to peer [%s].' % (self.client.peer_id, piece, peer))
      yield gen.Task(self.io_loop.add_timeout, self.io_loop.time() + 0.1)

  @gen.coroutine
  def rarest_pieces(self, count=20):
    """Return the top #count rarest pieces across all torrents."""

    def iter_rarest():
      for torrent, session in self.client.iter_sessions():
        for index in session.piece_set.rarest(session.bitfield)[:count]:
          yield (torrent, index)

    while self.active:
      rarest = list(iter_rarest())[:count]
      if not rarest:
        log.debug('[%s] has no pieces to request, waiting.' % self.client.peer_id)
        try:
          yield self._pieces_available.wait()
          log.debug('[%s] now has pieces available.' % self.client.peer_id)
          continue
        except toro.Timeout:
          log.debug('[%s] had no pieces available within timeout, retrying.' % self.client.peer_id)
          continue
      random.shuffle(rarest)
      raise gen.Return(rarest)

  def owners(self, torrent, index):
    for peer in self.client.get_session(torrent).peers:
      if peer.remote_bitfield[index]:
        yield peer

  @gen.coroutine
  def queue_requests(self):
    """Queue requests for most rare pieces across all sessions.

       Side effects:
         self.requests is populated with requests to send.

         'interested' bits are set on peers who have choked us who contain
         these rare pieces.
    """
    while self.active:
      # XXX if we are choked by everyone, we need to wait until unchokes have come before
      # we spinloop here.
      log.debug('[%s] queueing requests.' % self.client.peer_id)

      for torrent, index in (yield self.rarest_pieces()):
        # find owners of this piece that are not choking us or for whom we've not yet registered
        # intent to download
        owners = list(self.owners(torrent, index))
        if not owners:
          log.debug('[%s] found no owners for %s[%d]' % (self.client.peer_id, torrent, index))
          continue

        request_size = self._request_size
        broker = self.client.get_broker(torrent)

        for block in broker.iter_blocks(index, request_size):
          key = (torrent.handshake_prefix, block)
          random_peer = random.choice(owners)

          # There is technically a race condition between broker.iter_blocks and
          # requests/sent_requests -- it's possible for the bits to be flushed to disk
          # and the sent_request to be popped.  so we really really cared, we could
          # doublecheck "if block not in broker.sliceset" as well.
          if self.requests.contains(key, random_peer) or (
              self.sent_requests.contains(key, random_peer)):
            log.debug('[%s] already queued: %s' % (self.client.peer_id, block))
            continue

          if random_peer.peer_choking:
            if not random_peer.am_interested:
              log.debug('[%s] want to request %s from %s but we are choked, setting interested.' % (
                  self.client.peer_id, block, random_peer.id))
              yield gen.Task(random_peer.send_interested)

              owners.remove(random_peer)  # remove this peer since we're choked
              if not owners:
                log.debug('[%s] No more owners of this block, breaking.' % self.client.peer_id)
                break
            continue

          log.debug('[%s] requests %s %s' % (
              self.client.peer_id,
              'has' if self.requests.contains(key, random_peer) else 'does not have',
              block))
          log.debug('[%s] sent_requests %s %s' % (
              self.client.peer_id,
              'has' if self.sent_requests.contains(key, random_peer) else 'does not have',
              block))

          # TODO(wickman) Consider deferring the sem.acquire to before the piece selection
          # in case there is a large delay and the scheduling choice would otherwise change.
          yield self.requests.add(key, random_peer)
          log.debug('[%s] requesting %s from peer [%s].' % (self.client.peer_id, block, random_peer))

      # yield to somebody else
      yield gen.Task(self.io_loop.add_timeout, self.io_loop.time() + 0.1)

  def start(self):
    log.debug('[%s] scheduler starting.' % self.client.peer_id)
    self.active = True

    self._allocate_connections_timer = PeriodicCallback(
        self.allocate_connections,
        self.CONNECTION_REFRESH_INTERVAL.as_(Time.MILLISECONDS),
        self.io_loop)
    self._allocate_connections_timer.start()

    self._manage_chokes_timer = PeriodicCallback(
        self.manage_chokes,
        self.CHOKE_INTERVAL.as_(Time.MILLISECONDS),
        self.io_loop)
    self._manage_chokes_timer.start()

    self.io_loop.add_callback(self.queue_requests)
    self.io_loop.add_callback(self.flush_requests)
    self.io_loop.add_callback(self.flush_receipts)

  def stop(self):
    assert self.active
    self.active = False
    self._allocate_connections_timer.stop()
    self._manage_chokes_timer.stop()
