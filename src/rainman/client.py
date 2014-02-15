from binascii import hexlify
from collections import namedtuple
import errno
import functools
import hashlib
import math
import os
import random
import socket

from .bounded_map import BoundedDecayingMap
from .handshake import PeerHandshake
from .peer import Peer
from .peer_tracker import PeerTracker
from .piece_broker import PieceBroker
from .session import Session
from .time_decay_map import TimeDecayMap

from tornado import gen
from tornado.ioloop import PeriodicCallback
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
import toro
from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_mkdtemp
from twitter.common.quantity import Amount, Data, Time


class Client(TCPServer):
  class Error(Exception): pass
  class BindError(Error): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, peer_id, chroot=None, io_loop=None, session_impl=Session):
    self.peer_id = peer_id
    self._ip = socket.gethostbyname(socket.gethostname())
    self._chroot = chroot or safe_mkdtemp()
    safe_mkdir(self._chroot)
    self._torrents = {}  # map from handshake prefix => Torrent
    self._trackers = {}  # map from handshake prefix => PeerTracker
    self._sessions = {}  # map from handshake prefix => Session
    self._piece_brokers = {}  # map from handshake prefix => PieceBroker
    self._failed_handshakes = 0
    self._port = None
    self._session_impl = session_impl
    self._peer_callback = self.default_peer_callback
    super(Client, self).__init__(io_loop=io_loop)

  def log(self, msg):
    log.debug('[%s]: %s' % (self.peer_id, msg))

  @property
  def port(self):
    return self._port

  @property
  def sessions(self):
    return self._sessions.values()

  def iter_sessions(self):
    for hash, session in self._sessions.items():
      yield (self._torrents[hash], session)

  def get_session(self, torrent):
    return self._sessions.get(torrent.handshake_prefix)

  def get_tracker(self, torrent):
    return self._trackers.get(torrent.handshake_prefix)

  def get_broker(self, torrent):
    return self._piece_brokers.get(torrent.handshake_prefix)

  def get_torrent(self, handshake_prefix):
    return self._torrents.get(handshake_prefix)

  # Override the default listener to find a port in the port range
  def listen(self):
    for port in self.PORT_RANGE:
      try:
        self.log('Peer listener attempting to bind @ %s' % port)
        super(Client, self).listen(port)
        self._port = port
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise self.BindError('Could not bind to any port in range %s' % repr(self.PORT_RANGE))
    self.log('Client bound to port %d' % self._port)

  # TODO(wickman) implement unregister_torrent
  def register_torrent(self, torrent, root=None, callback=None):
    """Register a torrent with this client.

       :param torrent: :class:`Torrent`
       :param root: The path that may or may not exist for this torrent.  If None, create one.
    """
    if torrent.handshake_prefix in self._torrents:
      # skip dual register.
      return
    target_dir = root or os.path.join(self._chroot, hexlify(torrent.handshake_prefix))
    self._piece_brokers[torrent.handshake_prefix] = piece_broker = PieceBroker.from_metainfo(
        torrent.info, chroot=target_dir, io_loop=self.io_loop)
    # TODO(wickman) this needs to be asynchronized via iopool
    piece_broker.initialize()
    self._torrents[torrent.handshake_prefix] = torrent
    self._sessions[torrent.handshake_prefix] = session = self._session_impl(
        piece_broker, io_loop=self.io_loop)
    # by default do not start the tracker
    tracker = self._trackers[torrent.handshake_prefix] = PeerTracker.get(
        torrent, self.peer_id, session)
    tracker.start()
    session.start()
    if callback:
      callback(session)

  def set_peer_callback(self, callback):
    """Set the callback to be invoked when new peer connections are requested.  By default
       the callback is set to one that always adds incoming peers to a torrent session.

       :param callback: A callback that will be invoked with two arguments: the
           :class:`Torrent` the peer is registering with, the peer_id, and the
           :class:`tornado.iostream.IOStream` established with the peer.
    """
    self._peer_callback = callback

  def default_peer_callback(self, torrent, peer_id, iostream):
    _, session = self.establish_session(torrent.handshake_prefix)
    peer = Peer(peer_id, iostream, self.get_broker(torrent))
    session.add_peer(peer)

  # --- handshake logic
  @gen.engine
  def handshake(self, torrent, iostream, session, prefix='', callback=None):
    """Complete the handshake given a session.

       Uses:
         session.torrent
         session.peer_id
    """
    write_handshake = PeerHandshake.make(torrent.info, self.peer_id)
    read_handshake, _ = yield [
        gen.Task(iostream.read_bytes, PeerHandshake.LENGTH - len(prefix)),
        gen.Task(iostream.write, write_handshake)]
    read_handshake = prefix + read_handshake

    succeeded = False
    try:
      handshake = PeerHandshake(read_handshake)
      if handshake.hash == torrent.hash:
        succeeded = True
      else:
        self.log('Mismatched torrent hashes with %s' % handshake.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      self.log('Got bad handshake: %s' % e)
      iostream.close()

    if callback:
      callback(handshake.peer_id if succeeded else None)

  def establish_session(self, handshake_prefix):
    torrent = self._torrents.get(handshake_prefix)
    if not torrent:
      raise self.Error('Cannot initiate a connection for an unknown torrent.')
    session = self._sessions.get(handshake_prefix)
    if not session:
      raise self.Error('No session associated with torrent!')
    return torrent, session

  @gen.coroutine
  def maybe_add_peer(self, handshake_task, torrent, iostream, session):
    peer_id = yield handshake_task
    if peer_id in session.peer_ids:
      iostream.close()
      raise gen.Return(None)
    if peer_id:
      self._peer_callback(torrent, peer_id, iostream)
      raise gen.Return(peer_id)
    else:
      self._failed_handshakes += 1
      iostream.close()
      raise gen.Return(None)

  @gen.coroutine
  def initiate_connection(self, torrent, address):
    torrent, session = self.establish_session(torrent.handshake_prefix)
    iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
    yield gen.Task(iostream.connect, address)
    handshake_task = gen.Task(self.handshake, torrent, iostream, session)
    raise gen.Return((yield gen.Task(
        self.maybe_add_peer, handshake_task, torrent, iostream, session)))

  @gen.coroutine
  def handle_stream(self, iostream, address):
    self.log('Client got incoming connection from %s' % (address,))
    handshake_prefix = yield gen.Task(iostream.read_bytes, PeerHandshake.PREFIX_LENGTH)
    try:
      torrent, session = self.establish_session(handshake_prefix)
    except self.Error as e:
      self._failed_handshakes += 1
      iostream.close()
      return
    handshake_task = gen.Task(self.handshake, torrent, iostream, session, prefix=handshake_prefix)
    yield gen.Task(self.maybe_add_peer, handshake_task, torrent, iostream, session)


class Scheduler(object):
  """A fair scheduler that tries to maintain even ingress/egress bandwidth across all
     torrents.

     The currently deployed choking algorithm avoids fibrillation by only
     changing who's choked once every ten seconds.  It does reciprocation and
     number of uploads capping by unchoking the four peers which it has the
     best download rates from and are interested.  Peers which have a better
     upload rate but aren't interested get unchoked and if they become
     interested the worst uploader gets choked.  If a downloader has a complete
     file, it uses its upload rate rather than its download rate to decide who
     to unchoke.

     fastest_downloaders = sorted(iter_peers(), key=lambda peer.egress_bandwidth)
     fastest_uploaders = sorted(iter_peers(), key=lambda peer.ingress_bandwidth)


     For optimistic unchoking, at any one time there is a single peer which is
     unchoked regardless of it's upload rate (if interested, it counts as one
     of the four allowed downloaders.) Which peer is optimistically unchoked
     rotates every 30 seconds.  To give them a decent chance of getting a
     complete piece to upload, new connections are three times as likely to
     start as the current optimistic unchoke as anywhere else in the rotation.

     choke_timeout = Amount(10, Time.SECONDS)
     max_unchoked_peers = 4
     max_connections = N



     def order_peers_by_age(peers):
       pass

     def peer_bandwidth_metric(session, peer):
       if session.remaining_size == 0:
         return session.egress_bandwidth
       else:
         return session.ingress_bandwidth

     def iter_interested_peers():
       for session in client.sessions:
         for peer in session.peers.values():
           if peer.remote_interested:
             yield (session, peer)

     peers_by_interest = sorted(iter_interested_peers, lambda key: peer_bandwidth_metric, reverse=True)
     peers_by_interest, below_cut = peers_by_interest[:max_unchoked_peers], peers_by_interest[max_unchoked_peers:]
     min_max_bandwidth = peer_bandwidth_metric(peers_by_interest[-1])
     peers_to_unchoke = [peer for _, peer in peers_by_interest] + [
                         peer for (session, peer) in iter_interested_peers()
                         if peer_bandwidth_metric(session, peer) > min_max_bandwidth]
     unchoke_requests = [gen.Task(peer.send_unchoke) for peer in peers_to_unchoke]
     # isn't quite right.
     # if below_cut:
     #   unchoke_requests.append(gen.Task(random.choice(below_cut).send_unchoke))

     def decide_chokes():
       pass

     #------

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
    self.sent_requests.remove((torrent.handshake_prefix, piece))

  def notify_new_pieces(self, torrent, haves, _):
    # If we've received any new pieces, wake up consumers.
    log.debug('[%s] notify_new_pieces(%s)' % (self.client.peer_id, hexlify(torrent.handshake_prefix)))
    session = self.client.get_session(torrent)
    new_pieces = sum(not session.bitfield[index] for index in haves)
    if new_pieces:
      log.debug('[%s] %d pieces have become available for torrent %s' % (
          self.client.peer_id, new_pieces, hexlify(torrent.handshake_prefix)))
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
    sessions = [(torrent, session, int(math.ceil(1. * remaining / aggregate_bytes)))
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
      try:
        for session in self.client.sessions:
          for peer in session.peers:
            while True:
              log.debug('[%s] trying to send to %s.' % (self.client.peer_id, peer))
              piece = (yield peer.send_next_piece())
              if not piece:
                break
              log.debug('[%s] sent piece %s to peer [%s].' % (self.client.peer_id, piece, peer))
        yield gen.Task(self.io_loop.add_timeout, self.io_loop.time() + 0.1)
      except Exception as e:
        import traceback
        log.error(traceback.format_exc())

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
          yield self._pieces_available.wait() # (deadline=1.0)  # add a deadline to prevent deadlock
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
          #log.debug('[%s] queueing %s from %s' % (self.client.peer_id, block, random_peer))

          if self.requests.contains(key, random_peer):
            log.debug('[%s] already queued: %s' % (self.client.peer_id, block))
            continue

          if random_peer.peer_choking:
            if not random_peer.am_interested:
              log.debug('[%s] Cannot request [%s] status: am_choking:%s am_interested:%s peer_choking:%s peer_interested:%s' % (
                self.client.peer_id,
                random_peer,
                random_peer.am_choking,
                random_peer.am_interested,
                random_peer.peer_choking,
                random_peer.peer_interested))
              log.debug('[%s] want to request %s from %s but we are choked, setting interested.' % (
                  self.client.peer_id, block, random_peer.id))
              yield gen.Task(random_peer.send_interested)

              owners.remove(random_peer)  # remove this peer since we're choked
              if not owners:
                log.debug('[%s] No more owners of this block, breaking.' % self.client.peer_id)
                break
            log.debug('[%s] Cannot request [%s] status: am_choking:%s am_interested:%s peer_choking:%s peer_interested:%s' % (
              self.client.peer_id,
              random_peer,
              random_peer.am_choking,
              random_peer.am_interested,
              random_peer.peer_choking,
              random_peer.peer_interested))
            continue

          # TODO(wickman) Consider deferring the sem.acquire to before the piece selection
          # in case there is a large delay and the scheduling choice would otherwise change.
          log.debug('[%s] requesting %s from peer [%s].' % (self.client.peer_id, block, random_peer))
          yield self.requests.add(key, random_peer)

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

    log.debug('[%s] Queued queue_requests.' % self.client.peer_id)
    self.io_loop.add_callback(self.queue_requests)
    log.debug('[%s] Queued flush_queues.' % self.client.peer_id)
    self.io_loop.add_callback(self.flush_requests)
    log.debug('[%s] Queued flush_receipts.' % self.client.peer_id)
    self.io_loop.add_callback(self.flush_receipts)
    log.debug('[%s] scheduler started.' % self.client.peer_id)

  def stop(self):
    assert self.active
    self.active = False
    self._allocate_connections_timer.stop()
    self._manage_chokes_timer.stop()
