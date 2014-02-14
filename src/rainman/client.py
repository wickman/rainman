from binascii import hexlify
from collections import namedtuple
import errno
import functools
import hashlib
import math
import os
import socket

from .handshake import PeerHandshake
from .piece_broker import PieceBroker
from .piece_set import PieceSet
from .peer import Peer
from .peer_tracker import PeerTracker
from .time_decay_map import TimeDecayMap
from .session import Session

from tornado import gen
from tornado.iostream import IOStream
from tornado.tcpserver import TCPServer
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
        log.debug('Peer listener attempting to bind @ %s' % port)
        super(Client, self).listen(port)
        self._port = port
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise self.BindError('Could not bind to any port in range %s' % repr(self.PORT_RANGE))
    log.debug('Client bound to port %d' % self._port)

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
        log.debug('Mismatched torrent hashes with %s' % handshake.peer_id)
    except PeerHandshake.InvalidHandshake as e:
      log.debug('Got bad handshake: %s' % e)
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

  @gen.engine
  def maybe_add_peer(self, handshake_task, torrent, iostream, session, callback=None):
    peer_id = yield handshake_task
    if peer_id in session.peer_ids:
      iostream.close()
      callback(None)
      return
    if peer_id:
      self._peer_callback(torrent, peer_id, iostream)
      callback(peer_id)
    else:
      self._failed_handshakes += 1
      iostream.close()
      callback(None)

  @gen.engine
  def initiate_connection(self, torrent, address, callback=None):
    torrent, session = self.establish_session(torrent.handshake_prefix)
    iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
    yield gen.Task(iostream.connect, address)
    handshake_task = gen.Task(self.handshake, torrent, iostream, session)
    callback((yield gen.Task(self.maybe_add_peer, handshake_task, torrent, iostream, session)))

  @gen.engine
  def handle_stream(self, iostream, address):
    log.debug('Client got incoming connection from %s' % (address,))
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
  MAX_REQUESTS = 100
  REQUEST_SIZE = Amount(64, Data.KB)
  DEFAULT_BANDWIDTH_PER_SECOND = Amount(20, Data.MB)
  CONNECTION_REFRESH_INTERVAL = Amount(5, Time.SECONDS)

  def __init__(self,
               client,
               request_size=REQUEST_SIZE,
               max_requests=MAX_REQUESTS,
               max_connections=MAX_CONNECTIONS,
               target_bandwidth=DEFAULT_BANDWIDTH_PER_SECOND):
    self.client = client
    self.client.set_peer_callback(self.peer_callback)
    self.pieces = {}
    self.active = False
    self.requests = TimeDecayMap()
    self.sent_requests = TimeDecayMap()

    # scheduling constants
    self._request_size = request_size.as_(Data.BYTES)

  @property
  def num_connections(self):
    return sum(len(session.peer_ids) for session in self.client.sessions)

  def piece_receipt_callback(self, torrent, piece, finished):
    # TODO(wickman) if finished, remove all pieces with this index
    self.requests[(torrent.handshake_prefix, piece)].remove(piece)

  def bitfield_change_callback(self, torrent, haves, have_nots):
    piece_set = self.pieces[torrent.handshake_prefix]
    for have in haves:
      piece_set.have(have)
    for have_not in have_nots:
      piece_set.have_not(have_not, False)

  def peer_callback(self, torrent, peer_id, iostream):
    session = self.client.get_session(torrent)
    if peer_id in session.peer_ids:
      iostream.close()
      return
    if torrent.handshake_prefix not in self.pieces:
      self.pieces[torrent.handshake_prefix] = PieceSet(torrent.info.num_pieces)
    peer = Peer(peer_id, iostream, self.client.get_broker(torrent))
    peer.register_piece_receipt(functools.partial(self.piece_receipt_callback, torrent))
    peer.register_bitfield_change(functools.partial(self.bitfield_change_callback, torrent))
    session.add_peer(peer)

  @gen.coroutine
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
          peer_id, address = self.client.get_tracker(torrent).get_random()
        except PeerTracker.EmptySet:
          break  # continue onto next torrent
        if peer_id == self.client.peer_id:  # do not connection to self
          continue
        if peer_id not in session.peer_ids and (torrent, address) not in connections:
          connections.append((torrent, address))
        else:
          # TODO(wickman) Does it make sense to find another?
          continue
    raise gen.Return(connections)

  @gen.coroutine
  def allocate_connections(self):
    """Allocate the connections for this torrent client.

       Algorithm:
         Find the registered torrents with the largest remaining number of bytes
         and normalize by the total number of outstanding bytes.  Allocate connections
         proportionally until we've hit our connection limit.

       Does not disconnect any leechers, also does not provisionally connect to peers
       of finished torrents.
    """
    connections = yield self._allocate_connections()
    yield [gen.Task(self.client.initiate_connection, torrent, address)
           for torrent, address in connections]
    if self.active:
      self.io_loop.add_timeout(self.CONNECTION_REFRESH_INTERVAL.as_(Time.SECONDS),
          self.allocate_connections)

  @gen.engine
  def start(self):
    self.active = True

    self.io_loop.add_callback(self.allocate_connections)
    self.io_loop.add_callback(self.manage_chokes)

    while self.active:
      yield self.send_requests()

  def stop(self):
    self.active = False

  def rarest_pieces(self, count=20):
    """Return the top #count rarest pieces across all torrents."""

    def iter_rarest():
      for handshake_prefix in self.pieces:
        torrent = self.client.get_torrent(handshake_prefix)
        session = self.client.get_session(torrent)
        for index in self.pieces[handshake_prefix].rarest(session.bitfield)[:count]:
          yield (torrent, index)

    rarest = list(iter_rarest())[:count]
    random.shuffle(rarest)
    return rarest

  def owners(self, torrent, index):
    for peer in self.client.get_session(torrent).peers:
      if peer.bitfield[index]:
        yield peer

  @gen.engine
  def flush_queues(self):
    """
      Inspect the current aggregate ingress and egress bandwidth.

      Schedule ingress proportionally to egress performance and vice versa.
    """
    MIN_OUTSTANDING_REQUESTS = 32  # - how do we enforce this?
    MAX_OUTSTANDING_REQUESTS = 64

    aggregate_ingress = sum(peer.ingress_bandwidth for peer in session.peers
                            for session in self.client.sessions)
    aggregate_egress = sum(peer.egress_bandwidth for peer in session.peers
                            for session in self.client.sessions)


  @gen.engine
  def manage_chokes(self):
    pass

  @gen.engine
  def queue_requests(self):
    """Queue requests for most rare pieces across all sessions.

       Side effects:
         self._requests is populated with requests to send.

         'interested' bits are set on peers who have choked us who contain
         these rare pieces.
    """
    # XXX derp this is wrong?
    to_queue = max(0, MAX_OUTSTANDING_REQUESTS - len(self.requests))

    interests = []

    for torrent, index in self.rarest_pieces():
      # find owners of this piece that are not choking us or for whom we've not yet registered
      # intent to download
      if to_queue <= 0:
        break

      owners = [peer for peer in self.owners(torrent, index)]
      if not owners:
        continue

      request_size = self._request_size
      broker = self.client.get_broker(torrent)

      for block in broker.iter_blocks(piece_index, request_size):
        key = (torrent.handshake_prefix, block)
        random_peer = random.choice(owners)
        if self.requests.contains(key, random_peer):
          continue
        if random_peer.local_choked:
          if not random_peer.local_interested:
            log.debug('Want to request %s from %s but we are choked, setting interested.' % (
                block, random_peer))
            interests.append(gen.Task(random_peer.send_interested))
            owners.remove(random_peer)  # remove this peer since we're choked
          continue
        log.debug('Scheduler requesting %s from peer [%s].' % (block, random_peer))
        self.requests.add(key, random_peer)

        to_queue -= 1
        if to_queue == 0:
          break

    yield interests
