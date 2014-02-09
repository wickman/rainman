import array
import datetime
import errno
import functools
import hashlib
import random
import socket
import struct
import tempfile
import urllib

from .bitfield import Bitfield
from .codec import BDecoder
from .filemanager import FileManager
from .peer import Peer
from .scheduler import Scheduler

import tornado.ioloop
from tornado import httpclient
from tornado.netutil import TCPServer
from tornado.iostream import IOStream
from twitter.common import log
from twitter.common.quantity import Amount, Time


class PieceSet(object):
  RARE_THRESHOLD = 50

  def __init__(self, length):
    self._num_owners = array.array('B', [0] * length)
    self._changed_pieces = 0
    self._rarest = None

  def have(self, index, value=True):
    self._changed_pieces += 1
    self._num_owners[index] += int(value)

  def add(self, bitfield):
    for k in range(len(bitfield)):
      if bitfield[k]:
        self.have(k)

  def remove(self, bitfield):
    for k in range(len(bitfield)):
      if bitfield[k]:
        self.have(k, False)

  # TODO(wickman) Fix this if it proves to be too slow.
  def rarest(self, count=None, owned=None):
    if not self._rarest or self._changed_pieces >= PieceSet.RARE_THRESHOLD:
      def exists_but_not_owned(pair):
        index, count = pair
        return count > 0 and (not owned or not owned[index])
      self._rarest = sorted(filter(exists_but_not_owned, enumerate(self._num_owners)),
                            key=lambda val: val[1])
      self._changed_pieces = 0
    return [val[0] for val in (self._rarest[:count] if count else self._rarest)]


class Session(object):
  PEER_RETRY_INTERVAL  = Amount(10, Time.SECONDS)
  MAINTENANCE_INTERVAL = Amount(200, Time.MILLISECONDS)
  LOGGING_INTERVAL     = Amount(1, Time.SECONDS)

  def __init__(self, torrent, chroot=None, port=None, io_loop=None):
    self._torrent = torrent
    self._port = port
    self._peers = None     # PeerSet
    self._listener = None  # PeerListener
    self._connections = {} # address => Peer
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
  def add_peer(self, address, iostream=None):
    log.info('Adding peer: %s (%s)' % (address, 'inbound' if iostream else 'outbound'))
    if address in self._connections:
      log.debug('  - already seen peer, skipping.')
      if iostream:
        iostream.close()
      return
    log.debug('Setting self._connections[%s] = None' % repr(address))
    self._connections[address] = None
    new_peer = Peer(address, self, functools.partial(self._add_peer_cb, address))
    if iostream is None:
      iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
      iostream.connect(address)
    success = yield gen.Task(new_peer.handshake, iostream)
    yield gen.Task(self.validate_peer, new_peer.address, new_peer, succeeded)

  @gen.coroutine
  def validate_peer(self, address, peer, succeeded):
    log.debug('Add peer callback: %s:%s [peer value: %s]' % (address[0], address[1], peer))
    if succeeded:
      log.info('Session [%s] added peer %s:%s [%s]' % (self._peer_id, address[0], address[1],
          peer.id))
      # TODO(wickman) Check to see if the connected_peer is still connected, and replace if
      # it has expired.
      for connected_peer in self._connections.values():
        if connected_peer and peer.id == connected_peer.id:
          log.info('Session already established with %s, disconnecting new one.' % peer.id)
          peer.disconnect()
          break
      else:
        yield gen.Task(peer.send_bitfield, self._bitfield)
        log.debug('Setting self._connections[%s] = %s' % (repr(address), peer))
        self._connections[address] = peer
        log.info('Starting ioloop for %s' % peer)
        self._io_loop.add_callback(peer.run)
    else:
      log.debug('Session [%s] failed to negotiate with %s:%s' % (self._peer_id,
        address[0], address[1]))
      def expire_retry():
        log.debug('Expiring retry timer for %s:%s' % address)
        if address not in self._connections or self._connections[address] is not None:
          log.error('Unexpected connection state for %s!' % address)
        else:
          log.debug('Popping self._connections[%s]' % repr(address))
          self._connections.pop(address, None)
      self._io_loop.add_timeout(
          datetime.timedelta(0, self.PEER_RETRY_INTERVAL.as_(Time.SECONDS)),
          expire_retry)

  def maintenance(self):
    def add_new_connections():
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
            peer.send_have(k)
          self._bitfield[k] = True

    def ping_peers_if_necessary():
      for peer in filter(None, self._connections.values()):
        peer.send_keepalive()  # only sends if necessary

    add_new_connections()
    filter_dead_connections()
    broadcast_new_pieces()
    ping_peers_if_necessary()
    
    self._io_loop.add_timeout(
        Session.MAINTENANCE_INTERVAL.as_(Time.MILLISECONDS),
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
        Session.LOGGING_INTERVAL.as_(Time.MILLISECONDS), self._io_loop)
    self._logging_timer.start()
    self._scheduler.schedule()
    self._io_loop.start()

  def stop(self):
    self._listener.stop()
    self._logging_timer.start()
    self._filemanager.stop()
    self._scheduler.stop()
    self._io_loop.stop()
