import array
import datetime
import errno
import functools
import hashlib
import socket
import struct
import tempfile
import urllib

from twitter.common import log
from twitter.common.quantity import Amount, Time

import tornado.ioloop
from tornado import httpclient
from tornado.netutil import TCPServer
from tornado.iostream import IOStream

from .bitfield import Bitfield
from .codec import BDecoder
from .fileset import FileManager, Piece
from .peer import Peer, PeerId


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


class PeerSet(object):
  # Requiring only read-only access to the session, manages the set of peers
  # with which we can establish sessions.
  def __init__(self, session, io_loop=None):
    self._session = session
    self._tracker = session.torrent.announce
    self._io_loop = io_loop or session.io_loop or tornado.ioloop.IOLoop.instance()
    self._http_client = httpclient.AsyncHTTPClient(io_loop=self._io_loop)
    self._peerset = set() # This breaks my golden rule of abstraction.
    self._peermap = {}    #
    self._io_loop.add_callback(self.start)
    self._handle = None

  def request(self):
    session = self._session
    return {
      'info_hash': hashlib.sha1(session.torrent.info.raw()).digest(),
      'peer_id': session.peer_id,
      'ip': socket.gethostbyname(socket.gethostname()),  # TODO: how to get external IP?
      'port': session.port,
      'uploaded': session.uploaded_bytes,
      'downloaded': session.downloaded_bytes,
      'left': session.torrent.info.length - session.assembled_bytes,
    }

  def start(self):
    if not self._handle:
      log.debug('Starting tracker query.')
      self.enqueue_request(event='started')

  def stop(self):
    if self._handle:
      log.debug('Stopping tracker query.')
      self._io_loop.remove_timeout(self._handle)
      self._handle = None

  def enqueue_request(self, event=None):
    request = self.request()
    if event:
      request.update(event=event)
    url = '%s?%s' % (self._tracker, urllib.urlencode(request))
    log.debug('Sending tracker request: %s' % url)
    self._http_client.fetch(url, self.handle_response)
    log.debug('Tracker request sent')

  def iter_peers(self, peers):
    def iterate():
      if isinstance(peers, (tuple, list)):
        for peer in peers:
          yield (peer['ip'], peer['port'])
      elif isinstance(peers, str):
        for offset in range(0, len(peers), 6):
          ip = peers[offset:offset+4]
          port = peers[offset+4:offset+6]
          yield ('%d.%d.%d.%d' % struct.unpack('>BBBB', ip), struct.unpack('>H', port)[0])
    me = (socket.gethostbyname(socket.gethostname()), self._session.port)
    return (pair for pair in iterate() if pair != me)

  def handle_response(self, response):
    interval = 60
    if response.error:
      log.error('PeerSet failed to query %s' % self._tracker)
    else:
      try:
        response = BDecoder.decode(response.body)[0]
        log.debug('Raw response: %s' % response)
        interval = response.get('interval', 60)
        peers = response.get('peers', [])
        log.debug('Accepted peer list:')
        for peer in self.iter_peers(peers):
          if peer not in self._peermap:
            log.debug('  %s:%s' % (peer[0], peer[1]))
            self.add(peer[0], peer[1])
      except BDecoder.Error:
        log.error('Malformed tracker response.')
      except AssertionError:
        log.error('Malformed peer dictionary.')
    log.debug('Enqueueing next tracker request for %s seconds from now.' % interval)
    self._handle = self._io_loop.add_timeout(datetime.timedelta(0, interval), self.enqueue_request)

  def __iter__(self):
    return iter(self._peerset)

  def __contains__(self, peer):
    assert isinstance(peer, tuple) and len(peer) == 2
    return peer in self._peermap or peer in self._peerset

  def add(self, address, port):
    rs = socket.getaddrinfo(address, port, socket.AF_INET, socket.SOCK_STREAM, socket.SOL_TCP)
    log.debug('Adding resolved peers: %s' % ', '.join('%s:%s' % result[-1] for result in rs))
    self._peermap[(address, port)] = set(result[-1] for result in rs)
    self._peerset.update(result[-1] for result in rs)


class PeerListener(TCPServer):
  class BindError(Exception): pass

  PORT_RANGE = range(6881, 6890)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  def __init__(self, handler, io_loop=None, port=None):
    self._handler = handler
    super(PeerListener, self).__init__(io_loop=io_loop)
    port_range = [port] if port else PeerListener.PORT_RANGE
    for port in port_range:
      try:
        log.debug('Peer listener attempting to bind @ %s' % port)
        self.listen(port)
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise PeerListener.BindError('Could not bind to any port in range %s' % repr(port_range))
    log.debug('Bound at port %s' % port)
    self._port = port

  @property
  def port(self):
    return self._port

  def handle_stream(self, iostream, address):
    self._handler(address, iostream)




class Session(object):
  # Retry values
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
    self._maintenance_timer = None
    self._logging_timer = None
    self._chroot = chroot or tempfile.mkdtemp()
    self._filemanager = FileManager.from_session(self)
    self._bitfield = Bitfield(torrent.info.num_pieces)
    for k in range(torrent.info.num_pieces):
      self._bitfield[k] = self._filemanager.have(k)
    log.debug('Torrent says number of pieces is: %s' % self._torrent.info.num_pieces)
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

  def add_peer(self, address, iostream=None):
    log.info('Adding peer: %s (%s)' % (address, 'inbound' if iostream else 'outbound'))
    if address in self._connections:
      log.debug('  - already seen peer, skipping.')
      if iostream: iostream.close()
      return
    log.debug('Setting self._connections[%s] = None' % repr(address))
    self._connections[address] = None
    new_peer = Peer(address, self, functools.partial(self._add_peer_cb, address))
    if iostream is None:
      iostream = IOStream(socket.socket(socket.AF_INET, socket.SOCK_STREAM), io_loop=self.io_loop)
      iostream.connect(address)
    new_peer.handshake(iostream)

  def _add_peer_cb(self, address, peer, succeeded):
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
        peer.send_bitfield(self._bitfield)
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
      self._io_loop.add_timeout(datetime.timedelta(0, self.PEER_RETRY_INTERVAL.as_(Time.SECONDS)),
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
    
  def periodic_logging(self):
    for peer in filter(None, self._connections.values()):
      log.info('   = Peer [%s] in: %.1f Mbps  out: %.1f Mbps' % (peer.id,
         peer.ingress_bandwidth.bandwidth * 8. / 1048576,
         peer.egress_bandwidth.bandwidth * 8. / 1048576))

  def start(self):
    self._listener = PeerListener(self.add_peer, io_loop=self._io_loop, port=self._port)
    self._port = self._listener.port
    self._peers = PeerSet(self)
    self._maintenance_timer = tornado.ioloop.PeriodicCallback(self.maintenance,
        Session.MAINTENANCE_INTERVAL.as_(Time.MILLISECONDS), self._io_loop)
    self._logging_timer = tornado.ioloop.PeriodicCallback(self.periodic_logging,
        Session.LOGGING_INTERVAL.as_(Time.MILLISECONDS), self._io_loop)
    self._maintenance_timer.start()
    self._logging_timer.start()
    self._scheduler.schedule()
    self._io_loop.start()

  def stop(self):
    self._listener.stop()
    self._maintenance_timer.stop()
    self._logging_timer.start()
    self._filemanager.stop()
    self._scheduler.stop()
    self._io_loop.stop()
