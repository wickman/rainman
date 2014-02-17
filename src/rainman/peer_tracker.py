import datetime
import hashlib
import random
import socket
import struct
import urllib  # XXX PY3

from .codec import BDecoder

import tornado
from tornado import httpclient
from twitter.common import log
from twitter.common.lang import Compatibility

if Compatibility.PY3:
  import urllib.parse as urlparse
else:
  import urlparse


class PeerTracker(set):
  """Base class for PeerTracker.  Implements a set of addresses."""
  class Error(Exception): pass
  class UnknownScheme(Error): pass
  class EmptySet(Error): pass

  _REGISTRY = {}

  @classmethod
  def get(cls, torrent, client):
    if torrent.announce is None:
      return EmptyPeerTracker()
    fullurl = urlparse.urlparse(torrent.announce)
    if fullurl.scheme not in cls._REGISTRY:
      raise cls.UnknownScheme('Unknown announcer scheme: %s' % fullurl.scheme)
    tracker_impl = cls._REGISTRY[fullurl.scheme]
    return tracker_impl(torrent, client)

  @classmethod
  def register(cls, scheme, impl):
    if not issubclass(impl, cls):
      raise ValueError('%s.register expects instances of %s' % (
          cls.__name__, cls.__name__))
    cls._REGISTRY[scheme] = impl

  def start(self):
    pass

  def stop(self):
    pass

  def get_random(self, exclude=None):
    """Get a random peer excluding anything in the exclude set."""
    if not self:  # if no elements
      raise self.EmptySet('No peers available to allocate.')
    exclude = exclude or frozenset()
    return random.choice([address for address in self if address not in exclude])

  def __init__(self, *args, **kw):
    pass


class EmptyPeerTracker(PeerTracker):
  pass


PeerTracker.register('testing', EmptyPeerTracker)


class StaticPeerTracker(PeerTracker):
  def __init__(self, torrent, *_):
    fullurl = urlparse.urlparse(torrent.announce)
    assert fullurl.scheme in ('', 'file')
    self._filename = fullurl.path

  def start(self):
    with open(self._filename) as fp:
      for line in fp:
        try:
          _, host, port = line.strip().split()
          port = int(port)
        except ValueError:
          log.debug('StaticPeerTracker got bad line: %s' % line)
          continue
        self.add((host, port))

  def stop(self):
    self.clear()


PeerTracker.register('', StaticPeerTracker)
PeerTracker.register('file', StaticPeerTracker)


class ZookeeperPeerTracker(object):
  pass


class HttpPeerTracker(PeerTracker):
  """A set of Peers with whom connections may be established.

     In practice this periodically refreshes from a tracker.
  """

  @classmethod
  def resolve(cls, address, port):
    rs = socket.getaddrinfo(address, port, socket.AF_INET, socket.SOCK_STREAM, socket.SOL_TCP)
    log.debug('Adding resolved peers: %s' % ', '.join('%s:%s' % result[-1] for result in rs))
    return frozenset(result[-1] for result in rs)

  # Requiring only read-only access to the session, manages the set of peers
  # with which we can establish sessions.
  def __init__(self, torrent, client):
    self._peer_id = client.peer_id
    self._port = client.port
    self._ip = socket.gethostbyname(socket.gethostname()),  # TODO: is this the best way?
    self._torrent = torrent
    self._session = client.get_session(torrent)
    self._tracker = torrent.announce
    self._io_loop = session.io_loop
    self._http_client = httpclient.AsyncHTTPClient(io_loop=self._io_loop)
    self._valueset = set()
    self._io_loop.add_callback(self.start)
    self._handle = None

  def request(self):
    session = self._session
    return {
      'info_hash': hashlib.sha1(torrent.info.raw()).digest(),
      'peer_id': self._peer_id,
      'ip': self._ip,
      'port': self._port,
      'uploaded': session.uploaded_bytes,
      'downloaded': session.downloaded_bytes,
      'left': self._torrent.info.length - session.assembled_bytes,
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
          ip = peers[offset:offset + 4]
          port = peers[offset + 4:offset + 6]
          yield ('%d.%d.%d.%d' % struct.unpack('>BBBB', ip), struct.unpack('>H', port)[0])
    return [pair for pair in iterate() if pair != (self._ip, self._port)]

  def handle_response(self, response):
    new_peer_set = set()
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
          if peer not in self:
            log.debug('  %s:%s' % (peer[0], peer[1]))
            new_peer_set.update(self.resolve(peer[0], peer[1]))
      except BDecoder.Error:
        log.error('Malformed tracker response.')
      except AssertionError:
        log.error('Malformed peer dictionary.')
    log.debug('Enqueueing next tracker request for %s seconds from now.' % interval)
    if new_peer_set:
      self.clear()
      self.update(new_peer_set)
    self._handle = self._io_loop.add_timeout(datetime.timedelta(0, interval), self.enqueue_request)
