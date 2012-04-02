import datetime
import hashlib
import socket
import struct
import threading
import urllib

from twitter.common import log
import tornado.ioloop
from tornado import httpclient

from .bitfield import BitfieldPriorityQueue
from .codec import BDecoder
from .peer import PeerId

class Peer(object):
  def __init__(self):
    pass

class PeerSet(object):
  # Requiring only read-only access to the session, manages the set of peers
  # with which we can establish sessions.
  def __init__(self, session, io_loop=None):
    self._session = session
    self._tracker = session.torrent.announce
    self._io_loop = io_loop or session.io_loop or tornado.ioloop.IOLoop.instance()
    self._http_client = httpclient.AsyncHTTPClient(io_loop=self._io_loop)
    self._peers = {}
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

  @staticmethod
  def iter_peers(peers):
    if isinstance(peers, (tuple, list)):
      for peer in peers:
        yield (peer['ip'], peer['port'])
    elif isinstance(peers, str):
      for offset in range(0, len(peers), 6):
        ip = peers[offset:offset+4]
        port = peers[offset+4:offset+6]
        yield ('%d.%d.%d.%d' % struct.unpack('>BBBB', ip), struct.unpack('>H', port)[0])

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
        for peer in PeerSet.iter_peers(peers):
          if peer not in self._peers:
            log.debug('  %s:%s' % (peer[0], peer[1]))
            self._peers[peer] = Peer()
      except BDecoder.Error:
        log.error('Malformed tracker response.')
      except AssertionError:
        log.error('Malformed peer dictionary.')
    log.debug('Enqueueing next tracker request for %s seconds from now.' % interval)
    self._handle = self._io_loop.add_timeout(datetime.timedelta(0, interval), self.enqueue_request)

  def get(self, peer_id):
    """Return (ip, port) of peer with peer_id."""
    return self._peers.get(peer_id)


class Session(threading.Thread):
  """
    Initialized with:
      torrent

    Generates its own:
      peer id
      ioloop

    Start:
      - Bind to a port.
      - Register against tracker, requires:   |  PeerSet(tracker, Session.id)
         * peer_id                            |
         * port                               |
         * torrent                            |
      - Start listening.


    Collects the following:
      - set of peers
      - upload / download statistics
      - bitfield
      - peer bitfield priority queue

    Functionality:
      - the ability to spawn a peer for this torrent.
      - the ability to register a peer with a tracker.
      - locate peers via a tracker.
  """
  DEFAULT_RANGE = range(6181, 6190)

  def __init__(self, torrent, port=None):
    self._torrent = torrent
    self._port = port
    self._peers = None
    self._peer_id = None
    self._io_loop = tornado.ioloop.IOLoop()
    self._uploaded_bytes = 0
    self._downloaded_bytes = 0
    self._assembled_bytes = 0
    self._queue = BitfieldPriorityQueue(self._torrent.info.num_pieces)
    super(Session, self).__init__()

  @property
  def port(self):
    return self._port

  @property
  def peer_id(self):
    if self._peer_id is None:
      self._peer_id = PeerId.generate()
    return self._peer_id

  @property
  def torrent(self):
    return self._torrent

  @property
  def io_loop(self):
    return self._io_loop

  @property
  def downloaded_bytes(self):
    return self._downloaded_bytes

  @property
  def uploaded_bytes(self):
    return self._uploaded_bytes

  @property
  def assembled_bytes(self):
    return self._assembled_bytes

  @property
  def queue(self):
    return self._queue

  def run(self):
    # self._io_loop.start()
    # bind(...)
    # initialize peer set
    pass
