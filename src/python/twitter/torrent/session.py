import datetime
import threading

from twitter.common import log
import tornado.ioloop

from .bencode import BDecoder

class PeerSet(object):
  # Requiring only read-only access to the session, manages the set of peers
  # with which we can establish sessions.
  def __init__(self, session, io_loop=None):
    self._session = session
    self._tracker = session.torrent.announce
    self._io_loop = io_loop or session.io_loop or tornado.ioloop.IOLoop.instance()
    self._http_client = httpclient.AsyncHTTPClient()
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
      'uploaded': session.upload_bytes,
      'downloaded': session.download_bytes,
      'left': session.torrent.info.length - session.torrent.info.piece_length * session.queue.left,
    }

  def start(self):
    if not self._handle:
      self.enqueue_request(event='started')

  def stop(self):
    if self._handle:
      self._io_loop.remove_timeout(self._handle)
      self._handle = None

  def enqueue_request(self, event=None):
    request = self.request()
    if event:
      request.update(event=event)
    url = self._tracker + urllib.urlencode(request)
    self._http_client.fetch(url, self.handle_response)

  def handle_response(self, response):
    interval = 60
    if response.error:
      log.error('PeerSet failed to query %s' % self._tracker)
    else:
      try:
        response = BDecoder.decode(response.body)
        interval = response.get('interval', 60)
        peers = response.get('peers', [])
        for peer in peers:
          assert 'peer id' in peer and 'ip' in peer and 'port' in peer
          self._peers[peer['peer id']] = (peer['ip'], peer['port'])
      except BDecoder.Error:
        log.error('Malformed tracker response.')
      except AssertionError:
        log.error('Malformed peer dictionary.')
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
  def __init__(self, torrent, port=None):
    self._torrent = torrent
    self._port = None
    self._peers = None
    self._io_loop = tornado.ioloop.IOLoop()
    self._upload_bytes = 0
    self._download_bytes = 0
    self._queue = BitfieldPriorityQueue(self._torrent.info.num_pieces)

  def run(self):
    # self._io_loop.start()
    # bind(...)
    # initialize peer set
