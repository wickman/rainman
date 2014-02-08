from collections import defaultdict
import time

from rainman.codec import BEncoder

from twitter.common import app, log
from twitter.common.app.modules.http import RootServer
from twitter.common.http import HttpServer
from twitter.common.quantity import Amount, Time


class TrackerRequest(object):
  class MalformedRequestError(Exception): pass

  REQUIRED_KEYS = set([
    'info_hash',
    'peer_id',
    'port',
    'uploaded',
    'downloaded',
    'left',
  ])

  OPTIONAL_KEYS = set(['ip', 'event'])

  def __init__(self, request):
    self._request = request.GET
    self._ip = request.remote_addr
    for key in TrackerRequest.REQUIRED_KEYS:
      if key not in request.GET:
        raise TrackerRequest.MalformedRequestError('Missing key %s' % key)

  def get(self, key, default=None):
    return self._request.get(key, default)

  @property
  def hash(self):
    return self._request['info_hash']

  @property
  def ip(self):
    return self._ip

  def __getitem__(self, key):
    assert key in TrackerRequest.REQUIRED_KEYS or key in TrackerRequest.OPTIONAL_KEYS
    return self._request[key]


class Peer(object):
  def __init__(self, request):
    self._hash = request.hash
    self._port = request['port']
    self._ip   = request.ip
    self._id   = request['peer_id']
    self._uploaded = self._downloaded = self._left = self._event = None
    self.update(request)

  @property
  def id(self):
    return self._id

  @property
  def ip(self):
    return self._ip

  @property
  def address(self):
    return (self.ip, self.port)

  @property
  def port(self):
    return self._port

  @property
  def hash(self):
    return self._hash

  @property
  def hex(self):
    return ''.join(map(lambda byt: '%x' % byt, map(ord, self.hash)))

  def update(self, request):
    # TODO: Implement tracking of peer statistics.
    self._uploaded = request['uploaded']
    self._downloaded = request['downloaded']
    self._left = request['left']
    self._state = request.get('event', 'active')


class Tracker(HttpServer):
  DEFAULT_INTERVAL = Amount(30, Time.SECONDS)

  def __init__(self):
    self._torrents = defaultdict(dict)

  # TODO: This should respond in text/plain.
  @HttpServer.route('/')
  def main(self):
    try:
      request = TrackerRequest(self.Request)
    except TrackerRequest.MalformedRequestError as e:
      return BEncoder.encode({
        'failure reason': str(e)
      })
    self.update_peers(request)
    self.expire_peers()
    return BEncoder.encode({
      'interval': int(Tracker.DEFAULT_INTERVAL.as_(Time.SECONDS)),
      'peers': [
        { 'id': peer.id,
          'ip': peer.ip,
          'port': int(peer.port),
        } for peer in self._torrents[request.hash].values()
      ]
    })

  def update_peers(self, request):
    peer = Peer(request)
    if peer.address not in self._torrents[request.hash]:
      log.info('Registering peer [%s @ %s:%s] against torrent %s' % (
          peer.id, peer.ip, peer.port, peer.hex))
      self._torrents[request.hash][peer.address] = peer
      return

    if peer.id != self._torrents[request.hash][peer.address].id:
      log.info('Evicting old peer id %s on %s:%s' % (
          self._torrents[request.hash][peer.address].id, peer.ip, peer.port))
      self._torrents[request.hash][peer.address] = peer
    else:
      self._torrents[request.hash][peer.address].update(request)

  def expire_peers(self):
    # TODO: Implement culling of expired peers.
    pass


def main(args, options):
  tracker = Tracker()
  RootServer().mount_routes(tracker)
  while True:
    time.sleep(10)


app.configure(module='twitter.common.app.modules.http', enable=True, framework='tornado')
app.main()
