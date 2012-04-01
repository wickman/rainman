from collections import defaultdict
import time

try:
  from twitter.common import app, log
  from twitter.common.app.modules.http import RootServer
  _HAS_APP = True
except ImportError:
  _HAS_APP = False

from twitter.common.http import HttpServer
from twitter.common.quantity import Amount, Time

from .codec import BEncoder

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
    self.update(get)

  @property
  def id(self):
    return self._id

  @property
  def ip(self):
    return self._ip

  @property
  def port(self):
    return self._port

  def update(self, request):
    # TODO: Implement tracking of peer statistics.
    self._uploaded = get['uploaded']
    self._downloaded = get['downloaded']
    self._left = get['downloaded']
    self._state = get.get('event', 'active')


class Tracker(HttpServer):
  DEFAULT_INTERVAL = Amount(30, Time.SECONDS)

  def __init__(self):
    self._torrents = {}

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
          'port': peer.port,
        } for peer in self._torrents[request.hash]
      ]
    })

  def update_peers(self, request):
    if request.hash in self._torrents:
      if request['peer_id'] in self._torrents[request.hash]:
        self._torrents[request.hash][request['peer_id']].update(request)
      else:
        self._torrents[request.hash][request['peer_id']] = Peer(request)
    else:
      self._torrents[request.hash] = {request['peer_id']: Peer(request)}

  def expire_peers(self):
    # TODO: Implement culling of expired peers.
    pass


def main(args, options):
  tracker = Tracker()
  RootServer().mount_routes(tracker)
  while True:
    time.sleep(10)


if _HAS_APP:
  app.configure(module='twitter.common.app.modules.http', enable=True, framework='tornado')
  app.main()

