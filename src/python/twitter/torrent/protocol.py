from collections import namedtuple
import errno
import functools
import hashlib
import random
import socket
import time

from twitter.common.collections import OrderedSet
from twitter.common.quantity import Amount, Time

import tornado.ioloop


class PeerHandshaker(object):
  #  Handshake:
  #    1: chr(19) + 'BitTorrent protocol'
  #    2: chr(0) * 8
  #    3: 20-byte sha1 of bencoded metainfo  [if not correct, sever]
  #    4: 20-byte peer id                    [if not what we expect, sever]
  PEER_ID = None
  PEER_PREFIX = '-TW7712-'  # TWTTR

  @staticmethod
  def id():
    if cls.PEER_ID is None:
      cls.PEER_ID = cls.PREFIX + ''.join(random.sample('0123456789abcdef', 20 - len(cls.PREFIX)))
    return cls.PEER_ID

  @staticmethod
  def handshake(metainfo, peer_id):
    handshake  = chr(19) + 'BitTorrent protocol'
    handshake += chr(0) * 8
    handshake += hashlib.sha1(metainfo.raw()).digest()
    handshake += peer_id
    return handshake

  def __init__(self, metainfo, remote_peer_id):
    self._metainfo = metainfo
    self._remote_peer_id = remote_peer_id

  def my_handshake(self):
    return PeerHandshaker.handshake(self._metainfo, self.id())

  def peer_handshake(self):
    return PeerHandshaker.handshake(self._metainfo, self._remote_peer_id)


class PeerManager(object):
  def __init__(self, torrent, io_loop=None):
    self._io_loop = io_loop or tornado.ioloop.IOLoop.instance()
    self._tracker_url = torrent.announce
    self._next_interval = Amount(0, Time.SECONDS)
    self._peers = {}

  def start(self):
    # do get request, populate peers, set timer for next get
    pass

  def get(self, address):
    """Return peer id at address, None if none found."""
    return self._peers.get(address)


class PeerListener(TCPServer):
  class BindError(Exception): pass

  PORT_RANGE = range(6181, 6190)
  FILTER_INTERVAL = Amount(1, Time.MINUTES)

  @classmethod
  def log(cls, msg):
    print('PeerListener: %s' % msg)

  def __init__(self, torrent, io_loop=None, port=None):
    self._metainfo = torrent.info
    super(PeerListener, self).__init__(io_loop=io_loop)
    self._manager = PeerManager(self._metainfo, self.io_loop)
    port_range = [port] if port else BTProtocolListener.PORT_RANGE
    for port in port_range:
      try:
        self.listen(port)
      except OSError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise PeerListener.BindError('Could not bind to any port in range %s' % repr(port_range))
    self._port = port
    self._peers = {}
    self._filter_callback = tornado.ioloop.PeriodicCallback(self.filter_streams,
      PeerListener.FILTER_INTERVAL.as_(Time.MILLISECONDS), self.io_loop)
    self._filter_callback.start()

  def handle_stream(self, iostream, address):
    remote_peer_id = self._manager.get(address)
    if not remote_peer_id:
      iostream.close()
    self._peers[remote_peer_id] = PeerSession(
      iostream, self._metainfo, PeerHandshaker(self._metainfo, remote_peer_id))

  def filter_streams(self):
    closed = set()
    for peer_id, stream in self._peers:
      if stream.closed():
        cls.log('Peer ID %s hung up.' % peer_id)
        closed.add(peer_id)
    for peer_id in closed:
      self._peers.pop(peer_id, None)


class BTCommand(object):
  COMMANDS = {
    # Core Protocol
    '\x00': 'choke',
    '\x01': 'unchoke',
    '\x02': 'interested',
    '\x03': 'not interested',
    '\x04': 'have',
    '\x05': 'bitfield',
    '\x06': 'request',
    '\x07': 'piece',
    '\x08': 'cancel',

    # DHT (ignored)
    '\x09': 'port',

    # Fast (ignored)
    '\x0D': 'suggest',
    '\x0E': 'have all',
    '\x0F': 'have none',
    '\x10': 'reject request',
    '\x11': 'allowed fast',

    # LTEP (ignored)
    '\x14': 'LTEP handshake',
  }


PeerRequest = namedtuple('PeerRequest', 'index begin length')


class PeerConnection(object):
  @classmethod
  def log(cls, msg):
    print('PeerConnection: %s' % msg)

  def __init__(self, handshake):
    self._handshake = handshake
    self._queue = OrderedSet()
    self._last_keepalive = None
    self._accepted = False
    self._choked = True
    self._interested = False
    self._bitfield = Bitfield(metainfo.num_pieces, default=False)

  @property
  def accepted(self):
    return self._accepted

  def handshake(self, data):
    if self._handshake == data:
      self._accepted = True
    return self._accepted

  def keepalive(self):
    self._last_keepalive = time.time()

  def choke(self):
    self._choked = True
    if len(self._queue) > 0:
      self.log('Choked, dropping %s requests.' % len(self._queue))
    self._queue = OrderedSet()

  def unchoke(self):
    self._choked = False

  def interested(self):
    self._interested = True

  def not_interested(self):
    self._interested = False

  def have(self, index):
    self._bitfield[index] = True

  def bitfield(self, bitfield):
    self._bitfield = bitfield

  def request(self, index, begin, length):
    self._queue.add(PeerRequest(index, begin, length))

  def piece(self, index, begin, block):
    raise NotImplementedError

  def cancel(self, index, begin, length):
    self._queue.discard(PeerRequest(index, begin, length))


class PeerSession(object):
  KEEPALIVE_INTERNAL = Amount(2, Time.MINUTES)

  def __init__(self, iostream, metainfo, handshaker):
    self._iostream = iostream
    self._iostream.set_close_callback(self.on_close)

    self._ingress = PeerConnection(handshaker.peer_handshake())
    self._egress = PeerConnection(handshaker.my_handshake())

    def handshake_delegator(check_handshake_fn):
      if not check_handshake_fn(): self._iostream.close()
      if self._ingress.accepted and self._egress.accepted:
        self.message_start()

    egress_checker = functools.partial(handshake_delegator,
      functools.partial(self._egress.handshake, handshaker.my_handshake()))
    ingress_checker = functools.partial(handshake_delegator,
      self._ingress.handshake)

    self._iostream.write(handshaker.my_handshake(), egress_checker)
    self._iostream.read_bytes(len(handshaker.peer_handshake()), ingress_checker)

  def on_close(self):
    # Do something or just wait to be garbage collected?
    pass

  def message_start(self):
    self.read_bytes(4, self.message_starting)

  def message_starting(self, message_length):
    message_length = struct.unpack('>I', message_length)[0]
    if message_length == 0:
      self._ingress.keepalive()
    self.read_bytes(message_length, self.message_accept)

  def message_accept(self, data):
    message_type = struct.unpack('B', data[0])[0]
    self.dispatch(message_type, data[1:])

  def dispatch(self, message_type, data):
    pass
