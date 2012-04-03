import array
from collections import namedtuple
import errno
import functools
import hashlib
import random
import socket
import time
import urllib2

from twitter.common.collections import OrderedSet
from twitter.common.quantity import Amount, Time

import tornado.ioloop
from tornado.netutil import TCPServer
from tornado import httpclient


from .bitfield import Bitfield


class PeerHandshaker(object):
  #  Handshake:
  #    1: chr(19) + 'BitTorrent protocol'
  #    2: chr(0) * 8
  #    3: 20-byte sha1 of bencoded metainfo  [if not correct, sever]
  #    4: 20-byte peer id                    [if not what we expect, sever]
  PEER_ID = None
  PEER_PREFIX = '-TW7712-'  # TWTTR

  @classmethod
  def id(cls):
    if cls.PEER_ID is None:
      cls.PEER_ID = cls.PEER_PREFIX + ''.join(
          random.sample('0123456789abcdef', 20 - len(cls.PEER_PREFIX)))
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
