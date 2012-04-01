import errno
import hashlib
import socket

from twitter.common.quantity import Amount, Time

import tornado.ioloop

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


class PeerManager(object):
  def __init__(self, torrent, peer_id, io_loop=None):
    self._io_loop = io_loop or tornado.ioloop.IOLoop.instance()
    self._tracker_url = torrent.announce
    self._peer_id = peer_id
    self._peers = {}


class PeerListener(TCPServer):
  class BindError(Exception): pass

  PEER_PREFIX = '-TW7712-'
  PORT_RANGE = range(6181, 6190)

  @staticmethod
  def random_peer_id():
    import random
    return PeerListener.PREFIX + ''.join(random.sample('0123456789', 20 - len(PeerListener.PREFIX)))

  def __init__(self, torrent, io_loop=None, port=None, peer_id=None):
    self._metainfo = torrent.info
    self._peer_id = peer_id or PeerListener.random_peer_id()
    super(PeerListener, self).__init__(io_loop=io_loop)
    self._manager = PeerManager(self._metainfo, self._peer_id, self.io_loop)
    self._factory = PeerConnectionFactory(self._metainfo, self._peer_id)
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

  def handle_stream(self, stream, address):


class PeerConnectionFactory(object):
  def __init__(self, metainfo, peer_id):
    self._metainfo = metainfo
    self._peer_id = peer_id

  def __call__(self, iostream):
    return PeerConnection(iostream, self._metainfo, self._peer_id)


class PeerConnection(object):
  KEEPALIVE_INTERNAL = Amount(2, Time.MINUTES)
  HANDSHAKE_SIZE_BYTES = 1 + 19 + 8 + 20 + 20

  #  Handshake:
  #    1: chr(19) + 'BitTorrent protocol'
  #    2: chr(0) * 8
  #    3: 20-byte sha1 of bencoded metainfo  [if not correct, sever]
  #    4: 20-byte peer id                    [if not what we expect, sever]
  @staticmethod
  def handshake(metainfo, peer_id):
    handshake  = chr(19) + 'BitTorrent protocol'
    handshake += chr(0) * 8
    handshake += hashlib.sha1(metainfo.raw()).digest()
    handshake += peer_id
    return handshake

  def __init__(self, iostream, metainfo, peer_id):
    self._iostream = iostream
    self._iostream.set_close_callback(self.on_close)
    self._choked, self._interested = True, False
    self._peer_choked, self._peer_interested = True, False
    self._shakes = 0
    self._send_queue = []
    self._recv_queue = []
    self._iostream.write(PeerConnection.handshake(metainfo, peer_id), self.on_handshake_write)
    self._iostream.read_bytes(PeerConnection.HANDSHAKE_SIZE_BYTES, self.on_handshake_read)

  def on_close(self):
    # do something.

  def on_handshake_read(self, data):
    pass

  def on_handshake_write(self):
    self._shakes += 1


