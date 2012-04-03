import hashlib
import random

from twitter.common import log

from tornado import gen
from .metainfo import MetaInfo


class PeerId(object):
  PREFIX = '-TW7712-'  # TWTTR
  LENGTH = 20
  @classmethod
  def generate(cls):
    return cls.PREFIX + ''.join(random.sample('0123456789abcdef', cls.LENGTH - len(cls.PREFIX)))


class Peer(object):
  HANDSHAKE_LENGTH = 68  # 1 byte + 19 bytes + 8 bytes + 20 + 20

  @staticmethod
  def make_handshake(info, peer_id=None):
    """
      Make a Peer-Peer handshake.  If peer_id is omitted, make only the handshake prefix.
    """
    if isinstance(info, MetaInfo):
      hash = hashlib.sha1(info.raw()).digest()
    elif isinstance(info, str) and len(str) == 20:
      hash = info
    else:
      raise ValueError('Expected MetaInfo or hash, got %s' % repr(info))
    handshake = [chr(19), 'BitTorrent protocol', chr(0) * 8, hash, peer_id if peer_id else '']
    return ''.join(handshake)

  def __init__(self, address, session, handshake_cb=None):
    self._id = None
    self._address = address
    self._session = session
    self._hash = hashlib.sha1(session.torrent.info.raw()).digest()
    self._handshake_cb = handshake_cb or (lambda *x: x)
    self._iostream = None

  @property
  def id(self):
    return self._id

  @property
  def address(self):
    return self._address

  @gen.engine
  def handshake(self, iostream):
    log.debug('Session [%s] starting handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))
    handshake_prefix = Peer.make_handshake(self._session.torrent.info)
    handshake_full = Peer.make_handshake(self._session.torrent.info, self._session.peer_id)
    read_handshake, _ = yield [gen.Task(iostream.read_bytes, Peer.HANDSHAKE_LENGTH),
                                gen.Task(iostream.write, handshake_full)]
    if read_handshake.startswith(handshake_prefix):
      self._id = read_handshake[-PeerId.LENGTH:]
      self._iostream = iostream
      self._handshake_cb(self, True)
    else:
      iostream.close()
      self._handshake_cb(self, False)
    log.debug('Session [%s] finishing handshake with %s:%s' % (self._session.peer_id,
        self._address[0], self._address[1]))

  def disconnect(self):
    if self._iostream:
      self._iostream.close()
