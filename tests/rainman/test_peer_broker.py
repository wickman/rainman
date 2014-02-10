import socket
import threading

from rainman.handshake import PeerHandshake
from rainman.peer_broker import PeerBroker
from rainman.testing import make_metainfo, make_torrent

from tornado import gen, netutil
from tornado.iostream import IOStream
from tornado.testing import (
    AsyncTestCase,
    bind_unused_port,
    gen_test,
)


from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')


class SocketPeerBroker(PeerBroker):
  def __init__(self, sock, io_loop, port):
    self.__sock = sock
    super(SocketPeerBroker, self).__init__(io_loop=io_loop, port=port)

  def _do_bind(self, port):
    self.add_sockets([self.__sock])
    return port


class FakeSession(object):
  def __init__(self, torrent, peer_id):
    self.torrent = torrent
    self.peer_id = peer_id
    self.address, self.iostream = None, None

  def add_peer(self, address, iostream):
    self.address = address
    self.iostream = iostream


class TestPeerBroker(AsyncTestCase):
  SERVER_PEER_ID = '-RM1234-00000000000000'
  CLIENT_PEER_ID = '-RM1234-00000000000001'

  def test_closes(self):
    listener, port = bind_unused_port()
    peer_broker = SocketPeerBroker(listener, self.io_loop, port)
    with make_metainfo([('a.txt', 'hello world')], 4) as metainfo:
      handshake = PeerHandshake.make(metainfo, peer_id=self.CLIENT_PEER_ID)
      client_stream = IOStream(socket.socket(), io_loop=self.io_loop)
      yield gen.Task(client_stream.connect, ('localhost', port))
      yield gen.Task(client_stream.write, handshake)
      self.io_loop.clear_current()
      assert peer_broker._failed_handshakes == 1

  @gen_test
  def test_basic(self):
    listener, port = bind_unused_port()
    peer_broker = SocketPeerBroker(listener, self.io_loop, port)
    with make_torrent([('a.txt', 'hello world')], 4, 'asdfasdf') as torrent:
      fake_session = FakeSession(torrent, self.SERVER_PEER_ID)
      def session_provider(port):
        return fake_session
      peer_broker.register_torrent(torrent, session_provider=session_provider)
      handshake = PeerHandshake.make(torrent.info, peer_id=self.CLIENT_PEER_ID)
      client_stream = IOStream(socket.socket(), io_loop=self.io_loop)
      yield gen.Task(client_stream.connect, ('localhost', port))
      yield gen.Task(client_stream.write, handshake)
      return_handshake = yield gen.Task(client_stream.read_bytes, PeerHandshake.LENGTH)
      assert fake_session.address
      assert fake_session.iostream
