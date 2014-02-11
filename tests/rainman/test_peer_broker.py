from contextlib import contextmanager
import socket
import threading

from rainman.handshake import PeerHandshake
from rainman.peer_broker import PeerBroker
from rainman.peer_id import PeerId
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
  def __init__(self, peer_id, sock, io_loop, port):
    self.__sock = sock
    super(SocketPeerBroker, self).__init__(peer_id, io_loop=io_loop, port=port)

  def _do_bind(self, port):
    self.add_sockets([self.__sock])
    return port


class FakeSession(object):
  def __init__(self, torrent):
    self.torrent = torrent
    self.peer_id, self.iostream = None, None

  def add_peer(self, peer_id, iostream):
    self.peer_id = peer_id
    self.iostream = iostream


class TestPeerBroker(AsyncTestCase):
  SERVER_PEER_ID = PeerId.generate()
  CLIENT_PEER_ID = PeerId.generate()

  @contextmanager
  def make_peer_broker(self, torrent, peer_id):
    listener, port = bind_unused_port()
    peer_broker = SocketPeerBroker(peer_id, listener, self.io_loop, port)
    peer_broker.register_torrent(torrent, session_provider=FakeSession)
    yield peer_broker

  def test_unregistered_torrent(self):
    listener, port = bind_unused_port()
    peer_broker = SocketPeerBroker(self.SERVER_PEER_ID, listener, self.io_loop, port)
    with make_metainfo([('a.txt', 'hello world')], 4) as metainfo:
      handshake = PeerHandshake.make(metainfo, peer_id=self.CLIENT_PEER_ID)
      client_stream = IOStream(socket.socket(), io_loop=self.io_loop)
      yield gen.Task(client_stream.connect, ('localhost', port))
      yield gen.Task(client_stream.write, handshake)
      self.io_loop.clear_current()
      assert peer_broker._failed_handshakes == 1

  @gen_test
  def test_handle_stream(self):
    listener, port = bind_unused_port()
    peer_broker = SocketPeerBroker(self.SERVER_PEER_ID, listener, self.io_loop, port)
    with make_torrent([('a.txt', 'hello world')], 4, 'asdfasdf') as torrent:
      peer_broker.register_torrent(torrent, session_provider=FakeSession)
      handshake = PeerHandshake.make(torrent.info, peer_id=self.CLIENT_PEER_ID)
      client_stream = IOStream(socket.socket(), io_loop=self.io_loop)
      yield gen.Task(client_stream.connect, ('localhost', port))
      yield gen.Task(client_stream.write, handshake)
      yield gen.Task(client_stream.read_bytes, PeerHandshake.LENGTH)
      _, fake_session = peer_broker.establish_session(PeerHandshake.make(torrent.info))
      assert fake_session.peer_id
      assert fake_session.iostream

  @gen_test
  def test_initiate_connection(self):
    with make_torrent([('a.txt', 'hello world')], 4, 'asdfasdf') as torrent:
      with self.make_peer_broker(torrent, self.SERVER_PEER_ID) as server:
        with self.make_peer_broker(torrent, self.CLIENT_PEER_ID) as client:
          peer_id = yield gen.Task(client.initiate_connection, torrent, ('localhost', server.port))
          assert peer_id is not None

          _, client_session = client.establish_session(PeerHandshake.make(torrent.info))
          assert client_session.peer_id == self.SERVER_PEER_ID
          assert peer_id == self.SERVER_PEER_ID

          _, server_session = server.establish_session(PeerHandshake.make(torrent.info))
          assert server_session.peer_id == self.CLIENT_PEER_ID
