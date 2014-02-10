from tornado import netutil
from tornado.iostream import IOStream
from tornado.testing import (
    AsyncTestCase,
    bind_unused_port,
    gen_test,
)


class TestPeer(AsyncTestCase):
  def _make_server_iostream(self, connection, **kwargs):
    return IOStream(connection, **kwargs)

  def _make_client_iostream(self, connection, **kwargs):
    return IOStream(connection, **kwargs)

  # stolen from tornado
  def make_iostream_pair(self, **kwargs):
    listener, port = bind_unused_port()
    streams = [None, None]

    def accept_callback(connection, address):
      streams[0] = self._make_server_iostream(connection, **kwargs)
      self.stop()

    def connect_callback():
      streams[1] = client_stream
      self.stop()

    netutil.add_accept_handler(listener, accept_callback, io_loop=self.io_loop)
    client_stream = self._make_client_iostream(socket.socket(), **kwargs)
    client_stream.connect(('127.0.0.1', port), callback=connect_callback)
    self.wait(condition=lambda: all(streams))
    self.io_loop.remove_handler(listener.fileno())
    listener.close()
    return streams

  @gen_test
  def test_peer_request(self):
    """
    piece_manager1 = PieceManager(FileSet(), io_loop=self.io_loop)
    piece_manager2 = PieceManager(FileSet(), io_loop=self.io_loop)

    calls = []
    def on_registration(caller, haves, have_nots):
      calls.append(caller)
      assert haves == []
      assert have_nots == []

    peer1_stream, peer2_stream = self.make_iostream_pair()
    peer1 = Peer(peer1_stream, piece_manager1)
    peer2 = Peer(peer2_stream, piece_manager2)

    self.io_pool.add_callback(peer1.run)
    self.io_pool.add_callback(peer2.run)
    self.wait()
    """
    pass
