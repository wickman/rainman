from rainman.request import Piece, Request
from rainman.piece_broker import PieceBroker
from rainman.piece_manager import PieceManager
from rainman.testing import make_metainfo

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test


from twitter.common import log
from twitter.common.log.options import LogOptions
LogOptions.set_disk_log_level('NONE')
LogOptions.set_stderr_log_level('google:DEBUG')
log.init('derp')


class TestPieceBroker(AsyncTestCase):
  @gen_test
  def test_main(self):
    piece_size = 4
    filelist = (('a.txt', b'hello'), ('b.txt', b'world'), ('c.txt', b'hello world'))
    td, fs, metainfo = make_metainfo(filelist, piece_size)

    pm = PieceManager(fs, chroot=td)
    pm.initialize()

    log.info('pb1')
    pb1 = PieceBroker(
        fs,
        chroot=td,
        piece_hashes=list(pm.iter_hashes()),
        io_loop=self.io_loop)
    pb1.initialize()

    log.info('pb2')
    pb2 = PieceBroker(
        fs,
        piece_hashes=list(pm.iter_hashes()),
        io_loop=self.io_loop)
    pb2.initialize()

    # make sure pb1 is complete and pb2 is not
    for k in range(fs.num_pieces):
      assert pb1.whole_piece(k) in pb1
      assert pb1.whole_piece(k) not in pb2

    a_txt = b''.join((yield [
        gen.Task(pb1.read, Request(0, 0, 4)),
        gen.Task(pb1.read, Request(1, 0, 1))]))
    b_txt = b''.join((yield [
        gen.Task(pb1.read, Request(1, 1, 3)),
        gen.Task(pb1.read, Request(2, 0, 2))]))
    c_txt = yield gen.Task(pb1.read, Request(2, 2, 11))

    assert a_txt == b'hello'
    assert b_txt == b'world'
    assert c_txt == b'hello world'

    # Test request iter
    assert list(pm.iter_blocks(0, 1)) == [
        Request(0, 0, 1), Request(0, 1, 1), Request(0, 2, 1), Request(0, 3, 1)]
    assert list(pm.iter_blocks(0, 2)) == [Request(0, 0, 2), Request(0, 2, 2)]
    assert list(pm.iter_blocks(0, 3)) == [Request(0, 0, 3), Request(0, 3, 1)]
    assert list(pm.iter_blocks(0, 4)) == [Request(0, 0, 4)]
    assert list(pm.iter_blocks(0, 5)) == [Request(0, 0, 4)]

    # Test iter of last piece
    assert list(pm.iter_blocks(5, 1)) == [Request(5, 0, 1)]
    assert list(pm.iter_blocks(5, 4)) == [Request(5, 0, 1)]

    # Write hello
    assert pb2.assembled_size == 0
    assert Request(0, 0, 4) not in pb2
    yield gen.Task(pb2.write, Piece(0, 0, 5, b'hello'))
    assert Request(0, 0, 4) in pb2
    assert pb2.assembled_size == 4

    # swallow dupe
    yield gen.Task(pb2.write, Piece(0, 0, 3, b'hel'))
    assert Request(0, 0, 4) in pb2

    # only assert pieces are in if we've validated sha
    assert Request(1, 0, 1) not in pb2

    # but the slice is still in there
    assert pb2.to_slice(Request(1, 0, 1)) in pb2.slices

    # write garbage and Request(1, 0, 1) slice goes away b/c the whole extent is bad
    assert pb2.assembled_size == 4
    yield gen.Task(pb2.write, Piece(1, 1, 3, b'poo'))
    assert pb2.to_slice(Request(1, 0, 1)) not in pb2.slices
    assert pb2.assembled_size == 4

    # write owo, rld, to make sure partial chunk is not computed
    yield [gen.Task(pb2.write, Piece(1, 0, 3, b'owo')),
           gen.Task(pb2.write, Piece(1, 3, 3, b'rld'))]
    assert Request(1, 0, 4) in pb2
    assert pb2.assembled_size == 8

    """
    TODO(wickman) Fix the logic so that this works.
    yield gen.Task(pb2.write, Piece(0, 0, 21, b'helloworldhello world'))
    """
    yield gen.Task(pb2.write, Piece(5, 0, 1, b'd'))
    assert pb2.assembled_size == 9

    yield [
        gen.Task(pb2.write, Piece(0, 0, 4, b'hell')),
        gen.Task(pb2.write, Piece(1, 0, 4, b'owor')),
        gen.Task(pb2.write, Piece(2, 0, 4, b'ldhe')),
        gen.Task(pb2.write, Piece(3, 0, 4, b'llo ')),
        gen.Task(pb2.write, Piece(4, 0, 4, b'worl'))]
    assert pb2.assembled_size == 21

    for k in range(fs.num_pieces):
      assert pb2.whole_piece(k) in pb2

    # the safe_mkdtemp will auto destroy but do this to test the
    # code path.
    pb2.destroy()
