from tornado.testing import AsyncTestCase

from rainman.fileset import FileSet
from rainman.request import Request, Piece
from rainman.piece_broker import PieceBroker


# TODO(wickman) We should have a more IOPool-specific test, instead this
# just tests the IOPool via PieceBroker which seems slightly wrong.
class TestFileIOPool(AsyncTestCase):
  FILES = [('a.txt', 10000), ('b.txt', 20000), ('c.txt', 5000)]
  PIECE_SIZE = 4096

  @staticmethod
  def fileset():
    return FileSet(TestFileIOPool.FILES, TestFileIOPool.PIECE_SIZE)

  def test_basic(self):
    fs = self.fileset()
    pb = PieceBroker(fs, io_loop=self.io_loop)
    pb.initialize()

    all_files_size = sum(fp[1] for fp in self.FILES)

    # test reads
    pc = Request(0, 0, all_files_size)
    read_data = []
    def read_done(data):
      read_data.append(data)
      self.stop()
    pb.read(pc, read_done)
    self.wait()

    assert len(read_data) == 1 and len(read_data[0]) == all_files_size
    assert read_data[0] == b'\x00' * all_files_size  # by default filesets are zeroed out

    # write
    pc = Piece(2, 0, 5000, block=b'\x01'*5000)
    def write_done(_):
      self.stop()
    pb.write(pc, write_done)
    self.wait()

    read_data = []
    pb.read(Request(0, 0, all_files_size), read_done)
    self.wait()

    assert len(read_data) == 1 and len(read_data[0]) == all_files_size
    assert read_data[0] == b''.join([
        b'\x00' * 2 * 4096,
        b'\x01' * 5000,
        b'\x00' * (all_files_size - 2 * 4096 - 5000)])
    pb.destroy()
