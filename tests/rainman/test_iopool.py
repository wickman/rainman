from tornado.testing import AsyncTestCase

# we should have a full-on IOPool test too, but this is easier for now
from rainman.fileset import FileSet, Piece, Request
from rainman.filemanager import FileManager


class TestFileIOPool(AsyncTestCase):
  FILES = [('a.txt', 10000), ('b.txt', 20000), ('c.txt', 5000)]
  PIECE_SIZE = 4096

  @staticmethod
  def fileset():
    return FileSet(TestFileIOPool.FILES, TestFileIOPool.PIECE_SIZE)

  def test_basic(self):
    fs = self.fileset()
    fm = FileManager(fs, io_loop=self.io_loop)
    all_files_size = sum(fp[1] for fp in self.FILES)

    # test reads
    pc = Request(0, 0, all_files_size)
    read_data = []
    def read_done(data):
      read_data.append(data)
      self.stop()
    fm.read(pc, read_done)
    self.wait()

    assert len(read_data) == 1 and len(read_data[0]) == all_files_size
    assert read_data[0] == b'\x00' * all_files_size  # by default filesets are zeroed out

    # write
    pc = Piece(2, 0, 5000, block=b'\x01'*5000)
    def write_done():
      self.stop()
    fm.write(pc, write_done)
    self.wait()

    read_data = []
    fm.read(Request(0, 0, all_files_size), read_done)
    self.wait()

    assert len(read_data) == 1 and len(read_data[0]) == all_files_size
    assert read_data[0] == b''.join([
        b'\x00' * 2 * 4096,
        b'\x01' * 5000,
        b'\x00' * (all_files_size - 2 * 4096 - 5000)])
    fm.destroy()
