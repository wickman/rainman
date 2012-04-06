from tornado.testing import AsyncTestCase

# we should have a full-on IOPool test too, but this is easier for now
from twitter.torrent.fileset import FileIOPool, FileSet
from twitter.torrent.peer import Piece


class TestFileIOPool(AsyncTestCase):
  FILES = [('a.txt', 10000), ('b.txt', 20000), ('c.txt', 5000)]
  PIECE_SIZE = 4096

  def test_basic(self):
    fs = FileSet(self.FILES, piece_size=self.PIECE_SIZE)
    fiop = FileIOPool(fs, io_loop=self.io_loop)

    # read everything
    all_files_size = sum(fp[1] for fp in self.FILES)
    pc = Piece(0, 0, all_files_size)
    read_data = []

    def done(data):
      read_data.append(data)
      self.stop()

    fiop.read(pc, done)
    self.wait()
    fs.destroy()
    fiop.stop()

    assert len(read_data) == 1 and len(read_data[0]) == all_files_size
    assert read_data[0] == chr(0) * all_files_size  # by default filesets are zeroed out
