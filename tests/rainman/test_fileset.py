from rainman.fileset import FileSet


def test_num_pieces():
  fs = FileSet([('a.txt', 5)], 2)
  assert fs.size == 5
  assert fs.piece_size == 2
  assert fs.num_pieces == 3

  fs = FileSet([('a.txt', 5), ('b.txt', 3)], 4)
  assert fs.size == 8
  assert fs.piece_size == 4
  assert fs.num_pieces == 2
