from rainman.fileset import FileSet

import pytest


def test_num_pieces():
  fs = FileSet([('a.txt', 5)], 2)
  assert fs.size == 5
  assert fs.piece_size == 2
  assert fs.num_pieces == 3

  fs = FileSet([('a.txt', 5), ('b.txt', 3)], 4)
  assert fs.size == 8
  assert fs.piece_size == 4
  assert fs.num_pieces == 2


def test_bad_input():
  bad_inputs = [
    ((), -1),
    ((), 0),
    (('valid', 20), 1),
    ([('morf', -23)], 1),
    ([(int, type)], 'bork')
  ]
  for input in bad_inputs:
    with pytest.raises(ValueError):
      FileSet(*input)
