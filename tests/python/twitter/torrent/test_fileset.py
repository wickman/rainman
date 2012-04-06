import pytest
from twitter.torrent.fileset import FileSet


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
