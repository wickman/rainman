from math import ceil
import os
import pytest
import random
import struct
import tempfile

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


def test_empty_input():
  fs = FileSet((), 1)
  chunks = list(fs.iter_pieces())
  hashes = list(fs.iter_hashes())
  try:
    assert chunks == []
    assert hashes == []
  finally:
    fs.destroy()


def test_single():
  # filesize, piece size pairs
  pairs = [
    (3, 20),
    (20, 17),
    (10000, 32),
    (100, 20),
    (1, 131072),
  ]

  for pair in pairs:
    fs = FileSet([('hello_world', pair[0])], pair[1])
    chunks = list(fs.iter_pieces())
    hashes = list(fs.iter_hashes())

    try:
      assert len(chunks) == int(ceil(1.*pair[0]/pair[1]))
      assert len(hashes) == int(ceil(1.*pair[0]/pair[1]))
      for chunk in chunks[:-1]:
        assert len(chunk) == pair[1]
      _, leftover = divmod(pair[0], pair[1])
      if leftover:
        assert len(chunks[-1]) == leftover
      for hash in hashes:
        assert len(hash) == 20
    finally:
      fs.destroy()


def test_many():
  d1 = tempfile.mkdtemp()
  d2 = tempfile.mkdtemp()
  d3 = tempfile.mkdtemp()
  deadbeef = struct.pack('>I', 0xDEADBEEF)

  with open(os.path.join(d1, 'one.txt'), 'w') as fp:
    for k in range(1):
      fp.write(deadbeef)
  with open(os.path.join(d1, 'two.txt'), 'w') as fp:
    for k in range(2):
      fp.write(deadbeef)
  with open(os.path.join(d1, 'three.txt'), 'w') as fp:
    for k in range(3):
      fp.write(deadbeef)

  with open(os.path.join(d2, 'two.txt'), 'w') as fp:
    for k in range(2):
      fp.write(deadbeef)
  with open(os.path.join(d2, 'four.txt'), 'w') as fp:
    for k in range(4):
      fp.write(deadbeef)

  with open(os.path.join(d3, 'six.txt'), 'w') as fp:
    for k in range(6):
      fp.write(deadbeef)

  fs1 = FileSet([('one.txt', 4), ('two.txt', 8), ('three.txt', 12)], piece_size=13, chroot=d1)
  fs2 = FileSet([('two.txt', 8), ('four.txt', 16)], piece_size=13, chroot=d2)
  fs3 = FileSet([('six.txt', 24)], piece_size=13, chroot=d3)

  try:
    assert list(fs1.iter_pieces()) == list(fs2.iter_pieces())
    assert list(fs1.iter_pieces()) == list(fs3.iter_pieces())
    assert list(fs1.iter_hashes()) == list(fs2.iter_hashes())
    assert list(fs1.iter_hashes()) == list(fs3.iter_hashes())

  finally:
    fs1.destroy()
    fs2.destroy()
    fs3.destroy()
