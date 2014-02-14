from math import ceil
import os
import random
import struct
import tempfile

from rainman.fileset import FileSet
from rainman.piece_manager import PieceManager

import pytest


def test_empty_input():
  pm = PieceManager(FileSet((), 1))
  pm.initialize()
  chunks = list(pm.iter_pieces())
  hashes = list(pm.iter_hashes())
  try:
    assert chunks == []
    assert hashes == []
  finally:
    pm.destroy()


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
    pm = PieceManager(FileSet([('hello_world', pair[0])], pair[1]))
    pm.initialize()
    chunks = list(pm.iter_pieces())
    hashes = list(pm.iter_hashes())

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
      pm.destroy()


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

  pm1 = PieceManager(
      FileSet([('one.txt', 4), ('two.txt', 8), ('three.txt', 12)], piece_size=13),
      chroot=d1)
  pm2 = PieceManager(FileSet([('two.txt', 8), ('four.txt', 16)], piece_size=13), chroot=d2)
  pm3 = PieceManager(FileSet([('six.txt', 24)], piece_size=13), chroot=d3)
  pm1.initialize()
  pm2.initialize()
  pm3.initialize()

  try:
    assert list(pm1.iter_pieces()) == list(pm2.iter_pieces())
    assert list(pm1.iter_pieces()) == list(pm3.iter_pieces())
    assert list(pm1.iter_hashes()) == list(pm2.iter_hashes())
    assert list(pm1.iter_hashes()) == list(pm3.iter_hashes())

  finally:
    pm1.destroy()
    pm2.destroy()
    pm3.destroy()
