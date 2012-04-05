import pytest
from twitter.torrent.fileset import SliceSet

def test_empty_sliceset():
  ss = SliceSet()
  for val in (-1, 0, 1, slice(-1, -1), slice(0, 0), slice(1, 1), slice(-1, 1), slice(1,2,3),
              slice(0,3,2)):
    assert val not in ss
  for val in (None, "", "hello"):
    with pytest.raises(ValueError):
      val in ss


def test_simple_sliceset():
  ss = SliceSet()
  ss.add(slice(0,5))
  for k in range(5):
    assert k in ss
    assert slice(-1, k) not in ss
    assert slice(k, 6) not in ss
    assert slice(0, k) in ss


def test_set_merge():
  ss = SliceSet()
  ss.add(slice(0, 1))
  assert len(ss.slices) == 1
  ss.add(slice(2, 3))
  assert len(ss.slices) == 2
  ss.add(slice(1, 2))
  assert len(ss.slices) == 1
  assert ss.slices == [slice(0,3)]


def test_multi_merge():
  ss = SliceSet()
  for k in range(-100, 100, 2):
    ss.add(slice(k, k+1))
  assert len(ss.slices) == 100

  for k in range(50):
    assert slice(k, k+2) not in ss

  ss.add(slice(0,100))
  assert len(ss.slices) == 51
  ss.add(slice(-100,100))
  assert len(ss.slices) == 1


def test_iter():
  ss = SliceSet()

  counted = 0
  for sl in ss:
    counted += 1
  assert counted == 0

  for k in range(0, 100, 2):
    ss.add(slice(k, k+1))

  for sl in ss:
    counted += 1
  assert counted == 50


def test_big_missing():
  ss = SliceSet()
  for k in range(0, 20, 2):
    ss.add(slice(k, k + 1))
  assert list(ss.missing_in(slice(10, 30))) == [
    slice(11, 12), slice(13, 14), slice(15, 16), slice(17, 18), slice(19, 30)]



def test_missing():
  ss = SliceSet()
  assert list(ss.missing_in(slice(0, 100))) == [slice(0, 100)]

  ss = SliceSet()
  ss.add(slice(20, 40))

  assert list(ss.missing_in(slice(0, 19))) == [slice(0, 19)]
  assert list(ss.missing_in(slice(0, 20))) == [slice(0, 20)]
  assert list(ss.missing_in(slice(0, 21))) == [slice(0, 20)]
  assert list(ss.missing_in(slice(40, 60))) == [slice(40, 60)]
  assert list(ss.missing_in(slice(39, 60))) == [slice(40, 60)]
  assert list(ss.missing_in(slice(41, 60))) == [slice(41, 60)]
  assert list(ss.missing_in(slice(20, 40))) == []
  assert list(ss.missing_in(slice(20, 41))) == [slice(40, 41)]
  assert list(ss.missing_in(slice(0, 60))) == [slice(0, 20), slice(40, 60)]

  ss = SliceSet()
  ss.add(slice(20, 29))
  ss.add(slice(31, 40))

  assert list(ss.missing_in(slice(0, 19))) == [slice(0, 19)]
  assert list(ss.missing_in(slice(0, 20))) == [slice(0, 20)]
  assert list(ss.missing_in(slice(0, 21))) == [slice(0, 20)]
  assert list(ss.missing_in(slice(19, 30))) == [slice(19, 20), slice(29, 30)]
  assert list(ss.missing_in(slice(40, 60))) == [slice(40, 60)]
  assert list(ss.missing_in(slice(39, 60))) == [slice(40, 60)]
  assert list(ss.missing_in(slice(41, 60))) == [slice(41, 60)]
  assert list(ss.missing_in(slice(20, 40))) == [slice(29,31)]
  assert list(ss.missing_in(slice(20, 41))) == [slice(29,31), slice(40, 41)]
  assert list(ss.missing_in(slice(0, 60))) == [slice(0, 20), slice(29,31), slice(40, 60)]
