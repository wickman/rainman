# TODO(wickman) Use quicktest for something like this?
from rainman.sliceset import SliceSet

import pytest


def make_single(slc):
  ss = SliceSet()
  ss.add(slc)
  return ss


def test_empty_sliceset():
  ss = SliceSet()
  for val in (-1, 0, 1, slice(-1, -1), slice(0, 0), slice(1, 1), slice(-1, 1), slice(1,2,3),
              slice(0,3,2)):
    assert val not in ss
  for val in (None, "", "hello"):
    with pytest.raises(ValueError):
      val in ss


def test_simple_sliceset():
  ss = make_single(slice(0, 5))
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


def test_missing_all_single_cases():
  # len(range) < len(slice):
  rr = make_single(slice(6,10))
  mi = lambda sl: list(rr.missing_in(sl))
  assert mi(slice(0,5)) == [slice(0,5)]
  assert mi(slice(1,6)) == [slice(1,6)]
  assert mi(slice(5,10)) == [slice(5,6)]
  assert mi(slice(6,11)) == [slice(10,11)]
  assert mi(slice(10,15)) == [slice(10,15)]
  assert mi(slice(11,16)) == [slice(11,16)]

  # len(range) == len(slice)
  assert mi(slice(2,5)) == [slice(2,5)]
  assert mi(slice(4,7)) == [slice(4,6)]
  assert mi(slice(6,9)) == []
  assert mi(slice(7,10)) == []
  assert mi(slice(8,11)) == [slice(10,11)]
  assert mi(slice(11,14)) == [slice(11,14)]

  # len(range) > len(slice)
  assert mi(slice(5,11)) == [slice(5,6), slice(10,11)]


def test_many_missing():
  ss = SliceSet()
  for k in range(0, 20, 2):
    ss.add(slice(k, k + 1))
  assert list(ss.missing_in(slice(10, 30))) == [
    slice(11, 12), slice(13, 14), slice(15, 16), slice(17, 18), slice(19, 30)]


def test_missing_full_with_omission():
  mi = lambda sl: list(ss.missing_in(sl))

  ss = SliceSet()
  assert mi(slice(0, 100)) == [slice(0, 100)]

  ss = SliceSet()
  ss.add(slice(20, 40))
  assert mi(slice(0, 19)) == [slice(0, 19)]
  assert mi(slice(0, 20)) == [slice(0, 20)]
  assert mi(slice(0, 21)) == [slice(0, 20)]
  assert mi(slice(40, 60)) == [slice(40, 60)]
  assert mi(slice(39, 60)) == [slice(40, 60)]
  assert mi(slice(41, 60)) == [slice(41, 60)]
  assert mi(slice(20, 40)) == []
  assert mi(slice(20, 41)) == [slice(40, 41)]
  assert mi(slice(0, 60)) == [slice(0, 20), slice(40, 60)]

  ss = SliceSet()
  ss.add(slice(20, 29))
  ss.add(slice(31, 40))
  assert mi(slice(0, 19)) == [slice(0, 19)]
  assert mi(slice(0, 20)) == [slice(0, 20)]
  assert mi(slice(0, 21)) == [slice(0, 20)]
  assert mi(slice(19, 30)) == [slice(19, 20), slice(29, 30)]
  assert mi(slice(40, 60)) == [slice(40, 60)]
  assert mi(slice(39, 60)) == [slice(40, 60)]
  assert mi(slice(41, 60)) == [slice(41, 60)]
  assert mi(slice(20, 40)) == [slice(29,31)]
  assert mi(slice(20, 41)) == [slice(29,31), slice(40, 41)]
  assert mi(slice(0, 60)) == [slice(0, 20), slice(29,31), slice(40, 60)]



def test_erase():
  ss = make_single(slice(0,100))
  ss.erase(slice(20,40))
  assert ss.slices == [slice(0,20), slice(40,100)]
  ss.erase(slice(30,50))
  assert ss.slices == [slice(0,20), slice(50,100)]
  ss.erase(slice(51,100))
  assert ss.slices == [slice(0,20), slice(50,51)]

  ss = make_single(slice(0,100))
  number = 1
  for k in range(1,99,2):
    ss.erase(slice(k,k+1))
    number += 1
    assert len(ss.slices) == number


  ss = make_single(slice(0,100))
  ss.erase(slice(0,100))
  assert ss.slices == []
  ss = make_single(slice(0,100))
  ss.erase(slice(-1,100))
  assert ss.slices == []
  ss = make_single(slice(0,100))
  ss.erase(slice(0,101))
  assert ss.slices == []


  ss = make_single(slice(0,100))
  ss.erase(slice(-50,50))
  assert ss.slices == [slice(50,100)]

  ss = make_single(slice(0,100))
  ss.erase(slice(50,150))
  assert ss.slices == [slice(0,50)]
