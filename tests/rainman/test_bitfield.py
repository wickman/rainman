import pytest
import random
from rainman.bitfield import Bitfield

def test_corners():
  bf = Bitfield(0)
  assert len(bf) == 0
  for index in (0, -1, 1, 100):
    with pytest.raises(IndexError):
      bf[index]
  for index in (0, -1, 1, 100):
    with pytest.raises(IndexError):
      bf[index] = False
  for index in ('', 'hello', None, type):
    with pytest.raises(TypeError):
      bf[index]
    with pytest.raises(TypeError):
      bf[index] = True


def test_unit():
  bf = Bitfield(1, default=False)
  assert len(bf) == 1
  assert bf[0] == False
  bf = Bitfield(1, default=True)
  assert len(bf) == 1
  assert bf[0] == True
  bf[0] = False
  assert bf[0] == False


def test_bitops():
  # Randomly shuffle into place all values 0-255
  bf = Bitfield(8, default=False)
  shuffler = random.Random(31337).shuffle
  for k in range(0, 255):
    bits = range(8)
    shuffler(bits)
    for j in bits:
      values = [bf[m] for m in range(8)]
      bf[j] = bool(k & (1 << j))
      values[j] = bool(k & (1 << j))
      assert values == [bf[m] for m in range(8)]
