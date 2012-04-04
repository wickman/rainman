import array
import random


class Bitfield(object):
  def __init__(self, length, default=False):
    self._length = length
    num_bytes, leftover_bits = divmod(length, 8)
    self._array = array.array('B',
      chr(255 if default else 0) * (num_bytes + (1 if leftover_bits else 0)))

  @property
  def num_bytes(self):
    return len(self._array)

  def as_bytes(self):
    return ''.join(map(chr, self._array))

  def fill(self, value):
    for k in range(len(value)):
      self._array[k] = value[k]

  def __getitem__(self, index):
    if not isinstance(index, int):
      raise TypeError
    byte_index, bit_index = divmod(index, 8)
    if byte_index >= len(self._array):
      raise IndexError
    byte = self._array[byte_index]
    return True if (byte & (1 << bit_index)) else False

  def __setitem__(self, index, value):
    if not isinstance(index, int):
      raise TypeError
    if not isinstance(value, bool):
      raise TypeError
    byte_index, bit_index = divmod(index, 8)
    if byte_index >= len(self._array):
      raise IndexError
    if value:
      self._array[byte_index] |= (1 << bit_index)
    else:
      self._array[byte_index] &= (~(1 << bit_index) % 256)

  def __len__(self):
    return self._length


class BitfieldPriorityQueue(object):
  # The glaring bug of course is the case of > 255 peers.  Pretty sure you're supposed
  # to have fewer than 50.

  def __init__(self, length):
    self._length = length
    self._bitfield = Bitfield(length)
    self._owners = array.array('B', [0] * length)

  def have(self, index):
    self._bitfield[index] = True

  def add(self, bitfield):
    for k in range(len(bitfield)):
      self._owners[k] += bitfield[k]

  def remove(self, bitfield):
    for k in range(len(bitfield)):
      self._owners[k] -= bitfield[k]

  # This is inefficient as fuck
  def get(self, count=10):
    enumerated = sorted(
        [(self._owners[k], k) for k in range(self._length)
          if not self._bitfield[k] and self._owners[k]], reverse=True)
    first = enumerated[:count]
    random.shuffle(first)
    return first

  @property
  def left(self):
    return self._length - sum(self._bitfield[k] for k in range(self._length))
