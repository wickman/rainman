import array
import random


class Bitfield(object):
  """Naive implementation of a vector of booleans with __getitem__ and __setitem__."""

  def __init__(self, length, default=False):
    self._length = length
    num_bytes, leftover_bits = divmod(length, 8)
    self._array = array.array('B',
      chr(255 if default else 0) * (num_bytes + (1 if leftover_bits else 0)))

  @property
  def num_bytes(self):
    return len(self._array)

  def as_bytes(self):
    """Returns the bitfield in byte array form."""
    return ''.join(map(chr, self._array))

  def fill(self, value):
    """Fill the bitfield from a byte array."""
    for k in range(len(value)):
      self._array[k] = ord(value[k])

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
