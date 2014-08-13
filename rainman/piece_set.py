import array


class PieceSet(object):
  """A collection for pieces that provides metrics such as rarest piece."""

  def __init__(self, length):
    self._num_owners = array.array('B', [0] * length)

  def add_one(self, index):
    self._num_owners[index] += 1

  def remove_one(self, index):
    self._num_owners[index] -= 1

  def add(self, bitfield):
    for k in range(len(bitfield)):
      self._num_owners[k] += int(bitfield[k])

  def remove(self, bitfield):
    for k in range(len(bitfield)):
      self._num_owners[k] -= int(bitfield[k])

  def rarest(self, owned=None):
    """Compute the rarest pieces.

      :param owned: If supplied, a list of owned pieces that should not be considered.
    """
    return [index
            for index, count in sorted(enumerate(self._num_owners), key=lambda key: key[1])
            if count > 0 and (owned is None or index not in owned)]
