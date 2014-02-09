class PieceSet(object):
  """A collection for pieces that provides metrics such as rarest piece."""

  RARE_THRESHOLD = 50

  def __init__(self, length):
    self._num_owners = array.array('B', [0] * length)
    self._changed_pieces = 0
    self._rarest = None

  def have(self, index, value=True):
    self._changed_pieces += 1
    self._num_owners[index] += int(value)

  def add(self, bitfield):
    for k in range(len(bitfield)):
      if bitfield[k]:
        self.have(k)

  def remove(self, bitfield):
    for k in range(len(bitfield)):
      if bitfield[k]:
        self.have(k, False)

  # TODO(wickman) Fix this if it proves to be too slow.
  def rarest(self, count=None, owned=None):
    if not self._rarest or self._changed_pieces >= PieceSet.RARE_THRESHOLD:
      def exists_but_not_owned(pair):
        index, count = pair
        return count > 0 and (not owned or not owned[index])
      self._rarest = sorted(filter(exists_but_not_owned, enumerate(self._num_owners)),
                            key=lambda val: val[1])
      self._changed_pieces = 0
    return [val[0] for val in (self._rarest[:count] if count else self._rarest)]
