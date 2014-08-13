class Request(object):
  __slots__ = ('index', 'offset', 'length')

  def __init__(self, index, offset, length):
    self.index = index
    self.offset = offset
    self.length = length

  def __hash__(self):
    return hash((self.index, self.offset, self.length))

  def __str__(self):
    return 'Request(%s[%s:%s])' % (self.index, self.offset, self.offset + self.length)

  def __eq__(self, other):
    return (self.index == other.index and
            self.offset == other.offset and
            self.length == other.length)


class Piece(Request):
  __slots__ = ('block',)

  def __init__(self, index, offset, length, block):
    super(Piece, self).__init__(index, offset, length)
    self.block = block

  def to_request(self):
    return Request(self.index, self.offset, self.length)

  def __eq__(self, other):
    if not isinstance(other, Piece):
      return False
    return super(Piece, self).__eq__(other) and self.block == other.block

  def __str__(self):
    return 'Piece(%s[%s:%s]*)' % (self.index, self.offset, self.offset + self.length)
