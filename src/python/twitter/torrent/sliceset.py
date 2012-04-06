import sys

__all__ = ('SliceSet',)


# Cribbed directly from the bisect module, but allowing support for
# bisecting off the left or right key of the interval.
def bisect_left(a, x, start=True, lo=0, hi=None):
  if lo < 0:
    raise ValueError('lo must be non-negative')
  if hi is None:
    hi = len(a)
  while lo < hi:
    mid = (lo+hi)//2
    if (a[mid].start if start else a[mid].stop) < (x.start if start else x.stop):
      lo = mid+1
    else:
      hi = mid
  return lo


class SliceSet(object):
  def __init__(self):
    self._slices = []

  @property
  def slices(self):
    return self._slices

  @staticmethod
  def _contains(slice1, slice2):
    # slice1 \in slice2
    return slice1.start >= slice2.start and slice1.stop <= slice2.stop

  @staticmethod
  def _merge(slice1, slice2):
    # presuming they intersect
    return slice(min(slice1.start, slice2.start), max(slice1.stop, slice2.stop))

  @staticmethod
  def assert_valid_slice(slice_):
    assert slice_.step is None          # only accept contiguous slices
    assert slice_.stop >= slice_.start  # only accept ascending slices
                                        # consider accepting open intervals?

  # slices are [left, right) file intervals.
  def add(self, slice_):
    print 'Adding %s to %s' % (slice_, self.slices)
    SliceSet.assert_valid_slice(slice_)

    # find its spot
    k = bisect_left(self._slices, slice_)
    self._slices.insert(k, slice_)

    # merge any overlapping slices
    if k > 0 and self._slices[k-1].stop == slice_.start:
      k = k - 1
    while k < len(self._slices) - 1:
      if self._slices[k].stop < self._slices[k + 1].start:
        break
      self._slices[k] = SliceSet._merge(self._slices[k], self._slices.pop(k + 1))

  def erase(self, slice_):
    print 'Erasing %s from %s' % (slice_, self.slices)
    if len(self._slices) == 0:
      return
    L = max(0, bisect_left(self._slices, slice_) - 1)
    if self._slices[L].start <= slice_.start < self._slices[L].stop:
      tr = slice(self._slices[L].start, slice_.start)
      if tr.stop - tr.start > 0:
        self._slices.insert(L, slice(self._slices[L].start, slice_.start))
    while self._slices[L].stop <= slice_.start:
      L += 1
    while L < len(self._slices) and slice_.stop >= self._slices[L].stop:
      self._slices.pop(L)
    if L < len(self._slices) and slice_.stop > self._slices[L].start:
      self._slices[L] = slice(slice_.stop, self._slices[L].stop)

  def missing_in(self, slice_):
    SliceSet.assert_valid_slice(slice_)

    def top_iter():
      if len(self._slices) == 0:
        yield slice_
        return
      L = max(0, bisect_left(self._slices, slice_) - 1)
      R = bisect_left(self._slices, slice_, start=False)
      yield slice(-sys.maxint, self._slices[L].start)
      while L <= R and (L + 1) < len(self._slices):
        yield slice(self._slices[L].stop, self._slices[L+1].start)
        L += 1
      yield slice(self._slices[max(L, len(self._slices)-1)].stop,
                  sys.maxint if (L+1) >= len(self._slices) else self._slices[L+1].start)
    for element in top_iter():
      isect = slice(max(slice_.start, element.start), min(slice_.stop, element.stop))
      if isect.stop > isect.start:
        yield isect

  def __contains__(self, slice_):
    if isinstance(slice_, int):
      slice_ = slice(slice_, slice_)
    if not isinstance(slice_, slice):
      raise ValueError('SliceSet.__contains__ expects an integer or another slice.')
    k = bisect_left(self._slices, slice_)
    def check(index):
      return index >= 0 and len(self._slices) > index and (
          SliceSet._contains(slice_, self._slices[index]))
    return check(k) or check(k-1)

  def __iter__(self):
    return iter(self._slices)
