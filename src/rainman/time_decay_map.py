import time

from twitter.common.quantity import Amount, Time


class TimeDecayMap(object):
  """A time-decaying map that drops entries older than a timeout."""

  DEFAULT_WINDOW = Amount(15, Time.SECONDS)

  def __init__(self, window=DEFAULT_WINDOW, clock=time):
    self._window = window.as_(Time.SECONDS)
    self._clock = clock
    self._slices = {}  # slice => list(peer)
    self._outstanding = 0

  @property
  def outstanding(self):
    """The number of outstanding requests.  This number is only an estimate
       since request filtering takes place on __getitem__/__contains__"""
    return self._outstanding

  def add(self, slice_, peer_id):
    """Indicate that we have requested slice_ from peer_id"""
    if slice_ not in self._slices:
      self._slices[slice_] = []
    self._slices[slice_].append((self._clock.time(), peer_id))
    self._outstanding += 1

  def remove(self, slice_):
    """returns the list of peers to whom this request is outstanding."""
    if slice_ not in self._slices:
      return []
    requests = [peer for _, peer in self._slices.pop(slice_)]
    self._outstanding -= len(requests)
    return requests

  def _filter(self, slice_):
    """Filter expired slices."""
    now = self._clock.time()
    if slice_ not in self._slices:
      return
    requests = len(self._slices[slice_])
    def too_old(pair):
      timestamp, peer_id = pair
      return (now - pair[0]) >= self._window
    left = [pair for pair in self._slices[slice_] if not too_old(pair)]
    if left:
      self._slices[slice_] = left
      self._outstanding -= requests - len(left)
    else:
      self._slices.pop(slice_)
      self._outstanding -= requests

  def __getitem__(self, slice_):
    """Get the list of non-expired peers to whom this slice_ has been requested."""
    self._filter(slice_)
    if slice_ not in self._slices:
      return []
    return [peer for _, peer in self._slices[slice_]]

  def __contains__(self, slice_):
    self._filter(slice_)
    return slice_ in self._slices
