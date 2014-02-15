import time

from twitter.common.quantity import Amount, Time


class TimeDecayMap(object):
  """A time-decaying map that drops entries older than a timeout."""

  DEFAULT_WINDOW = Amount(15, Time.SECONDS)

  def __init__(self, window=DEFAULT_WINDOW, clock=time):
    self._window = window.as_(Time.SECONDS)
    self._clock = clock
    self._elements = {}
    self._outstanding = 0

  @property
  def outstanding(self):
    """The number of outstanding requests.  This number is only an estimate
       since request filtering takes place on __getitem__/__contains__"""
    return self._outstanding

  def add(self, key, peer_id):
    """Indicate that we have requested key from peer_id"""
    if key not in self._elements:
      self._elements[key] = []
    self._elements[key].append((self._clock.time(), peer_id))
    self._outstanding += 1

  def remove(self, key):
    """returns the list of peers to whom this request is outstanding."""
    self._filter(key)
    if key not in self._elements:
      return []
    requests = [peer for _, peer in self._elements.pop(key)]
    self._outstanding -= len(requests)
    return requests

  def __contains__(self, key):
    self._filter(key)
    return key in self._elements

  def contains(self, key, peer_id):
    now = self._clock.time()
    for timestamp, peer in self._elements.get(key, []):
      if (timestamp - now) < self._window and peer == peer_id:
        return True
    return False

  def _filter(self, key):
    """Filter expired slices."""
    now = self._clock.time()
    if key not in self._elements:
      return
    requests = len(self._elements[key])
    def too_old(pair):
      timestamp, peer_id = pair
      return (now - pair[0]) >= self._window
    left = [pair for pair in self._elements[key] if not too_old(pair)]
    if left:
      self._elements[key] = left
      self._outstanding -= requests - len(left)
    else:
      self._elements.pop(key)
      self._outstanding -= requests

  def __getitem__(self, key):
    """Get the list of non-expired peers to whom this key has been requested."""
    self._filter(key)
    if key not in self._elements:
      return []
    return [peer for _, peer in self._elements[key]]

  def __len__(self):
    return len(list(iter(self)))

  def __iter__(self):
    now = self._clock.time()
    for key, element_list in self._elements:
      old_size = len(element_list)
      element_list = self._elements[key] = [
          (timestamp, peer_id) for (timestamp, peer_id) in element_list
          if (now - timestamp) < self._window]
      self._outstanding -= old_size - len(element_list)
      for _, peer_id in element_list:
        yield key, peer_id




"""
class TimeDecaySet(dict):
  DEFAULT_WINDOW = Amount(15, Time.SECONDS)

  def __init__(self, window=DEFAULT_WINDOW, clock=time):
    self.__window = window.as_(Time.SECONDS)
    self.__clock = clock
    self.__timeouts = {}

  def add(self, element):
    self[element] = self.__clock.time() + self.__window

  def discard(self, element):
    self.pop(element)

  def __contains__(self, element):
    if not super(TimeDecayMap, self).__contains__(element):
      return False
    if self[element] <= self.__clock.time():
      self.pop(element)
      return False
    return True

  def __iter__(self):
    now = self.__clock.time():
    for element, timestamp in self.items():
      if timestamp >= now:
        yield element

class TimeDecaySet(object):
  DEFAULT_WINDOW = Amount(15, Time.SECONDS)

  def __init__(self, window=DEFAULT_WINDOW, clock=time):
    self.__window = window.as_(Time.SECONDS)
    self.__clock = clock
    self.__elements = {}

  def __len__(self):
    return len(list(iter(self)))

  def add(self, element, peer):
    self.__elements[element] = (self.__clock.time() + self.__window, peer)

  def discard(self, element):
    self.__elements.pop(element)

  def __contains__(self, element):
    if element not in self.__elements:
      return False
    if self.__elements[element][0] <= self.__clock.time():
      self.__elements.pop(element)
      return False
    return True

  def __iter__(self):
    now = self.__clock.time():
    to_discard = []
    for element, (timestamp, peer) in self.__elements.items():
      if timestamp >= now:
        yield element, peer
      else:
        to_discard.append(element)
    for element in to_discard:
      self.discard(element)
"""
