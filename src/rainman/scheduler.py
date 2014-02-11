import datetime
import random
import time

from tornado import gen
from twitter.common import log
from twitter.common.quantity import Amount, Data, Time


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


class Scheduler(object):
  """Given a piece broker and set of peers, schedule ingress/egress appropriately.

    From BEP-003:

    The currently deployed choking algorithm avoids fibrillation by only
    changing who's choked once every ten seconds.  It does reciprocation and
    number of uploads capping by unchoking the four peers which it has the
    best download rates from and are interested.  Peers which have a better
    upload rate but aren't interested get unchoked and if they become
    interested the worst uploader gets choked.  If a downloader has a complete
    file, it uses its upload rate rather than its download rate to decide who
    to unchoke.

    For optimistic unchoking, at any one time there is a single peer which is
    unchoked regardless of it's upload rate (if interested, it counts as one
    of the four allowed downloaders.) Which peer is optimistically unchoked
    rotates every 30 seconds.  To give them a decent chance of getting a
    complete piece to upload, new connections are three times as likely to
    start as the current optimistic unchoke as anywhere else in the rotation.

    Uses:
      session.pieces        (TODO: pull this into scheduler and use callbacks)
      session.filemanager
      session.io_loop
      session.owners
  """
  # Scheduling constants
  MAX_UNCHOKED_PEERS = 5
  MAX_PEERS = 50
  MAX_REQUESTS = 100
  CONSIDERED_PIECES = 20
  REQUEST_SIZE = Amount(16, Data.KB)
  OUTER_YIELD = datetime.timedelta(0, 0, Amount(10, Time.MILLISECONDS).as_(Time.MICROSECONDS))
  INNER_YIELD = datetime.timedelta(0, 0, Amount(1, Time.MILLISECONDS).as_(Time.MICROSECONDS))

  # TODO(wickman) Allow external control of:
  #    target connections
  #    target ingress
  #    target egress
  def __init__(self, session):  # piece_broker, add_peer(...), remove_peer(...)
    self._session = session
    self._requests = TimeDecayMap()
    self._pieces
    self._schedules = 0
    self._timer = None
    self._active = True

  @gen.engine
  def schedule(self):
    while self._active:
      session_bitfield = self._session.bitfield  # filemanager.bitfield
      rarest = [index for index in self._session.pieces.rarest() if not session_bitfield[index]]  # this should own pieces
      rarest = rarest[:self.CONSIDERED_PIECES]
      random.shuffle(rarest)

      for piece_index in rarest:
        # find owners of this piece that are not choking us or for whom we've not yet registered
        # intent to download
        owners = [peer for peer in self._session.owners(piece_index)]
        if not owners:
          continue

        # don't bother scheduling unless there are unchoked peers or peers
        # we have not told we are interested
        if len([peer for peer in owners if not peer.choked or not peer.interested]) == 0:
          continue
        if self._requests.outstanding > self.MAX_REQUESTS:
          log.debug('Hit max requests, waiting.')
          yield gen.Task(self._session.io_loop.add_timeout, self.INNER_YIELD)

        # subpiece nomenclature is "block"
        request_size = self.REQUEST_SIZE.as_(Data.BYTES)
        for block in self._session.piece_broker.iter_blocks(piece_index, request_size):
          if block not in self._session.piece_broker and block not in self._requests:
            random_peer = random.choice(owners)
            if random_peer.choked:
              if not random_peer.interested:
                log.debug('Want to request %s from %s but we are choked, setting interested.' % (
                  block, random_peer))
              random_peer.interested = True
              continue
            log.debug('Scheduler requesting %s from peer [%s].' % (block, random_peer))
            # XXX sync
            random_peer.send_request(block)
            self._requests.add(block, random_peer)

      yield gen.Task(self._session.io_loop.add_timeout, self.OUTER_YIELD)

  def received(self, piece, from_peer):
    if piece not in self._requests:
      log.error('Expected piece in request set, not there!')
    for peer in self._requests.remove(piece):
      if peer != from_peer:
        log.debug('Sending cancellation to peer [%s]' % peer.id)
        # XXX sync
        peer.send_cancel(piece)

  def stop(self):
    self._active = False
