class PeerId(object):
  PREFIX = '-TW7712-'  # TWTTR
  LENGTH = 20

  @classmethod
  def generate(cls):
    return cls.PREFIX + ''.join(random.sample('0123456789abcdef', cls.LENGTH - len(cls.PREFIX)))


class Client(object):
  def __init__(self, peer_id=None):
    self.peer_id = peer_id or PeerId.generate()
    self.listener = None
    self.torrents = {}   # hash => torrent
    self.databases = {}  # hash => FileManager
    self.sessions = {}   # hash => TorrentSession
  
  def add_torrent(self, torrent, chroot=None):
    pass
  
  def remove_torrent(self, torrent):
    pass

  def listen(self, port_or_range):
    pass
