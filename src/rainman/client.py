from .peer_id import PeerId


class Client(object):
  def __init__(self, peer_id=None):
    self.peer_id = peer_id or PeerId.generate()
    self.ip = socket.gethostbyname(socket.gethostname()) # TODO: how to get external IP?
    self.listener = None
    self.torrents = {}   # hash => torrent
    self.databases = {}  # hash => FileManager
    self.sessions = {}   # hash => TorrentSession

  def add_torrent(self, torrent, chroot=None):
    pass

  def torrent_info(self, torrent):
    session = self.torrents[PeerHandshake.make(torrent.info)].session
    return {
      'info_hash': hashlib.sha1(torrent.info.raw()).digest(),
      'peer_id': self.peer_id,
      'ip': self.ip,
      'port': self.port,
      'uploaded': session.uploaded_bytes,
      'downloaded': session.downloaded_bytes,
      'left': torrent.info.length - session.piece_broker.assembled_bytes,
    }

  def remove_torrent(self, torrent):
    pass

  def listen(self, port_or_range):
    pass
