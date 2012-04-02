import code
import time

from twitter.common import app
from twitter.torrent.session import Session, PeerSet
from twitter.torrent.metainfo import Torrent

app.add_option('--torrent', metavar='FILENAME', default=None,
               help='Torrent filename to start session on.')

def main(args, options):
  torrent = Torrent.from_file(options.torrent)
  print 'Got torrent: %s' % torrent
  session = Session(torrent, port=6181)
  peers = PeerSet(session)
  code.interact(local=locals())

app.main()
