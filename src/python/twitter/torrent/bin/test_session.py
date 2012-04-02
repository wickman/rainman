import code
import time

from twitter.common import app
from twitter.torrent.session import Session, PeerSet
from twitter.torrent.metainfo import Torrent

app.add_option('--torrent', metavar='FILENAME', default=None,
               help='Torrent filename to start session on.')
app.add_option('--interactive', default=False, action='store_true',
               help='Drop into an interpreter before activating session.')


def main(args, options):
  torrent = Torrent.from_file(options.torrent)
  session = Session(torrent)
  if options.interactive:
    code.interact(local=locals())
  else:
    session.start()
  session.join()

app.main()
