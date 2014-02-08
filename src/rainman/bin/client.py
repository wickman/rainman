import code
import time

from rainman.metainfo import Torrent
from rainman.session import Session, PeerSet

from twitter.common import app


app.add_option(
    '--torrent',
    metavar='FILENAME',
    default=None,
    help='Torrent filename to start session on.')

app.add_option(
    '--chroot',
    metavar='PATH',
    default=None,
    dest='chroot',
    help='Directory that roots the torrent.  Temporary directory will be '
          'created if unspecified.')

app.add_option(
    '-i', '--interactive',
    default=False,
    action='store_true',
    help='Drop into an interpreter before activating session.')


def main(args, options):
  torrent = Torrent.from_file(options.torrent)
  session = Session(torrent, options.chroot)
  if options.interactive:
    code.interact(local=locals())
  else:
    session.start()
  while True:
    time.sleep(10)


app.main()
