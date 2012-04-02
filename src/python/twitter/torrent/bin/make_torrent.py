from twitter.common import app, log
from twitter.torrent.metainfo import Torrent, MetaInfoBuilder

app.add_option('--torrent', metavar='FILENAME', default=None,
               help='Filename to store torrent.')
app.add_option('--tracker', metavar='URL', default=None,
               help='URL of tracker for this torrent.')

def main(args, options):
  mib = MetaInfoBuilder()
  for fn in args:
    mib.add(fn)
  mi = mib.build()
  torrent = Torrent()
  torrent.info = mi
  torrent.announce = options.tracker
  print('Writing torrent to file: %s' % options.torrent)
  torrent.to_file(options.torrent)

app.main()
