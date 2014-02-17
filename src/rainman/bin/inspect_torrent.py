from __future__ import print_function

import binascii
import os

from rainman.torrent import Torrent

from twitter.common import app


app.set_usage('''inspect_torrent <torrent filename>''')

def main(args, options):
  if len(args) != 1:
    app.help()

  torrent = Torrent.from_file(args[0])
  
  print('Torrent: %s' % args[0])
  print('Announce url: %s' % torrent.announce)

  print('Metainfo:')
  files = list(torrent.info.files(rooted_at=''))
  print('  name: %s' % torrent.info.name)
  print('  size: %d' % torrent.info.length)
  print('  files: %d' % len(files))
  print('  pieces: %d' % torrent.info.num_pieces)
  print('  piece size: %d' % torrent.info.piece_size)
  print()

  print('Hash manifest:')
  for index, hash in enumerate(torrent.info.piece_hashes):
    print('   [%4d]: %s' % (index, binascii.hexlify(hash)))
  print()
  
  print('File manifest:')
  for mif in files:
    print('  offset: [%9d-%9d] size: [%9d] filename: %s' % (mif.start, mif.end, mif.length, mif.name))
  

app.main()
