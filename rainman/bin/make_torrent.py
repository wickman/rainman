import os

from rainman.metainfo import MetaInfoBuilder
from rainman.torrent import Torrent

from twitter.common import app, log


app.set_usage(
'''
make_torrent [-r relpath] <torrent filename> <tracker url> path1 path2 ...

Paths may be either filenames or directories.  If a directory is specified,
its contents will be recursively walked and stored in the torrent.

If relpath is specified, all files in the torrent will be relative to that
path.
''')

app.add_option('-r', dest='relpath', default=None,
               help='The relative path for all files within the torrent.')


def main(args, options):
  if len(args) <= 2:
    app.help()

  torrent_name, tracker = args[0:2]

  filelist = []

  def add_file(filename):
    print('Adding %s' % filename)
    filelist.append(os.path.realpath(filename))

  def add_dir(dirname):
    for root, _, files in os.walk(dirname):
      for filename in files:
        add_file(os.path.join(root, filename))

  def add(file_or_dir):
    if os.path.isfile(file_or_dir):
      add_file(file_or_dir)
    elif os.path.isdir(file_or_dir):
      add_dir(file_or_dir)
    else:
      app.error('Unknown or non-existent file: %s' % file_or_dir)

  for path in args[2:]:
    add(path)

  if options.relpath:
    relpath = os.path.realpath(options.relpath)
  else:
    relpath = os.path.realpath('.')

  mib = MetaInfoBuilder(filelist, relpath=relpath)
  torrent = Torrent.from_metainfo(mib.build(), tracker)

  print('Writing torrent to file: %s' % torrent_name)
  torrent.to_file(torrent_name)


app.main()
