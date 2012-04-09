python_test_suite(name = 'torrent',
  dependencies = [
    pants(':test_bandwidth'),
    pants(':test_bitfield'),
    pants(':test_codec'),
    pants(':test_filemanager'),
    pants(':test_iopool'),
    pants(':test_sliceset'),
    pants(':test_tdm'),
    pants(':test_wire')
  ]
)

python_tests(name = 'test_bandwidth',
  sources = ['test_bandwidth.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_bitfield',
  sources = ['test_bitfield.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_codec',
  sources = ['test_codec.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_filemanager',
  sources = ['test_filemanager.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_iopool',
  sources = ['test_iopool.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_sliceset',
  sources = ['test_sliceset.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_tdm',
  sources = ['test_tdm.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_wire',
  sources = ['test_wire.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)
