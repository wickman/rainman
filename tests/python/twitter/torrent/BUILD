python_test_suite(name = 'torrent',
  dependencies = [
    pants(':test_codec'),
    pants(':test_bitfield'),
    pants(':test_fileset'),
  ]
)

python_tests(name = 'test_codec',
  sources = ['test_codec.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_bitfield',
  sources = ['test_bitfield.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)

python_tests(name = 'test_fileset',
  sources = ['test_fileset.py'],
  dependencies = [pants('src/python/twitter/torrent')]
)
