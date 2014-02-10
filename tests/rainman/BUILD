python_test_suite(name = 'all',
  dependencies = [
    pants(':test_bandwidth'),
    pants(':test_bitfield'),
    pants(':test_codec'),
    pants(':test_fileset'),
    pants(':test_iopool'),
    pants(':test_piece_broker'),
    pants(':test_piece_manager'),
    pants(':test_sliceset'),
    pants(':test_tdm'),
    pants(':test_wire')
  ]
)

python_tests(name = 'test_bandwidth',
  sources = ['test_bandwidth.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_bitfield',
  sources = ['test_bitfield.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_codec',
  sources = ['test_codec.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_fileset',
  sources = ['test_fileset.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_iopool',
  sources = ['test_iopool.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_piece_broker',
  sources = ['test_piece_broker.py'],
  dependencies = [
    python_requirement('twitter.common.contextutil'),
    pants('src/rainman'),
  ]
)

python_tests(name = 'test_piece_manager',
  sources = ['test_piece_manager.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_sliceset',
  sources = ['test_sliceset.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_tdm',
  sources = ['test_tdm.py'],
  dependencies = [pants('src/rainman')]
)

python_tests(name = 'test_wire',
  sources = ['test_wire.py'],
  dependencies = [pants('src/rainman')]
)
