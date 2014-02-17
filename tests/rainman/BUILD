python_test_suite(name = 'all',
  dependencies = [
    pants(':test_bandwidth'),
    pants(':test_bitfield'),
    pants(':test_bounded_map'),
    pants(':test_client'),
    pants(':test_codec'),
    pants(':test_fileset'),
    pants(':test_integration'),
    pants(':test_iopool'),
    pants(':test_peer_driver'),
    pants(':test_piece_broker'),
    pants(':test_piece_manager'),
    pants(':test_piece_set'),
    pants(':test_sliceset'),
  ]
)

python_tests(
  name = 'test_bandwidth',
  sources = ['test_bandwidth.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_bitfield',
  sources = ['test_bitfield.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_bounded_map',
  sources = ['test_bounded_map.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_client',
  sources = ['test_client.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_codec',
  sources = ['test_codec.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_fileset',
  sources = ['test_fileset.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_integration',
  sources = ['test_integration.py'],
  dependencies = [
    python_requirement('mock'),
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_iopool',
  sources = ['test_iopool.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_peer_driver',
  sources = ['test_peer_driver.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_piece_broker',
  sources = ['test_piece_broker.py'],
  dependencies = [
    python_requirement('twitter.common.contextutil'),
    pants('src/rainman'),
  ]
)

python_tests(
  name = 'test_piece_manager',
  sources = ['test_piece_manager.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_piece_set',
  sources = ['test_piece_set.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)

python_tests(
  name = 'test_sliceset',
  sources = ['test_sliceset.py'],
  dependencies = [
    pants('src/rainman'),
  ],
)
