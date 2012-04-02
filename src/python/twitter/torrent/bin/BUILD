python_binary(
  name = 'test_session',
  source = 'test_session.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/torrent')
  ]
)

python_binary(
  name = 'make_torrent',
  source = 'make_torrent.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/torrent')
  ]
)
