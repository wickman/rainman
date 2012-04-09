python_binary(
  name = 'client',
  source = 'client.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
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

python_binary(
  name = 'tracker',
  source = 'tracker.py',
  dependencies = [
    python_requirement('tornado'),
    pants('src/python/twitter/torrent'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:all'),
    pants('src/python/twitter/common/http'),
  ]
)
