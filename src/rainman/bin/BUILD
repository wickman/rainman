python_requirement('twitter.common.app')
python_requirement('twitter.common.http')


python_binary(
  name = 'client',
  source = 'client.py',
  dependencies = [
    pants(':twitter.common.app'),
    pants('src/rainman'),
  ]
)


python_binary(
  name = 'make_torrent',
  source = 'make_torrent.py',
  dependencies = [
    pants(':twitter.common.app'),
    pants('src/rainman'),
  ]
)


python_binary(
  name = 'tracker',
  source = 'tracker.py',
  dependencies = [
    pants('src/rainman'),
    pants('src/rainman:tornado'),
    pants(':twitter.common.app'),
    pants(':twitter.common.http'),
  ]
)
