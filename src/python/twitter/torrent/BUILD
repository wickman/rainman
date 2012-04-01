python_library(
  name = 'torrent',
  sources = globs('*.py'),
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/http'),
    pants('src/python/twitter/common/quantity'),
    python_requirement('tornado')
  ]
)

python_binary(
  name = 'tracker',
  entry_point = 'twitter.torrent.tracker',
  dependencies = [
    pants(':torrent'),
    python_requirement('tornado'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:all')
  ]
)
