python_library(
  name = 'torrent',
  sources = globs('*.py'),
  dependencies = [
    pants('src/python/twitter/common/collections'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),      # TODO(wickman): Consider pulling this out
    pants('src/python/twitter/common/quantity'),
    python_requirement('tornado')
  ]
)
