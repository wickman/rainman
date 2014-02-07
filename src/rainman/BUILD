python_requirement('tornado<3')

python_library(
  name = 'rainman',
  sources = globs('*.py'),
  dependencies = [
    python_requirement('twitter.common.collections'),
    python_requirement('twitter.common.dirutil'),
    python_requirement('twitter.common.lang'),
    python_requirement('twitter.common.log'),
    python_requirement('twitter.common.quantity'),
    pants(':tornado'),
  ]
)
