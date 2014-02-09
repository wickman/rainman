python_requirement('tornado>=3')

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
  ],
  provides = setup_py(
    name = 'rainman',
    version = '0.1.0',
    description = 'a pure python implementation of bittorrent on top of tornado.',
    license = 'MIT',
    author = 'Brian Wickman',
    author_email = 'wickman@gmail.com',
    url = 'https://github.com/wickman/rainman',
    classifiers = [
      'Programming Language :: Python',
      'Intended Audience :: Developers',
    ]
  )
)
