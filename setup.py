from setuptools import setup

COMMONS_VERSION = '>=0.3.1,<0.4'

def make_commons_requirement(name):
  return 'twitter.common.{0}{1}'.format(name, COMMONS_VERSION)


setup(
  name = 'rainman',
  version = '0.2.0-rc0',
  description = 'a pure python implementation of bittorrent on top of tornado.',
  license = 'MIT',
  author = 'Brian Wickman',
  author_email = 'wickman@gmail.com',
  url = 'https://github.com/wickman/rainman',
  classifiers = [
    'Programming Language :: Python',
    'Intended Audience :: Developers',
  ],
  packages = [
    'rainman',
    'rainman.bin',
  ],
  install_requires = [
    'tornado>=3,<4',
    'toro>=0.5,<1',
    make_commons_requirement('collections'),
    make_commons_requirement('contextutil'),
    make_commons_requirement('dirutil'),
    make_commons_requirement('lang'),
    make_commons_requirement('log'),
    make_commons_requirement('quantity'),
  ],
  extras_require = {
    'app': [make_commons_requirement('app'),],
    'http': [make_commons_requirement('http'),],
  },
  entry_points = {
    'console_scripts': [
      'rainman_client = rainman.bin.client:main [app]',
      'rainman_tracker = rainman.bin.tracker:main [app,http]',
      'inspect_torrent = rainman.bin.inspect_torrent:main [app]',
      'make_torrent = rainman.bin.make_torrent:main [app]',
    ]
  }
)
