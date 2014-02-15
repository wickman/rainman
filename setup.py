from setuptools import setup, find_packages

setup(
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
  ],
  packages = ['rainman'],
  package_dir = {'rainman': 'src/rainman'},
  install_requires = [
    'tornado>=3,<4',
    'toro>=0.5,<1',
  ],
)
