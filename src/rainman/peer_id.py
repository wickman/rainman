import random


class PeerId(object):
  PREFIX = '-TW7712-'  # TWTTR
  LENGTH = 20

  @classmethod
  def generate(cls):
    return cls.PREFIX + ''.join(random.sample('0123456789abcdef', cls.LENGTH - len(cls.PREFIX)))
