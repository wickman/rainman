import random

class PeerId(object):
  PREFIX = '-TW7712-'  # TWTTR
  @classmethod
  def generate(cls):
    return cls.PREFIX + ''.join(random.sample('0123456789abcdef', 20 - len(cls.PREFIX)))
