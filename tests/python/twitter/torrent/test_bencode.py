import random
import sys
from twitter.torrent.bencode import BEncoder, BDecoder


def test_encode_int():
  for number in range(0,11) + [31337]:
    assert BEncoder.encode(number) == BEncoder.encode_int(number)
    assert BEncoder.encode(-number) == BEncoder.encode_int(-number)
    assert BDecoder.decode(BEncoder.encode(number))[0] == number

def test_encode_string():
  unicode_string = '\U0001f648' if sys.version_info[0] == 3 else u'\U0001f648'
  for string in ('', ':', 'l', 'e', 'l:e', 'el', unicode_string):
    assert BEncoder.encode(string) == BEncoder.encode_str(string)
    assert BDecoder.decode(BEncoder.encode(string))[0] == string

def test_encode_list():
  SEED = 31337
  chooser = random.Random(SEED).choice
  values = ('', ':', 'l', 'e', 'l:e', 'el',
            0, 1, 2, 31337,
            [':', 'l:e', 0],
            {'hello': 'world', '': 27})

  for k in range(0, 10):
    random_list = [chooser(values) for j in range(k)]
    assert BEncoder.encode(random_list) == BEncoder.encode_list(random_list)
    assert BDecoder.decode(BEncoder.encode(random_list))[0] == random_list

def test_encode_dict():
  SEED = 31337
  rand = random.Random(SEED)

  keys = ('', ':', 'l', 'e', 'l:e', 'el', 'hello', 'world')
  values = ('', ':', 'l', 'e', 'l:e', 'el',
            0, 1, 2, 31337,
            [':', 'l:e', 0],
            {'hello': 'world', '': 27})

  for k in range(0, len(keys)):
    random_keys = rand.sample(keys, k)
    random_dict = dict((random_key, rand.choice(values)) for random_key in random_keys)
    assert BEncoder.encode(random_dict) == BEncoder.encode_dict(random_dict)
    assert BDecoder.decode(BEncoder.encode(random_dict))[0] == random_dict
