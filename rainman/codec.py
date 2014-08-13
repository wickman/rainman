"""
Encode and decode bencoded data structures.

To use:
  from .codec import BEncoder, BDecoder
  byte_array = BEncoder.encode({'any': {'data': ['structure', 'that'], 'can': 1},
                                'be': ['expressed'],
                                'as': 'bencoded',
                                'form': 23})
  dictionary = BDecoder.decode(byte_array)
"""

from twitter.common.lang import Compatibility


class BEncoder(object):
  class Error(Exception): pass

  @classmethod
  def encode_int(cls, input):
    return 'i%se' % input

  @classmethod
  def encode_str(cls, input):
    return '%d:%s' % (len(input), input)

  @classmethod
  def encode_list(cls, input):
    return 'l%se' % ''.join(cls.encode(element) for element in input)

  @classmethod
  def encode_dict(cls, input):
    if any(not isinstance(key, Compatibility.string) for key in input):
      raise cls.Error('Invalid dictionary: not all keys are strings.')
    return 'd%se' % ''.join(
        cls.encode(key) + cls.encode(value) for key, value in sorted(input.items()))

  @classmethod
  def encode(cls, input):
    if isinstance(input, Compatibility.integer):
      return cls.encode_int(input)
    elif isinstance(input, Compatibility.string):
      return cls.encode_str(input)
    elif isinstance(input, (list, tuple)):
      return cls.encode_list(input)
    elif isinstance(input, dict):
      return cls.encode_dict(input)
    raise cls.Error("Unknown input type: %s" % type(input))


class BDecoder(object):
  class Error(Exception): pass

  @classmethod
  def decode_int(cls, input):
    assert input[0] == 'i'
    e_index = input.index('e')
    return int(input[1:e_index]), e_index + 1

  @classmethod
  def decode_str(cls, input):
    assert ord(input[0]) >= ord('0') and ord(input[0]) <= ord('9'), 'Got %s' % input[0]
    colon_index = input.index(':')
    str_length = int(input[0:colon_index])
    return input[colon_index + 1:colon_index + 1 + str_length], colon_index + 1 + str_length

  @classmethod
  def decode_list(cls, input):
    assert input[0] == 'l'
    offset = 1
    output = []
    while input[offset] != 'e':
      element, extra = cls.decode(input[offset:])
      offset += extra
      output.append(element)
    return output, offset + 1

  @classmethod
  def decode_dict(cls, input):
    assert input[0] == 'd'
    offset = 1
    output = {}
    while input[offset] != 'e':
      key, extra = cls.decode(input[offset:])
      offset += extra
      value, extra = cls.decode(input[offset:])
      offset += extra
      if not isinstance(key, str):
        raise cls.Error('Dictionary expects keys to be string!')
      output[key] = value
    return output, offset + 1

  @classmethod
  def decode(cls, input):
    try:
      if input[0] == 'i':
        return cls.decode_int(input)
      elif input[0] == 'l':
        return cls.decode_list(input)
      elif input[0] == 'd':
        return cls.decode_dict(input)
      else:
        return cls.decode_str(input)
    except IndexError:
      raise cls.Error('Unexpected end of stream decoding integer')
    except ValueError:
      raise cls.Error('Value does not appear to be integer')
