from twitter.common.lang import Compatibility

class BEncoder(object):
  class Error(Exception): pass

  @staticmethod
  def assert_type(input, typ):
    if not isinstance(input, typ):
      raise BEncoder.Error("Input is of type %s, expected type %s" % (
        type(input).__name__, typ.__name__))

  @staticmethod
  def encode_int(input):
    BEncoder.assert_type(input, int)
    return 'i%se' % input

  @staticmethod
  def encode_str(input):
    BEncoder.assert_type(input, Compatibility.string)
    return '%d:%s' % (len(input), input)

  @staticmethod
  def encode_list(input):
    BEncoder.assert_type(input, list)
    return 'l%se' % ''.join(BEncoder.encode(element) for element in input)

  @staticmethod
  def encode_dict(input):
    BEncoder.assert_type(input, dict)
    for key in input:
      BEncoder.assert_type(key, str)
    return 'd%se' % ''.join(BEncoder.encode(key) + BEncoder.encode(value)
        for key, value in sorted(input.items()))

  @staticmethod
  def encode(input):
    input_type = type(input)
    if input_type not in BEncoder.INPUT_TYPES:
      raise BEncoder.Error("Unknown input type: %s" % input_type)
    return BEncoder.INPUT_TYPES[input_type](input)


BEncoder.INPUT_TYPES = {
  int: BEncoder.encode_int,
  str: BEncoder.encode_str,
  list: BEncoder.encode_list,
  dict: BEncoder.encode_dict
}

if Compatibility.PY2:
  BEncoder.INPUT_TYPES[unicode] = BEncoder.encode_str


class BDecoder(object):
  class Error(Exception): pass

  @staticmethod
  def decode_int(input):
    assert input[0] == 'i'
    e_index = input.index('e')
    return int(input[1:e_index]), e_index + 1

  @staticmethod
  def decode_str(input):
    assert ord(input[0]) >= ord('0') and ord(input[0]) <= ord('9'), 'Got %s' % input[0]
    colon_index = input.index(':')
    str_length = int(input[0:colon_index])
    return input[colon_index+1:colon_index+1+str_length], colon_index+1+str_length

  @staticmethod
  def decode_list(input):
    assert input[0] == 'l'
    offset = 1
    output = []
    while input[offset] != 'e':
      element, extra = BDecoder.decode(input[offset:])
      offset += extra
      output.append(element)
    return output, offset+1

  @staticmethod
  def decode_dict(input):
    assert input[0] == 'd'
    offset = 1
    output = {}
    while input[offset] != 'e':
      key, extra = BDecoder.decode(input[offset:])
      offset += extra
      value, extra = BDecoder.decode(input[offset:])
      offset += extra
      if not isinstance(key, str):
        raise BDecoder.Error('Dictionary expects keys to be string!')
      output[key] = value
    return output, offset+1

  @staticmethod
  def decode(input):
    try:
      if input[0] in BDecoder.INPUT_TYPES:
        return BDecoder.INPUT_TYPES[input[0]](input)
      else:
        return BDecoder.decode_str(input)
    except IndexError:
      raise BDecoder.Error('Unexpected end of stream decoding integer')
    except ValueError:
      raise BDecoder.Error('Value does not appear to be integer')


BDecoder.INPUT_TYPES = {
  'i': BDecoder.decode_int,
  'l': BDecoder.decode_list,
  'd': BDecoder.decode_dict
}
