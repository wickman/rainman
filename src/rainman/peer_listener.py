import errno

from tornado.tcpserver import TCPServer
from twitter.common import log


class PeerListener(TCPServer):
  class BindError(Exception): pass

  PORT_RANGE = range(6881, 6890)

  def __init__(self, handler, io_loop=None, port=None):
    self._handler = handler
    super(PeerListener, self).__init__(io_loop=io_loop)
    port_range = [port] if port else self.PORT_RANGE
    for port in port_range:
      try:
        log.debug('Peer listener attempting to bind @ %s' % port)
        self.listen(port)
        break
      except IOError as e:
        if e.errno == errno.EADDRINUSE:
          continue
    else:
      raise self.BindError('Could not bind to any port in range %s' % repr(port_range))
    log.debug('Bound at port %s' % port)
    self._port = port

  @property
  def port(self):
    return self._port

  def handle_stream(self, iostream, address):
    self._handler(address, iostream)
