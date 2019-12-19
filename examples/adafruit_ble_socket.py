# The MIT License (MIT)
#
# Copyright (c) 2019 ladyada for Adafruit Industries
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.

"""
`adafruit_ble_socket`
================================================================================

A socket compatible interface thru the nordic UART command set

* Author(s): ladyada, pascal@deneaux.de
"""

# pylint: disable=no-name-in-module

import time
import gc
from micropython import const

_the_interface = None   # pylint: disable=invalid-name
def set_interface(iface):
    """Helper to set the global internet interface"""
    global _the_interface   # pylint: disable=global-statement, invalid-name
    _the_interface = iface

SOCK_STREAM = const(1)
AF_INET = const(2)
NO_SOCKET_AVAIL = const(255)

MAX_PACKET = const(4000)

# pylint: disable=too-many-arguments, unused-argument
def getaddrinfo(host, port, family=0, socktype=0, proto=0, flags=0):
    """Given a hostname and a port name, return a 'socket.getaddrinfo'
    compatible list of tuples. Honestly, we ignore anything but host & port"""
    if not isinstance(port, int):
        raise RuntimeError("Port must be an integer")
    ipaddr = host
    return [(AF_INET, socktype, proto, '', (ipaddr, port))]
# pylint: enable=too-many-arguments, unused-argument

# pylint: disable=unused-argument, redefined-builtin, invalid-name
class socket:
    """A simplified implementation of the Python 'socket' class, for connecting
    through an interface to a remote device"""
    # pylint: disable=too-many-arguments
    def __init__(self, family=AF_INET, type=SOCK_STREAM, proto=0, fileno=None, socknum=None):
        if family != AF_INET:
            raise RuntimeError("Only AF_INET family supported")
        if type != SOCK_STREAM:
            raise RuntimeError("Only SOCK_STREAM type supported")
        self._buffer = b''

        self.settimeout(0)
    # pylint: enable=too-many-arguments

    def connect(self, address, conntype=None):
        """Connect the socket to the 'address' (which can be 32bit packed IP or
        a hostname string). 'conntype' is an extra that may indicate SSL or not,
        depending on the underlying interface"""
        host, port = address
        openSocket = bytearray()
        msg = "WebSocket('wss://" + host + ":" + str(port) + "/');"
        openSocket.extend(msg.encode('utf-8'))
        time.sleep(2)
        self.send(openSocket)
        time.sleep(1)
        ret = self.readline()
        if not ret == b'OK':
            raise RuntimeError("Failed to connect to host", host)
        self._buffer = b''

    def send(self, data):         # pylint: disable=no-self-use
        """Send some data to the socket"""
        #print("ble_socket send: " + ' '.join('{:02x}'.format(x) for x in data))
        _the_interface.write(data)
        gc.collect()

    def readline(self):
        """Attempt to return as many bytes as we can up to but not including '\r\n'"""
        return _the_interface.readline()

    def recv(self, bufsize=0):
        """Reads some bytes from the connected remote address.
        :param int bufsize: maximum number of bytes to receive
        """
        #print("Socket read", bufsize)
        if bufsize == 0:   # read as much as we can at the moment
            while True:
                avail = self.available()
                if avail:
                    self._buffer += _the_interface.read(avail)
                else:
                    break
            gc.collect()
            ret = self._buffer
            self._buffer = b''
            gc.collect()
            #print("ble_socket recv: " + ' '.join('{:02x}'.format(x) for x in ret)) # DEBUG Print
            return ret
        stamp = time.monotonic()

        to_read = bufsize - len(self._buffer)
        received = []
        while to_read > 0:
            #print("Bytes to read:", to_read) # it stucks here, waiting for an answer from mqtt server
            avail = self.available()
            if avail:
                stamp = time.monotonic()
                recv = _the_interface.read(min(to_read, avail))
                received.append(recv)
                to_read -= len(recv)
                gc.collect()
            if self._timeout > 0 and time.monotonic() - stamp > self._timeout:
                break
        self._buffer += b''.join(received)

        ret = None
        if len(self._buffer) == bufsize:
            ret = self._buffer
            self._buffer = b''
        else:
            ret = self._buffer[:bufsize]
            self._buffer = self._buffer[bufsize:]
        gc.collect()
        #print("ble_socket recv: " + ' '.join('{:02x}'.format(x) for x in ret)) # DEBUG Print
        return ret

    def read(self, size=0):
        """Read up to 'size' bytes from the socket, this may be buffered internally!
        If 'size' isnt specified, return everything in the buffer.
        NOTE: This method is deprecated and will be removed.
        """
        return self.recv(size)

    def settimeout(self, value):
        """Set the read timeout for sockets, if value is 0 it will block"""
        self._timeout = value

    def available(self):
        #print("def available(self): " + str(_the_interface.in_waiting))
        """Returns how many bytes of data are available to be read (up to the MAX_PACKET length)"""
        return _the_interface.in_waiting

    def connected(self):
        """Whether or not we are connected to the socket"""
        return _the_interface.in_waiting

    def close(self):
        """Close the socket, after reading whatever remains"""
        pass
# pylint: enable=unused-argument, redefined-builtin, invalid-name