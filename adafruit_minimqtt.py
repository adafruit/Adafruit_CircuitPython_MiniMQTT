# The MIT License (MIT)
#
# Copyright (c) 2019 Brent Rubell for Adafruit Industries
#
# Original Work Copyright (c) 2016 Paul Sokolovsky, uMQTT
# Modified Work Copyright (c) 2019 Bradley Beach, esp32spi_mqtt
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
`adafruit_minimqtt`
================================================================================

MQTT client library for CircuitPython


* Author(s): Brent Rubell

Implementation Notes
--------------------

**Software and Dependencies:**

* Adafruit CircuitPython firmware for the supported boards:
  https://github.com/adafruit/circuitpython/releases

.. todo:: Uncomment or remove the Bus Device and/or the Register library dependencies based on the library's use of either.

* Adafruit's ESP32SPI library: https://github.com/adafruit/Adafruit_CircuitPython_ESP32SPI/
"""
import time
import struct

__version__ = "0.0.0-auto.0"
__repo__ = "https://github.com/adafruit/Adafruit_CircuitPython_MiniMQTT.git"


class MQTT:
    """
    MQTT Client for CircuitPython.
    """
    def __init__(self, esp, socket, wifimanager, client_id, server_address, port=1883, user=None,
                    password = None, is_ssl=False):
        self._esp = esp
        self._socket = socket
        self._wifi_manager = wifimanager
        self.port = port
        self.user = user
        self._pass = password
        self._client_id = client_id
        self.server = server_address
        self._is_ssl = is_ssl
        self.packet_id = 0
        self._keep_alive = 0
        self._cb = None
        self._lw_topic = None
        self._lw_msg = None
        self._lw_retain = False
    
    def connect(self, clean_session=True):
        """Initiates connection with the MQTT Broker.
        :param bool clean_session: Establishes a persistent session
            with the broker. Defaults to a non-persistent session.
        """
        #TODO: This might be approachable without passing in a socket
        if self._esp:
            self._socket.set_interface(self._esp)
            conn_type = 2 # TCP Mode
            self._sock = self._socket.socket()
        else:
            raise TypeError('ESP32SPI interface required!')
        #addr = self._socket.getaddrinfo(self.server, self.port)[0][-1]
        #print(addr)
        self._sock.settimeout(10)
        if self._is_ssl:
            raise NotImplementedError('SSL not implemented yet!')
        else:
            self._sock.connect((self.server, self.port), conn_type)
        premsg = bytearray(b"\x10\0\0")
        msg = bytearray(b"\x04MQTT\x04\x02\0\0")
        msg[6] = clean_session << 1
        sz = 10 + 2 + len(self._client_id)
        if self.user is not None:
            sz += 2 + len(self.user) + 2 + len(self._pass)
            msg[6] |= 0xC0
        if self._keep_alive:
            assert self._keep_alive < 65536
            msg[7] |= self._keep_alive >> 8
            msg[8] |= self._keep_alive & 0x00FF
        if self._lw_topic:
            sz += 2 + len(self._lw_topic) + 2 + len(self._lw_msg)
            msg[6] |= 0x4 | (self._lw_qos & 0x1) << 3 | (self._lw_qos & 0x2) << 3
            msg[6] |= self._lw_retain << 5
        i = 1
        while sz > 0x7f:
            premsg[i] = (sz & 0x7f) | 0x80
            sz >>= 7
            i += 1
        premsg[i] = sz

        self._sock.write(premsg)
        self._sock.write(msg)
        self._send_str(self._client_id)
        if self._lw_topic:
            self._send_str(self._lw_topic)
            self._send_str(self._lw_msg)
        if self.user is not None:
            self._send_str(self.user)
            self._send_str(self._pass)
        resp = self._sock.read(4)
        assert resp[0] == 0x20 and resp[1] == 0x02
        if resp[3] !=0:
            raise TypeError(resp[3]) #todo: make this a mqttexception
        return resp[2] & 1
    
    def _send_str(self, string):
        """Packs a string into a struct. and writes it to a socket.
        :param str string: String to write to the socket.
        """
        self._sock.write(struct.pack("!H", len(string)))
        if type(string) == str:
            self._sock.write(str.encode(string, 'utf-8'))
        else:
            self._sock.write(string)