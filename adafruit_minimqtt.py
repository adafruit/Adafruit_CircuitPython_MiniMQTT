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

MQTT Library for CircuitPython.

* Author(s): Brent Rubell

Implementation Notes
--------------------

**Software and Dependencies:**

* Adafruit CircuitPython firmware for the supported boards:
  https://github.com/adafruit/circuitpython/releases

"""
import time
import struct
from micropython import const
from random import randint
import microcontroller

__version__ = "0.0.0-auto.0"
__repo__ = "https://github.com/adafruit/Adafruit_CircuitPython_MiniMQTT.git"


# length of maximum mqtt message
MQTT_MSG_MAX_SZ = const(268435455)
MQTT_TOPIC_SZ_LIMIT = const(65536)

# MQTT Connection Errors
MQTT_ERR_INCORRECT_SERVER = const(3)
MQTT_ERR_INVALID = const(4)

def handle_mqtt_error(mqtt_err):
    """Returns string associated with MQTT error number.
    :param int mqtt_err: MQTT error number.
    """
    if mqtt_err == MQTT_ERR_INCORRECT_SERVER:
        raise MiniMQTTException("Invalid server address defined.")
    elif mqtt_err == MQTT_ERR_INVALID:
        raise MiniMQTTException("Invalid method arguments provided.")
    else:
        raise MiniMQTTException("Unknown error!")

class MiniMQTTException(Exception):
    pass

class MQTT:
    """
    MQTT Client for CircuitPython.
    """
    TCP_MODE = const(0)
    TLS_MODE = const(2)
    def __init__(self, esp, socket, wifimanager, server_address, port=1883, user=None,
                    password = None, client_id=None, is_ssl=False):
        self._esp = esp
        self._socket = socket
        self._wifi_manager = wifimanager
        self.port = port
        if is_ssl:
            self.port = 8883
        self.user = user
        self._pass = password
        if client_id is not None:
            self._client_id = client_id
        else: # randomize client identifier, prevent duplicate devices on broker
            self._client_id = 'cpy-{0}{1}'.format(microcontroller.cpu.uid[randint(0, 15)], randint(0, 9))
        self.server = server_address
        self.packet_id = 0
        self._keep_alive = 0
        self._handler_methods = {}
        self._lw_topic = None
        self._lw_msg = None
        self._lw_retain = False
        self._is_connected = False
        self._pid = 0
        self._msg_size_lim = const(10000000)

    def reconnect(self):
        """Attempts to reconnect to the MQTT broker."""
        failure_count = 0
        while not self._is_connected:
            try:
                self.connect(False)
            except OSError as e:
                print('Failed to connect to the broker, retrying\n', e)
                failure_count+=1
                if failure_count >= 30:
                    failure_count = 0
                    self._wifi_manager.reset()
                continue

    def is_connected(self):
        """Returns if there is an active MQTT connection."""
        return self._is_connected

    def connect(self, clean_session=True):
        """Initiates connection with the MQTT Broker.
        :param bool clean_session: Establishes a persistent session
            with the broker. Defaults to a non-persistent session.
        """
        if self._esp:   #TODO: This might be approachable without passing in a socket
            self._socket.set_interface(self._esp)
            self._sock = self._socket.socket()
        else:
            raise TypeError('ESP32SPI interface required!')
        self._sock.settimeout(10)
        if self.port == 8883:
            try:
                self._sock.connect((self.server, self.port), TLS_MODE)
            except RuntimeError:
                handle_mqtt_error(MQTT_ERR_INCORRECT_SERVER)
        else:
            addr = self._socket.getaddrinfo(self.server, self.port)[0][-1]
            try:
                self._sock.connect(addr, TCP_MODE)
            except RuntimeError:
                self.handle_mqtt_error(MQTT_ERR_INCORRECT_SERVER)

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
            raise MiniMQTTException(resp[3])
        self._is_connected = True
        return resp[2] & 1

    def disconnect(self):
        """Disconnects from the broker.
        """
        self._sock.write(b"\xe0\0")
        self._sock.close()
        self._is_connected = False

    def ping(self):
        """Pings the broker.
        """
        self._sock.write(b"\xc0\0")


    @property
    def mqtt_msg(self):
        """Returns maximum MQTT payload and topic size."""
        return self._msg_size_lim, MQTT_TOPIC_SZ_LIMIT

    @mqtt_msg.setter
    def mqtt_msg(self, msg_size):
        """Sets the maximum MQTT message payload size.
        :param int msg_size: Maximum MQTT payload size.
        """
        if msg_size < MQTT_MSG_MAX_SZ:
            self.__msg_size_lim = msg_size

    def publish(self, topic, msg, retain=False, qos=0):
        """Publishes a message to the MQTT broker.
        :param str topic: Unique topic identifier.
        :param str msg: Data to send to the broker.
        :param bool retain: Whether the message is saved by the broker.
        :param int qos: Quality of Service level for the message.
        """
        # check topic kwarg
        if topic is None or len(topic) == 0:
            raise MQTTException('Invalid topic.')
        if b'+' in topic or b'#' in topic:
            raise MQTTException('Topic can not contain wildcards.')
        # check msg/qos args
        if len(msg) > MQTT_MSG_SZ_LIMIT:
            raise MQTTException('Message size larger than %db.'%MQTT_MSG_SZ_LIMIT)
        if qos < 0 or qos > 2:
            raise MQTTException('Invalid QoS, must be between 0 and 2.')
        # msg kwarg type conversions
        if msg is None:
            raise MQTTException('Message can not be None.')
        elif isinstance(msg, (int, float)):
            msg = str(msg).encode('ascii')
        elif isinstance(msg, unicode):
            msg = str(msg).encode('utf-8')
        pkt = bytearray(b"\x30\0")
        pkt[0] |= qos << 1 | retain
        sz = 2 + len(topic) + len(msg)
        if qos > 0:
            sz += 2
        assert sz < 2097152
        i = 1
        while sz > 0x7f:
            pkt[i] = (sz & 0x7f) | 0x80
            sz >>= 7
            i += 1
        pkt[i] = sz
        self._sock.write(pkt)
        self._send_str(topic)
        if qos > 0:
            self.pid += 1
            pid = self.pid
            struct.pack_into("!H", pkt, 0, pid)
            self._sock.write(pkt)
        if type(msg) == str:
            msg = str.encode(msg, 'utf-8')
        self._sock.write(msg)
        if qos == 1:
            while 1:
                op = self.wait_msg()
                if op == 0x40:
                    sz = self._sock.read(1)
                    assert sz == b"\x02"
                    rcv_pid = self._sock.read(2)
                    rcv_pid = rcv_pid[0] << 8 | rcv_pid[1]
                    if pid == rcv_pid:
                        return
        elif qos == 2:
            assert 0

    def subscribe(self, topic, handler_method=None, qos=0):
        """Sends a subscribe message to the MQTT broker.
        :param str topic: Unique topic identifier.
        :param method handler_method: handler method for subscription topic. Defaults to default_sub_handler if None.
        :param int qos: Quality of Service level for the topic.
        """
        if handler_method is None:
            self._handler_methods.update( {topic : self.default_sub_handler} )
        else:
            self._handler_methods.update( {topic : custom_handler_method} )
        pkt = bytearray(b"\x82\0\0\0")
        self._pid += 11
        struct.pack_into("!BH", pkt, 1, 2 + 2 + len(topic) + 1, self._pid)
        self._sock.write(pkt)
        self._send_str(topic)
        self._sock.write(qos.to_bytes(1, "little"))
        while 1:
            op = self.wait_msg()
            if op == 0x90:
                resp = self._sock.read(4)
                assert resp[1] == pkt[2] and resp[2] == pkt[3]
                if resp[3] == 0x80:
                    raise MQTTException(resp[3])
                return

    def wait_msg(self, timeout=0):
        """Waits for and processes an incoming MQTT message.
        :param int timeout: Socket timeout.
        """
        self._sock.settimeout(timeout)
        res = self._sock.read(1)
        if res in [None, b""]:
            return None
        if res == b"\xd0":  # PINGRESP
            sz = self._sock.read(1)[0]
            assert sz == 0
            return None
        op = res[0]
        if op & 0xf0 != 0x30:
            return op
        sz = self._recv_len()
        topic_len = self._sock.read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = self._sock.read(topic_len)
        sz -= topic_len + 2
        if op & 6:
            pid = self._sock.read(2)
            pid = pid[0] << 8 | pid[1]
            sz -= 2
        msg = self._sock.read(sz)
        # call the topic's handler method
        if str(topic, 'utf-8') in self._handler_methods:
            handler_method = self._handler_methods[str(topic, 'utf-8')]
            handler_method(str(topic, 'utf-8'), str(topic, 'utf-8'))
        if op & 6 == 2:
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self._sock.write(pkt)
        elif op & 6 == 4:
            assert 0

    def _recv_len(self):
        """Receives the size of the topic length."""
        n = 0
        sh = 0
        while 1:
            b = self._sock.read(1)[0]
            n |= (b & 0x7f) << sh
            if not b & 0x80:
                return n
            sh += 7

    def default_sub_handler(self, topic, msg):
        """Default feed subscription handler method.
        :param str topic: Subscription topic.
        :param str msg: Payload content.
        """
        print('New message on {0}: {1}'.format(topic, msg))

    def _send_str(self, string):
        """Packs a string into a struct. and writes it to a socket.
        :param str string: String to write to the socket.
        """
        self._sock.write(struct.pack("!H", len(string)))
        if type(string) == str:
            self._sock.write(str.encode(string, 'utf-8'))
        else:
            self._sock.write(string)