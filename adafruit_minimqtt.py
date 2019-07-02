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
MQTT_MSG_SZ_LIM = const(10000000)
MQTT_TOPIC_SZ_LIMIT = const(65536)

# General MQTT Errors
MQTT_ERR_INCORRECT_SERVER = const(3)
MQTT_ERR_INVALID = const(4)
MQTT_ERR_NO_CONN = const(5)
MQTT_INVALID_TOPIC = const(6)
MQTT_INVALID_QOS = const(7)
MQTT_INVALID_WILDCARD = const(8)

# PUBACK Errors
MQTT_PUBACK_OK = const(0x00)
MQTT_PUBACK_ERR_SUBS = const(0x10)
MQTT_PUBACK_ERR_UNSPECIFIED = const(0x80)
MQTT_PUBACK_ERR_IMPL = const(0x83)
MQTT_PUBACK_ERR_UNAUTHORIZED = const(0x87)
MQTT_PUBACK_ERR_INVALID_TOPIC = const(0x90)
MQTT_PUBACK_ERR_PACKETID = const(0x91)
MQTT_PUBACK_ERR_QUOTA = const(0x97)
MQTT_PUBACK_ERR_PAYLOAD = const(0x99)

# MQTT Spec. Commands
MQTT_TLS_PORT = const(8883)
MQTT_TCP_PORT = const(1883)
MQTT_PINGRESP = b'\xd0'
MQTT_SUB_PKT_TYPE = bytearray(b'\x82\0\0\0')
MQTT_DISCONNECT = b'\xe0\0'
MQTT_PING_REQ = b'\xc0\0'
MQTT_CON_PREMSG = bytearray(b"\x10\0\0") 
MQTT_CON_MSG = bytearray(b"\x04MQTT\x04\x02\0\0")

def handle_mqtt_error(mqtt_err):
    """Returns string associated with MQTT error number.
    :param int mqtt_err: MQTT error number.
    """
    if mqtt_err == MQTT_ERR_INCORRECT_SERVER:
        raise MMQTTException("Invalid server address defined.")
    elif mqtt_err == MQTT_ERR_INVALID:
        raise MMQTTException("Invalid method arguments provided.")
    elif mqtt_err == MQTT_ERR_NO_CONN:
        raise MMQTTException("MiniMQTT not connected.")
    elif mqtt_err == MQTT_INVALID_TOPIC:
        raise MMQTTException("Invalid MQTT Topic, must have length > 0.")
    elif mqtt_err == MQTT_INVALID_QOS:
        raise MMQTTException("Invalid QoS level,  must be between 0 and 2.")
    elif mqtt_err == MQTT_INVALID_WILDCARD:
        raise MMQTTException("Invalid MQTT Wildcard - must be * or #.")
    else:
        raise MMQTTException("Unknown error!")

class MMQTTException(Exception):
    pass

class MQTT:
    """
    MQTT Client for CircuitPython.
    :param esp: ESP32SPI object.
    :param socket: ESP32SPI Socket object.
    :param str server_address: Server URL or IP Address.
    :param int port: Optional port definition, defaults to 8883.
    :param str username: Username for broker authentication.
    :param str password: Password for broker authentication.
    :param str client_id: Optional client identifier, defaults to a randomly generated id.
    :param bool is_ssl: Enables TCP mode if false (port 1883). Defaults to True (port 8883).
    """
    TCP_MODE = const(0)
    TLS_MODE = const(2)
    def __init__(self, esp, socket, server_address, port=8883, username=None,
                    password = None, client_id=None, is_ssl=True):
        if esp and socket is not None:
            self._esp = esp
            self._socket = socket
        else:
            raise NotImplementedError('MiniMQTT currently only supports an ESP32SPI connection.')
        self.port = port
        if not is_ssl:
            self.port = MQTT_TCP_PORT
        self._user = username
        self._pass = password
        if client_id is not None:
            # user-defined client_id MAY allow client_id's > 23 bytes or
            # non-alpha-numeric characters
            self._client_id = client_id
        else:
            # assign a unique client_id
            self._client_id = 'cpy{0}{1}'.format(microcontroller.cpu.uid[randint(0, 15)], randint(0, 9))
            # generated client_id's enforce length rules
            if len(self._client_id) > 23 or len(self._client_id) < 1:
                raise ValueError('MQTT Client ID must be between 1 and 23 bytes')
        # subscription method handler dictionary
        self._method_handlers = {}
        self._is_connected = False
        self.server = server_address
        self.packet_id = 0
        self._keep_alive = 0
        self.last_will()
        self._pid = 0
        self._msg_size_lim = MQTT_MSG_SZ_LIM

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.deinit()

    def deinit(self):
        """Disconnects the MQTT client from the broker.
        """
        self.disconnect()

    def last_will(self, topic=None, message=None, qos=0, retain=False):
        """Sets the last will and testament properties. MUST be called before connect().
        :param str topic: MQTT Broker topic.
        :param str message: Last will disconnection message.
        :param int qos: Quality of Service level.
        :param bool retain: Specifies if the message is to be retained when it is published. 
        """
        if self._is_connected:
            raise MMQTTException('Last Will should be defined BEFORE connect() is called.')
        if qos < 0 or qos > 2:
            handle_mqtt_error(MQTT_INVALID_QOS)
        self._lw_qos = qos
        self._lw_topic = topic
        self._lw_msg = message
        self._lw_retain = retain

    def reconnect(self, retries=30):
        """Attempts to reconnect to the MQTT broker.
        :param int retries: Amount of retries before resetting the ESP32 hardware.
        """
        retries = 0
        while not self._is_connected:
            try:
                self.connect(False)
            except OSError as e:
                print('Failed to connect to the broker, retrying\n', e)
                retries+=1
                if retries >= 30:
                    retries = 0
                    self._esp.reset()
                continue
            # TODO: If we disconnected, we should re-subscribe to 
            # all the topics held in the dict!

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
                handle_mqtt_error(MQTT_ERR_INCORRECT_SERVER)

        premsg = MQTT_CON_PREMSG
        msg = MQTT_CON_MSG
        msg[6] = clean_session << 1
        sz = 12 + len(self._client_id)
        if self._user is not None:
            sz += 2 + len(self._user) + 2 + len(self._pass)
            msg[6] |= 0xC0
        if self._keep_alive:
            assert self._keep_alive < MQTT_TOPIC_SZ_LIMIT
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
        # [MQTT-3.1.3-4]
        self._send_str(self._client_id)
        if self._lw_topic:
            # [MQTT-3.1.3-11]
            self._send_str(self._lw_topic)
            self._send_str(self._lw_msg)
        if self._user is None:
            self._user = None
        else:
            self._send_str(self._user)
            self._send_str(self._pass)
        resp = self._sock.read(4)

        assert resp[0] == 0x20 and resp[1] == 0x02
        if resp[3] !=0:
            raise MMQTTException(resp[3])
        self._is_connected = True
        return resp[2] & 1

    def disconnect(self):
        """Disconnects from the broker.
        """
        if self._sock is None:
            handle_mqtt_error(MQTT_ERR_NO_CONN)
        self._sock.write(MQTT_DISCONNECT)
        self._sock.close()
        self._is_connected = False

    def ping(self):
        """Pings the broker.
        """
        self._sock.write(MQTT_PING_REQ)

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

    def publish_multiple(self, data, timeout=1.0):
        """Publishes to multiple MQTT broker topics.
        :param tuple data: A list of tuple format:
            :param str topic: Unique topic identifier.
            :param str msg: Data to send to the broker.
            :param bool retain: Whether the message is saved by the broker.
            :param int qos: Quality of Service level for the message.
        :param float timeout: Timeout between calls to publish(). This value
            is usually set by your MQTT broker. Defaults to 1.0
        """
        for i in range(len(data)):
            topic = data[i][0]
            msg = data[i][1]
            try:
                if data[i][2]:
                    retain = data[i][2]
            except IndexError:
                retain = False
                pass
            try:
                if data[i][3]:
                    qos = data[i][3]
            except IndexError:
                qos = 0
                pass
            self.publish(topic, msg, retain, qos)
            time.sleep(timeout)

    def publish(self, topic, msg, retain=False, qos=0):
        """Publishes a message to the MQTT broker.
        :param str topic: Unique topic identifier.
        :param str msg: Data to send to the broker.
        :param bool retain: Whether the message is saved by the broker.
        :param int qos: Quality of Service level for the message.
        """
        if topic is None or len(topic) == 0:
            handle_mqtt_error(MQTT_INVALID_TOPIC)
        if '+' in topic or '#' in topic:
            raise MMQTTException('Topic can not contain wildcards.')
        # check msg/qos kwargs
        if msg is None:
            raise MMQTTException('Message can not be None.')
        elif isinstance(msg, (int, float)):
            msg = str(msg).encode('ascii')
        elif isinstance(msg, str):
            msg = str(msg).encode('utf-8')
        else:
            raise MMQTTException('Invalid message data type.')
        if len(msg) > MQTT_MSG_MAX_SZ:
            raise MMQTTException('Message size larger than %db.'%MQTT_MSG_MAX_SZ)
        if qos < 0 or qos > 2:
            handle_mqtt_error(MQTT_INVALID_QOS)
        if self._sock is None:
            handle_mqtt_error(MQTT_ERR_NO_CONN)
        pkt = bytearray(b"\x30\0")
        pkt[0] |= qos << 1 | retain
        sz = 2 + len(topic) + len(msg)
        if qos > 0:
            sz += 2
        assert sz < 2097152
        i = 1
        while sz > 0x7f:
            pkt[i] = (sz & 0x7f) | const(0x80)
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
        self._sock.write(msg)
        if qos == 1:
            while 1:
                op = self.wait_for_msg()
                if op == const(0x40):
                    sz = self._sock.read(1)
                    assert sz == b"\x02"
                    rcv_pid = self._sock.read(2)
                    print('RCV PID: ', rcv_pid)
                    rcv_pid = rcv_pid[0] << 8 | rcv_pid[1]
                    if pid == rcv_pid:
                        return
        elif qos == 2:
            assert 0

    def subscribe(self, topic, method_handler=None, qos=0):
        """Subscribes to a topic on the MQTT Broker.
        This method can subscribe to one topics or multiple topics.
        :param str topic: Unique topic identifier.
        :param method method_handler: Predefined method for handling messages
            recieved from a topic. Defaults to default_sub_handler if None.
        :param int qos: Quality of Service level for the topic.

        Example of subscribing to one topic:
        .. code-block:: python
            mqtt_client.subscribe('topics/ledState')

        Example of subscribing to one topic and setting the Quality of Service level to 1:
        .. code-block:: python
            mqtt_client.subscribe('topics/ledState', 1)
        
        Example of subscribing to one topic and attaching a method handler:
        .. code-block:: python
            mqtt_client.subscribe('topics/ledState', led_setter)
        """
        if qos < 0 or qos > 2:
            raise MMQTTException('QoS level must be between 1 and 2.')
        if topic is None or len(topic) == 0:
            handle_mqtt_error(MQTT_INVALID_TOPIC)
        try:
            if self._method_handlers[topic]:
                raise MMQTTException('Already subscribed to %s!'%topic)
        except:
            pass
        # associate topic subscription with method_handler.
        if method_handler is None:
            self._method_handlers.update( {topic : self.default_sub_handler} )
        else:
            self._method_handlers.update( {topic : custom_method_handler} )
        if self._sock is None:
            handle_mqtt_error(MQTT_ERR_NO_CONN)
        pkt = MQTT_SUB_PKT_TYPE
        self._pid += 11
        struct.pack_into("!BH", pkt, 1, 2 + 2 + len(topic) + 1, self._pid)
        self._sock.write(pkt)
        # [MQTT-3.8.3-1]
        self._send_str(topic)
        self._sock.write(qos.to_bytes(1, "little"))
        while 1:
            op = self.wait_for_msg()
            if op == 0x90:
                resp = self._sock.read(4)
                assert resp[1] == pkt[2] and resp[2] == pkt[3]
                if resp[3] == 0x80:
                    raise MMQTTException(resp[3])
                return

    def subscribe_multiple(self, topic_info, timeout=1.0):
        """Subscribes to multiple MQTT broker topics.
        :param tuple topic_info: A list of tuple format:
            :param str topic: Unique topic identifier.
            :param method method_handler: Predefined method for handling messages
                recieved from a topic. Defaults to default_sub_handler if None.
            :param int qos: Quality of Service level for the topic. Defaults to 0.
        :param float timeout: Timeout between calls to subscribe().
        """
        print('topics:', topic_info)
        for i in range(len(topic_info)):
            topic = topic_info[i][0]
            try:
                if topic_info[i][1]:
                    method_handler = topic_info[i][1]
            except IndexError:
                method_handler = None
                pass
            try:
                if topic_info[i][2]:
                    qos = topic_info[i][2]
            except IndexError:
                qos = 0
                pass
            print('Subscribing to:', topic, method_handler, qos)
            self.subscribe(topic, method_handler, qos)
            time.sleep(timeout)


    def wait_for_msg(self, blocking=True):
        """Waits for and processes network events.
        :param bool blocking: Set the blocking or non-blocking mode of the socket.
        :param float timeout: The time in seconds to wait for network traffic before returning.
        """
        self._sock.settimeout(0.0)
        res = self._sock.read(1)
        if res in [None, b""]:
            return None
        if res == MQTT_PINGRESP:
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
        topic = str(topic, 'utf-8')
        sz -= topic_len + 2
        if op & 6:
            pid = self._sock.read(2)
            pid = pid[0] << 8 | pid[1]
            sz -= 2
        msg = self._sock.read(sz)
        # call the topic's handler method
        if topic in self._method_handlers:
            method_handler = self._method_handlers[topic]
            method_handler(topic, str(msg, 'utf-8'))
        if op & 6 == 2:
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self._sock.write(pkt)
        elif op & 6 == 4:
            assert 0
        # TODO: return a value if successful, RETURN OP

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
        print(msg)
        print('New message on {0}: {1}'.format(topic, msg))

    def _send_str(self, string):
        """Packs a string into a struct, and writes it to a socket as a utf-8 encoded string.
        :param str string: String to write to the socket.
        """
        self._sock.write(struct.pack("!H", len(string)))
        if type(string) == str:
            self._sock.write(str.encode(string, 'utf-8'))
        else:
            self._sock.write(string)