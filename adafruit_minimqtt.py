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
import struct
import time
from random import randint
import microcontroller
import adafruit_logging as logging
from micropython import const

__version__ = "0.0.0-auto.0"
__repo__ = "https://github.com/adafruit/Adafruit_CircuitPython_MiniMQTT.git"


# Client-specific variables
MQTT_MSG_MAX_SZ = const(268435455)
MQTT_MSG_SZ_LIM = const(10000000)
MQTT_TOPIC_SZ_LIMIT = const(65536)
MQTT_TCP_PORT = const(1883)

# MQTT Commands
MQTT_PING_REQ = b'\xc0'
MQTT_PINGRESP = b'\xd0'
MQTT_SUB = b'\x82'
MQTT_UNSUB = bytearray(b'\xA2\0\0\0')
MQTT_PUB = bytearray(b'\x30\0')
MQTT_CON = bytearray(b'\x10\0\0')
# Variable CONNECT header [MQTT 3.1.2]
MQTT_CON_HEADER = bytearray(b"\x04MQTT\x04\x02\0\0")
MQTT_DISCONNECT = b'\xe0\0'

CONNACK_ERRORS = {const(0x01) : 'Connection Refused - Incorrect Protocol Version',
                   const(0x02) : 'Connection Refused - ID Rejected',
                   const(0x03) : 'Connection Refused - Server unavailable',
                   const(0x04) : 'Connection Refused - Incorrect username/password',
                   const(0x05) : 'Connection Refused - Unauthorized'}

class MMQTTException(Exception):
    pass

class MQTT:
    """
    MQTT client interface for CircuitPython devices.
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
        self._logger = logging.getLogger('log')
        self._logger.setLevel(logging.INFO)
        # subscription method handler dictionary
        self._is_connected = False
        self._msg_size_lim = MQTT_MSG_SZ_LIM
        self.server = server_address
        self.packet_id = 0
        self._keep_alive = 0
        self._pid = 0
        self._user_data = None
        self._subscribed_topics = []
        # Server callbacks
        self.on_message = None
        self.on_connect = None
        self.on_disconnect = None
        self.on_publish = None
        self.on_subscribe = None
        self.on_unsubscribe = None
        self.on_log = None
        self.last_will()

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
            raise MMQTTException('Last Will should be defined before connect() is called.')
        if qos < 0 or qos > 2:
            raise MMQTTException("Invalid QoS level,  must be between 0 and 2.")
        self._logger.debug('Setting last will properties')
        self._lw_qos = qos
        self._lw_topic = topic
        self._lw_msg = message
        self._lw_retain = retain

    def reconnect(self, retries=30, resub_topics=False):
        """Attempts to reconnect to the MQTT broker.
        :param int retries: Amount of retries before resetting the ESP32 hardware.
        :param bool resub_topics: Client resubscribes to previously subscribed topics upon
            a successful reconnection.
        """
        retries = 0
        while not self._is_connected:
            self._logger.debug('Attempting to reconnect to broker')
            try:
                self.connect(False)
            except OSError as e:
                print('Failed to connect to the broker, retrying\n', e)
                retries+=1
                if retries >= 30:
                    retries = 0
                    self._esp.reset()
                continue
            self._is_connected = True
            self._logger.debug('Reconnected to broker')
            if resub_topics:
                self._logger.debug('Attempting to resubscribe to prv. subscribed topics.')
                while len(self._subscribed_topics) > 0:
                    feed = self._subscribed_topics.pop()
                    self.subscribe(feed)

    def is_connected(self):
        """Returns MQTT client session status."""
        return self._is_connected

    # Core MQTT Methods
    def connect(self, clean_session=True):
        """Initiates connection with the MQTT Broker.
        :param bool clean_session: Establishes a persistent session
            with the broker. Defaults to a non-persistent session.
        """
        if self._esp:
            self._logger.debug('Creating new socket')
            self._socket.set_interface(self._esp)
            self._sock = self._socket.socket()
        else:
            raise TypeError('ESP32SPI interface required!')
        self._sock.settimeout(10)
        if self.port == 8883:
            try:
                self._logger.debug('Attempting to establish secure MQTT connection with %s'%self.server)
                self._sock.connect((self.server, self.port), TLS_MODE)
            except RuntimeError:
                raise MMQTTException("Invalid server address defined.")
        else:
            addr = self._socket.getaddrinfo(self.server, self.port)[0][-1]
            try:
                self._logger.debug('Attempting to establish an insecure MQTT connection with %s'%self.server)
                self._sock.connect(addr, TCP_MODE)
            except RuntimeError:
                raise MMQTTException("Invalid server address defined.")
        premsg = MQTT_CON
        msg = MQTT_CON_HEADER
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
        self._logger.debug('Sending CONNECT packet to server')
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
        self._logger.debug('Receiving CONNACK packet from server')
        rc = self._sock.read(4)
        assert rc[0] == const(0x20) and rc[1] == const(0x02)
        if rc[3] !=0:
            raise MMQTTException(CONNACK_ERRORS[rc[3]])
        self._is_connected = True
        result = rc[2] & 1
        if self.on_connect is not None:
            self.on_connect(self, self._user_data, result, rc[3]) 
        return result

    def disconnect(self):
        """Disconnects from the broker.
        """
        if self._sock is None:
            raise MMQTTException("MiniMQTT is not connected.")
        self._logger.debug('Sending DISCONNECT packet to server')
        self._sock.write(MQTT_DISCONNECT)
        self._logger.debug('Closing socket')
        self._sock.close()
        self._is_connected = False
        if self.on_disconnect is not None:
            self.on_disconnect(self, self._user_data, 0)

    def ping(self):
        """Pings the MQTT Broker to confirm if the server is alive or if
        there is an active network connection.
        Raises a MMQTTException if the server does not respond with a PINGRESP packet.
        """
        self._logger.debug('Sending PINGREQ')
        self._sock.write(MQTT_PING_REQ)
        res = self._sock.read(1)
        self._logger.debug('Checking PINGRESP')
        if res == MQTT_PINGRESP:
            sz = self._sock.read(1)[0]
            assert sz == 0
            return None
        else:
            raise MMQTTException('Server did not return with PINGRESP')

    def publish(self, topic, msg, retain=False, qos=0):
        """Publishes a message to the MQTT broker.
        :param str topic: Unique topic identifier.
        :param str msg: Data to send to the broker.
        :param bool retain: Whether the message is saved by the broker.
        :param int qos: Quality of Service level for the message.
        """
        if topic is None or len(topic) == 0:
            raise MMQTTException("Invalid MQTT Topic, must have length > 0.")
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
            raise MMQTTException("Invalid QoS level,  must be between 0 and 2.")
        if self._sock is None:
            raise MMQTTException("MiniMQTT not connected.")
        pkt = MQTT_PUB
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
        self._logger.debug('Sending PUBLISH\nTopic: {0}\nMsg: {1}\nQoS: {2}\n Retain: {3}'.format(topic, msg, qos, retain))
        self._sock.write(pkt)
        self._send_str(topic)
        if qos == 0:
            if self.on_publish is not None:
                self.on_publish(self, self._user_data, self._pid)
        if qos > 0:
            self.pid += 1
            pid = self.pid
            struct.pack_into("!H", pkt, 0, pid)
            self._sock.write(pkt)
            if self.on_publish is not None:
                self.on_publish(self, self._user_data, pid)
        self._logger.debug('Sending PUBACK')
        self._sock.write(msg)
        if qos == 1:
            while 1:
                op = self.wait_for_msg()
                if op == const(0x40):
                    sz = self._sock.read(1)
                    assert sz == b"\x02"
                    rcv_pid = self._sock.read(2)
                    rcv_pid = rcv_pid[0] << 8 | rcv_pid[1]
                    if self.on_publish is not None:
                        self.on_publish(self, self._user_data, rcv_pid)
                    if pid == rcv_pid:
                        return
        elif qos == 2:
            if self.on_publish is not None:
                raise NotImplementedError('on_publish callback not implemented for QoS > 1.')
            assert 0

    def subscribe(self, topic, qos=0):
        """Subscribes to a topic on the MQTT Broker.
        This method can subscribe to one topics or multiple topics.
        :param str topic: Unique MQTT topic identifier.
        :param int qos: Quality of Service level for the topic.

        Example of subscribing to one topic:
        .. code-block:: python
            mqtt_client.subscribe('topics/ledState')

        Example of subscribing to one topic and setting the Quality of Service level to 1:
        .. code-block:: python
            mqtt_client.subscribe('topics/ledState', 1)
        """
        if self._sock is None:
            raise MMQTTException("MiniMQTT not connected.")
        topics = None
        if isinstance(topic, str):
            if topic is None or len(topic) == 0 or len(topic.encode('utf-8')) > 65536:
                raise MMQTTException("Invalid MQTT Topic, must have length > 0.")
            if qos < 0 or qos > 2:
                raise MMQTTException('QoS level must be between 1 and 2.')
            topics = [(topic, qos)]
        if isinstance(topic, list):
            topics = []
            for t, q in topic:
                if q < 0 or q > 2:
                    raise MMQTTException('QoS level must be between 1 and 2.')
                if t is None or len(t) == 0 or len(t.encode('utf-8')) > 65536:
                    raise MMQTTException("Invalid MQTT Topic, must have length > 0.")
                topics.append((t, q))
        # Assemble packet
        packet_length = 2 + (2 * len(topics)) + (1 * len(topics)) + sum(len(topic) for topic, qos in topics)
        packet_length_byte = packet_length.to_bytes(1, 'big')
        self._pid += 11
        packet_id_bytes = self._pid.to_bytes(2, 'big')

        # Packet with variable and fixed headers
        packet = MQTT_SUB + packet_length_byte + packet_id_bytes

        # attaching topic and QOS level to the packet 
        for topic, qos in topics:
            topic_size = len(topic).to_bytes(2, 'big')
            qos_byte = qos.to_bytes(1, 'big')
            packet += topic_size + topic + qos_byte
        self._logger.debug('SUBSCRIBING to topic(s)')
        self._sock.write(packet)
        while 1:
            op = self.wait_for_msg()
            print(op)
            if op == 0x90:
                rc = self._sock.read(4)
                assert rc[1] == packet[2] and rc[2] == packet[3]
                if rc[3] == 0x80:
                    raise MMQTTException('SUBACK Failure!')
                if self.on_subscribe is not None:
                    self.on_subscribe(self, self._user_data, rc[3])
                return

    def unsubscribe(self, topic):
        """Unsubscribes from a MQTT topic.
        :param str topic: Unique MQTT topic identifier.
        """
        if topic is None or len(topic) == 0:
            raise MMQTTException("Invalid MQTT topic - must have a length > 0.")
        if topic in self._subscribed_topics:
            raise MMQTTException('Topic must be subscribed to before attempting to unsubscribe.')
        pkt = MQTT_UNSUB
        self._pid+=1
        # variable header length
        remaining_length = 2
        remaining_length += 2 + len(topic)
        struct.pack_into("!BH", pkt, 1, remaining_length, self._pid)
        self._logger.debug('Unsubscribing from %s'%topic)
        self._sock.write(pkt)
        self._send_str(topic)
        while 1:
            try:
                # attempt to UNSUBACK
                self._logger.debug('Sending UNSUBACK')
                op = self.wait_for_msg(0.1)
                # remove topic from subscription list
                self._subscribed_topics.remove(topic)
                if self.on_unsubscribe is not None:
                    self.on_unsubscribe(self, self._user_data)
                return
            except RuntimeError:
                raise MMQTTException('Could not unsubscribe from feed.')

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

    def publish_multiple(self, data, timeout=0.0):
        """Publishes to multiple MQTT broker topics.
        :param tuple data: A list of tuple format:
            :param str topic: Unique topic identifier.
            :param str msg: Data to send to the broker.
            :param bool retain: Whether the message is saved by the broker.
            :param int qos: Quality of Service level for the message.
        :param float timeout: Timeout between calls to publish(). This value
            is usually set by your MQTT broker. Defaults to 1.0
        """
        # TODO: Untested!
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

    def wait_for_msg(self, timeout=0.1):
        """Waits for and processes network events. Returns if successful.
        :param float timeout: The time in seconds to wait for network before returning.
            Setting this to 0.0 will cause the socket to block until it reads.
        """
        self._sock.settimeout(timeout)
        res = self._sock.read(1)
        if res in [None, b""]:
            return None
        if res == MQTT_PINGRESP:
            sz = self._sock.read(1)[0]
            assert sz == 0
            return None
        if res[0] & const(0xf0) != const(0x30):
            return res[0]
        sz = self._recv_len()
        topic_len = self._sock.read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = self._sock.read(topic_len)
        topic = str(topic, 'utf-8')
        sz -= topic_len + 2
        if res[0] & const(0x06):
            pid = self._sock.read(2)
            pid = pid[0] << const(0x08) | pid[1]
            sz -= const(0x02)
        msg = self._sock.read(sz)
        if self.on_message is not None:
            self.on_message(self, topic, str(msg, 'utf-8'))
        if res[0] & const(0x06) == const(0x02):
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self._sock.write(pkt)
        elif res[0] & 6 == 4:
            assert 0
        return res[0]

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
        :param str msg: Message content.
        """
        self._logger.debug('default_sub_handler called')
        print('New message on {0}: {1}'.format(topic, msg))

    def _send_str(self, string):
        """Packs a string into a struct, and writes it to a socket as an utf-8 encoded string.
        :param str string: String to write to the socket.
        """
        self._sock.write(struct.pack("!H", len(string)))
        if type(string) == str:
            self._sock.write(str.encode(string, 'utf-8'))
        else:
            self._sock.write(string)

    # Network Loop Methods

    def loop(self, timeout=1.0):
        """Call regularly to process network events.
        This function blocks for up to timeout seconds. 
        Timeout must not exceed the keepalive value for the client or
        your client will be regularly disconnected by the broker.
        :param float timeout: Blocks between calls to wait_for_msg()
        """
        return None
    
    def loop_forever(self):
        """Blocking network loop, will not return until disconnect() is called from
        the client. Automatically handles the re-connection.
        """
        return None

    # Logging
    def logging(self, log_level):
        """Sets the level of the logger, if defined during init.
        :param string log_level: Level of logging to output to the REPL. Accepted
            levels are DEBUG, INFO, WARNING, EROR, and CRITICIAL.
        """
        if log_level == 'DEBUG':
            self._logger.setLevel(logging.DEBUG)
        elif log_level == 'INFO':
            self._logger.setLevel(logging.INFO)
        elif log_level == 'WARNING':
            self._logger.setLevel(logging.WARNING)
        elif log_level == 'ERROR':
            self._logger.setLevel(logging.CRITICIAL)
        else:
            raise MMQTTException('Incorrect logging level provided!')