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
from micropython import const
import adafruit_logging as logging

__version__ = "0.0.0-auto.0"
__repo__ = "https://github.com/adafruit/Adafruit_CircuitPython_MiniMQTT.git"

# Client-specific variables
MQTT_MSG_MAX_SZ = const(268435455)
MQTT_MSG_SZ_LIM = const(10000000)
MQTT_TOPIC_LENGTH_LIMIT = const(65535)
MQTT_TCP_PORT = const(1883)
MQTT_TLS_PORT = const(8883)
TCP_MODE = const(0)
TLS_MODE = const(2)

# MQTT Commands
MQTT_PINGREQ = b'\xc0\0'
MQTT_PINGRESP = const(0xd0)
MQTT_SUB = b'\x82'
MQTT_UNSUB = b'\xA2'
MQTT_PUB = bytearray(b'\x30\0')
# Variable CONNECT header [MQTT 3.1.2]
MQTT_VAR_HEADER = bytearray(b"\x04MQTT\x04\x02\0\0")
MQTT_DISCONNECT = b'\xe0\0'

CONNACK_ERRORS = {const(0x01) : 'Connection Refused - Incorrect Protocol Version',
                  const(0x02) : 'Connection Refused - ID Rejected',
                  const(0x03) : 'Connection Refused - Server unavailable',
                  const(0x04) : 'Connection Refused - Incorrect username/password',
                  const(0x05) : 'Connection Refused - Unauthorized'}

class MMQTTException(Exception):
    """MiniMQTT Exception class."""
    # pylint: disable=unnecessary-pass
    #pass

class MQTT:
    """MQTT Client for CircuitPython
    :param socket: Socket object for provided network interface
    :param str broker: MQTT Broker URL or IP Address.
    :param int port: Optional port definition, defaults to 8883.
    :param str username: Username for broker authentication.
    :param str password: Password for broker authentication.
    :param network_manager: NetworkManager object, such as WiFiManager from ESPSPI_WiFiManager.
    :param str client_id: Optional client identifier, defaults to a unique, generated string.
    :param bool is_ssl: Sets a secure or insecure connection with the broker.
    :param bool log: Attaches a logger to the MQTT client, defaults to logging level INFO.
    """
    # pylint: disable=too-many-arguments,too-many-instance-attributes, not-callable, invalid-name, no-member
    def __init__(self, socket, broker, port=None, username=None,
                 password=None, network_manager=None, client_id=None, is_ssl=True, log=False):
        # network management
        self._socket = socket
        network_manager_type = str(type(network_manager))
        if 'ESPSPI_WiFiManager' in network_manager_type:
            self._wifi = network_manager
        else:
            raise TypeError("This library requires a NetworkManager object.")
        # broker
        try: # set broker IP
            self.broker = self._wifi.esp.unpretty_ip(broker)
        except ValueError: # set broker URL
            self.broker = broker
        # port/ssl
        self.port = MQTT_TCP_PORT
        if is_ssl:
            self.port = MQTT_TLS_PORT
        if port is not None:
            self.port = port
        # session identifiers
        self._user = username
        # [MQTT-3.1.3.5]
        self._pass = password
        if self._pass is not None and len(password.encode('utf-8')) > MQTT_TOPIC_LENGTH_LIMIT:
            raise MMQTTException('Password length is too large.')
        if client_id is not None:
            # user-defined client_id MAY allow client_id's > 23 bytes or
            # non-alpha-numeric characters
            self._client_id = client_id
        else:
            # assign a unique client_id
            self._client_id = 'cpy{0}{1}'.format(microcontroller.cpu.uid[randint(0, 15)],
                                                 randint(0, 9))
            # generated client_id's enforce spec.'s length rules
            if len(self._client_id) > 23 or not self._client_id:
                raise ValueError('MQTT Client ID must be between 1 and 23 bytes')
        self._logger = None
        if log is True:
            self._logger = logging.getLogger('log')
            self._logger.setLevel(logging.INFO)
        self._sock = None
        self._is_connected = False
        self._msg_size_lim = MQTT_MSG_SZ_LIM
        self.packet_id = 0
        self._keep_alive = 60
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
        self.last_will()

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        self.deinit()

    def deinit(self):
        """De-initializes the MQTT client and disconnects from
        the mqtt broker.
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
        if self._logger is not None:
            self._logger.debug('Setting last will properties')
        self._lw_qos = qos
        self._lw_topic = topic
        self._lw_msg = message
        self._lw_retain = retain

    def reconnect(self, retries=30, resub_topics=True):
        """Attempts to reconnect to the MQTT broker.
        :param int retries: Amount of retries before resetting the network interface.
        :param bool resub_topics: Resubscribe to previously subscribed topics.
        """
        retries = 0
        while not self._is_connected:
            if self._logger is not None:
                self._logger.debug('Attempting to reconnect to broker')
            try:
                self.connect()
                if self._logger is not None:
                    self._logger.debug('Reconnected to broker')
                if resub_topics:
                    if self._logger is not None:
                        self._logger.debug('Attempting to resubscribe to prv. subscribed topics.')
                    while self._subscribed_topics:
                        feed = self._subscribed_topics.pop()
                        self.subscribe(feed)
            except OSError as e:
                if self._logger is not None:
                    self._logger.debug('Lost connection, reconnecting and resubscribing...', e)
                retries += 1
                if retries >= 30:
                    retries = 0
                time.sleep(1)
                continue

    # pylint: disable=too-many-branches, too-many-statements
    def connect(self, clean_session=True):
        """Initiates connection with the MQTT Broker.
        :param bool clean_session: Establishes a persistent session.
        """
        self._set_interface()
        if self._logger is not None:
            self._logger.debug('Creating new socket')
        self._sock = self._socket.socket()
        self._sock.settimeout(10)
        if self.port == 8883:
            try:
                if self._logger is not None:
                    self._logger.debug('Attempting to establish secure MQTT connection...')
                self._sock.connect((self.broker, self.port), TLS_MODE)
            except RuntimeError:
                raise MMQTTException("Invalid broker address defined.")
        else:
            if isinstance(self.broker, str):
                addr = self._socket.getaddrinfo(self.broker, self.port)[0][-1]
            else:
                addr = (self.broker, self.port)
            try:
                if self._logger is not None:
                    self._logger.debug('Attempting to establish insecure MQTT connection...')
                #self._sock.connect((self.broker, self.port), TCP_MODE)
                self._sock.connect(addr, TCP_MODE)
            except RuntimeError as e:
                raise MMQTTException("Invalid broker address defined.", e)

        # Fixed Header
        fixed_header = bytearray()
        fixed_header.append(0x10)

        # Variable Header
        var_header = MQTT_VAR_HEADER
        var_header[6] = clean_session << 1

        # Set up variable header and remaining_length
        remaining_length = 12 + len(self._client_id)
        if self._user is not None:
            remaining_length += 2 + len(self._user) + 2 + len(self._pass)
            var_header[6] |= 0xC0
        if self._keep_alive:
            assert self._keep_alive < MQTT_TOPIC_LENGTH_LIMIT
            var_header[7] |= self._keep_alive >> 8
            var_header[8] |= self._keep_alive & 0x00FF
        if self._lw_topic:
            remaining_length += 2 + len(self._lw_topic) + 2 + len(self._lw_msg)
            var_header[6] |= 0x4 | (self._lw_qos & 0x1) << 3 | (self._lw_qos & 0x2) << 3
            var_header[6] |= self._lw_retain << 5

        # Remaining length calculation
        large_rel_length = False
        if remaining_length > 0x7f:
            large_rel_length = True
            # Calculate Remaining Length [2.2.3]
            while remaining_length > 0:
                encoded_byte = remaining_length % 0x80
                remaining_length = remaining_length // 0x80
                # if there is more data to encode, set the top bit of the byte
                if remaining_length > 0:
                    encoded_byte |= 0x80
                fixed_header.append(encoded_byte)
        if large_rel_length:
            fixed_header.append(0x00)
        else:
            fixed_header.append(remaining_length)
            fixed_header.append(0x00)

        if self._logger is not None:
            self._logger.debug('Sending CONNECT to broker')
            self._logger.debug('Fixed Header: {}\nVariable Header: {}'.format(fixed_header,
                                                                              var_header))
        self._sock.write(fixed_header)
        self._sock.write(var_header)
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
        if self._logger is not None:
            self._logger.debug('Receiving CONNACK packet from broker')
        while True:
            op = self._wait_for_msg()
            if op == 32:
                rc = self._sock.read(3)
                assert rc[0] == 0x02
                if rc[2] != 0x00:
                    raise MMQTTException(CONNACK_ERRORS[rc[3]])
                self._is_connected = True
                result = rc[0] & 1
                if self.on_connect is not None:
                    self.on_connect(self, self._user_data, result, rc[2])
                return result

    def disconnect(self):
        """Disconnects the MiniMQTT client from the MQTT broker.
        """
        self.is_connected()
        if self._logger is not None:
            self._logger.debug('Sending DISCONNECT packet to broker')
        self._sock.write(MQTT_DISCONNECT)
        if self._logger is not None:
            self._logger.debug('Closing socket')
        self._sock.close()
        self._is_connected = False
        self._subscribed_topics = None
        if self.on_disconnect is not None:
            self.on_disconnect(self, self._user_data, 0)

    def ping(self):
        """Pings the MQTT Broker to confirm if the broker is alive or if
        there is an active network connection.
        """
        self.is_connected()
        if self._logger is not None:
            self._logger.debug('Sending PINGREQ')
        self._sock.write(MQTT_PINGREQ)
        if self._logger is not None:
            self._logger.debug('Checking PINGRESP')
        while True:
            op = self._wait_for_msg(0.5)
            if op == 208:
                ping_resp = self._sock.read(2)
                if ping_resp[0] != 0x00:
                    raise MMQTTException('PINGRESP not returned from broker.')
            return

    # pylint: disable=too-many-branches, too-many-statements
    def publish(self, topic, msg, retain=False, qos=0):
        """Publishes a message to a topic provided.
        :param str topic: Unique topic identifier.
        :param str msg: Data to send to the broker.
        :param int msg: Data to send to the broker.
        :param float msg: Data to send to the broker.
        :param bool retain: Whether the message is saved by the broker.
        :param int qos: Quality of Service level for the message.

        Example of sending an integer, 3, to the broker on topic 'piVal'.
        .. code-block:: python

            mqtt_client.publish('topics/piVal', 3)

        Example of sending a float, 3.14, to the broker on topic 'piVal'.
        .. code-block:: python

            mqtt_client.publish('topics/piVal', 3.14)

        Example of sending a string, 'threepointonefour', to the broker on topic piVal.
        .. code-block:: python

            mqtt_client.publish('topics/piVal', 'threepointonefour')

        """
        self.is_connected()
        self._check_topic(topic)
        if '+' in topic or '#' in topic:
            raise MMQTTException('Publish topic can not contain wildcards.')
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
        self._check_qos(qos)
        pkt = MQTT_PUB
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
        if self._logger is not None:
            self._logger.debug('Sending PUBLISH\nTopic: {0}\nMsg: {1}\
                                \nQoS: {2}\nRetain? {3}'.format(topic, msg, qos, retain))
        self._sock.write(pkt)
        self._send_str(topic)
        if qos == 0:
            if self.on_publish is not None:
                self.on_publish(self, self._user_data, topic, self._pid)
        if qos > 0:
            self._pid += 1
            pid = self._pid
            struct.pack_into("!H", pkt, 0, pid)
            self._sock.write(pkt)
            if self.on_publish is not None:
                self.on_publish(self, self._user_data, topic, pid)
        if self._logger is not None:
            self._logger.debug('Sending PUBACK')
        self._sock.write(msg)
        if qos == 1:
            while True:
                op = self._wait_for_msg()
                if op == 0x40:
                    sz = self._sock.read(1)
                    assert sz == b"\x02"
                    rcv_pid = self._sock.read(2)
                    rcv_pid = rcv_pid[0] << 0x08 | rcv_pid[1]
                    if pid == rcv_pid:
                        if self.on_publish is not None:
                            self.on_publish(self, self._user_data, topic, rcv_pid)
                        return
        elif qos == 2:
            assert 0
            if self.on_publish is not None:
                self.on_publish(self, self._user_data, topic, rcv_pid)

    def subscribe(self, topic, qos=0):
        """Subscribes to a topic on the MQTT Broker.
        This method can subscribe to one topics or multiple topics.
        :param str topic: Unique MQTT topic identifier.
        :param int qos: Quality of Service level for the topic, defaults to zero.
        :param tuple topic: Tuple containing topic identifier strings and qos level integers.
        :param list topic: List of tuples containing topic identifier strings and qos.

        Example of subscribing a topic string.
        .. code-block:: python

            mqtt_client.subscribe('topics/ledState')

        Example of subscribing to a topic and setting the qos level to 1.
        .. code-block:: python

            mqtt_client.subscribe('topics/ledState', 1)

        Example of subscribing to topic string and setting qos level to 1, as a tuple.
        .. code-block:: python

            mqtt_client.subscribe(('topics/ledState', 1))

        Example of subscribing to multiple topics with different qos levels.
        .. code-block:: python

            mqtt_client.subscribe([('topics/ledState', 1), ('topics/servoAngle', 0)])

        """
        self.is_connected()
        topics = None
        if isinstance(topic, tuple):
            topic, qos = topic
            self._check_topic(topic)
            self._check_qos(qos)
        if isinstance(topic, str):
            self._check_topic(topic)
            self._check_qos(qos)
            topics = [(topic, qos)]
        if isinstance(topic, list):
            topics = []
            for t, q in topic:
                self._check_qos(q)
                self._check_topic(t)
                topics.append((t, q))
        # Assemble packet
        packet_length = 2 + (2 * len(topics)) + (1 * len(topics))
        packet_length += sum(len(topic) for topic, qos in topics)
        packet_length_byte = packet_length.to_bytes(1, 'big')
        self._pid += 1
        packet_id_bytes = self._pid.to_bytes(2, 'big')
        # Packet with variable and fixed headers
        packet = MQTT_SUB + packet_length_byte + packet_id_bytes
        # attaching topic and QOS level to the packet
        for t, q in topics:
            topic_size = len(t).to_bytes(2, 'big')
            qos_byte = q.to_bytes(1, 'big')
            packet += topic_size + t + qos_byte
        if self._logger is not None:
            for t, q in topics:
                self._logger.debug('SUBSCRIBING to topic {0} with QoS {1}'.format(t, q))
        self._sock.write(packet)
        while True:
            op = self._wait_for_msg()
            if op == 0x90:
                rc = self._sock.read(4)
                assert rc[1] == packet[2] and rc[2] == packet[3]
                if rc[3] == 0x80:
                    raise MMQTTException('SUBACK Failure!')
                for t, q in topics:
                    if self.on_subscribe is not None:
                        self.on_subscribe(self, self._user_data, t, q)
                    self._subscribed_topics.append(t)
                return

    def unsubscribe(self, topic):
        """Unsubscribes from a MQTT topic.
        :param str topic: Unique MQTT topic identifier.
        :param list topic: List of tuples containing topic identifier strings.

        Example of unsubscribing from a topic string.
        .. code-block:: python

            mqtt_client.unsubscribe('topics/ledState')

        Example of unsubscribing from multiple topics.
        .. code-block:: python

            mqtt_client.unsubscribe([('topics/ledState'), ('topics/servoAngle')])

        """
        topics = None
        if isinstance(topic, str):
            self._check_topic(topic)
            topics = [(topic)]
        if isinstance(topic, list):
            topics = []
            for t in topic:
                self._check_topic(t)
                topics.append((t))
        for t in topics:
            if t not in self._subscribed_topics:
                raise MMQTTException('Topic must be subscribed to before attempting unsubscribe.')
        # Assemble packet
        packet_length = 2 + (2 * len(topics))
        packet_length += sum(len(topic) for topic in topics)
        packet_length_byte = packet_length.to_bytes(1, 'big')
        self._pid += 1
        packet_id_bytes = self._pid.to_bytes(2, 'big')
        packet = MQTT_UNSUB + packet_length_byte + packet_id_bytes
        for t in topics:
            topic_size = len(t).to_bytes(2, 'big')
            packet += topic_size + t
        if self._logger is not None:
            for t in topics:
                self._logger.debug('UNSUBSCRIBING from topic {0}.'.format(t))
        self._sock.write(packet)
        if self._logger is not None:
            self._logger.debug('Waiting for UNSUBACK...')
        while True:
            op = self._wait_for_msg()
            if op == 176:
                return_code = self._sock.read(3)
                assert return_code[0] == 0x02
                # [MQTT-3.32]
                assert return_code[1] == packet_id_bytes[0] and return_code[2] == packet_id_bytes[1]
                for t in topics:
                    if self.on_unsubscribe is not None:
                        self.on_unsubscribe(self, self._user_data, t, self._pid)
                    self._subscribed_topics.remove(t)
                return

    def loop_forever(self):
        """Starts a blocking message loop. Use this
        method if you want to run a program forever.
        Network reconnection is handled within this call.
        Your code will not execute anything below this call.
        """
        run = True
        while run:
            if self._is_connected:
                self._wait_for_msg(0.0)
            else:
                if self._logger is not None:
                    self._logger.debug('Lost connection, reconnecting and resubscribing...')
                self.reconnect(resub_topics=True)
                if self._logger is not None:
                    self._logger.debug('Connection restored, continuing to loop forever...')

    def loop(self):
        """Non-blocking message loop. Use this method to
        check incoming subscription messages. Does not handle
        network reconnection like loop_forever - reconnection must
        be handled within your code.
        """
        self._sock.settimeout(0.1)
        return self._wait_for_msg()

    def _wait_for_msg(self, timeout=30):
        """Reads and processes network events.
        Returns response code if successful.
        """
        res = self._sock.read(1)
        self._sock.settimeout(timeout)
        if res in [None, b""]:
            return None
        if res == MQTT_PINGRESP:
            sz = self._sock.read(1)[0]
            assert sz == 0
            return None
        if res[0] & 0xf0 != 0x30:
            return res[0]
        sz = self._recv_len()
        topic_len = self._sock.read(2)
        topic_len = (topic_len[0] << 8) | topic_len[1]
        topic = self._sock.read(topic_len)
        topic = str(topic, 'utf-8')
        sz -= topic_len + 2
        if res[0] & 0x06:
            pid = self._sock.read(2)
            pid = pid[0] << 0x08 | pid[1]
            sz -= 0x02
        msg = self._sock.read(sz)
        if self.on_message is not None:
            self.on_message(self, topic, str(msg, 'utf-8'))
        if res[0] & 0x06 == 0x02:
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self._sock.write(pkt)
        elif res[0] & 6 == 4:
            assert 0
        return res[0]

    def _recv_len(self):
        n = 0
        sh = 0
        while True:
            b = self._sock.read(1)[0]
            n |= (b & 0x7f) << sh
            if not b & 0x80:
                return n
            sh += 7

    def _send_str(self, string):
        """Packs and encodes a string to a socket.
        :param str string: String to write to the socket.
        """
        self._sock.write(struct.pack("!H", len(string)))
        if isinstance(string, str):
            self._sock.write(str.encode(string, 'utf-8'))
        else:
            self._sock.write(string)

    @staticmethod
    def _check_topic(topic):
        """Checks if topic provided is a valid mqtt topic.
        :param str topic: Topic identifier
        """
        if topic is None:
            raise MMQTTException('Topic may not be NoneType')
        # [MQTT-4.7.3-1]
        elif not topic:
            raise MMQTTException('Topic may not be empty.')
        # [MQTT-4.7.3-3]
        elif len(topic.encode('utf-8')) > MQTT_TOPIC_LENGTH_LIMIT:
            raise MMQTTException('Topic length is too large.')

    @staticmethod
    def _check_qos(qos_level):
        """Validates the quality of service level.
        :param int qos_level: Desired QoS level.
        """
        if isinstance(qos_level, int):
            if qos_level < 0 or qos_level > 2:
                raise MMQTTException('QoS must be between 1 and 2.')
        else:
            raise MMQTTException('QoS must be an integer.')

    def _set_interface(self):
        """Sets a desired network hardware interface.
        The network hardware must be set in init
        prior to calling this method.
        """
        if self._wifi:
            self._socket.set_interface(self._wifi.esp)
        else:
            raise TypeError('Network Manager Required.')

    def is_connected(self):
        """Returns MQTT client session status as True if connected, raises
        a MMQTTException if False.
        """
        if self._sock is None or self._is_connected is False:
            raise MMQTTException("MiniMQTT is not connected.")
        return self._is_connected

    @property
    def mqtt_msg(self):
        """Returns maximum MQTT payload and topic size."""
        return self._msg_size_lim, MQTT_TOPIC_LENGTH_LIMIT

    @mqtt_msg.setter
    def mqtt_msg(self, msg_size):
        """Sets the maximum MQTT message payload size.
        :param int msg_size: Maximum MQTT payload size.
        """
        if msg_size < MQTT_MSG_MAX_SZ:
            self._msg_size_lim = msg_size

    # Logging
    def attach_logger(self, logger_name='log'):
        """Initializes and attaches a logger to the MQTTClient.
        :param str logger_name: Name of the logger instance
        """
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(logging.INFO)

    def set_logger_level(self, log_level):
        """Sets the level of the logger, if defined during init.
        :param string log_level: Level of logging to output to the REPL.
        """
        if self._logger is None:
            raise MMQTTException('No logger attached - did you create it during initialization?')
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
