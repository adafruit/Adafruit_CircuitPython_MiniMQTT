# SPDX-FileCopyrightText: 2019-2021 Brent Rubell for Adafruit Industries
#
# SPDX-License-Identifier: MIT

# Original Work Copyright (c) 2016 Paul Sokolovsky, uMQTT
# Modified Work Copyright (c) 2019 Bradley Beach, esp32spi_mqtt
# Modified Work Copyright (c) 2012-2019 Roger Light and others, Paho MQTT Python

"""
`adafruit_minimqtt`
================================================================================

A minimal MQTT Library for CircuitPython.

* Author(s): Brent Rubell

Implementation Notes
--------------------

Adapted from https://github.com/micropython/micropython-lib/tree/master/umqtt.simple/umqtt

**Software and Dependencies:**

* Adafruit CircuitPython firmware for the supported boards:
  https://github.com/adafruit/circuitpython/releases

* Adafruit's Connection Manager library:
  https://github.com/adafruit/Adafruit_CircuitPython_ConnectionManager

"""

# ruff: noqa: PLR6104,PLR6201,PLR6301 non-augmented-assignment,literal-membership,no-self-use

import errno
import struct
import time
from random import randint

from adafruit_connection_manager import get_connection_manager
from adafruit_ticks import ticks_diff, ticks_ms

try:
    from typing import List, Optional, Tuple, Type, Union
except ImportError:
    pass

try:
    from types import TracebackType
except ImportError:
    pass

from micropython import const

from .matcher import MQTTMatcher

__version__ = "0.0.0+auto.0"
__repo__ = "https://github.com/adafruit/Adafruit_CircuitPython_MiniMQTT.git"

# Client-specific variables
MQTT_MSG_MAX_SZ = const(268435455)
MQTT_MSG_SZ_LIM = const(10000000)
MQTT_TOPIC_LENGTH_LIMIT = const(65535)
MQTT_TCP_PORT = const(1883)
MQTT_TLS_PORT = const(8883)

# MQTT Commands
MQTT_PINGREQ = b"\xc0\0"
MQTT_PINGRESP = const(0xD0)
MQTT_PUBLISH = const(0x30)
MQTT_SUB = const(0x82)
MQTT_SUBACK = const(0x90)
MQTT_UNSUB = const(0xA2)
MQTT_UNSUBACK = const(0xB0)
MQTT_DISCONNECT = b"\xe0\0"

MQTT_PKT_TYPE_MASK = const(0xF0)


CONNACK_ERROR_INCORRECT_PROTOCOL = const(0x01)
CONNACK_ERROR_ID_REJECTED = const(0x02)
CONNACK_ERROR_SERVER_UNAVAILABLE = const(0x03)
CONNACK_ERROR_INCORECT_USERNAME_PASSWORD = const(0x04)
CONNACK_ERROR_UNAUTHORIZED = const(0x05)

CONNACK_ERRORS = {
    CONNACK_ERROR_INCORRECT_PROTOCOL: "Connection Refused - Incorrect Protocol Version",
    CONNACK_ERROR_ID_REJECTED: "Connection Refused - ID Rejected",
    CONNACK_ERROR_SERVER_UNAVAILABLE: "Connection Refused - Server unavailable",
    CONNACK_ERROR_INCORECT_USERNAME_PASSWORD: "Connection Refused - Incorrect username/password",
    CONNACK_ERROR_UNAUTHORIZED: "Connection Refused - Unauthorized",
}

_default_sock = None
_fake_context = None


class MMQTTException(Exception):
    """
    MiniMQTT Exception class.

    Raised for various mostly protocol or network/system level errors.
    In general, the robust way to recover is to call reconnect().
    """

    def __init__(self, error, code=None):
        super().__init__(error, code)
        self.code = code


class MMQTTStateError(MMQTTException):
    """
    MiniMQTT invalid state error.

    Raised e.g. if a function is called in unexpected state.
    """


class NullLogger:
    """Fake logger class that does not do anything"""

    def nothing(self, msg: str, *args) -> None:
        """no action"""

    def __init__(self) -> None:
        for log_level in ["debug", "info", "warning", "error", "critical"]:
            setattr(NullLogger, log_level, self.nothing)


class MQTT:  # noqa: PLR0904  # too-many-public-methods
    """MQTT Client for CircuitPython.

    :param str broker: MQTT Broker URL or IP Address.
    :param int port: Optional port definition, defaults to MQTT_TLS_PORT if is_ssl is True,
        MQTT_TCP_PORT otherwise.
    :param str username: Username for broker authentication.
    :param str password: Password for broker authentication.
    :param str client_id: Optional client identifier, defaults to a unique, generated string.
    :param bool is_ssl: Sets a secure or insecure connection with the broker.
    :param int keep_alive: KeepAlive interval between the broker and the MiniMQTT client.
    :param int recv_timeout: receive timeout, in seconds.
    :param socket socket_pool: A pool of socket resources available for the given radio.
    :param ssl_context: SSL context for long-lived SSL connections.
    :param bool use_binary_mode: Messages are passed as bytearray instead of string to callbacks.
    :param int socket_timeout: How often to check socket state for read/write/connect operations,
        in seconds.
    :param int connect_retries: How many times to try to connect to the broker before giving up
        on connect or reconnect. Exponential backoff will be used for the retries.
    :param class user_data: arbitrary data to pass as a second argument to most of the callbacks.
        This works with all callbacks but the "on_message" and those added via add_topic_callback();
        for those, to get access to the user_data use the 'user_data' member of the MQTT object
        passed as 1st argument.

    """

    def __init__(  # noqa: PLR0915, PLR0913, Too many statements, Too many arguments
        self,
        *,
        broker: str,
        port: Optional[int] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        is_ssl: Optional[bool] = None,
        keep_alive: int = 60,
        recv_timeout: int = 10,
        socket_pool=None,
        ssl_context=None,
        use_binary_mode: bool = False,
        socket_timeout: int = 1,
        connect_retries: int = 5,
        user_data=None,
    ) -> None:
        self._connection_manager = get_connection_manager(socket_pool)
        self._socket_pool = socket_pool
        self._ssl_context = ssl_context
        self._sock = None
        self._backwards_compatible_sock = False
        self._use_binary_mode = use_binary_mode

        if recv_timeout <= socket_timeout:
            raise ValueError("recv_timeout must be strictly greater than socket_timeout")
        self._socket_timeout = socket_timeout
        self._recv_timeout = recv_timeout

        self.keep_alive = keep_alive
        self.user_data = user_data
        self._is_connected = False
        self._msg_size_lim = MQTT_MSG_SZ_LIM
        self._pid = 0
        self._last_msg_sent_timestamp: int = 0
        self.logger = NullLogger()
        """An optional logging attribute that can be set with with a Logger
        to enable debug logging."""

        self._reconnect_attempt = 0
        self._reconnect_timeout = float(0)
        self._reconnect_maximum_backoff = 32
        if connect_retries <= 0:
            raise ValueError("connect_retries must be positive")
        self._reconnect_attempts_max = connect_retries

        self.broker = broker
        self._username = username
        self._password = password
        if (
            self._password and len(password.encode("utf-8")) > MQTT_TOPIC_LENGTH_LIMIT
        ):  # [MQTT-3.1.3.5]
            raise ValueError("Password length is too large.")

        # The connection will be insecure unless is_ssl is set to True.
        # If the port is not specified, the security will be set based on the is_ssl parameter.
        # If the port is specified, the is_ssl parameter will be honored.
        self.port = MQTT_TCP_PORT
        if is_ssl is None:
            is_ssl = False
        self._is_ssl = is_ssl
        if self._is_ssl:
            self.port = MQTT_TLS_PORT
        if port:
            self.port = port

        self.session_id = None

        # define client identifier
        if client_id:
            # user-defined client_id MAY allow client_id's > 23 bytes or
            # non-alpha-numeric characters
            self.client_id = client_id
        else:
            # assign a unique client_id
            time_int = int(ticks_ms() / 10) % 1000
            self.client_id = f"cpy{randint(0, time_int)}{randint(0, 99)}"
            # generated client_id's enforce spec.'s length rules
            if len(self.client_id.encode("utf-8")) > 23 or not self.client_id:
                raise ValueError("MQTT Client ID must be between 1 and 23 bytes")

        # LWT
        self._lw_topic = None
        self._lw_qos = 0
        self._lw_msg = None
        self._lw_retain = False

        # List of subscribed topics, used for tracking
        self._subscribed_topics: List[str] = []
        self._on_message_filtered = MQTTMatcher()

        # Default topic callback methods
        self._on_message = None
        self.on_connect = None
        self.on_disconnect = None
        self.on_publish = None
        self.on_subscribe = None
        self.on_unsubscribe = None

    def __enter__(self):
        return self

    def __exit__(
        self,
        exception_type: Optional[Type[BaseException]],
        exception_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.deinit()

    def deinit(self) -> None:
        """De-initializes the MQTT client and disconnects from the mqtt broker."""
        self.disconnect()

    @property
    def mqtt_msg(self) -> Tuple[int, int]:
        """Returns maximum MQTT payload and topic size."""
        return self._msg_size_lim, MQTT_TOPIC_LENGTH_LIMIT

    @mqtt_msg.setter
    def mqtt_msg(self, msg_size: int) -> None:
        """Sets the maximum MQTT message payload size.

        :param int msg_size: Maximum MQTT payload size.
        """
        if msg_size < MQTT_MSG_MAX_SZ:
            self._msg_size_lim = msg_size

    def will_set(
        self,
        topic: str,
        msg: Union[str, int, float, bytes],
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        """Sets the last will and testament properties. MUST be called before `connect()`.

        :param str topic: MQTT Broker topic.
        :param str|int|float|bytes msg: Last will disconnection msg.
            msgs of type int & float are converted to a string.
            msgs of type byetes are left unchanged, as it is in the publish function.
        :param int qos: Quality of Service level, defaults to
            zero. Conventional options are ``0`` (send at most once), ``1``
            (send at least once), or ``2`` (send exactly once).
            .. note:: Only options ``1`` or ``0`` are QoS levels supported by this library.
        :param bool retain: Specifies if the msg is to be retained when
            it is published.
        """
        self.logger.debug("Setting last will properties")
        if self._is_connected:
            raise MMQTTStateError("Last Will should only be called before connect().")

        # check topic/msg/qos kwargs
        self._valid_topic(topic)
        if "+" in topic or "#" in topic:
            raise ValueError("Publish topic can not contain wildcards.")

        if msg is None:
            raise ValueError("Message can not be None.")
        if isinstance(msg, (int, float)):
            msg = str(msg).encode("ascii")
        elif isinstance(msg, str):
            msg = str(msg).encode("utf-8")
        elif isinstance(msg, bytes):
            pass
        else:
            raise ValueError("Invalid message data type.")
        if len(msg) > MQTT_MSG_MAX_SZ:
            raise ValueError(f"Message size larger than {MQTT_MSG_MAX_SZ} bytes.")

        self._valid_qos(qos)

        # fixed header. [3.3.1.2], [3.3.1.3]
        pub_hdr_fixed = bytearray([MQTT_PUBLISH | retain | qos << 1])

        # variable header = 2-byte Topic length (big endian)
        pub_hdr_var = bytearray(struct.pack(">H", len(topic.encode("utf-8"))))
        pub_hdr_var.extend(topic.encode("utf-8"))  # Topic name

        remaining_length = 2 + len(msg) + len(topic.encode("utf-8"))
        if qos > 0:
            # packet identifier where QoS level is 1 or 2. [3.3.2.2]
            remaining_length += 2
            self._pid = self._pid + 1 if self._pid < 0xFFFF else 1
            pub_hdr_var.append(self._pid >> 8)
            pub_hdr_var.append(self._pid & 0xFF)

        self._encode_remaining_length(pub_hdr_fixed, remaining_length)

        self._lw_qos = qos
        self._lw_topic = topic
        self._lw_msg = msg
        self._lw_retain = retain
        self.logger.debug("Last will properties successfully set")

    def add_topic_callback(self, mqtt_topic: str, callback_method) -> None:
        """Registers a callback_method for a specific MQTT topic.

        :param str mqtt_topic: MQTT topic identifier.
        :param function callback_method: The callback method.

        Expected method signature is ``on_message(client, topic, message)``
        To get access to the user_data, use the client argument.

        If a callback is called for the topic, then any "on_message" callback will not be called.
        """
        if mqtt_topic is None or callback_method is None:
            raise ValueError("MQTT topic and callback method must both be defined.")
        self._on_message_filtered[mqtt_topic] = callback_method

    def remove_topic_callback(self, mqtt_topic: str) -> None:
        """Removes a registered callback method.

        :param str mqtt_topic: MQTT topic identifier string.
        """
        if mqtt_topic is None:
            raise ValueError("MQTT Topic must be defined.")
        try:
            del self._on_message_filtered[mqtt_topic]
        except KeyError:
            raise KeyError("MQTT topic callback not added with add_topic_callback.") from None

    @property
    def on_message(self):
        """Called when a new message has been received on a subscribed topic.

        Expected method signature is ``on_message(client, topic, message)``
        To get access to the user_data, use the client argument.
        """
        return self._on_message

    @on_message.setter
    def on_message(self, method) -> None:
        self._on_message = method

    def _handle_on_message(self, topic: str, message: str):
        matched = False
        if topic is not None:
            for callback in self._on_message_filtered.iter_match(topic):
                callback(self, topic, message)  # on_msg with callback
                matched = True

        if not matched and self.on_message:  # regular on_message
            self.on_message(self, topic, message)

    def username_pw_set(self, username: str, password: Optional[str] = None) -> None:
        """Set client's username and an optional password.

        :param str username: Username to use with your MQTT broker.
        :param str password: Password to use with your MQTT broker.

        """
        if self._is_connected:
            raise MMQTTStateError("This method must be called before connect().")
        self._username = username
        if password is not None:
            self._password = password

    def connect(  # noqa: PLR0913, too many arguments in function definition
        self,
        clean_session: bool = True,
        host: Optional[str] = None,
        port: Optional[int] = None,
        keep_alive: Optional[int] = None,
        session_id: Optional[str] = None,
    ) -> int:
        """Initiates connection with the MQTT Broker. Will perform exponential back-off
        on connect failures.

        :param bool clean_session: Establishes a persistent session.
        :param str host: Hostname or IP address of the remote broker.
        :param int port: Network port of the remote broker.
        :param int keep_alive: Maximum period allowed for communication
            within single connection attempt, in seconds.
        :param str session_id: unique session ID,
            used for multiple simultaneous connections to the same host
        """

        last_exception = None
        backoff = False
        for i in range(0, self._reconnect_attempts_max):
            if i > 0:
                if backoff:
                    self._recompute_reconnect_backoff()
                else:
                    self._reset_reconnect_backoff()

            self.logger.debug(
                f"Attempting to connect to MQTT broker (attempt #{self._reconnect_attempt})"
            )

            try:
                ret = self._connect(
                    clean_session=clean_session,
                    host=host,
                    port=port,
                    keep_alive=keep_alive,
                    session_id=session_id,
                )
                self._reset_reconnect_backoff()
                return ret
            except (MemoryError, OSError, RuntimeError) as e:
                if isinstance(e, RuntimeError) and e.args == ("pystack exhausted",):
                    raise
                self._close_socket()
                self.logger.warning(f"Socket error when connecting: {e}")
                last_exception = e
                backoff = False
            except MMQTTException as e:
                self._close_socket()
                self.logger.info(f"MMQT error: {e}")
                if e.code in [
                    CONNACK_ERROR_INCORECT_USERNAME_PASSWORD,
                    CONNACK_ERROR_UNAUTHORIZED,
                ]:
                    # No sense trying these again, re-raise
                    raise
                last_exception = e
                backoff = True

        if self._reconnect_attempts_max > 1:
            exc_msg = "Repeated connect failures"
        else:
            exc_msg = "Connect failure"

        if last_exception:
            raise MMQTTException(exc_msg) from last_exception
        raise MMQTTException(exc_msg)

    def _send_bytes(
        self,
        buffer: Union[bytes, bytearray, memoryview],
    ):
        bytes_sent: int = 0
        bytes_to_send = len(buffer)
        view = memoryview(buffer)
        while bytes_sent < bytes_to_send:
            try:
                sent_now = self._sock.send(view[bytes_sent:])
                # Some versions of `Socket.send()` do not return the number of bytes sent.
                if not isinstance(sent_now, int):
                    return
                bytes_sent += sent_now
            except OSError as exc:
                if exc.errno == errno.EAGAIN:
                    continue
                raise

    def _connect(  # noqa: PLR0912, PLR0913, PLR0915, Too many branches, Too many arguments, Too many statements
        self,
        clean_session: bool = True,
        host: Optional[str] = None,
        port: Optional[int] = None,
        keep_alive: Optional[int] = None,
        session_id: Optional[str] = None,
    ) -> int:
        """Initiates connection with the MQTT Broker.

        :param bool clean_session: Establishes a persistent session.
        :param str host: Hostname or IP address of the remote broker.
        :param int port: Network port of the remote broker.
        :param int keep_alive: Maximum period allowed for communication, in seconds.
        :param str session_id: unique session ID,
            used for multiple simultaneous connections to the same host

        """
        if host:
            self.broker = host
        if port:
            self.port = port
        if keep_alive:
            self.keep_alive = keep_alive

        self.logger.debug("Attempting to establish MQTT connection...")

        if self._reconnect_attempt > 0:
            self.logger.debug(
                f"Sleeping for {self._reconnect_timeout:.3} seconds due to connect back-off"
            )
            time.sleep(self._reconnect_timeout)

        # Get a new socket
        self._sock = self._connection_manager.get_socket(
            self.broker,
            self.port,
            proto="mqtt:",
            session_id=session_id,
            timeout=self._socket_timeout,
            is_ssl=self._is_ssl,
            ssl_context=self._ssl_context,
        )
        self.session_id = session_id
        self._backwards_compatible_sock = not hasattr(self._sock, "recv_into")

        fixed_header = bytearray([0x10])

        # Variable CONNECT header [MQTT 3.1.2]
        # The byte array is used as a template.
        var_header = bytearray(b"\x00\x04MQTT\x04\x02\0\0")
        var_header[7] = clean_session << 1

        # Set up variable header and remaining_length
        remaining_length = 12 + len(self.client_id.encode("utf-8"))
        if self._username is not None:
            remaining_length += (
                2 + len(self._username.encode("utf-8")) + 2 + len(self._password.encode("utf-8"))
            )
            var_header[7] |= 0xC0
        if self.keep_alive:
            assert self.keep_alive < MQTT_TOPIC_LENGTH_LIMIT
            var_header[8] |= self.keep_alive >> 8
            var_header[9] |= self.keep_alive & 0x00FF
        if self._lw_topic:
            remaining_length += 2 + len(self._lw_topic.encode("utf-8")) + 2 + len(self._lw_msg)
            var_header[7] |= 0x4 | (self._lw_qos & 0x1) << 3 | (self._lw_qos & 0x2) << 3
            var_header[7] |= self._lw_retain << 5

        self._encode_remaining_length(fixed_header, remaining_length)
        self.logger.debug("Sending CONNECT to broker...")
        self.logger.debug(f"Fixed Header: {fixed_header}")
        self.logger.debug(f"Variable Header: {var_header}")
        self._send_bytes(fixed_header)
        self._send_bytes(var_header)
        # [MQTT-3.1.3-4]
        self._send_str(self.client_id)
        if self._lw_topic:
            # [MQTT-3.1.3-11]
            self._send_str(self._lw_topic)
            self._send_str(self._lw_msg)
        if self._username is not None:
            self._send_str(self._username)
            self._send_str(self._password)
        self._last_msg_sent_timestamp = ticks_ms()
        self.logger.debug("Receiving CONNACK packet from broker")
        stamp = ticks_ms()
        while True:
            op = self._wait_for_msg()
            if op == 32:
                rc = self._sock_exact_recv(3)
                assert rc[0] == 0x02
                if rc[2] != 0x00:
                    raise MMQTTException(CONNACK_ERRORS[rc[2]], code=rc[2])
                self._is_connected = True
                result = rc[0] & 1
                if self.on_connect is not None:
                    self.on_connect(self, self.user_data, result, rc[2])

                return result

            if op is None:
                if ticks_diff(ticks_ms(), stamp) / 1000 > self._recv_timeout:
                    raise MMQTTException(
                        f"No data received from broker for {self._recv_timeout} seconds."
                    )

    def _close_socket(self):
        if self._sock:
            self.logger.debug("Closing socket")
            self._connection_manager.close_socket(self._sock)
            self._sock = None

    def _encode_remaining_length(self, fixed_header: bytearray, remaining_length: int) -> None:
        """Encode Remaining Length [2.2.3]"""
        if remaining_length > 268_435_455:
            raise MMQTTException("invalid remaining length")

        # Remaining length calculation
        if remaining_length > 0x7F:
            while remaining_length > 0:
                encoded_byte = remaining_length % 0x80
                remaining_length = remaining_length // 0x80
                # if there is more data to encode, set the top bit of the byte
                if remaining_length > 0:
                    encoded_byte |= 0x80
                fixed_header.append(encoded_byte)
        else:
            fixed_header.append(remaining_length)

    def disconnect(self) -> None:
        """Disconnects the MiniMQTT client from the MQTT broker."""
        self._connected()
        self.logger.debug("Sending DISCONNECT packet to broker")
        try:
            self._send_bytes(MQTT_DISCONNECT)
        except (MemoryError, OSError, RuntimeError) as e:
            self.logger.warning(f"Unable to send DISCONNECT packet: {e}")
        self._close_socket()
        self._is_connected = False
        self._subscribed_topics = []
        self._last_msg_sent_timestamp = 0
        if self.on_disconnect is not None:
            self.on_disconnect(self, self.user_data, 0)

    def ping(self) -> list[int]:
        """Pings the MQTT Broker to confirm if the broker is alive or if
        there is an active network connection.
        Returns packet types of any messages received while waiting for PINGRESP.
        """
        self._connected()
        self.logger.debug("Sending PINGREQ")
        self._send_bytes(MQTT_PINGREQ)
        ping_timeout = self.keep_alive
        stamp = ticks_ms()

        self._last_msg_sent_timestamp = stamp
        rc, rcs = None, []
        while rc != MQTT_PINGRESP:
            rc = self._wait_for_msg()
            if rc:
                rcs.append(rc)
            if ticks_diff(ticks_ms(), stamp) / 1000 > ping_timeout:
                raise MMQTTException(
                    f"PINGRESP not returned from broker within {ping_timeout} seconds."
                )
        return rcs

    def publish(  # noqa:  PLR0912, Too many branches
        self,
        topic: str,
        msg: Union[str, int, float, bytes],
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        """Publishes a message to a topic provided.

        :param str topic: Unique topic identifier.
        :param str|int|float|bytes msg: Data to send to the broker.
        :param bool retain: Whether the message is saved by the broker.
        :param int qos: Quality of Service level for the message, defaults to zero.

        """
        self._connected()
        self._valid_topic(topic)
        if "+" in topic or "#" in topic:
            raise ValueError("Publish topic can not contain wildcards.")
        # check msg/qos kwargs
        if msg is None:
            raise ValueError("Message can not be None.")
        if isinstance(msg, (int, float)):
            msg = str(msg).encode("ascii")
        elif isinstance(msg, str):
            msg = str(msg).encode("utf-8")
        elif isinstance(msg, bytes):
            pass
        else:
            raise ValueError("Invalid message data type.")
        if len(msg) > MQTT_MSG_MAX_SZ:
            raise ValueError(f"Message size larger than {MQTT_MSG_MAX_SZ} bytes.")

        self._valid_qos(qos)

        # fixed header. [3.3.1.2], [3.3.1.3]
        pub_hdr_fixed = bytearray([MQTT_PUBLISH | retain | qos << 1])

        # variable header = 2-byte Topic length (big endian)
        pub_hdr_var = bytearray(struct.pack(">H", len(topic.encode("utf-8"))))
        pub_hdr_var.extend(topic.encode("utf-8"))  # Topic name

        remaining_length = 2 + len(msg) + len(topic.encode("utf-8"))
        if qos > 0:
            # packet identifier where QoS level is 1 or 2. [3.3.2.2]
            remaining_length += 2
            self._pid = self._pid + 1 if self._pid < 0xFFFF else 1
            pub_hdr_var.append(self._pid >> 8)
            pub_hdr_var.append(self._pid & 0xFF)

        self._encode_remaining_length(pub_hdr_fixed, remaining_length)

        self.logger.debug(
            "Sending PUBLISH\nTopic: %s\nMsg: %s\
                            \nQoS: %d\nRetain? %r",
            topic,
            msg,
            qos,
            retain,
        )
        self._send_bytes(pub_hdr_fixed)
        self._send_bytes(pub_hdr_var)
        self._send_bytes(msg)
        self._last_msg_sent_timestamp = ticks_ms()
        if qos == 0 and self.on_publish is not None:
            self.on_publish(self, self.user_data, topic, self._pid)
        if qos == 1:
            stamp = ticks_ms()
            while True:
                op = self._wait_for_msg()
                if op == 0x40:
                    sz = self._sock_exact_recv(1)
                    assert sz == b"\x02"
                    rcv_pid_buf = self._sock_exact_recv(2)
                    rcv_pid = rcv_pid_buf[0] << 0x08 | rcv_pid_buf[1]
                    if self._pid == rcv_pid:
                        if self.on_publish is not None:
                            self.on_publish(self, self.user_data, topic, rcv_pid)
                        return

                if op is None:
                    if ticks_diff(ticks_ms(), stamp) / 1000 > self._recv_timeout:
                        raise MMQTTException(
                            f"No data received from broker for {self._recv_timeout} seconds."
                        )

    def subscribe(  # noqa: PLR0912, PLR0915, Too many branches, Too many statements
        self, topic: Optional[Union[tuple, str, list]], qos: int = 0
    ) -> None:
        """Subscribes to a topic on the MQTT Broker.
        This method can subscribe to one topic or multiple topics.

        :param str|tuple|list topic: Unique MQTT topic identifier string. If
                                     this is a `tuple`, then the tuple should
                                     contain topic identifier string and qos
                                     level integer. If this is a `list`, then
                                     each list element should be a tuple containing
                                     a topic identifier string and qos level integer.
        :param int qos: Quality of Service level for the topic, defaults to
                        zero. Conventional options are ``0`` (send at most once), ``1``
                        (send at least once), or ``2`` (send exactly once).

        """
        self._connected()
        topics = None
        if isinstance(topic, tuple):
            topic, qos = topic
            self._valid_topic(topic)
            self._valid_qos(qos)
        if isinstance(topic, str):
            self._valid_topic(topic)
            self._valid_qos(qos)
            topics = [(topic, qos)]
        if isinstance(topic, list):
            topics = []
            for t, q in topic:
                self._valid_qos(q)
                self._valid_topic(t)
                topics.append((t, q))
        # Assemble packet
        self.logger.debug("Sending SUBSCRIBE to broker...")
        fixed_header = bytearray([MQTT_SUB])
        packet_length = 2 + (2 * len(topics)) + (1 * len(topics))
        packet_length += sum(len(topic.encode("utf-8")) for topic, qos in topics)
        self._encode_remaining_length(fixed_header, remaining_length=packet_length)
        self.logger.debug(f"Fixed Header: {fixed_header}")
        self._send_bytes(fixed_header)
        self._pid = self._pid + 1 if self._pid < 0xFFFF else 1
        packet_id_bytes = self._pid.to_bytes(2, "big")
        var_header = packet_id_bytes
        self.logger.debug(f"Variable Header: {var_header}")
        self._send_bytes(var_header)
        # attaching topic and QOS level to the packet
        payload = b""
        for t, q in topics:
            topic_size = len(t.encode("utf-8")).to_bytes(2, "big")
            qos_byte = q.to_bytes(1, "big")
            payload += topic_size + t.encode() + qos_byte
        for t, q in topics:
            self.logger.debug(f"SUBSCRIBING to topic {t} with QoS {q}")
        self.logger.debug(f"payload: {payload}")
        self._send_bytes(payload)
        stamp = ticks_ms()
        self._last_msg_sent_timestamp = stamp
        while True:
            op = self._wait_for_msg()
            if op is None:
                if ticks_diff(ticks_ms(), stamp) / 1000 > self._recv_timeout:
                    raise MMQTTException(
                        f"No data received from broker for {self._recv_timeout} seconds."
                    )
            else:
                if op == MQTT_SUBACK:
                    remaining_len = self._decode_remaining_length()
                    assert remaining_len > 0
                    rc = self._sock_exact_recv(2)
                    # Check packet identifier.
                    assert rc[0] == var_header[0] and rc[1] == var_header[1]
                    rc = self._sock_exact_recv(remaining_len - 2)
                    for i in range(0, remaining_len - 2):
                        if rc[i] not in [0, 1, 2]:
                            raise MMQTTException(
                                f"SUBACK Failure for topic {topics[i][0]}: {hex(rc[i])}"
                            )

                    for t, q in topics:
                        if self.on_subscribe is not None:
                            self.on_subscribe(self, self.user_data, t, q)
                        self._subscribed_topics.append(t)

                    return

                if op != MQTT_PUBLISH:
                    # [3.8.4] The Server is permitted to start sending PUBLISH packets
                    # matching the Subscription before the Server sends the SUBACK Packet.
                    raise MMQTTException(
                        f"invalid message received as response to SUBSCRIBE: {hex(op)}"
                    )

    def unsubscribe(  # noqa: PLR0912, Too many branches
        self, topic: Optional[Union[str, list]]
    ) -> None:
        """Unsubscribes from a MQTT topic.

        :param str|list topic: Unique MQTT topic identifier string or list.

        """
        topics = None
        if isinstance(topic, str):
            self._valid_topic(topic)
            topics = [(topic)]
        if isinstance(topic, list):
            topics = []
            for t in topic:
                self._valid_topic(t)
                topics.append(t)
        for t in topics:
            if t not in self._subscribed_topics:
                raise MMQTTStateError("Topic must be subscribed to before attempting unsubscribe.")
        # Assemble packet
        self.logger.debug("Sending UNSUBSCRIBE to broker...")
        fixed_header = bytearray([MQTT_UNSUB])
        packet_length = 2 + (2 * len(topics))
        packet_length += sum(len(topic.encode("utf-8")) for topic in topics)
        self._encode_remaining_length(fixed_header, remaining_length=packet_length)
        self.logger.debug(f"Fixed Header: {fixed_header}")
        self._send_bytes(fixed_header)
        self._pid = self._pid + 1 if self._pid < 0xFFFF else 1
        packet_id_bytes = self._pid.to_bytes(2, "big")
        var_header = packet_id_bytes
        self.logger.debug(f"Variable Header: {var_header}")
        self._send_bytes(var_header)
        payload = b""
        for t in topics:
            topic_size = len(t.encode("utf-8")).to_bytes(2, "big")
            payload += topic_size + t.encode()
        for t in topics:
            self.logger.debug(f"UNSUBSCRIBING from topic {t}")
        self._send_bytes(payload)
        self._last_msg_sent_timestamp = ticks_ms()
        self.logger.debug("Waiting for UNSUBACK...")
        while True:
            stamp = ticks_ms()
            op = self._wait_for_msg()
            if op is None:
                if ticks_diff(ticks_ms(), stamp) / 1000 > self._recv_timeout:
                    raise MMQTTException(
                        f"No data received from broker for {self._recv_timeout} seconds."
                    )
            else:
                if op == MQTT_UNSUBACK:
                    rc = self._sock_exact_recv(3)
                    assert rc[0] == 0x02
                    # [MQTT-3.32]
                    assert rc[1] == packet_id_bytes[0] and rc[2] == packet_id_bytes[1]
                    for t in topics:
                        if self.on_unsubscribe is not None:
                            self.on_unsubscribe(self, self.user_data, t, self._pid)
                        self._subscribed_topics.remove(t)
                    return
                if op != MQTT_PUBLISH:
                    # [3.10.4] The Server may continue to deliver existing messages buffered
                    # for delivery to the client prior to sending the UNSUBACK Packet.
                    raise MMQTTException(
                        f"invalid message received as response to UNSUBSCRIBE: {hex(op)}"
                    )

    def _recompute_reconnect_backoff(self) -> None:
        """
        Recompute the reconnection timeout. The self._reconnect_timeout will be used
        in self._connect() to perform the actual sleep.

        """
        self._reconnect_attempt = self._reconnect_attempt + 1
        self._reconnect_timeout = 2**self._reconnect_attempt
        self.logger.debug(f"Reconnect timeout computed to {self._reconnect_timeout:.2f}")

        if self._reconnect_timeout > self._reconnect_maximum_backoff:
            self.logger.debug(
                f"Truncating reconnect timeout to {self._reconnect_maximum_backoff} seconds"
            )
            self._reconnect_timeout = float(self._reconnect_maximum_backoff)

        # Add a sub-second jitter.
        # Even truncated timeout should have jitter added to it. This is why it is added here.
        jitter = randint(0, 1000) / 1000
        self.logger.debug(f"adding jitter {jitter:.2f} to {self._reconnect_timeout:.2f} seconds")
        self._reconnect_timeout += jitter

    def _reset_reconnect_backoff(self) -> None:
        """
        Reset reconnect back-off to the initial state.

        """
        self.logger.debug("Resetting reconnect backoff")
        self._reconnect_attempt = 0
        self._reconnect_timeout = float(0)

    def reconnect(self, resub_topics: bool = True) -> int:
        """Attempts to reconnect to the MQTT broker.
        Return the value from connect() if successful. Will disconnect first if already connected.
        Will perform exponential back-off on connect failures.

        :param bool resub_topics: Whether to resubscribe to previously subscribed topics.

        """

        self.logger.debug("Attempting to reconnect with MQTT broker")
        subscribed_topics = []
        if self.is_connected():
            # disconnect() will reset subscribed topics so stash them now.
            if resub_topics:
                subscribed_topics = self._subscribed_topics.copy()
            self.disconnect()

        ret = self.connect(session_id=self.session_id)
        self.logger.debug("Reconnected with broker")

        if resub_topics and subscribed_topics:
            self.logger.debug("Attempting to resubscribe to previously subscribed topics.")
            self._subscribed_topics = []
            while subscribed_topics:
                feed = subscribed_topics.pop()
                self.subscribe(feed)

        return ret

    def loop(self, timeout: float = 1.0) -> Optional[list[int]]:
        """Non-blocking message loop. Use this method to check for incoming messages.
        Returns list of packet types of any messages received or None.

        :param float timeout: return after this timeout, in seconds.

        """
        if timeout < self._socket_timeout:
            raise ValueError(
                f"loop timeout ({timeout}) must be >= "
                + f"socket timeout ({self._socket_timeout}))"
            )

        self._connected()
        self.logger.debug(f"waiting for messages for {timeout} seconds")

        stamp = ticks_ms()
        rcs = []

        while True:
            if ticks_diff(ticks_ms(), self._last_msg_sent_timestamp) / 1000 >= self.keep_alive:
                # Handle KeepAlive by expecting a PINGREQ/PINGRESP from the server
                self.logger.debug(
                    "KeepAlive period elapsed - requesting a PINGRESP from the server..."
                )
                rcs.extend(self.ping())
                # ping() itself contains a _wait_for_msg() loop which might have taken a while,
                # so check here as well.
                if ticks_diff(ticks_ms(), stamp) / 1000 > timeout:
                    self.logger.debug(f"Loop timed out after {timeout} seconds")
                    break

            rc = self._wait_for_msg()
            if rc is not None:
                rcs.append(rc)
            if ticks_diff(ticks_ms(), stamp) / 1000 > timeout:
                self.logger.debug(f"Loop timed out after {timeout} seconds")
                break

        return rcs if rcs else None

    def _wait_for_msg(  # noqa: PLR0912, Too many branches
        self, timeout: Optional[float] = None
    ) -> Optional[int]:
        """Reads and processes network events.
        Return the packet type or None if there is nothing to be received.

        :param float timeout: return after this timeout, in seconds.
        """
        # CPython socket module contains a timeout attribute
        if hasattr(self._socket_pool, "timeout"):
            try:
                res = self._sock_exact_recv(1)
            except self._socket_pool.timeout:
                return None
        else:  # socketpool, esp32spi, wiznet5k
            try:
                res = self._sock_exact_recv(1, timeout=timeout)
            except OSError as error:
                if error.errno in (errno.ETIMEDOUT, errno.EAGAIN):
                    # raised by a socket timeout if 0 bytes were present
                    return None
                raise MMQTTException("Unexpected error while waiting for messages") from error

        if res in [None, b""]:
            # If we get here, it means that there is nothing to be received
            return None
        pkt_type = res[0] & MQTT_PKT_TYPE_MASK
        self.logger.debug(f"Got message type: {hex(pkt_type)} pkt: {hex(res[0])}")
        if pkt_type == MQTT_PINGRESP:
            self.logger.debug("Got PINGRESP")
            sz = self._sock_exact_recv(1)[0]
            if sz != 0x00:
                raise MMQTTException(f"Unexpected PINGRESP returned from broker: {sz}.")
            return pkt_type

        if pkt_type != MQTT_PUBLISH:
            return pkt_type

        # Handle only the PUBLISH packet type from now on.
        sz = self._decode_remaining_length()
        # topic length MSB & LSB
        topic_len_buf = self._sock_exact_recv(2)
        topic_len = int((topic_len_buf[0] << 8) | topic_len_buf[1])

        if topic_len > sz - 2:
            raise MMQTTException(
                f"Topic length {topic_len} in PUBLISH packet exceeds remaining length {sz} - 2"
            )

        topic_buf = self._sock_exact_recv(topic_len)
        topic = str(topic_buf, "utf-8")
        sz -= topic_len + 2
        pid = 0
        if res[0] & 0x06:
            pid_buf = self._sock_exact_recv(2)
            pid = pid_buf[0] << 0x08 | pid_buf[1]
            sz -= 0x02

        # read message contents
        raw_msg = self._sock_exact_recv(sz)
        msg = raw_msg if self._use_binary_mode else str(raw_msg, "utf-8")
        self.logger.debug("Receiving PUBLISH \nTopic: %s\nMsg: %s\n", topic, raw_msg)
        self._handle_on_message(topic, msg)
        if res[0] & 0x06 == 0x02:
            pkt = bytearray(b"\x40\x02\0\0")
            struct.pack_into("!H", pkt, 2, pid)
            self._send_bytes(pkt)
        elif res[0] & 6 == 4:
            assert 0

        return pkt_type

    def _decode_remaining_length(self) -> int:
        """Decode Remaining Length [2.2.3]"""
        n = 0
        sh = 0
        while True:
            if sh > 28:
                raise MMQTTException("invalid remaining length encoding")
            b = self._sock_exact_recv(1)[0]
            n |= (b & 0x7F) << sh
            if not b & 0x80:
                return n
            sh += 7

    def _sock_exact_recv(self, bufsize: int, timeout: Optional[float] = None) -> bytearray:
        """Reads _exact_ number of bytes from the connected socket. Will only return
        bytearray with the exact number of bytes requested.

        The semantics of native socket receive is that it returns no more than the
        specified number of bytes (i.e. max size). However, it makes no guarantees in
        terms of the minimum size of the buffer, which could be 1 byte. This is a
        wrapper for socket recv() to ensure that no less than the expected number of
        bytes is returned or trigger a timeout exception.

        :param int bufsize: number of bytes to receive
        :param float timeout: timeout, in seconds. Defaults to keep_alive
        :return: byte array
        """
        stamp = ticks_ms()
        if not self._backwards_compatible_sock:
            # CPython, socketpool, esp32spi, wiznet5k
            rc = bytearray(bufsize)
            mv = memoryview(rc)
            recv_len = self._sock.recv_into(rc, bufsize)
            to_read = bufsize - recv_len
            if to_read < 0:
                raise MMQTTException(f"negative number of bytes to read: {to_read}")
            read_timeout = timeout if timeout is not None else self._recv_timeout
            mv = mv[recv_len:]
            while to_read > 0:
                recv_len = self._sock.recv_into(mv, to_read)
                to_read -= recv_len
                mv = mv[recv_len:]
                if ticks_diff(ticks_ms(), stamp) / 1000 > read_timeout:
                    raise MMQTTException(
                        f"Unable to receive {to_read} bytes within {read_timeout} seconds."
                    )
        else:  # Legacy: fona, esp_atcontrol
            # This will time out with socket timeout (not receive timeout).
            rc = self._sock.recv(bufsize)
            if not rc:
                self.logger.debug("_sock_exact_recv timeout")
                # If no bytes waiting, raise same exception as socketpool
                raise OSError(errno.ETIMEDOUT)
            # If any bytes waiting, try to read them all,
            # or raise exception if wait longer than read_timeout
            to_read = bufsize - len(rc)
            assert to_read >= 0
            read_timeout = self._recv_timeout
            while to_read > 0:
                recv = self._sock.recv(to_read)
                to_read -= len(recv)
                rc += recv
                if ticks_diff(ticks_ms(), stamp) / 1000 > read_timeout:
                    raise MMQTTException(
                        f"Unable to receive {to_read} bytes within {read_timeout} seconds."
                    )
        return rc

    def _send_str(self, string: str) -> None:
        """Encodes a string and sends it to a socket.

        :param str string: String to write to the socket.

        """
        if isinstance(string, str):
            self._send_bytes(struct.pack("!H", len(string.encode("utf-8"))))
            self._send_bytes(str.encode(string, "utf-8"))
        else:
            self._send_bytes(struct.pack("!H", len(string)))
            self._send_bytes(string)

    @staticmethod
    def _valid_topic(topic: str) -> None:
        """Validates if topic provided is proper MQTT topic format.

        :param str topic: Topic identifier

        """
        if topic is None:
            raise ValueError("Topic may not be NoneType")
        # [MQTT-4.7.3-1]
        if not topic:
            raise ValueError("Topic may not be empty.")
        # [MQTT-4.7.3-3]
        if len(topic.encode("utf-8")) > MQTT_TOPIC_LENGTH_LIMIT:
            raise ValueError(f"Encoded topic length is larger than {MQTT_TOPIC_LENGTH_LIMIT}")

    @staticmethod
    def _valid_qos(qos_level: int) -> None:
        """Validates if the QoS level is supported by this library

        :param int qos_level: Desired QoS level.

        """
        if isinstance(qos_level, int):
            if qos_level < 0 or qos_level > 2:
                raise NotImplementedError("QoS must be between 1 and 2.")
        else:
            raise ValueError("QoS must be an integer.")

    def _connected(self) -> None:
        """Returns MQTT client session status as True if connected, raises
        a `MMQTTStateError exception` if `False`.
        """
        if not self.is_connected():
            raise MMQTTStateError("MiniMQTT is not connected")

    def is_connected(self) -> bool:
        """Returns MQTT client session status as True if connected, False
        if not.
        """
        return self._is_connected and self._sock is not None

    # Logging
    def enable_logger(self, log_pkg, log_level: int = 20, logger_name: str = "log"):
        """Enables library logging by getting logger from the specified logging package
        and setting its log level.

        :param log_pkg: A Python logging package.
        :param log_level: Numeric value of a logging level, defaults to INFO.
        :param logger_name: name of the logger, defaults to "log".

        :return logger object

        """
        self.logger = log_pkg.getLogger(logger_name)
        self.logger.setLevel(log_level)

        return self.logger

    def disable_logger(self) -> None:
        """Disables logging."""
        self.logger = NullLogger()
