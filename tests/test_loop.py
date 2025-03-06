# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

# ruff: noqa: PLR6301 no-self-use

"""loop() tests"""

import errno
import random
import socket
import ssl
import time
from unittest import mock
from unittest.mock import patch

import pytest

import adafruit_minimqtt.adafruit_minimqtt as MQTT


class Nulltet:
    """
    Mock Socket that does nothing.

    Inspired by the Mocket class from Adafruit_CircuitPython_Requests
    """

    def __init__(self):
        self.sent = bytearray()

        self.timeout = mock.Mock()
        self.connect = mock.Mock()
        self.close = mock.Mock()

    def send(self, bytes_to_send):
        """
        Record the bytes. return the length of this bytearray.
        """
        self.sent.extend(bytes_to_send)
        return len(bytes_to_send)

    # MiniMQTT checks for the presence of "recv_into" and switches behavior based on that.
    def recv_into(self, retbuf, bufsize):
        """Always raise timeout exception."""
        exc = OSError()
        exc.errno = errno.ETIMEDOUT
        raise exc


class Pingtet:
    """
    Mock Socket tailored for PINGREQ testing.
    Records sent data, hands out PINGRESP for each PINGREQ received.

    Inspired by the Mocket class from Adafruit_CircuitPython_Requests
    """

    PINGRESP = bytearray([0xD0, 0x00])

    def __init__(self):
        self._to_send = self.PINGRESP

        self.sent = bytearray()

        self.timeout = mock.Mock()
        self.connect = mock.Mock()
        self.close = mock.Mock()

        self._got_pingreq = False

    def send(self, bytes_to_send):
        """
        Recognize PINGREQ and record the indication that it was received.
        Assumes it was sent in one chunk (of 2 bytes).
        Also record the bytes. return the length of this bytearray.
        """
        self.sent.extend(bytes_to_send)
        if bytes_to_send == b"\xc0\0":
            self._got_pingreq = True
        return len(bytes_to_send)

    # MiniMQTT checks for the presence of "recv_into" and switches behavior based on that.
    def recv_into(self, retbuf, bufsize):
        """
        If the PINGREQ indication is on, return PINGRESP, otherwise raise timeout exception.
        """
        if self._got_pingreq:
            size = min(bufsize, len(self._to_send))
            if size == 0:
                return size
            chop = self._to_send[0:size]
            retbuf[0:] = chop
            self._to_send = self._to_send[size:]
            if len(self._to_send) == 0:
                self._got_pingreq = False
                self._to_send = self.PINGRESP
            return size

        exc = OSError()
        exc.errno = errno.ETIMEDOUT
        raise exc


class TestLoop:
    """basic loop() test"""

    connect_times = []
    INITIAL_RCS_VAL = 42
    rcs_val = INITIAL_RCS_VAL

    def fake_wait_for_msg(self, timeout=1):
        """_wait_for_msg() replacement. Sleeps for 1 second and returns an integer."""
        time.sleep(timeout)
        retval = self.rcs_val
        self.rcs_val += 1
        return retval

    def test_loop_basic(self) -> None:
        """
        test that loop() returns only after the specified timeout, regardless whether
        _wait_for_msg() returned repeatedly within that timeout.
        """

        host = "172.40.0.3"
        port = 1883

        mqtt_client = MQTT.MQTT(
            broker=host,
            port=port,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
        )

        with patch.object(mqtt_client, "_wait_for_msg") as wait_for_msg_mock, patch.object(
            mqtt_client, "is_connected"
        ) as is_connected_mock:
            wait_for_msg_mock.side_effect = self.fake_wait_for_msg
            is_connected_mock.side_effect = lambda: True

            time_before = time.monotonic()
            timeout = random.randint(3, 8)
            mqtt_client._last_msg_sent_timestamp = MQTT.ticks_ms()
            rcs = mqtt_client.loop(timeout=timeout)
            time_after = time.monotonic()

            assert time_after - time_before >= timeout
            wait_for_msg_mock.assert_called()

            # Check the return value.
            assert rcs is not None
            assert len(rcs) >= 1
            expected_rc = self.INITIAL_RCS_VAL
            for ret_code in rcs:
                assert ret_code == expected_rc
                expected_rc += 1

    def test_loop_timeout_vs_socket_timeout(self):
        """
        loop() should throw MMQTTException if the timeout argument
        is bigger than the socket timeout.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            socket_timeout=1,
        )

        mqtt_client.is_connected = lambda: True
        with pytest.raises(MQTT.MMQTTException) as context:
            mqtt_client.loop(timeout=0.5)

        assert "loop timeout" in str(context)

    def test_loop_is_connected(self):
        """
        loop() should throw MMQTTException if not connected
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
        )

        with pytest.raises(MQTT.MMQTTException) as context:
            mqtt_client.loop(timeout=1)

        assert "not connected" in str(context)

    def test_loop_ping_timeout(self):
        """Verify that ping will be sent even with loop timeout bigger than keep alive timeout
        and no outgoing messages are sent."""

        recv_timeout = 2
        keep_alive_timeout = recv_timeout * 2
        mqtt_client = MQTT.MQTT(
            broker="localhost",
            port=1883,
            ssl_context=ssl.create_default_context(),
            connect_retries=1,
            socket_timeout=1,
            recv_timeout=recv_timeout,
            keep_alive=keep_alive_timeout,
        )

        # patch is_connected() to avoid CONNECT/CONNACK handling.
        mqtt_client.is_connected = lambda: True
        mocket = Pingtet()
        mqtt_client._sock = mocket

        start = time.monotonic()
        res = mqtt_client.loop(timeout=2 * keep_alive_timeout + recv_timeout)
        assert time.monotonic() - start >= 2 * keep_alive_timeout
        assert len(mocket.sent) > 0
        assert len(res) == 3
        assert set(res) == {0xD0}

    def test_loop_ping_vs_msgs_sent(self):
        """Verify that ping will not be sent unnecessarily."""

        recv_timeout = 2
        keep_alive_timeout = recv_timeout * 2
        mqtt_client = MQTT.MQTT(
            broker="localhost",
            port=1883,
            ssl_context=ssl.create_default_context(),
            connect_retries=1,
            socket_timeout=1,
            recv_timeout=recv_timeout,
            keep_alive=keep_alive_timeout,
        )

        # patch is_connected() to avoid CONNECT/CONNACK handling.
        mqtt_client.is_connected = lambda: True

        # With QoS=0 no PUBACK message is sent, so Nulltet can be used.
        mocket = Nulltet()
        mqtt_client._sock = mocket

        i = 0
        topic = "foo"
        message = "bar"
        for _ in range(3 * keep_alive_timeout):
            mqtt_client.publish(topic, message, qos=0)
            mqtt_client.loop(1)
            i += 1

        # This means no other messages than the PUBLISH messages generated by the code above.
        assert len(mocket.sent) == i * (2 + 2 + len(topic) + len(message))
