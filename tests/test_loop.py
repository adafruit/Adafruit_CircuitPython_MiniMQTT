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
from adafruit_ticks import ticks_add

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


class RecordingPoller:
    """
    Poller test double that records registration and ipoll timeout values.
    """

    def __init__(self, events=None, register_exception=None):
        self.events = list(events or [])
        self.registered = []
        self.unregistered = []
        self.timeouts = []
        self.register_exception = register_exception

    def register(self, sock, event_mask):
        """Record poll registration."""
        if self.register_exception:
            raise self.register_exception
        self.registered.append((sock, event_mask))

    def unregister(self, sock):
        """Record poll unregistration."""
        self.unregistered.append(sock)

    def ipoll(self, timeout):
        """Record timeout and return the next event batch."""
        self.timeouts.append(timeout)
        if not self.events:
            return iter(())
        return iter(self.events.pop(0))


class SelectModuleStub:
    """
    Select module stub for socket readiness tests.
    """

    POLLIN = MQTT._SELECT_POLLIN

    def __init__(self, poller=None):
        self.poller = poller

    def poll(self):
        """Return the configured poller."""
        return self.poller


class SelectWithoutPoll:
    """
    Select module stub without poll support.
    """

    POLLIN = 1


def make_registered_client(poller):
    """Create a client with a registered socket poller."""
    mqtt_client = MQTT.MQTT(
        broker="127.0.0.1",
        port=1883,
        socket_pool=socket,
        ssl_context=ssl.create_default_context(),
    )
    mqtt_client._sock = Nulltet()

    with patch.object(MQTT, "select", SelectModuleStub(poller=poller)):
        mqtt_client._open_socket_poller()

    return mqtt_client


def make_connection_manager(socket):
    """Create a connection manager mock that returns the given socket."""
    connection_manager = mock.Mock()
    connection_manager.get_socket.return_value = socket
    return connection_manager


def patch_successful_connack(mqtt_client):
    """Patch connect() packet reads to receive a successful CONNACK."""
    return (
        patch.object(mqtt_client, "_wait_for_msg", return_value=0x20),
        patch.object(
            mqtt_client,
            "_sock_exact_recv",
            return_value=bytearray([0x02, 0x00, 0x00]),
        ),
    )


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

    @pytest.mark.parametrize(
        "select_module",
        [
            None,
            SelectWithoutPoll(),
        ],
        ids=[
            "no_select",
            "select_without_poll",
        ],
    )
    def test_open_socket_poller_ignored_without_select_poll(self, select_module):
        """
        MiniMQTT should run without select.poll support.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
        )
        mqtt_client._sock = Nulltet()

        with patch.object(MQTT, "select", select_module):
            mqtt_client._open_socket_poller()

        assert mqtt_client._socket_poller is None
        assert mqtt_client._poll_socket is None

    def test_connect_creates_socket_poller(self):
        """
        Connecting should create a socket poller for the new socket.
        """
        sockettet = Nulltet()
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            socket_timeout=0.25,
        )
        connection_manager = make_connection_manager(sockettet)
        mqtt_client._connection_manager = connection_manager
        poller = RecordingPoller()
        connack_header, connack_body = patch_successful_connack(mqtt_client)

        with patch.object(MQTT, "select", SelectModuleStub(poller=poller)):
            with connack_header, connack_body:
                assert mqtt_client.connect() == 0

        assert connection_manager.get_socket.call_args.kwargs["timeout"] == 0.25
        assert mqtt_client._socket_poller is poller
        assert poller.registered == [(sockettet, MQTT._SELECT_POLLIN)]
        assert sockettet.sent

    @pytest.mark.parametrize(
        "register_exception",
        [
            OSError("stream operation not supported"),
            TypeError("fileno() returned a non-integer"),
        ],
    )
    def test_connect_succeeds_with_unpollable_socket(self, register_exception):
        """
        A socket can be usable by MiniMQTT even when select.poll cannot register it.
        """
        sockettet = Nulltet()
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
        )
        mqtt_client._connection_manager = make_connection_manager(sockettet)
        poller = RecordingPoller(register_exception=register_exception)
        connack_header, connack_body = patch_successful_connack(mqtt_client)

        with patch.object(MQTT, "select", SelectModuleStub(poller=poller)):
            with connack_header, connack_body:
                assert mqtt_client.connect() == 0

        assert mqtt_client._socket_poller is None
        assert sockettet.sent

    @pytest.mark.parametrize(
        ("events", "timeout", "expected", "expected_timeout"),
        [
            ([], 0, False, 0),
            ([[(object(), 0)]], 0, False, 0),
            ([[(object(), MQTT._SELECT_POLLIN)]], 250, True, 250),
            pytest.param(
                [[(object(), MQTT._SELECT_POLL_ERROR_FLAGS)]],
                0,
                True,
                0,
                marks=pytest.mark.skipif(
                    not MQTT._SELECT_POLL_ERROR_FLAGS,
                    reason="select error flags unavailable on this platform",
                ),
                id="error_event",
            ),
        ],
        ids=[
            "no_events",
            "non_readable_event",
            "readable_event",
            None,
        ],
    )
    def test_socket_poller_readable_events(self, events, timeout, expected, expected_timeout):
        """
        Socket polling should classify poll events and pass millisecond timeouts.
        """
        poller = RecordingPoller(events=events)
        mqtt_client = make_registered_client(poller)

        assert mqtt_client._socket_poller_readable(timeout) is expected
        assert poller.timeouts == [expected_timeout]

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

    def test_loop_timeout_can_be_less_than_socket_timeout(self):
        """
        loop() should allow a timeout smaller than the socket timeout when socket readiness
        can determine that no message is waiting.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            socket_timeout=1,
        )

        mqtt_client.is_connected = lambda: True
        mqtt_client._last_msg_sent_timestamp = MQTT.ticks_ms()
        socket_readiness = mock.Mock(side_effect=lambda timeout_ms: time.sleep(timeout_ms / 1000))
        mqtt_client._socket_poller = object()
        with patch.object(mqtt_client, "_wait_for_msg") as wait_for_msg_mock, patch.object(
            mqtt_client, "_socket_poller_readable", socket_readiness
        ):
            assert mqtt_client.loop(timeout=0.5) is None

        assert socket_readiness.call_args.args[0] == pytest.approx(500, abs=10)
        wait_for_msg_mock.assert_not_called()

    @pytest.mark.parametrize(
        ("last_msg_age_ms", "expected_poll_timeout_ms"),
        [
            (9500, 500),
            (9000, 1000),
        ],
        ids=["keepalive_before_loop_timeout", "keepalive_matches_loop_timeout"],
    )
    def test_loop_polls_until_keepalive_deadline_before_pinging(
        self, last_msg_age_ms, expected_poll_timeout_ms
    ):
        """
        Socket readiness waits should stop at the next keepalive deadline and then ping.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            keep_alive=10,
            socket_timeout=1,
        )
        mqtt_client.is_connected = lambda: True
        mqtt_client._last_msg_sent_timestamp = ticks_add(MQTT.ticks_ms(), -last_msg_age_ms)

        def wait_until_keepalive_deadline(_timeout_ms):
            mqtt_client._last_msg_sent_timestamp = ticks_add(MQTT.ticks_ms(), -10000)
            return False

        mqtt_client._socket_poller = object()
        socket_readiness = mock.Mock(side_effect=wait_until_keepalive_deadline)
        with patch.object(mqtt_client, "ping", side_effect=RuntimeError), patch.object(
            mqtt_client, "_socket_poller_readable", socket_readiness
        ):
            with pytest.raises(RuntimeError):
                mqtt_client.loop(timeout=1)

        assert socket_readiness.call_args.args[0] == pytest.approx(expected_poll_timeout_ms, abs=10)

    def test_loop_timeout_vs_socket_timeout(self):
        """
        loop() should reject a timeout smaller than the socket timeout when socket readiness
        is unavailable.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            socket_timeout=1,
        )

        mqtt_client.is_connected = lambda: True
        mqtt_client._last_msg_sent_timestamp = MQTT.ticks_ms()
        mqtt_client._socket_poller = None
        with pytest.raises(ValueError) as context:
            mqtt_client.loop(timeout=0.5)

        assert "socket readiness support" in str(context)

    def test_loop_rejects_negative_timeout(self):
        """
        loop() should reject negative timeout values.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            socket_timeout=1,
        )

        mqtt_client.is_connected = lambda: True
        with pytest.raises(ValueError) as context:
            mqtt_client.loop(timeout=-0.1)

        assert "loop timeout" in str(context)

    def test_loop_zero_timeout_processes_waiting_message(self):
        """
        loop() should process an already-waiting message when timeout is zero.
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            socket_timeout=1,
        )

        mqtt_client.is_connected = lambda: True
        mqtt_client._last_msg_sent_timestamp = MQTT.ticks_ms()
        mqtt_client._socket_poller = object()
        socket_readiness = mock.Mock(side_effect=[True, False])
        with patch.object(mqtt_client, "_wait_for_msg") as wait_for_msg_mock, patch.object(
            mqtt_client, "_socket_poller_readable", socket_readiness
        ):
            wait_for_msg_mock.return_value = MQTT.MQTT_PUBLISH
            assert mqtt_client.loop(timeout=0) == [MQTT.MQTT_PUBLISH]

        wait_for_msg_mock.assert_called_once()

    def test_loop_is_connected(self):
        """
        loop() should throw MMQTTStateError if not connected
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
        )

        with pytest.raises(MQTT.MMQTTStateError) as context:
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
        mqtt_client._last_msg_sent_timestamp = ticks_add(
            MQTT.ticks_ms(), -keep_alive_timeout * 1000
        )

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
