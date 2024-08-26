# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""exponential back-off tests"""

import socket
import ssl
import time
from unittest.mock import call, patch

import pytest

import adafruit_minimqtt.adafruit_minimqtt as MQTT


class TestExpBackOff:
    """basic exponential back-off test"""

    connect_times = []
    raise_exception = None

    def fake_connect(self, arg):
        """connect() replacement that records the call times and always raises OSError"""
        self.connect_times.append(time.monotonic())
        raise self.raise_exception

    def test_failing_connect(self) -> None:
        """test that exponential back-off is used when connect() always raises OSError"""
        # use RFC 1918 address to avoid dealing with IPv6 in the call list below
        host = "172.40.0.3"
        port = 1883
        self.connect_times = []
        error_code = MQTT.CONNACK_ERROR_SERVER_UNAVAILABLE
        self.raise_exception = MQTT.MMQTTException(MQTT.CONNACK_ERRORS[error_code], code=error_code)

        with patch.object(socket.socket, "connect") as mock_method:
            mock_method.side_effect = self.fake_connect

            connect_retries = 3
            mqtt_client = MQTT.MQTT(
                broker=host,
                port=port,
                socket_pool=socket,
                ssl_context=ssl.create_default_context(),
                connect_retries=connect_retries,
            )
            print("connecting")
            with pytest.raises(MQTT.MMQTTException) as context:
                mqtt_client.connect()
                assert mqtt_client._sock is None
            assert "Repeated connect failures" in str(context)

            mock_method.assert_called()
            calls = [call((host, port)) for _ in range(0, connect_retries)]
            mock_method.assert_has_calls(calls)

            print(f"connect() call times: {self.connect_times}")
            for i in range(1, connect_retries):
                assert self.connect_times[i] >= 2**i

    def test_unauthorized(self) -> None:
        """test that exponential back-off is used when connect() always raises OSError"""
        # use RFC 1918 address to avoid dealing with IPv6 in the call list below
        host = "172.40.0.3"
        port = 1883
        self.connect_times = []
        error_code = MQTT.CONNACK_ERROR_UNAUTHORIZED
        self.raise_exception = MQTT.MMQTTException(MQTT.CONNACK_ERRORS[error_code], code=error_code)

        with patch.object(socket.socket, "connect") as mock_method:
            mock_method.side_effect = self.fake_connect

            connect_retries = 3
            mqtt_client = MQTT.MQTT(
                broker=host,
                port=port,
                socket_pool=socket,
                ssl_context=ssl.create_default_context(),
                connect_retries=connect_retries,
            )
            print("connecting")
            with pytest.raises(MQTT.MMQTTException) as context:
                mqtt_client.connect()
                assert mqtt_client._sock is None
            assert "Connection Refused - Unauthorized" in str(context)

            mock_method.assert_called()
            assert len(self.connect_times) == 1
