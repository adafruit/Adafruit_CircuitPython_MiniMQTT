# SPDX-FileCopyrightText: 2024 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""receive timeout tests"""

import socket
import time
from unittest import TestCase, main
from unittest.mock import Mock

from mocket import Mocket

import adafruit_minimqtt.adafruit_minimqtt as MQTT


class RecvTimeout(TestCase):
    """This class contains tests for receive timeout handling."""

    def test_recv_timeout_vs_keepalive(self) -> None:
        """verify that receive timeout as used via ping() is different to keep alive timeout"""

        for side_effect in [lambda ret_buf, buf_size: 0, socket.timeout]:
            with self.subTest():
                host = "127.0.0.1"

                recv_timeout = 4
                keep_alive = recv_timeout * 2
                mqtt_client = MQTT.MQTT(
                    broker=host,
                    socket_pool=socket,
                    connect_retries=1,
                    socket_timeout=recv_timeout // 2,
                    recv_timeout=recv_timeout,
                    keep_alive=keep_alive,
                )

                # Create a mock socket that will accept anything and return nothing.
                socket_mock = Mocket(b"")
                socket_mock.recv_into = Mock(side_effect=side_effect)
                mqtt_client._sock = socket_mock

                mqtt_client._connected = lambda: True
                start = time.monotonic()
                with self.assertRaises(MQTT.MMQTTException):
                    mqtt_client.ping()

                now = time.monotonic()
                assert recv_timeout <= (now - start) <= (keep_alive + 0.5)


if __name__ == "__main__":
    main()
