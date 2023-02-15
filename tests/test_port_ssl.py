# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""tests that verify the connect behavior w.r.t. port number and TLS"""

import socket
import ssl
from unittest import TestCase, main
from unittest.mock import Mock, call, patch

import adafruit_minimqtt.adafruit_minimqtt as MQTT


class PortSslSetup(TestCase):
    """This class contains tests that verify how host/port and TLS is set for connect().
    These tests assume that there is no MQTT broker running on the hosts/ports they connect to.
    """

    def test_default_port(self) -> None:
        """verify default port value and that TLS is not used"""
        host = "127.0.0.1"
        port = 1883

        with patch.object(socket.socket, "connect") as connect_mock:
            ssl_context = ssl.create_default_context()
            mqtt_client = MQTT.MQTT(
                broker=host,
                socket_pool=socket,
                ssl_context=ssl_context,
                connect_retries=1,
            )

            ssl_mock = Mock()
            ssl_context.wrap_socket = ssl_mock

            with self.assertRaises(MQTT.MMQTTException):
                expected_port = port
                mqtt_client.connect()

            ssl_mock.assert_not_called()
            connect_mock.assert_called()
            # Assuming the repeated calls will have the same arguments.
            connect_mock.assert_has_calls([call((host, expected_port))])

    def test_connect_override(self):
        """Test that connect() can override host and port."""
        host = "127.0.0.1"
        port = 1883

        with patch.object(socket.socket, "connect") as connect_mock:
            connect_mock.side_effect = OSError("artificial error")
            mqtt_client = MQTT.MQTT(
                broker=host,
                port=port,
                socket_pool=socket,
                connect_retries=1,
            )

            with self.assertRaises(MQTT.MMQTTException):
                expected_host = "127.0.0.2"
                expected_port = 1884
                self.assertNotEqual(expected_port, port, "port override should differ")
                self.assertNotEqual(expected_host, host, "host override should differ")
                mqtt_client.connect(host=expected_host, port=expected_port)

            connect_mock.assert_called()
            # Assuming the repeated calls will have the same arguments.
            connect_mock.assert_has_calls([call((expected_host, expected_port))])

    def test_tls_port(self) -> None:
        """verify that when is_ssl=True is set, the default port is 8883
        and the socket is TLS wrapped. Also test that the TLS port can be overridden."""
        host = "127.0.0.1"

        for port in [None, 8884]:
            if port is None:
                expected_port = 8883
            else:
                expected_port = port
            with self.subTest():
                ssl_mock = Mock()
                mqtt_client = MQTT.MQTT(
                    broker=host,
                    port=port,
                    socket_pool=socket,
                    is_ssl=True,
                    ssl_context=ssl_mock,
                    connect_retries=1,
                )

                socket_mock = Mock()
                connect_mock = Mock(side_effect=OSError)
                socket_mock.connect = connect_mock
                ssl_mock.wrap_socket = Mock(return_value=socket_mock)

                with self.assertRaises(MQTT.MMQTTException):
                    mqtt_client.connect()

                ssl_mock.wrap_socket.assert_called()

                connect_mock.assert_called()
                # Assuming the repeated calls will have the same arguments.
                connect_mock.assert_has_calls([call((host, expected_port))])

    def test_tls_without_ssl_context(self) -> None:
        """verify that when is_ssl=True is set, the code will check that ssl_context is not None"""
        host = "127.0.0.1"

        mqtt_client = MQTT.MQTT(
            broker=host,
            socket_pool=socket,
            is_ssl=True,
            ssl_context=None,
            connect_retries=1,
        )

        with self.assertRaises(RuntimeError) as context:
            mqtt_client.connect()
            self.assertTrue("ssl_context must be set" in str(context))


if __name__ == "__main__":
    main()
