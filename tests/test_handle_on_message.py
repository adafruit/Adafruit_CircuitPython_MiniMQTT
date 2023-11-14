# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""_handle_on_message() tests"""

import socket
import ssl
from unittest import TestCase, main
from unittest.mock import MagicMock

import adafruit_minimqtt.adafruit_minimqtt as MQTT


class OnMessage(TestCase):
    """unit tests for _handle_on_message()"""

    def test_handle_on_message(self) -> None:
        """
        test that _handle_on_message() calls both regular message handlers if set.
        """

        host = "172.40.0.3"
        port = 1883

        user_data = "foo"
        mqtt_client = MQTT.MQTT(
            broker=host,
            port=port,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
            user_data=user_data,
        )

        mqtt_client.on_message_user_data = MagicMock()
        mqtt_client.on_message = MagicMock()

        topic = "devices/foo/bar"
        message = '{"foo": "bar"}'
        mqtt_client._handle_on_message(topic, message)
        mqtt_client.on_message.assert_called_with(mqtt_client, topic, message)
        mqtt_client.on_message_user_data.assert_called_with(
            mqtt_client, user_data, topic, message
        )


if __name__ == "__main__":
    main()
