# SPDX-FileCopyrightText: 2025 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""reconnect tests"""

import logging
import ssl
import sys

import pytest
from mocket import Mocket

import adafruit_minimqtt.adafruit_minimqtt as MQTT

if not sys.implementation.name == "circuitpython":
    from typing import Optional

    from circuitpython_typing.socket import (
        SocketType,
        SSLContextType,
    )


class FakeConnectionManager:
    """
    Fake ConnectionManager class
    """

    def __init__(self, socket):
        self._socket = socket
        self.close_cnt = 0

    def get_socket(  # noqa: PLR0913, Too many arguments
        self,
        host: str,
        port: int,
        proto: str,
        session_id: Optional[str] = None,
        *,
        timeout: float = 1.0,
        is_ssl: bool = False,
        ssl_context: Optional[SSLContextType] = None,
    ) -> SocketType:
        """
        Return the specified socket.
        """
        return self._socket

    def close_socket(self, socket) -> None:
        self.close_cnt += 1


def handle_subscribe(client, user_data, topic, qos):
    """
    Record topics into user data.
    """
    assert topic
    assert user_data["topics"] is not None
    assert qos == 0

    user_data["topics"].append(topic)


def handle_disconnect(client, user_data, zero):
    """
    Record disconnect.
    """

    user_data["disconnect"] = True


# The MQTT packet contents below were captured using Mosquitto client+server.
testdata = [
    (
        [],
        bytearray([
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x01,
            0x00,
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x02,
            0x00,
        ]),
    ),
    (
        [("foo/bar", 0)],
        bytearray([
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x01,
            0x00,
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x02,
            0x00,
        ]),
    ),
    (
        [("foo/bar", 0), ("bah", 0)],
        bytearray([
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x01,
            0x00,
            0x00,
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x02,
            0x00,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x03,
            0x00,
        ]),
    ),
]


@pytest.mark.parametrize(
    "topics,to_send",
    testdata,
    ids=[
        "no_topic",
        "single_topic",
        "multi_topic",
    ],
)
def test_reconnect(topics, to_send) -> None:
    """
    Test reconnect() handling, mainly that it performs disconnect on already connected socket.

    Nothing will travel over the wire, it is all fake.
    """
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    host = "localhost"
    port = 1883

    user_data = {"topics": [], "disconnect": False}
    mqtt_client = MQTT.MQTT(
        broker=host,
        port=port,
        ssl_context=ssl.create_default_context(),
        connect_retries=1,
        user_data=user_data,
    )

    mocket = Mocket(to_send)
    mqtt_client._connection_manager = FakeConnectionManager(mocket)
    mqtt_client.connect()

    mqtt_client.logger = logger

    if topics:
        logger.info(f"subscribing to {topics}")
        mqtt_client.subscribe(topics)

    logger.info("reconnecting")
    mqtt_client.on_subscribe = handle_subscribe
    mqtt_client.on_disconnect = handle_disconnect
    mqtt_client.reconnect()

    assert user_data.get("disconnect") == True
    assert mqtt_client._connection_manager.close_cnt == 1
    assert set(user_data.get("topics")) == set([t[0] for t in topics])


def test_reconnect_not_connected() -> None:
    """
    Test reconnect() handling not connected.
    """
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    host = "localhost"
    port = 1883

    user_data = {"topics": [], "disconnect": False}
    mqtt_client = MQTT.MQTT(
        broker=host,
        port=port,
        ssl_context=ssl.create_default_context(),
        connect_retries=1,
        user_data=user_data,
    )

    mocket = Mocket(
        bytearray([
            0x20,  # CONNACK
            0x02,
            0x00,
            0x00,
        ])
    )
    mqtt_client._connection_manager = FakeConnectionManager(mocket)

    mqtt_client.logger = logger
    mqtt_client.on_disconnect = handle_disconnect
    mqtt_client.reconnect()

    assert user_data.get("disconnect") == False
    assert mqtt_client._connection_manager.close_cnt == 0
