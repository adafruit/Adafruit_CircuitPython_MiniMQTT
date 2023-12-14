# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""subscribe tests"""

import logging
import ssl

import pytest
from mocket import Mocket

import adafruit_minimqtt.adafruit_minimqtt as MQTT


# pylint: disable=unused-argument
def handle_subscribe(client, user_data, topic, qos):
    """
    Record topics into user data.
    """
    assert topic
    assert qos == 0

    user_data.append(topic)


# The MQTT packet contents below were captured using Mosquitto client+server.
testdata = [
    # short topic with remaining length encoded as single byte
    (
        "foo/bar",
        bytearray([0x90, 0x03, 0x00, 0x01, 0x00]),
        bytearray(
            [
                0x82,  # fixed header
                0x0C,  # remaining length
                0x00,
                0x01,  # message ID
                0x00,
                0x07,  # topic length
                0x66,  # topic
                0x6F,
                0x6F,
                0x2F,
                0x62,
                0x61,
                0x72,
                0x00,  # QoS
            ]
        ),
    ),
    # remaining length is encoded as 2 bytes due to long topic name.
    (
        "f" + "o" * 257,
        bytearray([0x90, 0x03, 0x00, 0x01, 0x00]),
        bytearray(
            [
                0x82,  # fixed header
                0x87,  # remaining length
                0x02,
                0x00,  # message ID
                0x01,
                0x01,  # topic length
                0x02,
                0x66,  # topic
            ]
            + [0x6F] * 257
            + [0x00]  # QoS
        ),
    ),
]


@pytest.mark.parametrize(
    "topic,to_send,exp_recv", testdata, ids=["short_topic", "long_topic"]
)
def test_subscribe(topic, to_send, exp_recv) -> None:
    """
    Protocol level testing of SUBSCRIBE and SUBACK packet handling.

    Nothing will travel over the wire, it is all fake.
    """
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    host = "localhost"
    port = 1883

    subscribed_topics = []
    mqtt_client = MQTT.MQTT(
        broker=host,
        port=port,
        ssl_context=ssl.create_default_context(),
        connect_retries=1,
        user_data=subscribed_topics,
    )

    mqtt_client.on_subscribe = handle_subscribe

    # patch is_connected() to avoid CONNECT/CONNACK handling.
    mqtt_client.is_connected = lambda: True
    mocket = Mocket(to_send)
    # pylint: disable=protected-access
    mqtt_client._sock = mocket

    mqtt_client.logger = logger

    # pylint: disable=logging-fstring-interpolation
    logger.info(f"subscribing to {topic}")
    mqtt_client.subscribe(topic)

    assert topic in subscribed_topics
    assert mocket.sent == exp_recv
