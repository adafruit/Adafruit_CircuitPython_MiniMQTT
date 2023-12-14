# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""unsubscribe tests"""

import logging
import ssl

import pytest
from mocket import Mocket

import adafruit_minimqtt.adafruit_minimqtt as MQTT


# pylint: disable=unused-argument
def handle_unsubscribe(client, user_data, topic, pid):
    """
    Record topics into user data.
    """
    assert topic

    user_data.append(topic)


# The MQTT packet contents below were captured using Mosquitto client+server.
# These are verbatim, except message ID that was changed from 2 to 1 since in the real world
# capture the UNSUBSCRIBE packet followed the SUBSCRIBE packet.
testdata = [
    # short topic with remaining length encoded as single byte
    (
        "foo/bar",
        bytearray([0xB0, 0x02, 0x00, 0x01]),
        bytearray(
            [
                0xA2,  # fixed header
                0x0B,  # remaining length
                0x00,  # message ID
                0x01,
                0x00,  # topic length
                0x07,
                0x66,  # topic
                0x6F,
                0x6F,
                0x2F,
                0x62,
                0x61,
                0x72,
            ]
        ),
    ),
    # remaining length is encoded as 2 bytes due to long topic name.
    (
        "f" + "o" * 257,
        bytearray([0xB0, 0x02, 0x00, 0x01]),
        bytearray(
            [
                0xA2,  # fixed header
                0x86,  # remaining length
                0x02,
                0x00,  # message ID
                0x01,
                0x01,  # topic length
                0x02,
                0x66,  # topic
            ]
            + [0x6F] * 257
        ),
    ),
]


@pytest.mark.parametrize(
    "topic,to_send,exp_recv", testdata, ids=["short_topic", "long_topic"]
)
def test_unsubscribe(topic, to_send, exp_recv) -> None:
    """
    Protocol level testing of UNSUBSCRIBE and UNSUBACK packet handling.

    Nothing will travel over the wire, it is all fake.
    Also, the topics are not subscribed into.
    """
    logging.basicConfig()
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    host = "localhost"
    port = 1883

    unsubscribed_topics = []
    mqtt_client = MQTT.MQTT(
        broker=host,
        port=port,
        ssl_context=ssl.create_default_context(),
        connect_retries=1,
        user_data=unsubscribed_topics,
    )

    mqtt_client.on_unsubscribe = handle_unsubscribe

    # patch is_connected() to avoid CONNECT/CONNACK handling.
    mqtt_client.is_connected = lambda: True
    mocket = Mocket(to_send)
    # pylint: disable=protected-access
    mqtt_client._sock = mocket

    mqtt_client.logger = logger

    # pylint: disable=protected-access
    mqtt_client._subscribed_topics = [topic]

    # pylint: disable=logging-fstring-interpolation
    logger.info(f"unsubscribing from {topic}")
    mqtt_client.unsubscribe(topic)

    assert topic in unsubscribed_topics
    assert mocket.sent == exp_recv
