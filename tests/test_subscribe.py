# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""subscribe tests"""

import logging
import ssl

import pytest
from mocket import Mocket

import adafruit_minimqtt.adafruit_minimqtt as MQTT


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
        bytearray([0x90, 0x03, 0x00, 0x01, 0x00]),  # SUBACK
        bytearray([
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
        ]),
    ),
    # same as before but with tuple
    (
        ("foo/bar", 0),
        bytearray([0x90, 0x03, 0x00, 0x01, 0x00]),  # SUBACK
        bytearray([
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
        ]),
    ),
    # remaining length is encoded as 2 bytes due to long topic name.
    (
        "f" + "o" * 257,
        bytearray([0x90, 0x03, 0x00, 0x01, 0x00]),  # SUBACK
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
    # SUBSCRIBE responded to by PUBLISH followed by SUBACK
    (
        "foo/bar",
        bytearray([
            0x30,  # PUBLISH
            0x0C,
            0x00,
            0x07,
            0x66,
            0x6F,
            0x6F,
            0x2F,
            0x62,
            0x61,
            0x72,
            0x66,
            0x6F,
            0x6F,
            0x90,  # SUBACK
            0x03,
            0x00,
            0x01,
            0x00,
        ]),
        bytearray([
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
        ]),
    ),
    # use list of topics for more coverage. If the range was (1, 10000), that would be
    # long enough to use 3 bytes for remaining length, however that would make the test
    # run for many minutes even on modern systems, so 1001 is used instead.
    # This results in 2 bytes for the remaining length.
    (
        [(f"foo/bar{x:04}", 0) for x in range(1, 1001)],
        bytearray(
            [
                0x90,
                0xEA,  # remaining length
                0x07,
                0x00,  # message ID
                0x01,
            ]
            + [0x00] * 1000  # success for all topics
        ),
        bytearray(
            [
                0x82,  # fixed header
                0xB2,  # remaining length
                0x6D,
                0x00,  # message ID
                0x01,
            ]
            + sum(
                [
                    [0x00, 0x0B] + list(f"foo/bar{x:04}".encode("ascii")) + [0x00]
                    for x in range(1, 1001)
                ],
                [],
            )
        ),
    ),
]


@pytest.mark.parametrize(
    "topic,to_send,exp_recv",
    testdata,
    ids=[
        "short_topic",
        "short_topic_tuple",
        "long_topic",
        "publish_first",
        "topic_list_long",
    ],
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
    mqtt_client._sock = mocket

    mqtt_client.logger = logger

    logger.info(f"subscribing to {topic}")
    mqtt_client.subscribe(topic)

    if isinstance(topic, str):
        assert topic in subscribed_topics
    elif isinstance(topic, list):
        for topic_name, _ in topic:
            assert topic_name in subscribed_topics
    assert mocket.sent == exp_recv
    assert len(mocket._to_send) == 0
