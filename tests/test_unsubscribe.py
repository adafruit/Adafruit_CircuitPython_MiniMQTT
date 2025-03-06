# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""unsubscribe tests"""

import logging
import ssl

import pytest
from mocket import Mocket

import adafruit_minimqtt.adafruit_minimqtt as MQTT


def handle_unsubscribe(client, user_data, topic, pid):
    """
    Record topics into user data.
    """
    assert topic

    user_data.append(topic)


# The MQTT packet contents below were captured using Mosquitto client+server.
# These are all verbatim, except:
#   - message ID that was changed from 2 to 1 since in the real world
#     the UNSUBSCRIBE packet followed the SUBSCRIBE packet.
#   - the long list topics is sent as individual UNSUBSCRIBE packets by Mosquitto
testdata = [
    # short topic with remaining length encoded as single byte
    (
        "foo/bar",
        bytearray([0xB0, 0x02, 0x00, 0x01]),
        bytearray([
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
        ]),
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
    # UNSUBSCRIBE responded to by PUBLISH followed by UNSUBACK
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
            0xB0,  # UNSUBACK
            0x02,
            0x00,
            0x01,
        ]),
        bytearray([
            0xA2,  # fixed header
            0x0B,  # remaining length
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
        ]),
    ),
    # use list of topics for more coverage. If the range was (1, 10000), that would be
    # long enough to use 3 bytes for remaining length, however that would make the test
    # run for many minutes even on modern systems, so 1000 is used instead.
    # This results in 2 bytes for the remaining length.
    (
        [f"foo/bar{x:04}" for x in range(1, 1000)],
        bytearray([0xB0, 0x02, 0x00, 0x01]),
        bytearray(
            [
                0xA2,  # fixed header
                0xBD,  # remaining length
                0x65,
                0x00,  # message ID
                0x01,
            ]
            + sum(
                [[0x00, 0x0B] + list(f"foo/bar{x:04}".encode("ascii")) for x in range(1, 1000)],
                [],
            )
        ),
    ),
]


@pytest.mark.parametrize(
    "topic,to_send,exp_recv",
    testdata,
    ids=["short_topic", "long_topic", "publish_first", "topic_list_long"],
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
    mqtt_client._sock = mocket

    mqtt_client.logger = logger

    if isinstance(topic, str):
        mqtt_client._subscribed_topics = [topic]
    elif isinstance(topic, list):
        mqtt_client._subscribed_topics = topic

    logger.info(f"unsubscribing from {topic}")
    mqtt_client.unsubscribe(topic)

    if isinstance(topic, str):
        assert topic in unsubscribed_topics
    elif isinstance(topic, list):
        for topic_name in topic:
            assert topic_name in unsubscribed_topics
    assert mocket.sent == exp_recv
    assert len(mocket._to_send) == 0
