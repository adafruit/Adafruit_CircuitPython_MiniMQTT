# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""loop() tests"""

import random
import socket
import ssl
import time
from unittest import TestCase, main
from unittest.mock import patch

import adafruit_minimqtt.adafruit_minimqtt as MQTT


class Loop(TestCase):
    """basic loop() test"""

    connect_times = []
    INITIAL_RCS_VAL = 42
    rcs_val = INITIAL_RCS_VAL

    def fake_wait_for_msg(self):
        """_wait_for_msg() replacement. Sleeps for 1 second and returns an integer."""
        time.sleep(1)
        retval = self.rcs_val
        self.rcs_val += 1
        return retval

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

        with patch.object(
            mqtt_client, "_wait_for_msg"
        ) as wait_for_msg_mock, patch.object(
            mqtt_client, "is_connected"
        ) as is_connected_mock:
            wait_for_msg_mock.side_effect = self.fake_wait_for_msg
            is_connected_mock.side_effect = lambda: True

            time_before = time.monotonic()
            timeout = random.randint(3, 8)
            rcs = mqtt_client.loop(timeout=timeout)
            time_after = time.monotonic()

            assert time_after - time_before >= timeout
            wait_for_msg_mock.assert_called()

            # Check the return value.
            assert rcs is not None
            assert len(rcs) > 1
            expected_rc = self.INITIAL_RCS_VAL
            for ret_code in rcs:
                assert ret_code == expected_rc
                expected_rc += 1

    def test_loop_is_connected(self):
        """
        loop() should throw MMQTTException if not connected
        """
        mqtt_client = MQTT.MQTT(
            broker="127.0.0.1",
            port=1883,
            socket_pool=socket,
            ssl_context=ssl.create_default_context(),
        )

        with self.assertRaises(MQTT.MMQTTException) as context:
            mqtt_client.loop(timeout=1)

        assert "not connected" in str(context.exception)


if __name__ == "__main__":
    main()
