# SPDX-FileCopyrightText: 2023 Vladimír Kotal
#
# SPDX-License-Identifier: Unlicense

"""fake socket class for protocol level testing"""

from unittest import mock


class Mocket:
    """
    Mock Socket tailored for MiniMQTT testing. Records sent data,
    hands out pre-recorded reply.

    Inspired by the Mocket class from Adafruit_CircuitPython_Requests
    """

    def __init__(self, to_send):
        self._to_send = to_send

        self.sent = bytearray()

        self.timeout = mock.Mock()
        self.connect = mock.Mock()
        self.close = mock.Mock()

    def send(self, bytes_to_send):
        """merely record the bytes. return the length of this bytearray."""
        self.sent.extend(bytes_to_send)
        return len(bytes_to_send)

    # MiniMQTT checks for the presence of "recv_into" and switches behavior based on that.
    def recv_into(self, retbuf, bufsize):
        """return data from internal buffer"""
        size = min(bufsize, len(self._to_send))
        if size == 0:
            return size
        chop = self._to_send[0:size]
        retbuf[0:] = chop
        self._to_send = self._to_send[size:]
        return size
