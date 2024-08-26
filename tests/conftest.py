# SPDX-FileCopyrightText: 2023 Justin Myers for Adafruit Industries
#
# SPDX-License-Identifier: Unlicense

"""PyTest Setup"""

import adafruit_connection_manager
import pytest


@pytest.fixture(autouse=True)
def reset_connection_manager(monkeypatch):
    """Reset the ConnectionManager, since it's a singlton and will hold data"""
    monkeypatch.setattr(
        "adafruit_minimqtt.adafruit_minimqtt.get_connection_manager",
        adafruit_connection_manager.ConnectionManager,
    )
