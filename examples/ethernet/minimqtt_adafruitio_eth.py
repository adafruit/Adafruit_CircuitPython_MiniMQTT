# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

import time
import board
import busio
from digitalio import DigitalInOut

from adafruit_wiznet5k.adafruit_wiznet5k import WIZNET5K
import adafruit_wiznet5k.adafruit_wiznet5k_socket as socket

import adafruit_minimqtt.adafruit_minimqtt as MQTT

# Get Adafruit IO details and more from a secrets.py file
try:
    from secrets import secrets
except ImportError:
    print("Adafruit IO secrets are kept in secrets.py, please add them there!")
    raise

cs = DigitalInOut(board.D10)
spi_bus = busio.SPI(board.SCK, MOSI=board.MOSI, MISO=board.MISO)

# Initialize ethernet interface with DHCP
eth = WIZNET5K(spi_bus, cs)

### Feeds ###

# Setup a feed named 'photocell' for publishing to a feed
photocell_feed = secrets["aio_username"] + "/feeds/photocell"

# Setup a feed named 'onoff' for subscribing to changes
onoff_feed = secrets["aio_username"] + "/feeds/onoff"

### Code ###

# Define callback methods which are called when events occur
# pylint: disable=unused-argument, redefined-outer-name
def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print("Connected to Adafruit IO! Listening for topic changes on %s" % onoff_feed)
    # Subscribe to all changes on the onoff_feed.
    client.subscribe(onoff_feed)


def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print("Disconnected from Adafruit IO!")


def message(client, topic, message):
    # This method is called when a topic the client is subscribed to
    # has a new message.
    print("New message on topic {0}: {1}".format(topic, message))


# Initialize MQTT interface with the ethernet interface
MQTT.set_socket(socket, eth)

# Set up a MiniMQTT Client
# NOTE: We'll need to connect insecurely for ethernet configurations.
mqtt_client = MQTT.MQTT(
    broker="io.adafruit.com",
    username=secrets["aio_username"],
    password=secrets["aio_key"],
    is_ssl=False,
)

# Setup the callback methods above
mqtt_client.on_connect = connected
mqtt_client.on_disconnect = disconnected
mqtt_client.on_message = message

# Connect the client to the MQTT broker.
print("Connecting to Adafruit IO...")
mqtt_client.connect()

photocell_val = 0
while True:
    # Poll the message queue
    mqtt_client.loop()

    # Send a new message
    print("Sending photocell value: %d..." % photocell_val)
    mqtt_client.publish(photocell_feed, photocell_val)
    print("Sent!")
    photocell_val += 1
    time.sleep(5)
