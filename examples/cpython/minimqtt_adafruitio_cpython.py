# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

import socket
import ssl
import time
from os import getenv

import adafruit_minimqtt.adafruit_minimqtt as MQTT

### Key Setup ###

# Add your Adafruit IO username and key to your env.
# example:
# export ADAFRUIT_AIO_USERNAME=your-aio-username
# export ADAFRUIT_AIO_KEY=your-aio-key

aio_username = getenv("ADAFRUIT_AIO_USERNAME")
aio_key = getenv("ADAFRUIT_AIO_KEY")

### Feeds ###

# Setup a feed named 'photocell' for publishing to a feed
photocell_feed = f"{aio_username}/feeds/photocell"

# Setup a feed named 'onoff' for subscribing to changes
onoff_feed = f"{aio_username}/feeds/onoff"

### Code ###


# Define callback methods which are called when events occur
def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print(f"Connected to Adafruit IO! Listening for topic changes on {onoff_feed}")
    # Subscribe to all changes on the onoff_feed.
    client.subscribe(onoff_feed)


def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print("Disconnected from Adafruit IO!")


def message(client, topic, message):
    # This method is called when a topic the client is subscribed to
    # has a new message.
    print(f"New message on topic {topic}: {message}")


# Set up a MiniMQTT Client
mqtt_client = MQTT.MQTT(
    broker="io.adafruit.com",
    username=aio_username,
    password=aio_key,
    socket_pool=socket,
    is_ssl=True,
    ssl_context=ssl.create_default_context(),
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
    print(f"Sending photocell value: {photocell_val}...")
    mqtt_client.publish(photocell_feed, photocell_val)
    print("Sent!")
    photocell_val += 1
    time.sleep(1)
