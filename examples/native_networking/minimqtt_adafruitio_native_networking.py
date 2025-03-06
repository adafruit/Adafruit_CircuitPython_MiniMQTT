# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

import time
from os import getenv

import adafruit_connection_manager
import wifi

import adafruit_minimqtt.adafruit_minimqtt as MQTT

# Get WiFi details and Adafruit IO keys, ensure these are setup in settings.toml
# (visit io.adafruit.com if you need to create an account, or if you need your Adafruit IO key.)
ssid = getenv("CIRCUITPY_WIFI_SSID")
password = getenv("CIRCUITPY_WIFI_PASSWORD")
aio_username = getenv("ADAFRUIT_AIO_USERNAME")
aio_key = getenv("ADAFRUIT_AIO_KEY")
broker = getenv("broker", "io.adafruit.com")

print(f"Connecting to {ssid}")
wifi.radio.connect(ssid, password)
print(f"Connected to {ssid}!")
### Feeds ###

# Setup a feed named 'photocell' for publishing to a feed
photocell_feed = "photocell"

# Setup a feed named 'onoff' for subscribing to changes
onoff_feed = "onoff"

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


# Create a socket pool and ssl_context
pool = adafruit_connection_manager.get_radio_socketpool(wifi.radio)
ssl_context = adafruit_connection_manager.get_radio_ssl_context(wifi.radio)

# If you need to use certificate/key pair authentication (e.g. X.509), you can load them in the
# ssl context by uncommenting the lines below and adding the following keys to your settings.toml:
# "device_cert_path" - Path to the Device Certificate
# "device_key_path" - Path to the RSA Private Key
# ssl_context.load_cert_chain(
#     certfile=getenv("device_cert_path"), keyfile=getenv("device_key_path")
# )

# Set up a MiniMQTT Client
mqtt_client = MQTT.MQTT(
    broker=broker,
    username=aio_username,
    password=aio_key,
    socket_pool=pool,
    ssl_context=ssl_context,
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
    mqtt_client.loop(timeout=1)

    # Send a new message
    print(f"Sending photocell value: {photocell_val}...")
    mqtt_client.publish(photocell_feed, photocell_val)
    print("Sent!")
    photocell_val += 1
    time.sleep(5)
