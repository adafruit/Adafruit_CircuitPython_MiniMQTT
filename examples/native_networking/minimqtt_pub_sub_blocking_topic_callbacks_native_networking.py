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

print(f"Connecting to {ssid}")
wifi.radio.connect(ssid, password)
print(f"Connected to {ssid}!")

### Code ###


# Define callback methods which are called when events occur
def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print("Connected to MQTT Broker!")


def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print("Disconnected from MQTT Broker!")


def subscribe(client, userdata, topic, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print(f"Subscribed to {topic} with QOS level {granted_qos}")


def unsubscribe(client, userdata, topic, pid):
    # This method is called when the client unsubscribes from a feed.
    print(f"Unsubscribed from {topic} with PID {pid}")


def on_battery_msg(client, topic, message):
    # Method called when device/batteryLife has a new value
    print(f"Battery level: {message}v")

    # client.remove_topic_callback(f"{aio_username}/feeds/device.batterylevel")


def on_message(client, topic, message):
    # Method callled when a client's subscribed feed has a new value.
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
client = MQTT.MQTT(
    broker="io.adafruit.com",
    username=aio_username,
    password=aio_key,
    socket_pool=pool,
    ssl_context=ssl_context,
)

# Setup the callback methods above
client.on_connect = connected
client.on_disconnect = disconnected
client.on_subscribe = subscribe
client.on_unsubscribe = unsubscribe
client.on_message = on_message
client.add_topic_callback(f"{aio_username}/feeds/device.batterylevel", on_battery_msg)

# Connect the client to the MQTT broker.
print("Connecting to MQTT broker...")
client.connect()

# Subscribe to all notifications on the device group
client.subscribe(f"{aio_username}/groups/device", 1)

# Start a blocking message loop...
# NOTE: NO code below this loop will execute
while True:
    try:
        client.loop(timeout=1)
    except (ValueError, RuntimeError) as e:
        print("Failed to get data, retrying\n", e)
        wifi.reset()
        client.reconnect()
        continue
    time.sleep(1)
