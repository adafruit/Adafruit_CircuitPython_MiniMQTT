# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

import time
from os import getenv

import adafruit_connection_manager
import adafruit_fona.adafruit_fona_network as network
import adafruit_fona.adafruit_fona_socket as pool
import board
import busio
import digitalio
from adafruit_fona.adafruit_fona import FONA

import adafruit_minimqtt.adafruit_minimqtt as MQTT

# Get FONA details and Adafruit IO keys, ensure these are setup in settings.toml
# (visit io.adafruit.com if you need to create an account, or if you need your Adafruit IO key.)
apn = getenv("apn")
apn_username = getenv("apn_username")
apn_password = getenv("apn_password")
aio_username = getenv("ADAFRUIT_AIO_USERNAME")
aio_key = getenv("ADAFRUIT_AIO_KEY")
broker = getenv("broker", "io.adafruit.com")

# Create a serial connection for the FONA connection
uart = busio.UART(board.TX, board.RX)
rst = digitalio.DigitalInOut(board.D4)
# Initialize FONA
fona = FONA(uart, rst)

### Topic Setup ###

# MQTT Topic
# Use this topic if you'd like to connect to a standard MQTT broker
mqtt_topic = "test/topic"

# Adafruit IO-style Topic
# Use this topic if you'd like to connect to io.adafruit.com
# mqtt_topic = 'aio_user/feeds/temperature'

### Code ###


# Define callback methods which are called when events occur
def connect(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print("Connected to MQTT Broker!")
    print(f"Flags: {flags}\n RC: {rc}")


def disconnect(client, userdata, rc):
    # This method is called when the client disconnects
    # from the broker.
    print("Disconnected from MQTT Broker!")


def subscribe(client, userdata, topic, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print(f"Subscribed to {topic} with QOS level {granted_qos}")


def unsubscribe(client, userdata, topic, pid):
    # This method is called when the client unsubscribes from a feed.
    print(f"Unsubscribed from {topic} with PID {pid}")


def publish(client, userdata, topic, pid):
    # This method is called when the client publishes data to a feed.
    print(f"Published to {topic} with PID {pid}")


# Initialize cellular data network
network = network.CELLULAR(fona, (apn, apn_username, apn_password))

while not network.is_attached:
    print("Attaching to network...")
    time.sleep(0.5)
print("Attached!")

while not network.is_connected:
    print("Connecting to network...")
    network.connect()
    time.sleep(0.5)
print("Network Connected!")

ssl_context = adafruit_connection_manager.create_fake_ssl_context(pool, fona)

# Set up a MiniMQTT Client
client = MQTT.MQTT(
    broker=broker,
    username=aio_username,
    password=aio_key,
    is_ssl=False,
    socket_pool=pool,
    ssl_context=ssl_context,
)

# Connect callback handlers to client
client.on_connect = connect
client.on_disconnect = disconnect
client.on_subscribe = subscribe
client.on_unsubscribe = unsubscribe
client.on_publish = publish

print(f"Attempting to connect to {client.broker}")
client.connect()

print(f"Subscribing to {mqtt_topic}")
client.subscribe(mqtt_topic)

print(f"Publishing to {mqtt_topic}")
client.publish(mqtt_topic, "Hello Broker!")

print(f"Unsubscribing from {mqtt_topic}")
client.unsubscribe(mqtt_topic)

print(f"Disconnecting from {client.broker}")
client.disconnect()
