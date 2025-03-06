# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

import socket
import ssl
from os import getenv

import adafruit_minimqtt.adafruit_minimqtt as MQTT

# Add your Adafruit IO username and key to your env.
# (visit io.adafruit.com if you need to create an account, or if you need your Adafruit IO key.)
# example:
# export ADAFRUIT_AIO_USERNAME=your-aio-username
# export ADAFRUIT_AIO_KEY=your-aio-key
# export broker=io.adafruit.com

aio_username = getenv("ADAFRUIT_AIO_USERNAME")
aio_key = getenv("ADAFRUIT_AIO_KEY")
broker = getenv("broker", "io.adafruit.com")

### Topic Setup ###

# MQTT Topic
# Use this topic if you'd like to connect to a standard MQTT broker
mqtt_topic = "test/topic"

# Adafruit IO-style Topic
# Use this topic if you'd like to connect to io.adafruit.com
# mqtt_topic = f"{aio_username}/feeds/temperature"


### Code ###
# Define callback methods which are called when events occur
def connect(mqtt_client, userdata, flags, rc):
    # This function will be called when the mqtt_client is connected
    # successfully to the broker.
    print("Connected to MQTT Broker!")
    print(f"Flags: {flags}\n RC: {rc}")


def disconnect(mqtt_client, userdata, rc):
    # This method is called when the mqtt_client disconnects
    # from the broker.
    print("Disconnected from MQTT Broker!")


def subscribe(mqtt_client, userdata, topic, granted_qos):
    # This method is called when the mqtt_client subscribes to a new feed.
    print(f"Subscribed to {topic} with QOS level {granted_qos}")


def unsubscribe(mqtt_client, userdata, topic, pid):
    # This method is called when the mqtt_client unsubscribes from a feed.
    print(f"Unsubscribed from {topic} with PID {pid}")


def publish(mqtt_client, userdata, topic, pid):
    # This method is called when the mqtt_client publishes data to a feed.
    print(f"Published to {topic} with PID {pid}")


def message(client, topic, message):
    # Method callled when a client's subscribed feed has a new value.
    print(f"New message on topic {topic}: {message}")


# Set up a MiniMQTT Client
mqtt_client = MQTT.MQTT(
    broker=broker,
    username=aio_username,
    password=aio_key,
    socket_pool=socket,
    ssl_context=ssl.create_default_context(),
)

# Connect callback handlers to mqtt_client
mqtt_client.on_connect = connect
mqtt_client.on_disconnect = disconnect
mqtt_client.on_subscribe = subscribe
mqtt_client.on_unsubscribe = unsubscribe
mqtt_client.on_publish = publish
mqtt_client.on_message = message

print(f"Attempting to connect to {mqtt_client.broker}")
mqtt_client.connect()

print(f"Subscribing to {mqtt_topic}")
mqtt_client.subscribe(mqtt_topic)

print(f"Publishing to {mqtt_topic}")
mqtt_client.publish(mqtt_topic, "Hello Broker!")

print(f"Unsubscribing from {mqtt_topic}")
mqtt_client.unsubscribe(mqtt_topic)

print(f"Disconnecting from {mqtt_client.broker}")
mqtt_client.disconnect()
