# SPDX-FileCopyrightText: 2021 ladyada for Adafruit Industries
# SPDX-License-Identifier: MIT

import time
from os import getenv

import adafruit_connection_manager
import adafruit_pyportal

import adafruit_minimqtt.adafruit_minimqtt as MQTT

pyportal = adafruit_pyportal.PyPortal()

# Get Adafruit IO keys, ensure these are setup in settings.toml
# (visit io.adafruit.com if you need to create an account, or if you need your Adafruit IO key.)
aio_username = getenv("ADAFRUIT_AIO_USERNAME")
aio_key = getenv("ADAFRUIT_AIO_KEY")
broker = getenv("broker", "io.adafruit.com")

# ------------- MQTT Topic Setup ------------- #
mqtt_topic = "test/topic"


### Code ###
# Define callback methods which are called when events occur
def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print(f"Subscribing to {mqtt_topic}")
    client.subscribe(mqtt_topic)


def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print("Disconnected from MQTT Broker!")


def message(client, topic, message):
    """Method callled when a client's subscribed feed has a new
    value.
    :param str topic: The topic of the feed with a new value.
    :param str message: The new value
    """
    print(f"New message on topic {topic}: {message}")


# Connect to WiFi
print("Connecting to WiFi...")
pyportal.network.connect()
print("Connected!")

pool = adafruit_connection_manager.get_radio_socketpool(pyportal.network._wifi.esp)
ssl_context = adafruit_connection_manager.get_radio_ssl_context(pyportal.network._wifi.esp)

# Set up a MiniMQTT Client
mqtt_client = MQTT.MQTT(
    broker=broker,
    username=aio_username,
    password=aio_key,
    is_ssl=False,
    socket_pool=pool,
    ssl_context=ssl_context,
)

# Setup the callback methods above
mqtt_client.on_connect = connected
mqtt_client.on_disconnect = disconnected
mqtt_client.on_message = message

# Connect the client to the MQTT broker.
mqtt_client.connect()

photocell_val = 0
while True:
    # Poll the message queue
    mqtt_client.loop()

    # Send a new message
    print(f"Sending photocell value: {photocell_val}")
    mqtt_client.publish(mqtt_topic, photocell_val)
    photocell_val += 1
    time.sleep(1)
