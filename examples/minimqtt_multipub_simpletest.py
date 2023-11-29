# SPDX-FileCopyrightText: 2023 DJDevon3
# SPDX-License-Identifier: MIT
# MQTT Multi-Feed Publish Example
# Coded for Circuit Python 8.2.x

import traceback
import os
import time
import ssl
import wifi
import socketpool
import adafruit_minimqtt.adafruit_minimqtt as MQTT
from adafruit_minimqtt.adafruit_minimqtt import MMQTTException

# Initialize Web Sockets (This should always be near the top of a script!)
# There can be only one pool
pool = socketpool.SocketPool(wifi.radio)

# Add settings.toml to your filesystem
# CIRCUITPY_WIFI_SSID and CIRCUITPY_WIFI_PASSWORD keys
# with your WiFi credentials. Add your Adafruit IO username and key as well.
# DO NOT share that file or commit it into Git or other source control.
ssid = os.getenv("CIRCUITPY_WIFI_SSID")
appw = os.getenv("CIRCUITPY_WIFI_PASSWORD")
aio_username = os.getenv("aio_username")
aio_key = os.getenv("aio_key")

# MQTT Topic
# Use this format for a standard MQTT broker
feed_01 = aio_username + "/feeds/BME280-RealTemp"
feed_02 = aio_username + "/feeds/BME280-Pressure"
feed_03 = aio_username + "/feeds/BME280-Humidity"
feed_04 = aio_username + "/feeds/BME280-Altitude"

# Time in seconds between updates (polling)
# 600 = 10 mins, 900 = 15 mins, 1800 = 30 mins, 3600 = 1 hour
sleep_time = 900

# Converts seconds to minutes/hours/days
# Attribution: Written by DJDevon3 & refined by Elpekenin
def time_calc(input_time):
    if input_time < 60:
        return f"{input_time:.0f} seconds"
    if input_time < 3600:
        return f"{input_time / 60:.0f} minutes"
    if input_time < 86400:
        return f"{input_time / 60 / 60:.0f} hours"
    return f"{input_time / 60 / 60 / 24:.1f} days"


# Define callback methods when events occur
def connect(client, userdata, flags, rc):
    # Method when mqtt_client connected to the broker.
    print("| | ✅ Connected to MQTT Broker!")


def disconnect(client, userdata, rc):
    # Method when the mqtt_client disconnects from broker.
    print("| | ✂️ Disconnected from MQTT Broker")


def publish(client, userdata, topic, pid):
    # Method when the mqtt_client publishes data to a feed.
    print("| | | Published to {0} with PID {1}".format(topic, pid))


# Initialize a new MQTT Client object
mqtt_client = MQTT.MQTT(
    broker="io.adafruit.com",
    port=8883,
    username=aio_username,
    password=aio_key,
    socket_pool=pool,
    ssl_context=ssl.create_default_context(),
    is_ssl=True,
)

# Connect callback handlers to mqtt_client
mqtt_client.on_connect = connect
mqtt_client.on_disconnect = disconnect
mqtt_client.on_publish = publish

while True:
    # These are fake values, replace with sensor variables.
    BME280_temperature = 80
    BME280_pressure = round(1014.89, 1)
    BME280_humidity = round(49.57, 1)
    BME280_altitude = round(100.543, 2)
    print("===============================")

    # Board Uptime
    print("Board Uptime: ", time_calc(time.monotonic()))
    print("| Connecting to WiFi...")

    while not wifi.radio.ipv4_address:
        try:
            wifi.radio.connect(ssid, appw)
        except ConnectionError as e:
            print("Connection Error:", e)
            print("Retrying in 10 seconds")
            time.sleep(10)
    print("| ✅ WiFi!")

    while wifi.radio.ipv4_address:
        try:
            # Connect to MQTT Broker
            mqtt_client.connect()
            mqtt_client.publish(feed_01, BME280_temperature)
            # slight delay required between publishes!
            # otherwise only the 1st publish will succeed
            time.sleep(0.001)
            mqtt_client.publish(feed_02, BME280_pressure)
            time.sleep(1)
            mqtt_client.publish(feed_03, BME280_humidity)
            time.sleep(1)
            mqtt_client.publish(feed_04, BME280_altitude)
            time.sleep(1)
        except MMQTTException as e:
            print("| | ❌ MMQTTException", e)
            traceback.print_exception(e, e, e.__traceback__)
            break

        mqtt_client.disconnect()
        print("| ✂️ Disconnected from Wifi")
        print("Next Update: ", time_calc(sleep_time))

        time.sleep(sleep_time)
        break
