import time
import board
import busio
import digitalio
from adafruit_fona.adafruit_fona import FONA
from adafruit_fona.adafruit_fona_gsm import GSM
import adafruit_fona.adafruit_fona_socket as socket

import adafruit_minimqtt as MQTT

### Cellular ###

# Get cellular details and more from a secrets.py file
try:
    from secrets import secrets
except ImportError:
    print("Cellular secrets are kept in secrets.py, please add them there!")
    raise

# Create a serial connection for the FONA connection using 4800 baud.
# These are the defaults you should use for the FONA Shield.
# For other boards set RX = GPS module TX, and TX = GPS module RX pins.
uart = busio.UART(board.TX, board.RX, baudrate=4800)
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
# pylint: disable=unused-argument, redefined-outer-name
def connect(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print("Connected to MQTT Broker!")
    print("Flags: {0}\n RC: {1}".format(flags, rc))


def disconnect(client, userdata, rc):
    # This method is called when the client disconnects
    # from the broker.
    print("Disconnected from MQTT Broker!")


def subscribe(client, userdata, topic, granted_qos):
    # This method is called when the client subscribes to a new feed.
    print("Subscribed to {0} with QOS level {1}".format(topic, granted_qos))


def unsubscribe(client, userdata, topic, pid):
    # This method is called when the client unsubscribes from a feed.
    print("Unsubscribed from {0} with PID {1}".format(topic, pid))


def publish(client, userdata, topic, pid):
    # This method is called when the client publishes data to a feed.
    print("Published to {0} with PID {1}".format(topic, pid))


# Initialize GSM modem
gsm = GSM(fona, (secrets["apn"], secrets["apn_username"], secrets["apn_password"]))

while not gsm.is_attached:
    print("Attaching to network...")
    time.sleep(0.5)
print("Attached to network!")

while not gsm.is_connected:
    print("Connecting to network...")
    gsm.connect()
    time.sleep(5)
print("Connected to network!")

# Initialize MQTT interface with the cellular interface
MQTT.set_socket(socket, fona)

# Set up a MiniMQTT Client
client = MQTT.MQTT(
    broker=secrets["broker"], username=secrets["user"], password=secrets["pass"]
)

# Connect callback handlers to client
client.on_connect = connect
client.on_disconnect = disconnect
client.on_subscribe = subscribe
client.on_unsubscribe = unsubscribe
client.on_publish = publish

print("Attempting to connect to %s" % client.broker)
client.connect()

print("Subscribing to %s" % mqtt_topic)
client.subscribe(mqtt_topic)

print("Publishing to %s" % mqtt_topic)
client.publish(mqtt_topic, "Hello Broker!")

print("Unsubscribing from %s" % mqtt_topic)
client.unsubscribe(mqtt_topic)

print("Disconnecting from %s" % client.broker)
client.disconnect()
