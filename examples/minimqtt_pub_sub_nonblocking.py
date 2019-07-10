# CircuitPython MiniMQTT Library
# Adafruit IO SSL/TLS Example for WiFi (ESP32SPI)
import time
import board
import busio
from digitalio import DigitalInOut
from adafruit_esp32spi import adafruit_esp32spi
import adafruit_esp32spi.adafruit_esp32spi_socket as socket
from adafruit_minimqtt import MQTT

### WiFi ###

# Get wifi details and more from a secrets.py file
try:
    from secrets import secrets
except ImportError:
    print("WiFi secrets are kept in secrets.py, please add them there!")
    raise

# If you are using a board with pre-defined ESP32 Pins:
esp32_cs = DigitalInOut(board.ESP_CS)
esp32_ready = DigitalInOut(board.ESP_BUSY)
esp32_reset = DigitalInOut(board.ESP_RESET)

# If you have an externally connected ESP32:
# esp32_cs = DigitalInOut(board.D9)
# esp32_ready = DigitalInOut(board.D10)
# esp32_reset = DigitalInOut(board.D5)

spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
esp = adafruit_esp32spi.ESP_SPIcontrol(spi, esp32_cs, esp32_ready, esp32_reset)

### Adafruit IO Setup ###

# Setup a feed named `testfeed` for publishing.
default_topic = secrets['user']+'/feeds/testfeed'

### Code ###

def connect_wifi():
    print("Connecting to %s..."%secrets['ssid'])
    while not esp.is_connected:
        try:
            esp.connect_AP(secrets['ssid'], secrets['password'])
        except RuntimeError as e:
            print("could not connect to AP, retrying: ",e)
            continue
    print("Connected to", str(esp.ssid, 'utf-8'), "\tRSSI:", esp.rssi)
    print("IP: ", esp.pretty_ip(esp.ip_address))

# Define callback methods which are called when events occur
# pylint: disable=unused-argument
def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print('Connected to MQTT broker! Listening for topic changes on %s'%default_topic)
    # Subscribe to all changes on the default_topic feed.
    client.subscribe(default_topic)

def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print('Disconnected from MQTT Broker!')

def message(client, topic, message):
    """Method callled when a client's subscribed feed has a new
    value.
    :param str topic: The topic of the feed with a new value.
    :param str message: The new value
    """
    print('New message on topic {0}: {1}'.format(topic, message))

# Connect to WiFi
connect_wifi()

# Initialize a MiniMQTT Client
mqtt_client = MQTT(socket,
                   broker = secrets['broker'],
                   username = secrets['user'],
                   password = secrets['pass'],
                   esp = esp)

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
    print('Sending photocell value: %d'%photocell_val)
    mqtt_client.publish(default_topic, photocell_val)
    photocell_val += 1
    time.sleep(0.5)
