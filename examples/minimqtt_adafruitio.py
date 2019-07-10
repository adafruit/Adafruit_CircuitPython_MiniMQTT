# CircuitPython MiniMQTT Library
# Adafruit IO SSL/TLS Example for WiFi (ESP32SPI)
import time
import board
import busio
from digitalio import DigitalInOut
import neopixel
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

# Setup a feed named `photocell` for publishing.
aio_publish_feed = secrets['user']+'/feeds/photocell'

# Setup a feed named `onoffbutton` for subscribing to changes.
aio_subscribe_feed = secrets['user']+'/feeds/onoffbutton'

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

def on_message(client, topic, message):
    # This method is called whenever a new message is received
    # from the server.
    print('New message on topic {0}: {1}'.format(topic, message))

# Connect to WiFi
connect_wifi()

# Set up a MiniMQTT Client
mqtt_client = MQTT(socket,
                    secrets['broker'],
                    username=secrets['user'],
                    password=secrets['pass'],
                    esp = esp)

# Attach on_message method to the MQTT Client
mqtt_client.on_message = on_message

# Initialize the MQTT Client
mqtt_client.connect()

# Subscribe the client to topic aio_subscribe_feed
mqtt_client.subscribe(aio_subscribe_feed)

photocell_val = 0
while True:
    # Poll the message queue
    mqtt_client.wait_for_msg()

    print('Sending photocell value: %d'%photocell_val)
    mqtt_client.publish(aio_publish_feed, photocell_val)
    photocell_val += 1
    time.sleep(0.5)
