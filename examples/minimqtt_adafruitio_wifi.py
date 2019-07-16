import time
import board
import busio
from digitalio import DigitalInOut
import neopixel
from adafruit_esp32spi import adafruit_esp32spi
from adafruit_esp32spi import adafruit_esp32spi_wifimanager
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
"""Use below for Most Boards"""
status_light = neopixel.NeoPixel(board.NEOPIXEL, 1, brightness=0.2) # Uncomment for Most Boards
"""Uncomment below for ItsyBitsy M4"""
# status_light = dotstar.DotStar(board.APA102_SCK, board.APA102_MOSI, 1, brightness=0.2)
# Uncomment below for an externally defined RGB LED
# import adafruit_rgbled
# from adafruit_esp32spi import PWMOut
# RED_LED = PWMOut.PWMOut(esp, 26)
# GREEN_LED = PWMOut.PWMOut(esp, 27)
# BLUE_LED = PWMOut.PWMOut(esp, 25)
# status_light = adafruit_rgbled.RGBLED(RED_LED, BLUE_LED, GREEN_LED)
wifi = adafruit_esp32spi_wifimanager.ESPSPI_WiFiManager(esp, secrets, status_light)

### Adafruit IO Setup ###

# Setup a feed named `photocell` for publishing.
aio_publish_feed = secrets['user']+'/feeds/photocell'

# Setup a feed named `onoffbutton` for subscribing to changes.
aio_subscribe_feed = secrets['user']+'/feeds/onoffbutton'

### Code ###

# pylint: disable=unused-argument
def on_message(client, topic, message):
    # This method is called whenever a new message is received
    # from the server.
    print('New message on topic {0}: {1}'.format(topic, message))

# Connect to WiFi
wifi.connect()

# Set up a MiniMQTT Client
mqtt_client = MQTT(socket,
                   broker = secrets['broker'],
                   username = secrets['user'],
                   password = secrets['pass'],
                   network_manager = wifi)

# Attach on_message method to the MQTT Client
mqtt_client.on_message = on_message

# Initialize the MQTT Client
mqtt_client.connect()

# Subscribe the client to topic aio_subscribe_feed
mqtt_client.subscribe(aio_subscribe_feed)

photocell_val = 0
while True:
    # Poll the message queue
    mqtt_client.loop()

    print('Sending photocell value: %d'%photocell_val)
    mqtt_client.publish(aio_publish_feed, photocell_val)
    photocell_val += 1
    time.sleep(0.5)
