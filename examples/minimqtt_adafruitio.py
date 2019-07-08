import time
import board
import busio
from digitalio import DigitalInOut
import neopixel
from adafruit_esp32spi import adafruit_esp32spi
import adafruit_esp32spi.adafruit_esp32spi_socket as socket

from adafruit_minimqtt import MQTT

# Get wifi details and more from a secrets.py file
try:
    from secrets import secrets
except ImportError:
    print("WiFi secrets are kept in secrets.py, please add them there!")
    raise

try:
    esp32_cs = DigitalInOut(board.ESP_CS)
    esp32_ready = DigitalInOut(board.ESP_BUSY)
    esp32_reset = DigitalInOut(board.ESP_RESET)
except:
    esp32_cs = DigitalInOut(board.D9)
    esp32_ready = DigitalInOut(board.D10)
    esp32_reset = DigitalInOut(board.D5)

spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
esp = adafruit_esp32spi.ESP_SPIcontrol(spi, esp32_cs, esp32_ready, esp32_reset)

"""Use below for Most Boards"""
status_light = neopixel.NeoPixel(board.NEOPIXEL, 1, brightness=0.2) # Uncomment for Most Boards
"""Uncomment below for ItsyBitsy M4"""
# status_light = dotstar.DotStar(board.APA102_SCK, board.APA102_MOSI, 1, brightness=0.2)
"""Uncomment below for an externally defined RGB LED"""
# import adafruit_rgbled
# from adafruit_esp32spi import PWMOut
# RED_LED = PWMOut.PWMOut(esp, 26)
# GREEN_LED = PWMOut.PWMOut(esp, 27)
# BLUE_LED = PWMOut.PWMOut(esp, 25)
# status_light = adafruit_rgbled.RGBLED(RED_LED, BLUE_LED, GREEN_LED)

# Setup a feed called `photocell` for publishing.
AIO_Publish_Feed = secrets['aio_user']+'feeds/photocell'

# Setup a feed called `on_off_button` for subscribing to changes.
AIO_Subscribe_Feed = secrets['aio_user']+'feeds/on_off_button'

# Set up a MiniMQTT Client
mqtt_client = MQTT(socket, secrets['aio_url'], username=secrets['aio_user'], password=secrets['aio_password'],
              esp = esp, log = True)

print("Connecting to %s..."%secrets['ssid'])
while not esp.is_connected:
    try:
        esp.connect_AP(secrets['ssid'], secrets['password'])
    except RuntimeError as e:
        print("could not connect to AP, retrying: ",e)
        continue
print("Connected to", str(esp.ssid, 'utf-8'), "\tRSSI:", esp.rssi)
print("IP: ", esp.pretty_ip(esp.ip_address))

mqtt_client.connect()

mqtt_client.subscribe(on_off_button)

photocell_val = 0
while True:
    # ping the server to keep the mqtt connection alive
    mqtt_client.ping()

    subscription_data = mqtt_client.wait_for_msg()

    if subscription_data:
        print('Got: {0}'.format(subscription_data))

    print('Sending photocell value: %d'%photocell_val)
    mqtt.publish(AIO_Publish_Feed, photocell_value)
