import time
import board
import busio
from digitalio import DigitalInOut
import neopixel
from adafruit_esp32spi import adafruit_esp32spi
import adafruit_esp32spi.adafruit_esp32spi_socket as socket

from adafruit_minimqtt import MQTT

print("CircuitPython MiniMQTT WiFi Test")

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

# Instanciate a MQTT Client
mqtt_client = MQTT(esp, socket, secrets['aio_url'],
                    username=secrets['aio_user'], password=secrets['aio_password'],
                    is_ssl = True)

print("Connecting to AP...")
while not esp.is_connected:
    try:
        esp.connect_AP(secrets['ssid'], secrets['password'])
    except RuntimeError as e:
        print("could not connect to AP, retrying: ",e)
        continue
print("Connected to", str(esp.ssid, 'utf-8'), "\tRSSI:", esp.rssi)
print("My IP address is", esp.pretty_ip(esp.ip_address))

print('Connecting to {0}:{1}...'.format(mqtt_client.server, mqtt_client.port))


# ACK Callbacks for testing
def connect(client, userdata, flags, rc):
    """.connect() called"""
    print('Connected to MQTT Broker!')
    print('Flags: {0}\n RC: {1}'.format(flags, rc))

def disconnect(client, userdata, rc):
    """.disconnect() called"""
    print('Disconnected from MQTT Broker!')
    print('RC: {0}\n'.format(rc))

def subscribe(client, userdata, qos):
    """.subscribe() called"""
    print('Client successfully subscribed to feed with a QOS of %d'%qos)

def publish(client, userdata, pid):
    """.publish() called"""
    print('Client successfully published to feed with a PID of %d'%pid)

# Set the user_data to a generated client_id
mqtt_client.user_data = mqtt_client._client_id

# define callbacks
mqtt_client.on_connect = connect
mqtt_client.on_subscribe = subscribe
mqtt_client.on_publish = publish
mqtt_client.on_disconnect = disconnect

# Connect MQTT Client
print('connecting...')
mqtt_client.connect()

# Subscribe to feed
print('subscribing...')
mqtt_client.subscribe('brubell/feeds/temperature')

print('publishing...')
mqtt_client.publish('brubell/feeds/temperature', 50)

# Disconnect from MQTT Client
print('disconnecting...')
mqtt_client.disconnect()

