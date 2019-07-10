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

print("Connecting to AP...")
while not esp.is_connected:
    try:
        esp.connect_AP(secrets['ssid'], secrets['password'])
    except RuntimeError as e:
        print("could not connect to AP, retrying: ",e)
        continue
print("Connected to", str(esp.ssid, 'utf-8'), "\tRSSI:", esp.rssi)
print("My IP address is", esp.pretty_ip(esp.ip_address))

# MQTT Topic
mqtt_topic = 'test/topic'

# Adafruit IO-style Topic
# mqtt_topic = 'aio_user/feeds/temperature'

# MiniMQTT Callback Handlers
def connect(client, userdata, flags, rc):
    # This method is called when client.connect() is called.
    print('Connected to MQTT Broker!')
    print('Flags: {0}\n RC: {1}'.format(flags, rc))

def disconnect(client, userdata, rc):
    # This method is called when client.disconnect() is called.
    print('Disconnected from MQTT Broker!')

def subscribe(client, userdata, topic, granted_qos):
    # This method is called when client.subscribe() is called.
    print('Subscribed to {0} with QOS level {1}'.format(topic, granted_qos))

def unsubscribe(client, userdata, topic, pid):
    # This method is called when client.unsubscribe() is called.
    print('Unsubscribed from {0} with PID {1}'.format(topic, pid))

def publish(client, userdata, topic, pid):
    # This method is called when client.publish() is called.
    print('Published to {0} with PID {1}'.format(topic, pid))

# Instanciate a new MiniMQTT Client
client = MQTT(socket, secrets['broker'],
                    username=secrets['user'],
                    password=secrets['pass'],
                    esp= esp)

# Connect callback handlers
client.on_connect = connect
client.on_disconnect = disconnect
client.on_subscribe = subscribe
client.on_unsubscribe = unsubscribe
client.on_publish = publish

print('Attempting to connect to %s'%client.broker)
client.connect()

print('Subscribing to %s'%mqtt_topic)
client.subscribe(mqtt_topic)

print('Publishing to %s'%mqtt_topic)
client.publish(mqtt_topic, 'Hello Broker!')

print('Unsubscribing from %s'%mqtt_topic)
client.unsubscribe(mqtt_topic)

print('Disconnecting from %s'%client.broker)
client.disconnect()
