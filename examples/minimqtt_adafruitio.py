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

# Initialize an ESP32SPI network interface
esp32_cs = DigitalInOut(board.ESP_CS)
esp32_ready = DigitalInOut(board.ESP_BUSY)
esp32_reset = DigitalInOut(board.ESP_RESET)
spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
esp = adafruit_esp32spi.ESP_SPIcontrol(spi, esp32_cs, esp32_ready, esp32_reset)

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
    """This method is called whenever a new message is received
    from the server.
    """
    print('Value on topic {0}: {1}'.format(topic, message))

# Setup a feed named `photocell` for publishing.
AIO_Publish_Feed = secrets['aio_user']+'/'+'feeds/photocell'

# Setup a feed named `onoffbutton` for subscribing to changes.
AIO_Subscribe_Feed = secrets['aio_user']+'/'+'feeds/onoffbutton'

# Set up a MiniMQTT Client
mqtt_client = MQTT(socket, secrets['aio_url'], username=secrets['aio_user'], password=secrets['aio_password'],
              esp = esp, log = True)

# Attach on_message method to the MQTT Client
mqtt_client.on_message = on_message

# Initialize the MQTT Client
mqtt_client.connect()

# Subscribe the client to topic AIO_SUBSCRIBE_FEED
mqtt_client.subscribe(AIO_Subscribe_Feed)

photocell_val = 0
while True:
    # Poll the message queue and ping the server
    mqtt_client.wait_for_msg()

    print('Sending photocell value: %d'%photocell_val)
    mqtt_client.publish(AIO_Publish_Feed, photocell_val)
    photocell_val += 1
    time.sleep(0.5)
