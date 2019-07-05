"""
CircuitPython_MiniMQTT Module Tester

by Brent Rubell for Adafruit Industries, 2019
"""
import time
import board
import busio
from digitalio import DigitalInOut
import neopixel
from adafruit_esp32spi import adafruit_esp32spi
import adafruit_esp32spi.adafruit_esp32spi_socket as socket
# MiniMQTT
from adafruit_minimqtt import MQTT

# Get wifi details and more from a secrets.py file
try:
    from secrets import secrets
except ImportError:
    print("WiFi secrets are kept in secrets.py, please add them there!")
    raise

esp32_cs = DigitalInOut(board.ESP_CS)
esp32_ready = DigitalInOut(board.ESP_BUSY)
esp32_reset = DigitalInOut(board.ESP_RESET)

spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
esp = adafruit_esp32spi.ESP_SPIcontrol(spi, esp32_cs, esp32_ready, esp32_reset)
status_light = neopixel.NeoPixel(board.NEOPIXEL, 1, brightness=0.2) # Uncomment for Most Boards


"""
Generic Unittest Assertions
"""
#pylint: disable=keyword-arg-before-vararg
def assertAlmostEqual(x, y, places=None, msg=''):
    """Raises an AssertionError if two float values are not equal.
    (from https://github.com/micropython/micropython-lib/blob/master/unittest/unittest.py)
    """
    if x == y:
        return
    if places is None:
        places = 2
    if round(abs(y-x), places) == 0:
        return
    if not msg:
        msg = '%r != %r within %r places' % (x, y, places)
    assert False, msg

def assertRaises(exc,func=None, *args, **kwargs):
    """Raises based on context.
    (from https://github.com/micropython/micropython-lib/blob/master/unittest/unittest.py)
    """
    try:
        func(*args, **kwargs)
        assert False, "%r not raised" % exc
    except Exception as e:
        if isinstance(e, exc):
            return
        raise

def assertIsNone(x):
    """Raises an AssertionError if x is None.
    """
    if x is None:
        raise AssertionError('%r is None'%x)

def assertEqual(val_1, val_2):
    """Raises an AssertionError if the two specified values are not equal.
    """
    if val_1 != val_2:
        raise AssertionError('Values are not equal:', val_1, val_2)

# Default MiniMQTT Callback Message Handlers
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

def unsubscribe(P1, P2):
    # unsubscribe() called from the mqtt_client.
    print('Client successfully unsubscribed from feed!')

# MQTT Client Tests
def test_create_insecure_mqtt_client_esp32spi():
    """Creates an insecure MQTT client using an ESP32SPI socket connection."""
    mqtt_client = MQTT(esp, socket, secrets['aio_url'],
                        username=secrets['aio_user'], password=secrets['aio_password'],
                        is_ssl=False)
    assertEqual(mqtt_client.port, 1883)

def test_create_secure_mqtt_client_esp32spi():
    """Creates a secure MQTT client using an ESP32SPI socket connection."""
    mqtt_client = MQTT(esp, socket, secrets['aio_url'],
                        username=secrets['aio_user'], password=secrets['aio_password'],
                        is_ssl=True)
    assertEqual(mqtt_client.port, 8883)

def test_connect_mqtts_esp32spi():
    """Securely connects to a MQTT broker using an ESP32SPI_Socket connection."""
    mqtt_client = MQTT(esp, socket, secrets['aio_url'],
                    username=secrets['aio_user'], password=secrets['aio_password'],
                    is_ssl=True)
    mqtt_client.connect()
    assertEqual(mqtt_client._is_connected, True)

def test_connect_mqtt_esp32spi():
    """Inesecurely connects to a MQTT broker using an ESP32SPI_Socket connection."""
    mqtt_client = MQTT(esp, socket, secrets['aio_url'],
                    username=secrets['aio_user'], password=secrets['aio_password'],
                    is_ssl=False)
    mqtt_client.connect()
    assertEqual(mqtt_client._is_connected, True)


# Timeout between tests, in seconds. This value depends on the timeout of your MQTT broker.
TEST_TIMEOUT = 1

# Tests methods
tests = [test_create_insecure_mqtt_client_esp32spi, test_create_secure_mqtt_client_esp32spi,
            test_connect_mqtts_esp32spi, test_connect_mqtt_esp32spi]


# Establish ESP32SPI connection
print("Connecting to AP...")
while not esp.is_connected:
    try:
        esp.connect_AP(secrets['ssid'], secrets['password'])
    except RuntimeError as e:
        print("could not connect to AP, retrying: ",e)
        continue
print("Connected to", str(esp.ssid, 'utf-8'), "\tRSSI:", esp.rssi)

# Test harness

start_time = time.monotonic()
for i in enumerate(tests):
    print('Running test: ', i)
    i[1]()
    print('OK!')
    time.sleep(TEST_TIMEOUT)
print('Ran {0} tests in {1}s.'.format(len(tests), time.monotonic() - start_time))
