"""
CircuitPython_MiniMQTT Methods Test Suite

by Brent Rubell for Adafruit Industries, 2019
"""
import time
import board
import busio
from digitalio import DigitalInOut
import neopixel
from adafruit_esp32spi import adafruit_esp32spi
import adafruit_esp32spi.adafruit_esp32spi_socket as socket

from adafruit_minimqtt import MQTT, MMQTTException

"""
Generic cpython3 unittest-like assertions
"""

#pylint: disable=keyword-arg-before-vararg
def assertRaises(self, exc, func=None, *args, **kwargs):
    if func is None:
        return AssertRaisesContext(exc)

    try:
        func(*args, **kwargs)
        assert False, "%r not raised" % exc
    except Exception as e:
        if isinstance(e, exc):
            return
        raise

def assertAlmostEqual(x, y, places=None, msg=''):
    """Raises an AssertionError if two float values are not equal.
    (from https://github.com/micropython/micropython-lib/blob/master/unittest/unittest.py)."""
    if x == y:
        return
    if places is None:
        places = 2
    if round(abs(y-x), places) == 0:
        return
    if not msg:
        msg = '%r != %r within %r places' % (x, y, places)
    assert False, msg

def assertIsNone(x):
    """Raises an AssertionError if x is None."""
    if x is None:
        raise AssertionError('%r is None'%x)

def assertEqual(val_1, val_2):
    """Raises an AssertionError if the two specified values are not equal."""
    if val_1 != val_2:
        raise AssertionError('Values are not equal:', val_1, val_2)

def assertRaises(exc, func=None, *args, **kwargs):
    if func is None:
        return AssertRaisesContext(exc)

    try:
        func(*args, **kwargs)
        assert False, "%r not raised" % exc
    except Exception as e:
        if isinstance(e, exc):
            return
        raise

# MQTT Client Tests
def test_mqtt_connect_disconnect_esp32spi():
    """Creates an INSECURE MQTT client, connects, and attempts a disconnection."""
    mqtt_client = MQTT(socket,
                        settings['url'],
                        username=settings['username'],
                        password=settings['password'],
                        esp = esp,
                        is_ssl = False)
    mqtt_client.connect()
    assertEqual(mqtt_client.port, 1883)
    assertEqual(mqtt_client._is_connected, True)
    mqtt_client.disconnect()
    assertEqual(mqtt_client._is_connected, False)

def test_mqtts_connect_disconnect_esp32spi():
    """Creates a SECURE MQTT client, connects, and attempts a disconnection."""
    mqtt_client = MQTT(socket,
                        settings['url'],
                        username=settings['username'],
                        password=settings['password'],
                        esp = esp)
    mqtt_client.connect()
    assertEqual(mqtt_client.port, 8883)
    assertEqual(mqtt_client._is_connected, True)
    mqtt_client.disconnect()
    assertEqual(mqtt_client._is_connected, False)

def test_sub_pub():
    """Creates a MQTTS client, connects, subscribes, publishes, and checks data
    received from broker matches data sent by client"""
    mqtt_client = MQTT(socket,
                        settings['url'],
                        username=settings['username'],
                        password=settings['password'],
                        esp = esp)
    # listen in on the logger...
    mqtt_client.set_logger_level = 'DEBUG'
    # Callback responses
    callback_msgs = []
    def on_message(client, topic, msg):
        callback_msgs.append([topic, msg])
    mqtt_client.on_message = on_message
    mqtt_client.connect()
    assertEqual(mqtt_client._is_connected, True)
    mqtt_client.subscribe(['default_topic'])
    mqtt_client.publish(settings['default_topic'], settings['default_data_int'], 1)
    start_timer = time.monotonic()
    while len(callback_msgs) == 0 and (time.monotonic() - start_timer < 30):
        mqtt_client.wait_for_msg()
    # check message and topic has been RX'd by the client's callback
    assertEqual(callback_msgs[0][0], settings['default_topic'])
    assertEqual(callback_msgs[0][1], str(settings['default_data_int']))
    mqtt_client.unsubscribe(settings['default_topic'])
    mqtt_client.disconnect()
    return

def test_sub_pub_multiple():
    """Subscribe to multiple topics, publish to one, unsubscribe from both.
    """
    TOPIC_1 = settings['default_topic']
    TOPIC_2 = settings['alt_topic']
    mqtt_client = MQTT(socket,
                        settings['url'],
                        username=settings['username'],
                        password=settings['password'],
                        esp = esp)
    # Callback responses
    callback_msgs = []
    def on_message(client, topic, msg):
        callback_msgs.append([topic, msg])
    mqtt_client.on_message = on_message
    mqtt_client.connect()
    assertEqual(mqtt_client._is_connected, True)
    # subscribe to two topics with different QoS levels
    mqtt_client.subscribe([(TOPIC_1, 1), (TOPIC_2, 0)])
    mqtt_client.publish(TOPIC_2, 42)
    start_timer = time.monotonic()
    while len(callback_msgs) == 0 and (time.monotonic() - start_timer < 30):
        mqtt_client.wait_for_msg()
    # check message and topic has been RX'd by the client's callback
    assertEqual(callback_msgs[0][0], TOPIC_2)
    assertEqual(callback_msgs[0][1], str(42))
    mqtt_client.unsubscribe([(TOPIC_1), (TOPIC_2)])
    mqtt_client.disconnect()



# Connection/Client Tests
conn_tests = [test_mqtt_connect_disconnect_esp32spi, test_mqtts_connect_disconnect_esp32spi]

# Publish/Subscribe tests
pub_sub_tests = [test_sub_pub, test_sub_pub_multiple]

# The test routine will run the following test(s):
tests = pub_sub_tests

try:
    from secrets import secrets
    # Test Setup
    from settings import settings
except ImportError:
    print("WiFi secrets are kept in secrets.py, please add them there!")
    raise

# Define an ESP32SPI network interface
esp32_cs = DigitalInOut(board.ESP_CS)
esp32_ready = DigitalInOut(board.ESP_BUSY)
esp32_reset = DigitalInOut(board.ESP_RESET)
spi = busio.SPI(board.SCK, board.MOSI, board.MISO)
esp = adafruit_esp32spi.ESP_SPIcontrol(spi, esp32_cs, esp32_ready, esp32_reset)
status_light = neopixel.NeoPixel(board.NEOPIXEL, 1, brightness=0.2) # Uncomment for Most Boards


# Establish ESP32SPI connection
print("Connecting to AP...")
while not esp.is_connected:
    try:
        esp.connect_AP(secrets['ssid'], secrets['password'])
    except RuntimeError as e:
        print("could not connect to AP, retrying: ",e)
        continue
print("Connected to", str(esp.ssid, 'utf-8'), "\tRSSI:", esp.rssi)

## Test Routine ##
start_time = time.monotonic()
for i in enumerate(tests):
    print('Running test: ', i)
    i[1]()
    print('OK!')
    time.sleep(settings['test_timeout'])
print('Ran {0} tests in {1}s.'.format(len(tests), time.monotonic() - start_time))
