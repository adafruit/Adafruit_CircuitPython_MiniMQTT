import time
from adafruit_ble import BLERadio
from adafruit_ble.advertising.standard import ProvideServicesAdvertisement
from adafruit_ble.services.nordic import UARTService
import adafruit_ble_socket as socket
import adafruit_minimqtt as MQTT

# Get mqtt details and more from a secrets.py file
try:
    from secrets import secrets
except ImportError:
    print("MQTT broker, username and password are kept in secrets.py, please add them there!")
    raise

## BLE ##

ble = BLERadio()
uart = UARTService()
advertisement = ProvideServicesAdvertisement(uart)

### Adafruit IO Setup ###

# Setup a feed named `testfeed` for publishing.
default_topic = secrets["user"] + "/feeds/testfeed"

### Code ###
# Define callback methods which are called when events occur
# pylint: disable=unused-argument, redefined-outer-name
def connected(client, userdata, flags, rc):
    # This function will be called when the client is connected
    # successfully to the broker.
    print("Connected to MQTT broker! Listening for topic changes on %s" % default_topic)
    # Subscribe to all changes on the default_topic feed.
    client.subscribe(default_topic)


def disconnected(client, userdata, rc):
    # This method is called when the client is disconnected
    print("Disconnected from MQTT Broker!")


def message(client, topic, message):
    """Method callled when a client's subscribed feed has a new
    value.
    :param str topic: The topic of the feed with a new value.
    :param str message: The new value
    """
    print("New message on topic {0}: {1}".format(topic, message))

# Initialize MQTT interface with the esp interface
MQTT.set_socket(socket, uart)

# Set up a MiniMQTT Client
mqtt_client = MQTT.MQTT(broker = secrets['broker'],
                        port = MQTT.MQTT_WSS_PORT,
                        username = secrets['user'],
                        password = secrets['pass'])

# Setup the callback methods above
mqtt_client.on_connect = connected
mqtt_client.on_disconnect = disconnected
mqtt_client.on_message = message

photocell_val = 0

while True:
    print("start_advertising")
    ble.start_advertising(advertisement)
    while not ble.connected:
        pass
    if ble.connected:
        # Connect the client to the MQTT broker.
        print("ble connected. Try to connect to MQTT")
        mqtt_client.connect()
    while ble.connected:
        # Poll the message queue
        mqtt_client.loop()
        mqtt_client.loop()

        # Send a new message
        print('Sending photocell value: %d'%photocell_val)
        mqtt_client.publish(default_topic, photocell_val)
        photocell_val += 1
        time.sleep(5)