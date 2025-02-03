from pymodbus.client.sync import ModbusTcpClient
import paho.mqtt.client as mqtt
import json
import time
import configparser
import csv
from pathlib import Path

# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# MQTT Configuration
MQTT_ENABLED = config['MQTT'].getboolean('ENABLED')
MQTT_BROKER = config['MQTT']['BROKER']
MQTT_PORT = int(config['MQTT']['PORT'])
MQTT_USER = config['MQTT']['USER']
MQTT_PASSWORD = config['MQTT']['PASSWORD']
MQTT_TOPIC = config['MQTT']['TOPIC']
TLS_ENABLED = config['MQTT'].getboolean('TLS_ENABLED', fallback=False)
CA_CERT = config['MQTT'].get('CA_CERT', None)
CLIENT_CERT = config['MQTT'].get('CLIENT_CERT', None)
CLIENT_KEY = config['MQTT'].get('CLIENT_KEY', None)

# Inverter Configuration
INVERTER_1_IP = config['INVERTERS']['INVERTER_1_IP']
INVERTER_1_PORT = int(config['INVERTERS']['INVERTER_1_PORT'])
INVERTER_2_IP = config['INVERTERS']['INVERTER_2_IP']
INVERTER_2_PORT = int(config['INVERTERS']['INVERTER_2_PORT'])

# Logging Configuration
CSV_LOGGING = config['LOGGING'].getboolean('CSV_LOGGING')
CSV_FILE = config['LOGGING']['CSV_FILE']

# Initialize MQTT Client if enabled
if MQTT_ENABLED:
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

    # Enable TLS if configured
    if TLS_ENABLED:
        mqtt_client.tls_set(
            ca_certs=None,
            certfile=None,
            keyfile=None)
        mqtt_client.tls_insecure_set(True)  # Set to True for testing with self-signed certificates

    # Set MQTT callbacks
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT broker.")
        else:
            print(f"Failed to connect to MQTT broker. Error code: {rc}")

    def on_disconnect(client, userdata, rc):
        print("Disconnected from MQTT broker.")

    def on_publish(client, userdata, mid):
        print(f"Message {mid} published successfully.")

    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_publish = on_publish

# Prepare CSV file if logging enabled
if CSV_LOGGING:
    csv_file = Path(CSV_FILE)
    if not csv_file.exists():
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp", "metric", "value"])

def process_register_value(registers, data_type, scale=1):
    """Process Modbus register values based on data type and scaling."""
    if data_type == "U16":
        value = registers[0]
    elif data_type == "S16":
        value = registers[0] if registers[0] <= 32767 else registers[0] - 65536
    elif data_type == "U32":
        value = (registers[0] << 16) | registers[1]
    elif data_type == "S32":
        value = (registers[0] << 16) | registers[1]
        if value > 0x7FFFFFFF:
            value -= 0x100000000
    else:
        raise ValueError(f"Unsupported data type: {data_type}")
    return value / scale

def handle_data(description, value):
    """Handle processed data according to configuration."""
    timestamp = int(time.time())
    
    # MQTT Publishing
    if MQTT_ENABLED:
        payload = json.dumps({
            "timestamp": timestamp,
            "metric": description,
            "value": value
        })
        result = mqtt_client.publish(MQTT_TOPIC, payload)
        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            print(f"Sent to MQTT: {payload}")
        else:
            print(f"Failed to send to MQTT: {payload}")
    
    # CSV Logging
    if CSV_LOGGING:
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, description, value])

def read_and_process_register(client, address, description, count=1, unit=1, data_type="U16", scale=1):
    """Read and process a single register."""
    response = client.read_input_registers(address=address, count=count, unit=unit)
    if response.isError():
        print(f"Error reading {description} (Register {address}): {response}")
        return
    
    try:
        value = process_register_value(response.registers, data_type, scale)
        print(f"{description} (Register {address}): {value}")
        handle_data(description, value)
    except ValueError as e:
        print(f"Error processing {description} (Register {address}): {e}")

# Initialize Modbus clients
modbus_client_1 = ModbusTcpClient(INVERTER_1_IP, port=INVERTER_1_PORT)
modbus_client_2 = ModbusTcpClient(INVERTER_2_IP, port=INVERTER_2_PORT)

if modbus_client_1.connect() and modbus_client_2.connect():
    print("Connected to both Modbus servers.")
    
    if MQTT_ENABLED:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_start()  # Start the MQTT network loop

    # Register configurations
    registers_inverter_1 = [
        # (address, description, count, unit, data_type, scale)
        (5016, "solar/inverter1/LeistungWatt", 1, 1, "U16", 1),
        (13006, "solar/grid/exportWatt", 2, 1, "S32", 1),
        (13010, "solar/grid/importWatt", 2, 1, "S32", 1),
        (13022, "solar/battery/levelPercent", 1, 1, "U16", 10),
        (13023, "solar/battery/healthPercent", 1, 1, "U16", 10),
        (13021, "solar/battery/powerWatt", 1, 1, "U16", 1),
    ]

    registers_inverter_2 = [
        (5016, "solar/inverter2/LeistungWatt", 1, 1, "U16", 1),
    ]

    try:
        while True:
            # Process Inverter 1 registers
            for params in registers_inverter_1:
                read_and_process_register(modbus_client_1, *params)
            
            # Process Inverter 2 DC Power only
            for params in registers_inverter_2:
                read_and_process_register(modbus_client_2, *params)
            
            print("Waiting 60 seconds...")
            time.sleep(60)

    except KeyboardInterrupt:
        print("Program stopped by user.")
    finally:
        # Cleanup
        modbus_client_1.close()
        modbus_client_2.close()
        if MQTT_ENABLED:
            mqtt_client.loop_stop()  # Stop the MQTT network loop
            mqtt_client.disconnect()
        print("Disconnected from all services.")

else:
    print("Failed to connect to one or both Modbus servers.")
