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
        mqtt_client.publish(MQTT_TOPIC, payload)
        print(f"Sent to MQTT: {payload}")
    
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
        print("Connected to MQTT broker.")

        # Register configurations
    registers_inverter_1 = [
        # (address, description, count, unit, data_type, scale)
        (5016, "Total DC Power WR1", 1, 1, "U16", 1),
        (13006, "Import From Grid WR1", 2, 1, "S32", 1),
        (13010, "Export To Grid WR1", 2, 1, "S32", 1),
        (13022, "Battery Level WR1", 1, 1, "U16", 10),
        (13023, "Battery Health WR1", 1, 1, "U16", 10),
    ]

    registers_inverter_2 = [
        (5016, "Total DC Power WR2", 1, 1, "U16", 1),
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
    
    # Cleanup
    modbus_client_1.close()
    modbus_client_2.close()
    if MQTT_ENABLED:
        mqtt_client.disconnect()
    print("Disconnected from all services.")

else:
    print("Failed to connect to one or both Modbus servers.")