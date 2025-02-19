from pymodbus.client.sync import ModbusTcpClient
import aiomqtt
import asyncio
import json
import time
import configparser
import csv
import datetime
from pathlib import Path
import ssl

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
MQTT_CLIENT_ID = config['MQTT'].get('CLIENT_ID', '')
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


# Prepare CSV file if logging enabled
if CSV_LOGGING:
    csv_file = Path(CSV_FILE)
    if not csv_file.exists():
        with open(csv_file, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["timestamp", "metric", "value"])

#mqtt function async (steffen)
async def send_to_mqtt(payload: str, metric):
    async with aiomqtt.Client(
        MQTT_BROKER,
        MQTT_PORT,
        username=MQTT_USER,
        password=MQTT_PASSWORD,
        tls_context=ssl.create_default_context(),
    ) as mqttClient:
        await mqttClient.publish((MQTT_TOPIC + metric), payload)


def process_register_value(registers, data_type, scale=1):
    """Process Modbus register values based on data type and scaling."""
    if data_type == "U16":
        value = registers[0]
    elif data_type == "S16":
        value = registers[0] if registers[0] <= 32767 else registers[0] - 65536
    elif data_type == "U32":
        value = (registers[0] << 16) | registers[1]
    elif data_type == "13034":
        value = registers[0]
        if value > 50000:
            value = 0
    elif data_type == "13010":
        #Export or import to Grid. Dirty fix
        value = registers[0]
        if value > 50000:
            value = (65535 - value) * -1
    else:
        raise ValueError(f"Unsupported data type: {data_type}")
    return value / scale

async def handle_data(description, value):
    timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()

    if MQTT_ENABLED:
        payload = json.dumps({
            "timestamp": timestamp,  # Now properly formatted
            "metric": "henning_demo/" + description,
            "value": value
        })
        await send_to_mqtt(payload, description)
        print(f"Sent to MQTT: {payload}")        
    if CSV_LOGGING:
        with open(CSV_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([timestamp, description, value])

async def read_and_process_register(client, address, description, count=1, unit=1, data_type="U16", scale=1):
    response = client.read_input_registers(address=address, count=count, unit=unit)
    if response.isError():
        print(f"Error reading {description} (Register {address}): {response}")
        return
    
    try:
        value = process_register_value(response.registers, data_type, scale)
        print(f"{description} (Register {address}): {value}")
        await handle_data(description, value)
    except ValueError as e:
        print(f"Error processing {description} (Register {address}): {e}")

async def main():
    modbus_client_1 = ModbusTcpClient(INVERTER_1_IP, port=INVERTER_1_PORT)
    modbus_client_2 = ModbusTcpClient(INVERTER_2_IP, port=INVERTER_2_PORT)

    if modbus_client_1.connect() and modbus_client_2.connect():
        print("Connected to both Modbus servers.")

        # Register configurations
        registers_inverter_1 = [
            # (address, description, count, unit, data_type, scale)
            (5016, "solar/inverter1/powerWatt", 1, 1, "U16", 1),
            #(13006, "solar/grid/exportWatt", 2, 1, "S32", 1),
            (13009, "solar/grid/exportWatt", 2, 1, "13010", 1),
            (13022, "solar/battery/levelPercent", 1, 1, "U16", 10),
            (13023, "solar/battery/healthPercent", 1, 1, "U16", 10),
            (13021, "solar/battery/powerWatt", 1, 1, "U16", 1),
            (13033, "solar/grid/usedPower", 2, 1, "13034", 1),
        ]

        registers_inverter_2 = [
            (5016, "solar/inverter2/powerWatt", 1, 1, "U16", 1),
        ]

        try:
            while True:
                # Process Inverter 1 registers
                for params in registers_inverter_1:
                    await read_and_process_register(modbus_client_1, *params)
                
                # Process Inverter 2 DC Power only
                for params in registers_inverter_2:
                    await read_and_process_register(modbus_client_2, *params)
                
                print("Waiting 60 seconds...")
                time.sleep(60)

        except KeyboardInterrupt:
            print("Program stopped by user.")
        finally:
            # Cleanup
            modbus_client_1.close()
            modbus_client_2.close()
            print("Disconnected from all services.")

    else:
        print("Failed to connect to one or both Modbus servers.")


if __name__ == "__main__":
    asyncio.run(main())
