from pymodbus.client.sync import ModbusTcpClient

def process_register_value(registers, data_type, scale=1):
    """Processes Modbus register values based on their data type and applies scaling."""
    if data_type == "U16":
        # Unsigned 16-bit integer
        value = registers[0]
    elif data_type == "S16":
        # Signed 16-bit integer
        value = registers[0]
        if value > 32767:
            value -= 65536
    elif data_type == "U32":
        # Unsigned 32-bit integer (combine two 16-bit registers)
        value = (registers[0] << 16) | registers[1]
    elif data_type == "S32":
        # Signed 32-bit integer (combine two 16-bit registers)
        value = (registers[0] << 16) | registers[1]
        if value > 0x7FFFFFFF:
            value -= 0x100000000
    else:
        raise ValueError(f"Unsupported data type: {data_type}")
    
    return value / scale

def read_and_print_register(client, address, description, count=1, unit=1, data_type="U16", scale=1):
    """Reads a register, processes its value, and prints it."""
    response = client.read_input_registers(address=address, count=count, unit=unit)
    if response.isError():
        print(f"Error reading {description} (Register {address}): {response}")
    else:
        try:
            value = process_register_value(response.registers, data_type, scale)
            print(f"{description} (Register {address}): {value}")
        except ValueError as e:
            print(f"Error processing {description} (Register {address}): {e}")

client = ModbusTcpClient('192.168.178.171', port=502)
client2 = ModbusTcpClient('192.168.178.172', port=502)
if client.connect() and client2.connect():
    registers = [
        #Index der Register ist immer weniger 1 als in der Dokumentation im PDF
        (5016, "Total DC Power WR1", 1, "U16", 1),  # U16: Unsigned 16-bit integer, no scaling
        (13006, "++Import From Grid", 2, "S32", 1),  # S32: Signed 32-bit integer, no scaling
        (13009, "Export To Grid", 2, "S32", 1),  # S32: Signed 32-bit integer, no scaling
        (13022, "++Battery Level Percent", 1, "U16", 10),  # S32: Signed 32-bit integer, no scaling
        (13023, "++Battery Health", 1, "U16", 10),  # U16: Unsigned 16-bit integer, scaled by 10
    ]

    registers2 = [
        #Index der Register ist immer weniger 1 als in der Dokumentation im PDF
        (5016, "Total DC Power WR2", 1, "U16", 1),  # U16: Unsigned 16-bit integer, no scaling
    ]

    # Read and process each register
    for address, description, count, data_type, scale in registers:
        read_and_print_register(
            client, 
            address=address, 
            description=description, 
            count=count, 
            unit=1, 
            data_type=data_type,
            scale=scale
        )
    for address, description, count, data_type, scale in registers2:
        read_and_print_register(
            client2, 
            address=address, 
            description=description, 
            count=count, 
            unit=1, 
            data_type=data_type,
            scale=scale
        )

    client.close()
    client2.close()
else:
    print("Failed to connect to the Modbus server.")
