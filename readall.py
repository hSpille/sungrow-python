from pymodbus.client.sync import ModbusTcpClient

def read_and_print_register(client, address, description, count=1, unit=1):
    # For holding registers:
    # response = client.read_holding_registers(address=address, count=count, unit=unit)
    
    # Or for input registers:
    response = client.read_input_registers(address=address, count=count, unit=unit)
    
    if response.isError():
        print(f"Fehler beim Auslesen des Registers {address}: {response}")
    else:
        print(f"{description} (Register {address}): {response.registers}")

client = ModbusTcpClient('192.168.178.171', port=502)
if client.connect():
    # Example: addresses that might be holding registers, offset by -1 if doc is 1-based
    registers = [
        (5015, "Total DC Power"),  # or 5016 if doc states offset is 0
        (13006, "Confirmed: Import From Grid: "),
        (13008, "Export To Grid?"),
        (13022, "Battery Power"),
        (13023, "Battery Percent"),
    ]
    
    for address, description in registers:
        read_and_print_register(
            client, 
            address=address, 
            description=description, 
            count=2,      # try 2 if the doc says it’s a 32-bit value
            unit=1        # check your device's slave ID
        )
        
    client.close()
else:
    print("Verbindung zum Modbus-Server fehlgeschlagen.")
