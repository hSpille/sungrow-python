from pymodbus.client.sync import ModbusTcpClient

def read_and_print_register(client, address, description, count=1, unit=1):
    # For holding registers:
    # response = client.read_holding_registers(address=address, count=count, unit=unit)
    
    # Or for input registers:
    response = client.read_input_registers(address=address, count=count, unit=unit)
    
    if response.isError():
        print(f"Fehler beim Auslesen des Registers {address}: {response}")
    else:
        print(f"{description} (Register {address+1}): {response.registers}")

client = ModbusTcpClient('192.168.178.171', port=502)
if client.connect():
    # Example: addresses that might be holding registers, offset by -1 if doc is 1-based
    registers = [
        (13022, "Battery Power 1W U16"),
        (13024, "Battery Health Percent"),
        (13023, "Battery Level Percent 0.1%"),
        (13034, "Active Power S32 W"),
        (13018, "Total direct Energy Consumption U32"),
    ]
    
    for address, description in registers:
        read_and_print_register(
            client, 
            address=address-1, 
            description=description, 
            count=2,      # try 2 if the doc says it’s a 32-bit value
            unit=1        # check your device's slave ID
        )
        
    client.close()
else:
    print("Verbindung zum Modbus-Server fehlgeschlagen.")
