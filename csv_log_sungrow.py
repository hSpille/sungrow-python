import time
import datetime
from pymodbus.client.sync import ModbusTcpClient

def read_register(client, address, scale=1, signed=False, function="input"):
    """Liest ein Modbus-Register aus und gibt den skalierten Wert zurück."""
    try:
        if function == "input":
            response = client.read_input_registers(address=address, count=1)
        elif function == "holding":
            response = client.read_holding_registers(address=address, count=1)
        else:
            raise ValueError("Invalid function type. Use 'input' or 'holding'.")

        if response.isError():
            print(f"Fehler beim Auslesen des Registers {address}: {response}")
            return None
        else:
            value = response.registers[0]
            if signed and value > 32767:  # Vorzeichenbehaftete Werte verarbeiten
                value -= 65536
            return value / scale
    except Exception as e:
        print(f"Ausnahmefehler beim Lesen von Register {address}: {e}")
        return None

def read_int32sw(client, address, scale=1):
    """Liest ein int32sw-Register aus (zwei Register, geswappte Reihenfolge)."""
    try:
        response = client.read_input_registers(address=address, count=2)
        if response.isError():
            print(f"Fehler beim Auslesen der Register {address}-{address + 1}: {response}")
            return None
        else:
            # Register in geswappter Reihenfolge kombinieren
            low = response.registers[1]
            high = response.registers[0]
            value = (high << 16) | low

            # Vorzeichenbehaftete Interpretation
            if value > 0x7FFFFFFF:  # Vorzeichen prüfen
                value -= 0x100000000
            return value / scale
    except Exception as e:
        print(f"Ausnahmefehler beim Lesen von Register {address}: {e}")
        return None

def main():
    client = ModbusTcpClient('192.168.178.172', port=502)  # IP-Adresse des Winet-S-Moduls

    if client.connect():
        try:
            while True:
                # Liste der Register und ihre Konfiguration
                registers = [
                    (5016, "Total_DC_Power", 1, False, "input"),
                    (13007, "Load_Power", 1, True, "int32sw"),  # Angepasst für int32sw
                    (13009, "Export_Power", 1, True, "input"),
                    (13021, "Battery_Power", 1, True, "input")
                ]

                for address, metric, scale, signed, function in registers:
                    if function == "int32sw":
                        value = read_int32sw(client, address, scale)
                    else:
                        value = read_register(client, address, scale, signed, function)

                    if value is not None:
                        print(f"{datetime.datetime.now(datetime.timezone.utc).isoformat()} - {metric}: {value}")

                time.sleep(3)  # Alle 3 Sekunden aktualisieren
        except KeyboardInterrupt:
            print("Programm beendet.")
        finally:
            client.close()
    else:
        print("Verbindung zum Modbus-Server fehlgeschlagen.")

if __name__ == "__main__":
    main()
