#!/usr/bin/env python3
"""Diagnostic script to dump all motor register information."""

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from motors import FeetechMotorsBus
from motors.registers import (
    REGISTER_SPECS,
    RegisterSpec,
    BOOLEAN_REGISTER_NAMES,
    POSITION_REGISTER_NAMES,
    VOLTAGE_REGISTER_NAMES,
    decode_register_value,
)
from motors.tables import (
    MODE_POSITION,
    MODE_VELOCITY,
    MODE_PWM,
    MODE_STEP,
)
from so101.robot import SO101_MOTORS


# Human-readable interpretations for certain registers
OPERATING_MODE_NAMES = {
    MODE_POSITION: "Position",
    MODE_VELOCITY: "Velocity",
    MODE_PWM: "PWM",
    MODE_STEP: "Step",
}

BAUD_RATE_VALUES = {
    0: "1000000 bps",
    1: "500000 bps",
    2: "250000 bps",
    3: "128000 bps",
    4: "115200 bps",
    5: "76800 bps",
    6: "57600 bps",
    7: "38400 bps",
}


def format_register_value(spec: RegisterSpec, raw_value: int, decoded_value: int) -> str:
    """Format a register value with human-readable interpretation."""
    name = spec.name

    # Boolean registers
    if name in BOOLEAN_REGISTER_NAMES:
        return f"{decoded_value} ({'ON' if decoded_value else 'OFF'})"

    # Position registers (0-4095 range)
    if name in POSITION_REGISTER_NAMES:
        degrees = (decoded_value / 4095) * 360
        return f"{decoded_value} ({degrees:.1f}°)"

    # Voltage registers (in 0.1V units)
    if name in VOLTAGE_REGISTER_NAMES:
        volts = decoded_value / 10.0
        return f"{decoded_value} ({volts:.1f}V)"

    # Temperature
    if name == "Present_Temperature" or name == "Max_Temperature_Limit":
        return f"{decoded_value}°C"

    # Operating mode
    if name == "Operating_Mode":
        mode_name = OPERATING_MODE_NAMES.get(decoded_value, "Unknown")
        return f"{decoded_value} ({mode_name})"

    # Baud rate
    if name == "Baud_Rate":
        baud_name = BAUD_RATE_VALUES.get(decoded_value, "Unknown")
        return f"{decoded_value} ({baud_name})"

    # Velocity (signed)
    if "Velocity" in name and spec.sign_bit is not None:
        return f"{decoded_value} (signed)"

    # Load (signed, in 0.1% units)
    if name == "Present_Load":
        percent = decoded_value / 10.0
        return f"{decoded_value} ({percent:.1f}%)"

    # Current (in mA)
    if name == "Present_Current" or name == "Protection_Current":
        return f"{decoded_value} ({decoded_value * 6.5:.1f}mA)"

    # Torque limit (0-1000 = 0-100%)
    if "Torque" in name and "Limit" in name:
        percent = decoded_value / 10.0
        return f"{decoded_value} ({percent:.1f}%)"

    # PID coefficients
    if name in ("P_Coefficient", "D_Coefficient", "I_Coefficient"):
        return f"{decoded_value}"

    # Firmware version
    if "Firmware" in name:
        return f"{decoded_value}"

    # Model number
    if name == "Model_Number":
        return f"{decoded_value} (0x{decoded_value:04X})"

    # Homing offset (signed)
    if name == "Homing_Offset":
        return f"{decoded_value} (signed)"

    # Return delay time (in 2µs units)
    if name == "Return_Delay_Time":
        us = decoded_value * 2
        return f"{decoded_value} ({us}µs)"

    # Default: just show the value
    if raw_value != decoded_value:
        return f"{decoded_value} (raw: {raw_value})"
    return str(decoded_value)


def read_all_registers_for_motor(
    bus: FeetechMotorsBus,
    motor_id: int,
    motor_name: str,
) -> Dict[str, Dict]:
    """Read all registers for a single motor."""
    results = {}

    for spec in REGISTER_SPECS:
        try:
            raw_value = bus.read_register_by_id(spec.name, motor_id, decode=False)
            decoded_value = decode_register_value(spec, raw_value)
            results[spec.name] = {
                "raw": raw_value,
                "decoded": decoded_value,
                "formatted": format_register_value(spec, raw_value, decoded_value),
                "spec": spec,
                "error": None,
            }
        except Exception as e:
            results[spec.name] = {
                "raw": None,
                "decoded": None,
                "formatted": f"ERROR: {e}",
                "spec": spec,
                "error": str(e),
            }

    return results


def print_motor_info(
    motor_id: int,
    motor_name: str,
    registers: Dict[str, Dict],
    verbose: bool = False,
    filter_storage: Optional[str] = None,
) -> None:
    """Print motor information in a formatted way."""
    print(f"\n{'=' * 70}")
    print(f" Motor {motor_id}: {motor_name}")
    print(f"{'=' * 70}")

    # Group by storage type
    eeprom_regs = []
    sram_regs = []

    for name, data in registers.items():
        spec = data["spec"]
        if filter_storage and spec.storage.lower() != filter_storage.lower():
            continue
        if spec.storage == "EEPROM":
            eeprom_regs.append((name, data))
        else:
            sram_regs.append((name, data))

    def print_register_group(title: str, regs: List):
        if not regs:
            return
        print(f"\n{title}:")
        print("-" * 68)
        for name, data in regs:
            spec = data["spec"]
            formatted = data["formatted"]
            access = "RW" if spec.writable else "RO"
            addr = f"@{spec.address:3d}"
            size = f"{spec.length}B"

            if data["error"]:
                print(f"  {name:30s} {addr} {size} [{access}]  {formatted}")
            else:
                print(f"  {name:30s} {addr} {size} [{access}]  {formatted}")

    if not filter_storage or filter_storage.upper() == "EEPROM":
        print_register_group("EEPROM (Non-volatile)", eeprom_regs)
    if not filter_storage or filter_storage.upper() == "SRAM":
        print_register_group("SRAM (Volatile)", sram_regs)


def print_summary_table(
    all_motors: Dict[int, Dict[str, Dict]], registers_to_show: List[str]
) -> None:
    """Print a summary table comparing registers across all motors."""
    print(f"\n{'=' * 90}")
    print(" Summary Table")
    print(f"{'=' * 90}")

    # Header
    motor_ids = sorted(all_motors.keys())
    header = f"{'Register':30s}"
    for mid in motor_ids:
        header += f"  M{mid:d}".rjust(12)
    print(header)
    print("-" * 90)

    for reg_name in registers_to_show:
        row = f"{reg_name:30s}"
        for mid in motor_ids:
            data = all_motors[mid].get(reg_name, {})
            if data.get("error"):
                row += "       ERR".rjust(12)
            elif data.get("decoded") is not None:
                val = data["decoded"]
                row += f"{val:>12}"
            else:
                row += "         -".rjust(12)
        print(row)


def main():
    parser = argparse.ArgumentParser(
        description="Dump all motor register information for diagnostics"
    )
    parser.add_argument(
        "--port",
        "-p",
        required=True,
        help="Serial port (e.g., /dev/tty.usbmodem5B141129631)",
    )
    parser.add_argument(
        "--motor",
        "-m",
        type=int,
        help="Only show info for specific motor ID (1-6)",
    )
    parser.add_argument(
        "--storage",
        "-s",
        choices=["eeprom", "sram"],
        help="Only show registers from specific storage type",
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Show summary table comparing key registers across motors",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip preflight check during connect",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Show verbose output including raw values",
    )

    args = parser.parse_args()

    print(f"\n{'#' * 70}")
    print(f" SO101 Motor Register Dump")
    print(f"{'#' * 70}")
    print(f"Port: {args.port}")

    # Initialize motor bus
    print(f"\nConnecting to motor bus...")
    bus = FeetechMotorsBus(port=args.port, motors=SO101_MOTORS, calibration=None)

    try:
        bus.connect(preflight_check=not args.skip_preflight)
        print(f"✓ Connected to {args.port}")

        # Determine which motors to read
        if args.motor:
            motor_items = [(name, m) for name, m in SO101_MOTORS.items() if m.id == args.motor]
            if not motor_items:
                print(f"Error: Motor ID {args.motor} not found")
                return 1
        else:
            motor_items = list(SO101_MOTORS.items())

        all_motors = {}

        for motor_name, motor in motor_items:
            print(f"\nReading registers for {motor_name} (ID: {motor.id})...")
            registers = read_all_registers_for_motor(bus, motor.id, motor_name)
            all_motors[motor.id] = registers
            print_motor_info(
                motor.id,
                motor_name,
                registers,
                verbose=args.verbose,
                filter_storage=args.storage,
            )

        # Summary table
        if args.summary or (not args.motor and not args.storage):
            key_registers = [
                "Firmware_Major_Version",
                "Firmware_Minor_Version",
                "Model_Number",
                "ID",
                "Baud_Rate",
                "Operating_Mode",
                "Torque_Enable",
                "Present_Position",
                "Present_Velocity",
                "Present_Load",
                "Present_Voltage",
                "Present_Temperature",
                "Present_Current",
                "P_Coefficient",
                "D_Coefficient",
                "I_Coefficient",
                "Min_Position_Limit",
                "Max_Position_Limit",
                "Homing_Offset",
                "Max_Torque_Limit",
                "Protection_Current",
            ]
            print_summary_table(all_motors, key_registers)

        # Check for any errors
        error_count = 0
        for mid, regs in all_motors.items():
            for name, data in regs.items():
                if data.get("error"):
                    error_count += 1

        if error_count > 0:
            print(f"\n⚠ {error_count} register read errors occurred")
        else:
            print(f"\n✓ All registers read successfully")

    except Exception as e:
        print(f"\n✗ Error: {e}")
        return 1
    finally:
        if bus.connected:
            bus.disconnect()
            print(f"\nDisconnected from {args.port}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
