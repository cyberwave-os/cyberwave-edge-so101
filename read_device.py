"""Command-line script to read and display data from SO101 device.

Example:
    python -m so101_lib.read_device --port /dev/tty.usbmodem123
    python -m so101_lib.read_device --port /dev/tty.usbmodem123 --continuous
    python -m so101_lib.read_device --find-port
"""

import argparse
import sys
import time
from typing import Dict, List, Optional

from leader import SO101_LEADER_MOTORS
from utils import (
    detect_voltage_rating,
    find_available_ports,
    find_port,
)


def read_motor_data(
    port: str,
    motor_ids: List[int],
    baudrate: int = 1000000,
    read_voltage_limits: bool = True,
) -> Dict:
    """
    Read comprehensive data from motors.

    Args:
        port: Serial port path
        motor_ids: List of motor IDs to read from
        baudrate: Serial communication baudrate

    Returns:
        Dictionary with motor data
    """
    try:
        from scservo_sdk import PacketHandler, PortHandler
    except ImportError as err:
        raise ImportError(
            "scservo_sdk is required. Install it with: pip install feetech-servo-sdk"
        ) from err

    from motors.encoding import decode_sign_magnitude
    from motors.tables import (
        ADDR_GOAL_POSITION,
        ADDR_ID,
        ADDR_MAX_VOLTAGE_LIMIT,
        ADDR_MIN_VOLTAGE_LIMIT,
        ADDR_MODEL_NUMBER,
        ADDR_MOVING,
        ADDR_PRESENT_LOAD,
        ADDR_PRESENT_POSITION,
        ADDR_PRESENT_TEMPERATURE,
        ADDR_PRESENT_VELOCITY,
        ADDR_PRESENT_VOLTAGE,
        ADDR_TORQUE_ENABLE,
        ENCODING_BIT_VELOCITY,
    )

    port_handler = PortHandler(port)
    port_handler.setBaudRate(baudrate)

    if not port_handler.openPort():
        raise ConnectionError(f"Failed to open port: {port}")

    packet_handler = PacketHandler(1.0)
    motor_data = {}

    for motor_id in motor_ids:
        data: Dict = {"id": motor_id, "error": None}

        try:
            # Read motor ID (verify it matches)
            motor_id_read, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_ID[0]
            )
            if result != 0:
                data["error"] = f"Failed to read motor ID: result={result}, error={error}"
                motor_data[motor_id] = data
                continue

            # Read model number (address 3, 2 bytes)
            # Use read2ByteTxRx to read both bytes at once
            model_number, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_MODEL_NUMBER[0]
            )
            if result == 0:
                data["model_number"] = model_number

            # Read present position (address 56, 2 bytes)
            # Position is absolute (0-4095 for STS3215), not sign-magnitude
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            position_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_POSITION[0]
            )
            if result == 0:
                # Position is absolute, not sign-magnitude encoded
                # Clamp to 12-bit encoder range (0-4095) using mask and bounds
                position_raw = position_raw & 0x0FFF  # Mask to 12 bits
                data["present_position"] = max(0, min(4095, position_raw))

            # Read goal position (address 42, 2 bytes)
            # Position is absolute (0-4095 for STS3215), not sign-magnitude
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            goal_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_GOAL_POSITION[0]
            )
            if result == 0:
                # Position is absolute, not sign-magnitude encoded
                # Clamp to 12-bit encoder range (0-4095) using mask and bounds
                goal_raw = goal_raw & 0x0FFF  # Mask to 12 bits
                data["goal_position"] = max(0, min(4095, goal_raw))

            # Read present velocity (address 58, 2 bytes, bit 15 for sign)
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            speed_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_VELOCITY[0]
            )
            if result == 0:
                data["present_speed_raw"] = speed_raw
                data["present_speed"] = decode_sign_magnitude(
                    speed_raw, sign_bit=ENCODING_BIT_VELOCITY
                )

            # Read present load (address 60, 2 bytes)
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            load_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_LOAD[0]
            )
            if result == 0:
                data["present_load_raw"] = load_raw
                # Load might use sign-magnitude, but check documentation
                # For now, try standard decoding
                data["present_load"] = decode_sign_magnitude(load_raw)

            # Read voltage (address 62, 1 byte)
            voltage, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_VOLTAGE[0]
            )
            if result == 0:
                # Store raw value for debugging
                data["voltage_raw"] = voltage
                # Voltage is in 0.1V units (e.g., 50 = 5.0V)
                # According to Feetech docs, voltage register is at address 62
                data["voltage"] = voltage / 10.0  # Voltage is in 0.1V units

            # Read voltage limits (for detecting voltage rating)
            # Addresses: Max=14, Min=15
            if read_voltage_limits:
                max_voltage, result, error = packet_handler.read1ByteTxRx(
                    port_handler, motor_id, ADDR_MAX_VOLTAGE_LIMIT[0]
                )
                if result == 0:
                    data["max_limit_voltage"] = max_voltage / 10.0  # In 0.1V units

                min_voltage, result, error = packet_handler.read1ByteTxRx(
                    port_handler, motor_id, ADDR_MIN_VOLTAGE_LIMIT[0]
                )
                if result == 0:
                    data["min_limit_voltage"] = min_voltage / 10.0  # In 0.1V units

            # Read temperature
            temperature, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_TEMPERATURE[0]
            )
            if result == 0:
                data["temperature"] = temperature  # Temperature in Celsius

            # Read torque enable
            torque_enable, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_TORQUE_ENABLE[0]
            )
            if result == 0:
                data["torque_enabled"] = bool(torque_enable)

            # Read moving status
            moving, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_MOVING[0]
            )
            if result == 0:
                data["moving"] = bool(moving)

        except Exception as e:
            data["error"] = str(e)

        motor_data[motor_id] = data

    port_handler.closePort()
    return motor_data


def format_motor_data(
    motor_data: Dict,
    motor_names: Optional[Dict[int, str]] = None,
    voltage_rating: Optional[int] = None,
    show_raw: bool = False,
) -> str:
    """
    Format motor data for display.

    Args:
        motor_data: Dictionary of motor data from read_motor_data
        motor_names: Optional mapping of motor ID to name

    Returns:
        Formatted string
    """
    lines = []
    lines.append("=" * 80)
    lines.append("SO101 Motor Data")
    if voltage_rating:
        lines.append(f"Voltage Rating: {voltage_rating}V (detected)")
    lines.append("=" * 80)

    for motor_id, data in sorted(motor_data.items()):
        motor_name = motor_names.get(motor_id, f"Motor {motor_id}") if motor_names else f"Motor {motor_id}"
        lines.append(f"\n{motor_name} (ID: {motor_id})")
        lines.append("-" * 80)

        if data.get("error"):
            lines.append(f"  ERROR: {data['error']}")
            continue

        if "model_number" in data:
            lines.append(f"  Model Number: {data['model_number']}")

        if "present_position" in data:
            lines.append(f"  Present Position: {data['present_position']}")

        if "goal_position" in data:
            lines.append(f"  Goal Position: {data['goal_position']}")

        if "present_speed" in data:
            speed_str = f"  Present Speed: {data['present_speed']}"
            if show_raw and "present_speed_raw" in data:
                speed_str += f" (raw: {data['present_speed_raw']})"
            lines.append(speed_str)

        if "present_load" in data:
            load_str = f"  Present Load: {data['present_load']}"
            if show_raw and "present_load_raw" in data:
                load_str += f" (raw: {data['present_load_raw']})"
            lines.append(load_str)

        if "voltage" in data:
            voltage_str = f"  Voltage: {data['voltage']:.1f} V"
            if show_raw and "voltage_raw" in data:
                voltage_str += f" (raw: {data['voltage_raw']})"
            lines.append(voltage_str)
            # Warn if voltage seems incorrect
            if data["voltage"] > 15.0:
                lines.append("    ⚠️  WARNING: Voltage reading seems unusually high!")
                lines.append("    This might indicate a measurement error or wrong register.")
                if "voltage_raw" in data:
                    lines.append(f"    Raw value: {data['voltage_raw']} (if > 255, register might be wrong)")

        if "max_limit_voltage" in data or "min_limit_voltage" in data:
            voltage_range = []
            if "min_limit_voltage" in data:
                voltage_range.append(f"{data['min_limit_voltage']:.1f}")
            else:
                voltage_range.append("?")
            voltage_range.append("-")
            if "max_limit_voltage" in data:
                voltage_range.append(f"{data['max_limit_voltage']:.1f}")
            else:
                voltage_range.append("?")
            lines.append(f"  Voltage Limits: {' '.join(voltage_range)} V")

        if "temperature" in data:
            lines.append(f"  Temperature: {data['temperature']} °C")

        if "torque_enabled" in data:
            lines.append(f"  Torque Enabled: {data['torque_enabled']}")

        if "moving" in data:
            lines.append(f"  Moving: {data['moving']}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def main():
    """Main entry point for read_device script."""
    parser = argparse.ArgumentParser(
        description="Read and display data from SO101 device motors"
    )
    parser.add_argument(
        "--port",
        type=str,
        help="Serial port path (e.g., /dev/tty.usbmodem123)",
    )
    parser.add_argument(
        "--find-port",
        action="store_true",
        help="Interactively find the port",
    )
    parser.add_argument(
        "--motor-ids",
        type=str,
        default="1,2,3,4,5,6",
        help="Comma-separated list of motor IDs to read (default: 1,2,3,4,5,6)",
    )
    parser.add_argument(
        "--baudrate",
        type=int,
        default=1000000,
        help="Serial communication baudrate (default: 1000000)",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Continuously read and display data (press Ctrl+C to stop)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="Update interval in seconds for continuous mode (default: 0.1)",
    )
    parser.add_argument(
        "--voltage-rating",
        type=int,
        choices=[5, 12],
        help="Voltage rating (5V or 12V). If not specified, will attempt to detect.",
    )
    parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Show raw register values for debugging",
    )

    args = parser.parse_args()

    # Determine port
    port = args.port
    if args.find_port:
        try:
            port = find_port()
        except Exception as e:
            print(f"Error finding port: {e}", file=sys.stderr)
            sys.exit(1)
    elif not port:
        # Try to find available ports
        available_ports = find_available_ports()
        if len(available_ports) == 1:
            port = available_ports[0]
            print(f"Using auto-detected port: {port}")
        elif len(available_ports) > 1:
            print("Multiple ports available. Please specify --port or use --find-port:")
            for p in available_ports:
                print(f"  {p}")
            sys.exit(1)
        else:
            print("No ports found. Please specify --port or use --find-port.", file=sys.stderr)
            sys.exit(1)

    # Parse motor IDs
    try:
        motor_ids = [int(id.strip()) for id in args.motor_ids.split(",")]
    except ValueError:
        print(f"Invalid motor IDs: {args.motor_ids}", file=sys.stderr)
        sys.exit(1)

    # Create motor name mapping from SO101_LEADER_MOTORS
    motor_names = {motor.id: name for name, motor in SO101_LEADER_MOTORS.items()}

    # Detect or use specified voltage rating
    voltage_rating = args.voltage_rating
    if voltage_rating is None:
        print("Attempting to detect voltage rating...")
        voltage_rating = detect_voltage_rating(port, motor_ids[0] if motor_ids else 1, args.baudrate)
        if voltage_rating:
            print(f"Detected voltage rating: {voltage_rating}V")
        else:
            print("Could not detect voltage rating. Use --voltage-rating to specify (5 or 12).")

    try:
        if args.continuous:
            print(f"Reading from {port} (press Ctrl+C to stop)...\n")
            try:
                while True:
                    # Clear screen (works on most terminals)
                    print("\033[2J\033[H", end="")

                    motor_data = read_motor_data(port, motor_ids, args.baudrate)
                    output = format_motor_data(motor_data, motor_names, voltage_rating, args.show_raw)
                    print(output)
                    print(f"\nLast update: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                    print(f"Update interval: {args.interval}s")

                    time.sleep(args.interval)
            except KeyboardInterrupt:
                print("\n\nStopped by user.")
        else:
            motor_data = read_motor_data(port, motor_ids, args.baudrate)
            output = format_motor_data(motor_data, motor_names, voltage_rating, args.show_raw)
            print(output)

    except Exception as e:
        print(f"Error reading device: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

