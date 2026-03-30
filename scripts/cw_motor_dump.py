#!/usr/bin/env python3
"""Diagnostic script to collect ALL motor register data and save to JSON."""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from motors import FeetechMotorsBus
from motors.registers import REGISTER_SPECS, decode_register_value
from so101.robot import SO101_MOTORS

logger = logging.getLogger(__name__)


def collect_motor_data(
    bus: FeetechMotorsBus,
    motor_id: int,
    motor_name: str,
) -> Dict[str, Any]:
    """Read all registers for a single motor and return JSON-serializable dict."""
    registers: Dict[str, Any] = {}

    for spec in REGISTER_SPECS:
        try:
            raw_value = bus.read_register_by_id(spec.name, motor_id, decode=False)
            decoded_value = decode_register_value(spec, raw_value)

            registers[spec.name] = {
                "raw": raw_value,
                "decoded": decoded_value,
                "address": spec.address,
                "length": spec.length,
                "storage": spec.storage,
                "access": spec.access,
            }
        except Exception as e:
            registers[spec.name] = {
                "raw": None,
                "decoded": None,
                "address": spec.address,
                "length": spec.length,
                "storage": spec.storage,
                "access": spec.access,
                "error": str(e),
            }

    return {
        "id": motor_id,
        "name": motor_name,
        "registers": registers,
    }


def collect_motor_dump_for_port(
    port: str,
    *,
    skip_preflight: bool = True,
) -> Optional[Dict[str, Any]]:
    """Collect motor register data for a given serial port.

    Returns the same structure as the CLI output: {"metadata": {...}, "motors": {...}}.
    Returns None on connection or read failure.
    """
    bus = FeetechMotorsBus(port=port, motors=SO101_MOTORS, calibration=None)
    try:
        bus.connect(preflight_check=not skip_preflight)
    except Exception as e:
        logger.warning("Motor dump: failed to connect to %s: %s", port, e)
        return None

    try:
        motor_items = list(SO101_MOTORS.items())
        metadata = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "port": port,
            "baudrate": bus.baudrate,
            "motor_count": len(motor_items),
            "motor_names": [name for name, _ in motor_items],
        }
        motors_data: Dict[str, Any] = {}
        for motor_name, motor in motor_items:
            motors_data[motor_name] = collect_motor_data(bus, motor.id, motor_name)
        return {"metadata": metadata, "motors": motors_data}
    except Exception as e:
        logger.warning("Motor dump: failed to read from %s: %s", port, e)
        return None
    finally:
        if bus.connected:
            bus.disconnect()


def main():
    parser = argparse.ArgumentParser(
        description="Collect ALL motor register data and save to JSON file"
    )
    parser.add_argument(
        "--port",
        "-p",
        required=True,
        help="Serial port (e.g., /dev/tty.usbmodem5B141129631)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=None,
        help="Output JSON file path (default: motor_dump_YYYYMMDD_HHMMSS.json)",
    )
    parser.add_argument(
        "--motor",
        "-m",
        type=int,
        help="Only collect from specific motor ID (1-6)",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip preflight check during connect",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON output",
    )

    args = parser.parse_args()

    # Default output path
    if args.output is None:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        args.output = Path(f"motor_dump_{timestamp}.json")

    args.output = args.output.resolve()

    print(f"\n{'#'*60}")
    print(" SO101 Motor Data Collection")
    print(f"{'#'*60}")
    print(f"Port: {args.port}")
    print(f"Output: {args.output}")

    bus = FeetechMotorsBus(port=args.port, motors=SO101_MOTORS, calibration=None)

    try:
        print("\nConnecting to motor bus...")
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

        # Collect metadata
        metadata = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "port": args.port,
            "baudrate": bus.baudrate,
            "motor_count": len(motor_items),
            "motor_names": [name for name, _ in motor_items],
        }

        # Collect all motor data
        motors_data: Dict[str, Any] = {}
        for motor_name, motor in motor_items:
            print(f"Reading {motor_name} (ID: {motor.id})...")
            motors_data[motor_name] = collect_motor_data(bus, motor.id, motor_name)

        # Build output structure
        output = {
            "metadata": metadata,
            "motors": motors_data,
        }

        # Write JSON
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2 if args.pretty else None)

        print(f"\n✓ Saved to {args.output}")

        # Summary
        error_count = 0
        for motor_data in motors_data.values():
            for _, reg_data in motor_data["registers"].items():
                if reg_data.get("error"):
                    error_count += 1

        if error_count > 0:
            print(f"⚠ {error_count} register read errors occurred")
        else:
            print("✓ All registers read successfully")

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
