"""Command-line script to calibrate SO101 leader or follower devices.

Example:
    python calibrate.py --type leader --port /dev/tty.usbmodem123 --id blue
    python calibrate.py --type follower --port /dev/tty.usbmodem456 --id red
    python calibrate.py --type leader --port /dev/tty.usbmodem123 --id blue --find-port
"""

import argparse
import logging
import sys
from pathlib import Path

from config import FollowerConfig, LeaderConfig
from follower import SO101Follower
from leader import SO101Leader
from utils import find_port, setup_logging

logger = logging.getLogger(__name__)


def main():
    """Main entry point for calibration script."""
    parser = argparse.ArgumentParser(
        description="Calibrate SO101 leader or follower device",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calibrate a leader device
  python calibrate.py --type leader --port /dev/tty.usbmodem123 --id blue

  # Calibrate a follower device
  python calibrate.py --type follower --port /dev/tty.usbmodem456 --id red

  # Find port interactively and then calibrate
  python calibrate.py --type leader --find-port --id blue

  # Use custom calibration directory
  python calibrate.py --type leader --port /dev/tty.usbmodem123 --id blue \\
      --calibration-dir ~/my_calibrations
        """,
    )

    parser.add_argument(
        "--type",
        type=str,
        choices=["leader", "follower"],
        required=True,
        help="Device type: 'leader' or 'follower'",
    )
    parser.add_argument(
        "--port",
        type=str,
        help="Serial port path (e.g., /dev/tty.usbmodem123). Required unless --find-port is used.",
    )
    parser.add_argument(
        "--find-port",
        action="store_true",
        help="Interactively find the port by disconnecting/reconnecting the device",
    )
    parser.add_argument(
        "--id",
        type=str,
        default="device1",
        help="Device identifier for calibration file management (default: device1)",
    )
    parser.add_argument(
        "--calibration-dir",
        type=str,
        help="Directory for calibration files (default: ~/.so101_lib/calibrations)",
    )
    parser.add_argument(
        "--voltage-rating",
        type=int,
        choices=[5, 12],
        help="Voltage rating (5V or 12V). If not specified, will attempt to auto-detect.",
    )
    parser.add_argument(
        "--use-degrees",
        action="store_true",
        default=True,
        help="Use degrees for position values (default: True)",
    )
    parser.add_argument(
        "--no-degrees",
        dest="use_degrees",
        action="store_false",
        help="Don't use degrees (use raw position values)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(level=log_level)

    # Determine port
    port = args.port
    if args.find_port:
        if port:
            logger.warning("Both --port and --find-port specified. Using --find-port.")
        try:
            port = find_port()
            logger.info(f"Found port: {port}")
        except Exception as e:
            logger.error(f"Error finding port: {e}", exc_info=True)
            sys.exit(1)
    elif not port:
        logger.error("Either --port or --find-port must be specified")
        parser.print_help()
        sys.exit(1)

    # Setup calibration directory
    calibration_dir = None
    if args.calibration_dir:
        calibration_dir = Path(args.calibration_dir).expanduser().resolve()
        calibration_dir.mkdir(parents=True, exist_ok=True)

    try:
        # Create device instance
        if args.type == "leader":
            config = LeaderConfig(
                port=port,
                use_degrees=args.use_degrees,
                id=args.id,
                calibration_dir=calibration_dir,
                voltage_rating=args.voltage_rating,
            )
            device = SO101Leader(
                port=config.port,
                use_degrees=config.use_degrees,
                id=config.id,
                calibration_dir=config.calibration_dir,
            )
        else:  # follower
            config = FollowerConfig(
                port=port,
                use_degrees=args.use_degrees,
                id=args.id,
                calibration_dir=calibration_dir,
                voltage_rating=args.voltage_rating,
            )
            device = SO101Follower(
                port=config.port,
                use_degrees=config.use_degrees,
                id=config.id,
                calibration_dir=config.calibration_dir,
            )

        logger.info(f"Calibrating {args.type} device '{args.id}' on port {port}")
        logger.info(f"Calibration directory: {config.calibration_dir}")

        # Connect to device
        logger.info("Connecting to device...")
        device.connect(calibrate=False)
        logger.info("Device connected successfully")

        # Perform calibration
        logger.info("Starting calibration...")
        device.calibrate()

        # Save calibration
        logger.info("Saving calibration...")
        device.save_calibration()
        logger.info(f"Calibration saved to {config.calibration_dir / f'{args.id}.json'}")

        # Disconnect
        logger.info("Disconnecting from device...")
        device.disconnect()
        logger.info("Calibration completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\nCalibration interrupted by user")
        if device.connected:
            device.disconnect()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during calibration: {e}", exc_info=True)
        if "device" in locals() and device.connected:
            device.disconnect()
        sys.exit(1)


if __name__ == "__main__":
    main()
