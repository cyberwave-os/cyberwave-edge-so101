"""Command-line script to calibrate SO101 leader or follower devices.

Example:
    python calibrate.py --type leader --port /dev/tty.usbmodem123 --id blue
    python calibrate.py --type follower --port /dev/tty.usbmodem456 --id red
    python calibrate.py --type leader --port /dev/tty.usbmodem123 --id blue --find-port
"""

import argparse
import logging
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from scripts.cw_setup import update_setup_port
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import FollowerConfig, LeaderConfig
from utils.utils import find_port, setup_logging

logger = logging.getLogger(__name__)

# Throttle joint progress updates to avoid API spam (seconds)
JOINT_PROGRESS_THROTTLE = 1.5


def main():
    """Main entry point for calibration script."""
    load_dotenv()
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
        default=None,
        help="Device identifier for calibration file management (default: {type}1, e.g., leader1 or follower1)",
    )
    parser.add_argument(
        "--calibration-dir",
        type=str,
        help="Directory for calibration files (default: ~/.cyberwave/so101_lib/calibrations)",
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
    parser.add_argument(
        "--alert-uuid",
        type=str,
        help="Alert UUID to update with calibration state and joint progress (for frontend)",
    )
    parser.add_argument(
        "--twin-uuid",
        type=str,
        help="Twin UUID (required when --alert-uuid is set, for fetching/updating alert)",
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    setup_logging(level=log_level)

    # Determine port
    port = args.port

    # Use find_port if:
    # 1. --find-port is explicitly set, OR
    # 2. No port is provided (find_port by default)
    if args.find_port or not port:
        if port and args.find_port:
            logger.warning("Both --port and --find-port specified. Using --find-port.")
        try:
            port = find_port()
            logger.info(f"Found port: {port}")
            # Wait for user to reconnect the USB cable before proceeding
            print("\nPlease reconnect the USB cable and press Enter when ready to continue.")
            input()
            logger.info("Proceeding with calibration...")
        except Exception as e:
            logger.error(f"Error finding port: {e}", exc_info=True)
            sys.exit(1)
    # If port is provided and find_port is not set, use the provided port directly

    # Setup calibration directory
    calibration_dir = None
    if args.calibration_dir:
        calibration_dir = Path(args.calibration_dir).expanduser().resolve()
        calibration_dir.mkdir(parents=True, exist_ok=True)

    # Set default ID based on type if not provided
    device_id = args.id if args.id is not None else f"{args.type}1"

    # Alert update helpers (when --alert-uuid and --twin-uuid provided)
    alert_uuid = args.alert_uuid
    twin_uuid = args.twin_uuid
    update_alert = None
    if alert_uuid and twin_uuid:
        token = os.getenv("CYBERWAVE_API_KEY")
        if token:
            try:
                from cyberwave import Cyberwave

                _client = Cyberwave(api_key=token, source_type="edge")
                _robot = _client.twin(twin_id=twin_uuid)
                _alert = _robot.alerts.get(alert_uuid)

                _calibration_meta = dict(_alert.metadata or {}).get("calibration") or {}

                def _update_alert_metadata(calibration_updates: dict) -> None:
                    try:
                        _calibration_meta.update(calibration_updates)
                        meta = dict(_alert.metadata or {})
                        meta["calibration"] = dict(_calibration_meta)
                        _alert.update(metadata=meta)
                    except Exception as e:
                        logger.debug("Failed to update alert metadata: %s", e)

                update_alert = _update_alert_metadata
                # Set initial state
                _update_alert_metadata({"state": "started"})
            except Exception as e:
                logger.warning("Could not init alert update for calibration: %s", e)

    _last_joint_progress_time = 0.0

    def _on_state_change(state: str) -> None:
        if update_alert:
            update_alert({"state": state})

    def _on_joint_progress(
        current: dict, range_mins: dict, range_maxes: dict
    ) -> None:
        nonlocal _last_joint_progress_time
        if not update_alert:
            return
        now = time.monotonic()
        if now - _last_joint_progress_time < JOINT_PROGRESS_THROTTLE:
            return
        _last_joint_progress_time = now
        joints = {}
        for name in current:
            min_val = range_mins.get(name, float("inf"))
            max_val = range_maxes.get(name, float("-inf"))
            joints[name] = {
                "actual": round(current[name], 1),
                "min": round(min_val, 1) if min_val != float("inf") else None,
                "max": round(max_val, 1) if max_val != float("-inf") else None,
                "status": "recording",
            }
        update_alert({"state": "joint_calibration_waiting", "joints": joints})

    try:
        # Create device instance
        if args.type == "leader":
            config = LeaderConfig(
                port=port,
                use_degrees=args.use_degrees,
                id=device_id,
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
                id=device_id,
                calibration_dir=calibration_dir,
                voltage_rating=args.voltage_rating,
            )
            device = SO101Follower(
                port=config.port,
                use_degrees=config.use_degrees,
                id=config.id,
                calibration_dir=config.calibration_dir,
            )

        logger.info(f"Calibrating {args.type} device '{device_id}' on port {port}")
        logger.info(f"Calibration directory: {config.calibration_dir}")

        # Check if calibration file already exists
        calibration_path = config.calibration_dir / f"{device_id}.json"
        already_calibrated = calibration_path.exists()

        # Connect to device
        # For uncalibrated followers, connect() runs calibration internally - pass callbacks
        # so the frontend receives state updates (zero_pose_waiting, joint_calibration_waiting)
        # and can show the "Go ahead" button.
        logger.info("Connecting to device...")
        if args.type == "follower" and not already_calibrated and update_alert:
            device.connect(
                calibrate=False,
                on_state_change=_on_state_change,
                on_joint_progress=_on_joint_progress,
            )
        else:
            device.connect(calibrate=False)
        logger.info("Device connected successfully")

        if args.type == "leader" or (args.type == "follower" and already_calibrated):
            # Perform calibration (will re-calibrate if already calibrated)
            logger.info("Starting calibration...")
            device.calibrate(
                on_state_change=_on_state_change if update_alert else None,
                on_joint_progress=_on_joint_progress if update_alert else None,
            )
            if update_alert:
                update_alert({"state": "completed", "joints": {}})

            # Save calibration
            logger.info("Saving calibration...")
            device.save_calibration()
            logger.info(f"Calibration saved to {calibration_path}")
        else:
            # Follower that wasn't calibrated: connect() already calibrated and saved
            # Just ensure it's saved (redundant but safe)
            logger.info("Calibration completed during connection")
            if update_alert:
                update_alert({"state": "completed", "joints": {}})
            device.save_calibration()
            logger.info(f"Calibration saved to {calibration_path}")

        # Disconnect
        logger.info("Disconnecting from device...")
        device.disconnect()

        # Save port to setup.json for teleoperate --setup
        try:
            update_setup_port(args.type, port)
            logger.info(f"Saved {args.type}_port to setup.json")
        except Exception as e:
            logger.warning(f"Could not save port to setup: {e}")

        logger.info("Calibration completed successfully!")

    except KeyboardInterrupt:
        logger.warning("\nCalibration interrupted by user")
        if update_alert:
            try:
                update_alert({"state": "cancelled"})
            except Exception:
                pass
        if device.connected:
            device.disconnect()
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during calibration: {e}", exc_info=True)
        if update_alert:
            try:
                update_alert({"state": "error"})
            except Exception:
                pass
        if "device" in locals() and device.connected:
            device.disconnect()
        sys.exit(1)


if __name__ == "__main__":
    main()
