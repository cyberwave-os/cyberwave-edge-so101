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
from typing import Any, Callable, Optional

from dotenv import load_dotenv

from scripts.cw_setup import update_setup_port
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import FollowerConfig, LeaderConfig, get_default_mqtt_port
from utils.errors import DeviceConnectionError, InsufficientCalibrationRangeError
from utils.utils import (
    EXIT_CODE_CALIBRATION_ERROR,
    EXIT_CODE_DEVICE_CONNECTION_ERROR,
    EXIT_CODE_INSUFFICIENT_RANGE,
    EXIT_CODE_USER_CANCELLED,
    find_port,
    setup_logging,
    validate_calibration_ranges_sufficient,
)

logger = logging.getLogger(__name__)

# Throttle joint progress updates to avoid API spam (seconds)
JOINT_PROGRESS_THROTTLE = 1.5


def _check_and_report_calibration_warnings(
    device: Any,
    update_alert: Optional[Callable[[dict], None]],
) -> None:
    """Check calibration ranges for warnings (5-20%) and report them to alert metadata."""
    if not device.calibration:
        logger.debug("No calibration data available, skipping warning check")
        return
    if not update_alert:
        logger.debug("No update_alert callback provided, skipping warning report")
        return
    calib_data = {
        name: {"range_min": c.range_min, "range_max": c.range_max}
        for name, c in device.calibration.items()
    }
    is_valid, warnings, severity = validate_calibration_ranges_sufficient(calib_data)
    if is_valid and warnings and severity == "warning":
        # Calibration passed 5% threshold but has warnings (5-20% range)
        logger.warning("Calibration has limited ranges (5-20%%):")
        for w in warnings:
            logger.warning("  - %s", w)
        update_alert({
            "state": "completed",
            "warnings": warnings,
            "severity": "warning",
        })


def _upload_calibration_to_twin_if_available(
    device, robot, robot_type: str
) -> None:
    """Best-effort calibration upload so UI reflects calibration immediately."""
    if robot is None:
        return
    try:
        # Reuse the same mapping/upload path used by teleoperate/remoteoperate.
        from utils.cw_remoteoperate_helpers import upload_calibration_to_twin

        upload_calibration_to_twin(device, robot, robot_type=robot_type)
    except Exception as e:
        logger.warning(
            "Could not upload %s calibration to twin immediately: %s",
            robot_type,
            e,
        )


# Path for storing connection error details (read by main.py)
CONNECTION_ERROR_FILE = Path("/tmp/cyberwave_calibration_error.json")


def _write_connection_error_details(
    error_type: str, description: str, details: str
) -> None:
    """Write connection error details to temp file for main.py to read."""
    import json

    try:
        CONNECTION_ERROR_FILE.write_text(
            json.dumps({
                "error_type": error_type,
                "description": description,
                "details": details,
                "timestamp": time.time(),
                "pid": os.getpid(),
            })
        )
    except Exception as e:
        logger.warning("Could not write connection error details: %s", e)


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

    # Set default ID based on type if not provided (handle empty string)
    device_id = args.id if args.id else f"{args.type}1"

    # Alert update helpers
    alert_uuid = args.alert_uuid
    twin_uuid = args.twin_uuid or os.getenv("CYBERWAVE_TWIN_UUID")
    update_alert = None
    _robot = None
    _client = None
    device = None  # Initialize device to avoid NameError in exception handler

    token = os.getenv("CYBERWAVE_API_KEY")
    if twin_uuid and token:
        try:
            from cyberwave import Cyberwave

            _client = Cyberwave(
                api_key=token,
                mqtt_port=get_default_mqtt_port(),
                source_type="edge",
            )
            _robot = _client.twin(twin_id=twin_uuid)

            if alert_uuid:
                _alert = _robot.alerts.get(alert_uuid)

                _calibration_meta = dict(_alert.metadata or {}).get("calibration") or {}

                def _update_alert_metadata(calibration_updates: dict) -> None:
                    nonlocal _calibration_meta
                    try:
                        alert = _robot.alerts.get(alert_uuid)
                        if alert is None:
                            return
                        # Re-fetch calibration meta from alert to avoid overwriting server state
                        server_meta = dict(alert.metadata or {})
                        server_calibration = dict(server_meta.get("calibration") or {})
                        # Merge local changes into server state
                        server_calibration.update(_calibration_meta)
                        server_calibration.update(calibration_updates)
                        _calibration_meta = server_calibration
                        server_meta["calibration"] = server_calibration
                        alert.update(metadata=server_meta)
                    except Exception as e:
                        logger.debug("Failed to update alert metadata: %s", e)

                update_alert = _update_alert_metadata
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
                robot=None,
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
                robot=None,
            )

            # Robot validates before saving; if insufficient it raises InsufficientCalibrationRangeError
            # Check for warnings (5-20% range) and report them
            _check_and_report_calibration_warnings(device, update_alert)

            # Save calibration (redundant since calibrate() already saved, but safe)
            logger.info("Saving calibration...")
            device.save_calibration()
            logger.info(f"Calibration saved to {calibration_path}")
            _upload_calibration_to_twin_if_available(device, _robot, args.type)
        else:
            # Follower that wasn't calibrated: connect() already calibrated and saved
            # Just ensure it's saved (redundant but safe)
            logger.info("Calibration completed during connection")

            # Robot validates before saving; if insufficient it raises InsufficientCalibrationRangeError
            # Check for warnings (5-20% range) and report them
            _check_and_report_calibration_warnings(device, update_alert)

            device.save_calibration()
            logger.info(f"Calibration saved to {calibration_path}")
            _upload_calibration_to_twin_if_available(device, _robot, args.type)

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
        if device is not None and device.connected:
            device.disconnect()
        sys.exit(EXIT_CODE_USER_CANCELLED)
    except InsufficientCalibrationRangeError as e:
        logger.error("Calibration ranges insufficient:")
        for warning in e.warnings:
            logger.error("  - %s", warning)
        # Build a detailed error message with all joint issues
        joint_issues = "\n".join(f"  - {w}" for w in e.warnings)
        error_description = (
            "Calibration failed: insufficient joint movement detected.\n"
            f"{joint_issues}\n\n"
            "Move all joints through their full ranges during calibration."
        )
        # Write error details for main.py to create proper error alert
        _write_connection_error_details(
            "insufficient_range",
            error_description,
            str(e),
        )
        if device is not None and device.connected:
            device.disconnect()
        sys.exit(EXIT_CODE_INSUFFICIENT_RANGE)
    except DeviceConnectionError as e:
        # Don't update calibration alert - let main.py handle this by resolving
        # the calibration alert and creating a separate error alert
        logger.error(f"Device connection error during calibration: {e}", exc_info=True)
        # Write error details to temp file for main.py to read
        _write_connection_error_details(e.error_type, e.description, str(e))
        if device is not None and device.connected:
            device.disconnect()
        # Use special exit code so main.py knows this is a connection error
        sys.exit(EXIT_CODE_DEVICE_CONNECTION_ERROR)
    except ConnectionError as e:
        # Don't update calibration alert - let main.py handle this
        logger.error(f"Connection error during calibration: {e}", exc_info=True)
        error_msg = str(e)
        # Determine error type from message
        if "No such file or directory" in error_msg or "could not open port" in error_msg:
            error_type = "device_disconnected"
            description = f"Device not found on {port}. Check that the robot is connected and powered on."
        elif "Pre-flight check failed" in error_msg:
            error_type = "preflight_failed"
            description = f"Cannot connect to motors on {port}. Check that all motors are powered and connected."
        else:
            error_type = "connection_failed"
            description = f"Connection failed: {error_msg}"
        _write_connection_error_details(error_type, description, error_msg)
        if device is not None and device.connected:
            device.disconnect()
        # Use special exit code so main.py knows this is a connection error
        sys.exit(EXIT_CODE_DEVICE_CONNECTION_ERROR)
    except Exception as e:
        logger.error(f"Error during calibration: {e}", exc_info=True)
        error_msg = str(e)
        # Write error details for main.py to create proper error alert with slug
        _write_connection_error_details(
            "calibration_failed",
            f"Calibration failed: {error_msg}",
            error_msg,
        )
        if device is not None and device.connected:
            device.disconnect()
        sys.exit(EXIT_CODE_CALIBRATION_ERROR)


if __name__ == "__main__":
    main()
