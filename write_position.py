#!/usr/bin/env python
"""Script to write normalized position values to SO101 motors."""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

from config import FollowerConfig, LeaderConfig
from follower import SO101Follower
from leader import SO101Leader
from motors import MotorNormMode

logger = logging.getLogger(__name__)

# Motor ID to name and normalization mode mapping
MOTOR_CONFIG = {
    1: ("shoulder_pan", MotorNormMode.RANGE_M100_100),
    2: ("shoulder_lift", MotorNormMode.RANGE_M100_100),
    3: ("elbow_flex", MotorNormMode.RANGE_M100_100),
    4: ("wrist_flex", MotorNormMode.RANGE_M100_100),
    5: ("wrist_roll", MotorNormMode.RANGE_M100_100),
    6: ("gripper", MotorNormMode.RANGE_0_100),
}


def validate_position(motor_id: int, position: float) -> tuple[bool, str]:
    """
    Validate that the normalized position is within the correct range for the motor.

    Args:
        motor_id: Motor ID (1-6)
        position: Normalized position value

    Returns:
        Tuple of (is_valid, error_message)
    """
    if motor_id not in MOTOR_CONFIG:
        return False, f"Invalid motor ID: {motor_id}. Must be 1-6."

    motor_name, norm_mode = MOTOR_CONFIG[motor_id]

    if norm_mode == MotorNormMode.RANGE_M100_100:
        if position < -100.0 or position > 100.0:
            return False, (
                f"Position {position} is out of range for {motor_name} (motor {motor_id}). "
                f"Expected range: [-100, 100]"
            )
    elif norm_mode == MotorNormMode.RANGE_0_100:
        if position < 0.0 or position > 100.0:
            return False, (
                f"Position {position} is out of range for {motor_name} (motor {motor_id}). "
                f"Expected range: [0, 100]"
            )
    elif norm_mode == MotorNormMode.DEGREES:
        # No fixed range for degrees mode
        pass

    return True, ""


def write_position(
    device_type: str,
    port: str,
    motor_id: int,
    position: float,
    max_relative_target: Optional[float] = None,
    wait_for_position: bool = False,
    timeout: float = 5.0,
    tolerance: float = 2.0,
    device_id: Optional[str] = None,
    calibration_dir: Optional[Path] = None,
) -> None:
    """
    Write normalized position to a motor.

    Args:
        device_type: "leader" or "follower"
        port: Serial port path
        motor_id: Motor ID (1-6)
        position: Normalized position value ([-100, 100] or [0, 100] depending on motor)
        max_relative_target: Maximum change per update (in normalized units). Controls movement speed:
            - Smaller values = slower movement (e.g., 5.0)
            - Larger values = faster movement (e.g., 50.0)
            - None = no limit (move as fast as possible in one update)
        wait_for_position: If True, wait until motor reaches target position
        timeout: Maximum time to wait for position (seconds)
        tolerance: Position tolerance for considering target reached (normalized units)
        device_id: Device identifier for calibration file (default: "leader1" or "follower1")
        calibration_dir: Custom calibration directory (default: ~/.cyberwave/so101_lib/calibrations)
    """
    # Validate motor ID and position
    is_valid, error_msg = validate_position(motor_id, position)
    if not is_valid:
        logger.error(error_msg)
        sys.exit(1)

    motor_name, norm_mode = MOTOR_CONFIG[motor_id]

    # Set default device ID if not provided
    if device_id is None:
        device_id = f"{device_type}1"

    logger.info(
        f"Writing position {position} to {motor_name} (motor {motor_id}) on {device_type} '{device_id}' at {port}"
    )
    if calibration_dir:
        logger.info(f"Using calibration directory: {calibration_dir}")

    # Initialize device with config (calibration will be loaded automatically)
    if device_type == "follower":
        config = FollowerConfig(
            port=port,
            max_relative_target=max_relative_target,
            id=device_id,
            calibration_dir=calibration_dir,
        )
        device = SO101Follower(config=config)
    elif device_type == "leader":
        config = LeaderConfig(
            port=port,
            id=device_id,
            calibration_dir=calibration_dir,
        )
        device = SO101Leader(config=config)
    else:
        logger.error(f"Invalid device type: {device_type}. Must be 'leader' or 'follower'")
        sys.exit(1)

    # Verify calibration is loaded (required for normalization)
    if not device.is_calibrated:
        logger.warning(
            f"Device '{device_id}' is not calibrated. "
            f"Calibration file expected at: {config.calibration_dir / f'{device_id}.json'}. "
            f"Normalization may not work correctly. "
            f"Run calibration first: so101-calibrate --type {device_type} --port {port} --id {device_id}"
        )
    else:
        logger.info(f"Calibration loaded for device '{device_id}'")

    try:
        # Connect to device
        logger.info(f"Connecting to {device_type}...")
        device.connect(calibrate=False)

        # Enable torque if needed
        if not device.torque_enabled:
            logger.info("Enabling torque...")
            device.enable_torque()

        # Get current position
        current_obs = device.get_observation()
        # Handle both .pos suffix format and direct name format
        current_key = f"{motor_name}.pos"
        if current_key in current_obs:
            current_pos = current_obs[current_key]
        elif motor_name in current_obs:
            current_pos = current_obs[motor_name]
        else:
            logger.warning(f"Could not find current position for {motor_name}, assuming 0.0")
            current_pos = 0.0

        logger.info(f"Current position: {current_pos:.2f} (normalized)")

        # Send position based on device type
        target_pos = position  # Default target position
        if device_type == "follower":
            # Follower: use send_action which handles safety limits
            # Prepare action (format: {motor_name.pos: position})
            action = {f"{motor_name}.pos": position}

            # Send action
            logger.info(f"Sending position {position} to {motor_name}...")
            sent_action = device.send_action(action)

            # Log what was actually sent (may differ due to safety limits)
            sent_key = f"{motor_name}.pos"
            if sent_key in sent_action:
                actual_sent = sent_action[sent_key]
                target_pos = actual_sent  # Use actual sent position for waiting
                if abs(actual_sent - position) > 0.01:
                    logger.warning(
                        f"Position was limited by safety: requested {position:.2f}, "
                        f"sent {actual_sent:.2f} (difference: {abs(actual_sent - position):.2f})"
                    )
                else:
                    logger.info(f"Position sent successfully: {actual_sent:.2f}")
        else:
            # Leader: write directly to bus (no safety limits, no send_action method)
            logger.warning(
                "Leader device: writing directly to bus (no safety limits applied). "
                "Consider using 'follower' type for safer operation."
            )
            logger.info(f"Sending position {position} to {motor_name}...")
            # Write normalized position directly to bus
            device.bus.sync_write("Goal_Position", {motor_name: position}, normalize=True)
            logger.info(f"Position sent successfully: {position:.2f}")

        # Wait for position if requested
        if wait_for_position:
            logger.info(
                f"Waiting for motor to reach position {target_pos:.2f} (tolerance: {tolerance:.2f})..."
            )
            start_time = time.time()
            while time.time() - start_time < timeout:
                current_obs = device.get_observation()
                if current_key in current_obs:
                    current_pos = current_obs[current_key]
                elif motor_name in current_obs:
                    current_pos = current_obs[motor_name]
                else:
                    logger.warning("Could not read current position")
                    break

                error = abs(current_pos - target_pos)
                if error <= tolerance:
                    logger.info(
                        f"Motor reached target position! Current: {current_pos:.2f}, "
                        f"Target: {target_pos:.2f}, Error: {error:.2f}"
                    )
                    return

                time.sleep(0.1)

            logger.warning(
                f"Timeout waiting for position. Current: {current_pos:.2f}, "
                f"Target: {target_pos:.2f}, Error: {abs(current_pos - target_pos):.2f}"
            )
        else:
            # Just read final position once
            time.sleep(0.5)  # Give motor time to start moving
            current_obs = device.get_observation()
            if current_key in current_obs:
                current_pos = current_obs[current_key]
            elif motor_name in current_obs:
                current_pos = current_obs[motor_name]
            else:
                current_pos = None

            if current_pos is not None:
                logger.info(f"Current position after move: {current_pos:.2f} (normalized)")

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error writing position: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Ensure torque is enabled before disconnecting (keep motors in position)
        if device.connected:
            if not device.torque_enabled:
                logger.info("Enabling torque before disconnect...")
                device.enable_torque()
            else:
                logger.info("Torque is enabled, keeping it enabled on disconnect")

        # Disconnect: for follower, pass torque=True to keep torque enabled
        # For leader, disconnect() doesn't disable torque anyway
        if device_type == "follower":
            device.disconnect(torque=True)
        else:
            device.disconnect()
        logger.info("Disconnected from device (torque remains enabled)")


def main():
    """Main entry point for write_position script."""
    parser = argparse.ArgumentParser(
        description="Write normalized position values to SO101 motors",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Move motor 1 (shoulder_pan) to position 50 on follower
  so101-write-position --type follower --port /dev/ttyACM0 --motor 1 --position 50

  # Move motor 6 (gripper) to position 80 on follower slowly (smaller steps)
  so101-write-position --type follower --port /dev/ttyACM0 --motor 6 --position 80 --max-relative-target 5.0

  # Move motor 1 to position 50 quickly (larger steps)
  so101-write-position --type follower --port /dev/ttyACM0 --motor 1 --position 50 --max-relative-target 50.0

  # Move motor 2 to -50 and wait until it reaches the position
  so101-write-position --type follower --port /dev/ttyACM0 --motor 2 --position -50 --wait

  # Use specific device ID to load correct calibration file
  so101-write-position --type follower --port /dev/ttyACM0 --id follower2 --motor 1 --position 50

  # Use custom calibration directory
  so101-write-position --type follower --port /dev/ttyACM0 --id my_follower \\
      --calibration-dir ~/my_calibrations --motor 1 --position 50

Motor IDs:
  1: shoulder_pan   (range: -100 to 100)
  2: shoulder_lift  (range: -100 to 100)
  3: elbow_flex     (range: -100 to 100)
  4: wrist_flex     (range: -100 to 100)
  5: wrist_roll     (range: -100 to 100)
  6: gripper        (range: 0 to 100)
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
        required=True,
        help="Serial port path (e.g., /dev/ttyACM0)",
    )
    parser.add_argument(
        "--id",
        type=str,
        default=None,
        help="Device identifier for calibration file (default: 'leader1' or 'follower1'). "
        "This determines which calibration file to load from the calibration directory.",
    )
    parser.add_argument(
        "--calibration-dir",
        type=str,
        default=None,
        help="Custom calibration directory (default: ~/.cyberwave/so101_lib/calibrations). "
        "Calibration file is expected at: {calibration_dir}/{id}.json",
    )
    parser.add_argument(
        "--motor",
        type=int,
        required=True,
        choices=[1, 2, 3, 4, 5, 6],
        help="Motor ID (1-6). 1-5: body joints (range -100 to 100), 6: gripper (range 0 to 100)",
    )
    parser.add_argument(
        "--position",
        type=float,
        required=True,
        help="Normalized position value. For motors 1-5: -100 to 100, for motor 6: 0 to 100",
    )
    parser.add_argument(
        "--max-relative-target",
        type=float,
        default=None,
        help="Maximum change per update in normalized units. Controls movement speed: "
        "smaller values = slower movement (e.g., 5.0), larger values = faster movement (e.g., 50.0). "
        "None = no limit (move as fast as possible). Only applies to follower.",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait until motor reaches target position before exiting",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Maximum time to wait for position (seconds, default: 5.0)",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=2.0,
        help="Position tolerance for considering target reached in normalized units (default: 2.0)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Convert calibration_dir string to Path if provided
    calibration_dir = None
    if args.calibration_dir:
        calibration_dir = Path(args.calibration_dir).expanduser()

    # Write position
    write_position(
        device_type=args.type,
        port=args.port,
        motor_id=args.motor,
        position=args.position,
        max_relative_target=args.max_relative_target,
        wait_for_position=args.wait,
        timeout=args.timeout,
        tolerance=args.tolerance,
        device_id=args.id,
        calibration_dir=calibration_dir,
    )


if __name__ == "__main__":
    main()
