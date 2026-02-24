"""Utility functions for so101_lib."""

import json
import logging
import math
import platform
import time
from pathlib import Path
from pprint import pformat
from typing import Any, Dict, List, Optional, Tuple

from cyberwave import Cyberwave as cw
from cyberwave import EdgeController
from cyberwave.sensor import Resolution

from motors import MotorNormMode

logger = logging.getLogger(__name__)


def setup_logging(level: int = logging.INFO) -> None:
    """Set up basic logging configuration."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def radians_to_normalized(
    radians: float,
    norm_mode: MotorNormMode,
    calib: Optional[Any] = None,
) -> float:
    """
    Convert radians to normalized position based on motor normalization mode and calibration.

    This is the inverse of teleoperate's normalized->radians conversion.
    Uses calibration data when available for accurate conversion.

    Args:
        radians: Position in radians
        norm_mode: Motor normalization mode
        calib: Calibration data with range_min and range_max (optional)

    Returns:
        Normalized position value
    """
    if calib is None:
        raise ValueError("Calibration data is required")
    r_min = calib.range_min
    r_max = calib.range_max
    delta_r = (r_max - r_min) / 2.0

    if norm_mode == MotorNormMode.RANGE_M100_100:
        raw_offset = radians / (2.0 * math.pi / 4095.0)
        normalized = (raw_offset / delta_r) * 100.0
        return normalized
    elif norm_mode == MotorNormMode.RANGE_0_100:
        delta_r_full = r_max - r_min
        raw_value = radians / (2.0 * math.pi / 4095.0) + r_min
        normalized = ((raw_value - r_min) / delta_r_full) * 100.0
        return normalized
    else:  # DEGREES
        return radians * 180.0 / math.pi

def parse_resolution_to_enum(resolution_str: str) -> Resolution:
    """Parse resolution string to Resolution enum (cyberwave.sensor.Resolution)."""
    resolution_str = resolution_str.upper().strip()
    resolution_map = {
        "QVGA": Resolution.QVGA,
        "VGA": Resolution.VGA,
        "SVGA": Resolution.SVGA,
        "HD": Resolution.HD,
        "720P": Resolution.HD,
        "FULL_HD": Resolution.FULL_HD,
        "1080P": Resolution.FULL_HD,
    }
    if resolution_str in resolution_map:
        return resolution_map[resolution_str]
    if "x" in resolution_str.lower():
        try:
            width, height = resolution_str.lower().split("x")
            width, height = int(width), int(height)
            match = Resolution.from_size(width, height)
            if match:
                return match
            return Resolution.closest(width, height)
        except ValueError:
            pass
    raise ValueError(
        f"Invalid resolution: {resolution_str}. "
        "Use QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT format."
    )


def load_calibration(calibration_path: Path) -> Dict[str, Any]:
    """Load motor calibration from JSON file."""
    if not calibration_path.exists():
        raise FileNotFoundError(f"Calibration file not found: {calibration_path}")

    with open(calibration_path, "r") as f:
        calib_data: Dict[str, Any] = json.load(f)
        return calib_data


def save_calibration(calibration: Dict[str, Any], calibration_path: Path) -> None:
    """Save motor calibration to JSON file."""
    calibration_path.parent.mkdir(parents=True, exist_ok=True)

    with open(calibration_path, "w") as f:
        json.dump(calibration, f, indent=2)


def validate_calibration_ranges(
    range_mins: Dict[str, float],
    range_maxes: Dict[str, float],
    motors: Dict[str, Any],
) -> List[Tuple[str, float, float, bool, bool, bool]]:
    """
    Validate recorded calibration ranges and return list of invalid joints.

    Invalid ranges:
    - Full range (0-4095 or very close): Not physically possible due to physical constraints
    - Min equals max (0-0 or 4095-4095): Invalid, no motion recorded

    Args:
        range_mins: Dictionary mapping motor names to minimum recorded positions
        range_maxes: Dictionary mapping motor names to maximum recorded positions
        motors: Dictionary mapping motor names to Motor objects (must have .id attribute)

    Returns:
        List of tuples: (motor_name, min_val, max_val, is_full_range, is_zero_range, is_suspiciously_large)
    """
    invalid_joints = []
    FULL_RANGE_MIN = 0
    FULL_RANGE_MAX = 4095
    # Threshold for "suspiciously close to full range" (within 10 units)
    FULL_RANGE_THRESHOLD = 10

    for motor_name in motors.keys():
        min_val = range_mins.get(motor_name, float("inf"))
        max_val = range_maxes.get(motor_name, float("-inf"))

        # Skip if values are still inf (no data recorded)
        if min_val == float("inf") or max_val == float("-inf"):
            continue

        # Check for invalid ranges
        # Full range: min is very close to 0 AND max is very close to 4095
        is_full_range = (
            min_val <= FULL_RANGE_MIN + FULL_RANGE_THRESHOLD
            and max_val >= FULL_RANGE_MAX - FULL_RANGE_THRESHOLD
        )
        # Zero range: min equals max
        is_zero_range = min_val == max_val

        # Check if range span is suspiciously large (close to full 4095 range)
        range_span = max_val - min_val
        is_suspiciously_large = range_span >= FULL_RANGE_MAX - FULL_RANGE_THRESHOLD

        if is_full_range or is_zero_range or is_suspiciously_large:
            invalid_joints.append(
                (motor_name, min_val, max_val, is_full_range, is_zero_range, is_suspiciously_large)
            )

    return invalid_joints


def format_calibration_warnings(
    invalid_joints: List[Tuple[str, float, float, bool, bool, bool]],
    motors: Dict[str, Any],
    include_action_required: bool = True,
) -> str:
    """
    Format invalid joint warnings as a string for display.

    Args:
        invalid_joints: List of tuples from validate_calibration_ranges
        motors: Dictionary mapping motor names to Motor objects (must have .id attribute)
        include_action_required: Whether to include action required instructions

    Returns:
        Formatted warning string (empty if no invalid joints)
    """
    if not invalid_joints:
        return ""

    warnings_text = "\n\033[91m" + "=" * 90 + "\033[0m\n"
    warnings_text += "\033[91m" + "⚠️  WARNING: Invalid joint ranges detected!" + "\033[0m\n"
    warnings_text += "\033[91m" + "=" * 90 + "\033[0m"

    for (
        motor_name,
        min_val,
        max_val,
        is_full_range,
        is_zero_range,
        is_suspiciously_large,
    ) in invalid_joints:
        motor_id = motors[motor_name].id
        range_span = max_val - min_val
        warnings_text += f"\n\033[91m{motor_name:<20} (ID: {motor_id:<3}) - Range: [{min_val:.1f}, {max_val:.1f}] (span: {range_span:.1f})\033[0m\n"
        if is_full_range:
            warnings_text += "\033[91m  ❌ Full range (0-4095 or very close) detected - not physically possible\033[0m\n"
        elif is_suspiciously_large and not is_zero_range:
            warnings_text += f"\033[91m  ❌ Range span ({range_span:.1f}) is suspiciously large - not physically possible\033[0m\n"
        if is_zero_range:
            warnings_text += "\033[91m  ❌ Zero range detected - no motion recorded\033[0m\n"

        if include_action_required:
            warnings_text += "\033[91m  → Action required:\033[0m\n"
            warnings_text += "\033[91m     1. Exit calibration (Ctrl+C)\033[0m\n"
            warnings_text += "\033[91m     2. Unplug power and USB cables from the robot\033[0m\n"
            warnings_text += "\033[91m     3. Wait 5 seconds\033[0m\n"
            warnings_text += "\033[91m     4. Reconnect and retry calibration\033[0m\n"

    warnings_text += "\033[91m" + "=" * 90 + "\033[0m\n"
    return warnings_text


def validate_and_display_calibration_ranges(
    range_mins: Dict[str, float],
    range_maxes: Dict[str, float],
    motors: Dict[str, Any],
) -> None:
    """
    Validate recorded calibration ranges and display red alerts for invalid joints.

    Convenience function that combines validate_calibration_ranges and format_calibration_warnings.

    Args:
        range_mins: Dictionary mapping motor names to minimum recorded positions
        range_maxes: Dictionary mapping motor names to maximum recorded positions
        motors: Dictionary mapping motor names to Motor objects (must have .id attribute)
    """
    invalid_joints = validate_calibration_ranges(range_mins, range_maxes, motors)

    if invalid_joints:
        import sys

        sys.stdout.flush()
        warnings_text = format_calibration_warnings(
            invalid_joints, motors, include_action_required=True
        )
        print(warnings_text)
        sys.stdout.flush()


def ensure_safe_goal_position(
    goal_present_pos: Dict[str, Tuple[float, float]],
    max_relative_target: float | Dict[str, float],
) -> Dict[str, float]:
    """
    Caps relative action target magnitude for safety.

    Args:
        goal_present_pos: Dictionary mapping joint names to tuples of (goal_pos, present_pos)
        max_relative_target: Maximum allowed change per update. Can be:
            - float: Same limit for all joints
            - Dict[str, float]: Per-joint limits (keys must match goal_present_pos)

    Returns:
        Dictionary mapping joint names to safe goal positions

    Raises:
        ValueError: If max_relative_target is a dict and keys don't match goal_present_pos
        TypeError: If max_relative_target is neither float nor dict
    """
    # Handle max_relative_target as float or dict
    if isinstance(max_relative_target, float):
        diff_cap = dict.fromkeys(goal_present_pos, max_relative_target)
    elif isinstance(max_relative_target, dict):
        if not set(goal_present_pos) == set(max_relative_target):
            raise ValueError("max_relative_target keys must match those of goal_present_pos.")
        diff_cap = max_relative_target
    else:
        raise TypeError(
            f"max_relative_target must be float or dict, got {type(max_relative_target)}"
        )

    warnings_dict = {}
    safe_goal_positions = {}

    for key, (goal_pos, present_pos) in goal_present_pos.items():
        diff = goal_pos - present_pos
        max_diff = diff_cap[key]

        # Clamp diff to [-max_diff, max_diff]
        safe_diff = min(diff, max_diff)
        safe_diff = max(safe_diff, -max_diff)

        safe_goal_pos = present_pos + safe_diff
        safe_goal_positions[key] = safe_goal_pos

        # Log warning if clamping occurred
        if abs(safe_goal_pos - goal_pos) > 1e-4:
            warnings_dict[key] = {
                "original goal_pos": goal_pos,
                "safe goal_pos": safe_goal_pos,
            }

    if warnings_dict:
        logger.warning(
            "Relative goal position magnitude had to be clamped to be safe.\n"
            f"{pformat(warnings_dict, indent=4)}"
        )

    return safe_goal_positions


def find_available_ports() -> List[str]:
    """
    Find all available serial ports on the system.

    Returns:
        List of available port paths (e.g., ["/dev/tty.usbmodem123", ...])
    """
    try:
        from serial.tools import list_ports  # Part of pyserial library
    except ImportError as err:
        raise ImportError(
            "pyserial is required for port detection. Install it with: pip install pyserial"
        ) from err

    if platform.system() == "Windows":
        # List COM ports using pyserial
        ports = [port.device for port in list_ports.comports()]
    else:  # Linux/macOS
        # List /dev/tty* ports for Unix-based systems
        ports = [str(path) for path in Path("/dev").glob("tty*")]
    return sorted(ports)


def is_port_available(port: str) -> bool:
    """
    Check if a specific port exists and is available.

    Args:
        port: Port path to check (e.g., "/dev/tty.usbmodem123")

    Returns:
        True if port exists, False otherwise
    """
    available_ports = find_available_ports()
    return port in available_ports


def test_device_connection(
    port: str,
    baudrate: int = 1000000,
    motor_ids: Optional[List[int]] = None,
    timeout: float = 2.0,
) -> bool:
    """
    Test if a device is connected and responsive on the given port.

    Attempts to open the port and read from motors to verify communication.

    Args:
        port: Serial port path (e.g., "/dev/tty.usbmodem123")
        baudrate: Serial communication baudrate (default: 1000000)
        motor_ids: List of motor IDs to test. If None, tests with ID 1.
        timeout: Timeout in seconds for the test

    Returns:
        True if device is connected and responsive, False otherwise
    """
    if not is_port_available(port):
        return False

    try:
        from scservo_sdk import PacketHandler, PortHandler
    except ImportError:
        logger.warning("scservo_sdk not available, cannot test device connection")
        return False

    port_handler = None
    try:
        # Initialize port handler
        port_handler = PortHandler(port)
        port_handler.setBaudRate(baudrate)

        # Open port
        if not port_handler.openPort():
            return False

        # Initialize packet handler
        packet_handler = PacketHandler(1.0)  # Protocol version 1.0

        # Test with motor IDs (default to ID 1 if not specified)
        test_ids = motor_ids if motor_ids else [1]

        # Try to read from at least one motor
        for motor_id in test_ids:
            try:
                # Try reading model number (address 0-1) as a simple test
                model_l, result, error = packet_handler.read1ByteTxRx(port_handler, motor_id, 0)
                if result == 0:  # Success
                    return True
            except Exception:
                continue

        return False

    except Exception as e:
        logger.debug(f"Error testing device connection: {e}")
        return False
    finally:
        if port_handler:
            try:
                port_handler.closePort()
            except Exception:
                pass


def find_port() -> str:
    """
    Interactive function to find the port of a MotorsBus device.

    Prompts user to disconnect and reconnect the device to identify the port.

    Returns:
        The port path of the device

    Raises:
        OSError: If the port cannot be determined
    """
    logger.info("Finding all available ports for the MotorsBus.")
    ports_before = find_available_ports()
    logger.info(f"Ports before disconnecting: {ports_before}")

    print("Remove the USB cable from your MotorsBus and press Enter when done.")
    input()  # Wait for user to disconnect the device

    time.sleep(0.5)  # Allow some time for port to be released
    ports_after = find_available_ports()
    ports_diff = list(set(ports_before) - set(ports_after))

    if len(ports_diff) == 1:
        port = ports_diff[0]
        logger.info(f"The port of this MotorsBus is '{port}'")
        print(f"The port of this MotorsBus is '{port}'")
        print("Reconnect the USB cable.")
        return port
    elif len(ports_diff) == 0:
        raise OSError(f"Could not detect the port. No difference was found ({ports_diff}).")
    else:
        raise OSError(f"Could not detect the port. More than one port was found ({ports_diff}).")


def detect_voltage_rating(port: str, motor_id: int = 1, baudrate: int = 1000000) -> Optional[int]:
    """
    Attempt to detect the voltage rating (5V or 12V) from the device.

    This function tries to infer the voltage rating from:
    1. Voltage limit registers (MAX_LIMIT_VOLTAGE, MIN_LIMIT_VOLTAGE)
    2. Present voltage reading (if device is powered)
    3. Model number (if it encodes voltage information)

    Args:
        port: Serial port path
        motor_id: Motor ID to read from (default: 1)
        baudrate: Serial communication baudrate

    Returns:
        Voltage rating (5 or 12) if detected, None if cannot be determined
    """
    try:
        from scservo_sdk import PacketHandler, PortHandler
    except ImportError:
        logger.warning("scservo_sdk not available, cannot detect voltage rating")
        return None

    from motors.tables import (
        ADDR_MAX_VOLTAGE_LIMIT,
        ADDR_MIN_VOLTAGE_LIMIT,
        ADDR_PRESENT_VOLTAGE,
    )

    port_handler = None
    try:
        port_handler = PortHandler(port)
        port_handler.setBaudRate(baudrate)

        if not port_handler.openPort():
            return None

        packet_handler = PacketHandler(1.0)

        # Try reading voltage limits
        try:
            max_voltage, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_MAX_VOLTAGE_LIMIT[0]
            )
            min_voltage, result2, error2 = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_MIN_VOLTAGE_LIMIT[0]
            )

            if result == 0 and result2 == 0:
                # Voltage limits are in 0.1V units
                max_v = max_voltage / 10.0

                # 5V servos typically have limits around 4.5-6.5V
                # 12V servos typically have limits around 10-14V
                if max_v < 8.0:
                    return 5
                elif max_v > 8.0:
                    return 12
        except Exception:
            pass

        # Try reading present voltage as a hint
        try:
            present_voltage, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_VOLTAGE[0]
            )
            if result == 0:
                voltage = present_voltage / 10.0
                # If voltage is between 4-78, likely 5V system
                # If voltage is between 10-14V, likely 12V system
                if 4.0 <= voltage <= 8.0:
                    return 5
                elif 10.0 <= voltage <= 14.0:
                    return 12
        except Exception:
            pass

        return None

    except Exception as e:
        logger.debug(f"Error detecting voltage rating: {e}")
        return None
    finally:
        if port_handler:
            try:
                port_handler.closePort()
            except Exception:
                pass


def get_cyberwave_controller(token: str, twin_uuid: str) -> EdgeController:
    client = cw(token=token)
    controller = client.controller(twin_uuid=twin_uuid)
    controller.start()
    return controller


def convert_position_with_calibration(
    raw_position: float,
    joint_name: str,
    calibration_data: Dict,
    norm_mode: MotorNormMode,
    use_radians: bool = False,
    drive_mode: int = 0,
) -> float:
    """
    Convert raw position using calibration data and normalization mode.

    Args:
        raw_position: Raw position value (0-4095)
        joint_name: Name of the joint
        calibration_data: Calibration data dictionary (required, contains raw values)
        norm_mode: Motor normalization mode (RANGE_M100_100, RANGE_0_100, or DEGREES)
        use_radians: If True and norm_mode is DEGREES, return radians; if False, return degrees
        drive_mode: Drive mode (0 or 1) - affects sign for RANGE modes

    Returns:
        Converted position based on normalization mode:
        - RANGE_M100_100: -100 to 100 (may be inverted if drive_mode=1)
        - RANGE_0_100: 0 to 100 (may be inverted if drive_mode=1)
        - DEGREES: degrees relative to midpoint (can be negative), or radians if use_radians=True

    Raises:
        KeyError: If joint_name is not found in calibration_data
        ValueError: If range_min == range_max (invalid calibration)
    """
    if joint_name not in calibration_data:
        raise KeyError(
            f"Joint '{joint_name}' not found in calibration data. "
            f"Available joints: {list(calibration_data.keys())}"
        )

    calib = calibration_data[joint_name]

    range_min_raw = calib["range_min"]
    range_max_raw = calib["range_max"]

    # Check for invalid calibration
    if range_max_raw == range_min_raw:
        raise ValueError(f"Invalid calibration for joint '{joint_name}': min and max are equal.")

    # Clamp raw position to [min_, max_] FIRST
    bounded_val = min(range_max_raw, max(range_min_raw, raw_position))
    # logger.info(f"Joint {joint_name}: Bounded value: {bounded_val}, range_min_raw: {range_min_raw}, range_max_raw: {range_max_raw}")
    # Convert based on normalization mode
    match norm_mode:
        case MotorNormMode.RANGE_M100_100:
            # Formula: (((bounded_val - min_) / (max_ - min_)) * 200) - 100
            norm = (
                ((bounded_val - range_min_raw) / (range_max_raw - range_min_raw)) * 200.0
            ) - 100.0
            # Apply drive_mode inversion (if drive_mode=1, invert sign)
            position = -norm if drive_mode else norm
        case MotorNormMode.RANGE_0_100:
            # Formula: ((bounded_val - min_) / (max_ - min_)) * 100
            norm = ((bounded_val - range_min_raw) / (range_max_raw - range_min_raw)) * 100.0
            # Apply drive_mode inversion (if drive_mode=1, use 100 - norm)
            position = 100.0 - norm if drive_mode else norm
        case MotorNormMode.DEGREES:
            # Formula: degrees = (raw_encoder_value - mid) * 360 / max_res
            # where:
            #   mid = (range_min + range_max) / 2 (center of calibrated range)
            #   max_res = 4095 (12-bit encoder resolution for STS3215)
            mid = (range_min_raw + range_max_raw) / 2.0
            max_res = 4095.0  # 12-bit encoder resolution
            position_degrees = (bounded_val - mid) * 360.0 / max_res
            # Convert to radians if requested
            if use_radians:
                position = position_degrees * math.pi / 180.0
            else:
                position = position_degrees
    return position


def denormalize_position(
    normalized_position: float,
    joint_name: str,
    calibration_data: Dict,
    norm_mode: MotorNormMode,
    drive_mode: int = 0,
) -> float:
    """
    Convert normalized position back to raw encoder value using calibration data.

    This is the inverse of convert_position_with_calibration.

    Args:
        normalized_position: Normalized position value (e.g., -100 to 100 for RANGE_M100_100)
        joint_name: Name of the joint
        calibration_data: Calibration data dictionary
        norm_mode: Motor normalization mode
        drive_mode: Drive mode (0 or 1) - affects sign for RANGE modes

    Returns:
        Raw encoder value clamped to the calibrated range (not 0-4095, but range_min to range_max)
    """
    if joint_name not in calibration_data:
        raise KeyError(f"Joint '{joint_name}' not found in calibration data")

    calib = calibration_data[joint_name]

    range_min_raw = calib["range_min"]
    range_max_raw = calib["range_max"]

    # Clamp normalized position to valid range FIRST to prevent wrapping
    # This is critical when leader reaches limits and normalized values might slightly exceed bounds
    match norm_mode:
        case MotorNormMode.RANGE_M100_100:
            # Clamp to [-100, 100] BEFORE any processing
            original_norm = normalized_position
            normalized_position = max(-100.0, min(100.0, normalized_position))
            if abs(original_norm - normalized_position) > 1e-6:
                logger.debug(
                    f"{joint_name}: Clamped normalized position from {original_norm:.6f} to {normalized_position:.6f}"
                )
            # Inverse of: norm = (((bounded_val - min_) / (max_ - min_)) * 200) - 100
            # With drive_mode: position = -norm if drive_mode else norm
            # So: norm = -position if drive_mode else position
            norm = -normalized_position if drive_mode else normalized_position
            # Clamp norm to [-100, 100] after drive_mode inversion (safety)
            norm = max(-100.0, min(100.0, norm))
            # norm = (((raw - min_) / (max_ - min_)) * 200) - 100
            # (norm + 100) / 200 = (raw - min_) / (max_ - min_)
            # raw = min_ + (norm + 100) / 200 * (max_ - min_)
            raw_position = range_min_raw + ((norm + 100.0) / 200.0) * (
                range_max_raw - range_min_raw
            )
            # Additional safety: clamp to calibrated range immediately (before any other processing)
            raw_position = max(range_min_raw, min(range_max_raw, raw_position))
        case MotorNormMode.RANGE_0_100:
            # Clamp to [0, 100] BEFORE any processing
            original_norm = normalized_position
            normalized_position = max(0.0, min(100.0, normalized_position))
            if abs(original_norm - normalized_position) > 1e-6:
                logger.debug(
                    f"{joint_name}: Clamped normalized position from {original_norm:.6f} to {normalized_position:.6f}"
                )
            # Inverse of: norm = ((bounded_val - min_) / (max_ - min_)) * 100
            # With drive_mode: position = 100 - norm if drive_mode else norm
            # So: norm = 100 - position if drive_mode else position
            norm = 100.0 - normalized_position if drive_mode else normalized_position
            # Clamp norm to [0, 100] after drive_mode inversion (safety)
            norm = max(0.0, min(100.0, norm))
            # norm = ((raw - min_) / (max_ - min_)) * 100
            # raw = min_ + (norm / 100) * (max_ - min_)
            raw_position = range_min_raw + (norm / 100.0) * (range_max_raw - range_min_raw)
            # Additional safety: clamp to calibrated range immediately (before any other processing)
            raw_position = max(range_min_raw, min(range_max_raw, raw_position))
        case MotorNormMode.DEGREES:
            # For degrees, we don't have a fixed range, but we should still clamp to reasonable values
            # Inverse of: degrees = (raw - mid) * 360 / max_res
            # where mid = (range_min + range_max) / 2
            # raw = mid + (degrees * max_res) / 360
            mid = (range_min_raw + range_max_raw) / 2.0
            max_res = 4095.0
            raw_position = mid + (normalized_position * max_res) / 360.0
            # Clamp to calibrated range immediately
            raw_position = max(range_min_raw, min(range_max_raw, raw_position))

    # Final safety clamp: ensure within physical encoder range (0-4095)
    # This is the LAST line of defense against wrapping
    # However, we should prefer staying within the calibrated range if possible
    # Only clamp to 0-4095 if the calibrated range itself extends beyond these bounds
    if raw_position < 0.0 or raw_position > 4095.0:
        logger.warning(
            f"{joint_name}: Raw position {raw_position:.1f} outside physical encoder range [0, 4095]. "
            f"Calibrated range: [{range_min_raw:.1f}, {range_max_raw:.1f}]. "
            f"Clamping to physical limits."
        )
        raw_position = max(0.0, min(4095.0, raw_position))
    elif raw_position < range_min_raw or raw_position > range_max_raw:
        # This should not happen as we clamp to calibrated range above, but log if it does
        logger.warning(
            f"{joint_name}: Raw position {raw_position:.1f} outside calibrated range "
            f"[{range_min_raw:.1f}, {range_max_raw:.1f}]. This should not happen."
        )

    return raw_position
