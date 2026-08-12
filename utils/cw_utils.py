"""
Calibration and joint mapping utilities for SO101 edge scripts.

Bridges the twin's universal schema (the-robot-studio/so101) with SO101 motor/joint
definitions, and resolves calibration from either the local device or the twin API.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

from so101.robot import SO101_MOTORS

logger = logging.getLogger(__name__)

SO101_ASSET_KEY = "the-robot-studio/so101"


@dataclass
class CalibrationEntry:
    """Calibration data for a single joint (edge-friendly format)."""

    range_min: float
    range_max: float
    homing_offset: float = 0.0
    drive_mode: int = 0
    id: int = 0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CalibrationEntry":
        """Create from dict (handles both string and int for id/drive_mode)."""
        return cls(
            range_min=float(data["range_min"]),
            range_max=float(data["range_max"]),
            homing_offset=float(data.get("homing_offset", 0)),
            drive_mode=int(data.get("drive_mode", 0)),
            id=int(data.get("id", 0)),
        )


def _motor_id_to_joint_name(motor_id: int) -> Optional[str]:
    """Map motor ID to SO101 joint name."""
    for name, motor in SO101_MOTORS.items():
        if motor.id == motor_id:
            return name
    return None


def resolve_calibration_for_edge(
    twin: Any,
    device_calibration: Optional[Dict[str, Any]],
    robot_type: str,
) -> Optional[Dict[str, CalibrationEntry]]:
    """
    Resolve calibration for edge use, keyed by SO101 joint name.

    Prefers device calibration from the local JSON file written by cw_calibrate.py
    (~/.cyberwave/so101_lib/calibrations/{id}.json). This is the canonical source.

    When device_calibration is None (no calibration file), falls back to twin API

    Args:
        twin: Cyberwave Twin instance (robot twin)
        device_calibration: Local calibration from device (joint_name -> calib dict/object).
            Loaded by SO101Leader/SO101Follower from calibrations/{id}.json written by cw_calibrate.
        robot_type: "leader" or "follower"

    Returns:
        Dict mapping joint name (shoulder_pan, etc.) to CalibrationEntry, or None
    """
    if device_calibration is not None:
        result: Dict[str, CalibrationEntry] = {}
        for joint_name, calib in device_calibration.items():
            if joint_name not in SO101_MOTORS:
                continue
            if hasattr(calib, "range_min"):
                result[joint_name] = CalibrationEntry(
                    range_min=calib.range_min,
                    range_max=calib.range_max,
                    homing_offset=getattr(calib, "homing_offset", 0),
                    drive_mode=getattr(calib, "drive_mode", 0),
                    id=getattr(calib, "id", SO101_MOTORS[joint_name].id),
                )
            elif isinstance(calib, dict):
                result[joint_name] = CalibrationEntry.from_dict(calib)
            else:
                logger.warning(f"Unknown calibration format for {joint_name}")
        return result if result else None

    try:
        twin_calib = twin.get_calibration(robot_type=robot_type)
        if not twin_calib or not hasattr(twin_calib, "joint_calibration"):
            return None

        joint_calib = twin_calib.joint_calibration
        if not joint_calib:
            return None

        result = {}
        for key, calib in joint_calib.items():
            joint_name = None
            motor_id = None

            if key in SO101_MOTORS:
                joint_name = key
                motor_id = SO101_MOTORS[key].id
            elif isinstance(key, str) and key.isdigit():
                motor_id = int(key)
                joint_name = _motor_id_to_joint_name(motor_id)
            else:
                motor_id = int(getattr(calib, "id", 0))
                joint_name = _motor_id_to_joint_name(motor_id)

            if joint_name is None:
                continue

            if hasattr(calib, "range_min"):
                result[joint_name] = CalibrationEntry(
                    range_min=calib.range_min,
                    range_max=calib.range_max,
                    homing_offset=getattr(calib, "homing_offset", 0),
                    drive_mode=int(getattr(calib, "drive_mode", 0)),
                    id=motor_id or SO101_MOTORS[joint_name].id,
                )
            elif isinstance(calib, dict):
                result[joint_name] = CalibrationEntry.from_dict(calib)
        return result if result else None
    except Exception as e:
        logger.debug("Could not fetch calibration from twin: %s", e)
        return None


def build_motor_id_to_schema_joint(
    twin: Any, motors: Optional[Dict[str, Any]] = None
) -> Dict[int, str]:
    """
    Build mapping from motor ID to twin schema joint name (e.g. 1 -> "_1").

    Uses twin.get_controllable_joint_names() for the-robot-studio/so101 schema.
    """
    motors = motors or SO101_MOTORS
    try:
        schema_joint_names = twin.get_controllable_joint_names()
        mapping = {}
        for _, motor in motors.items():
            motor_id = motor.id
            idx = motor_id - 1
            if idx < len(schema_joint_names):
                mapping[motor_id] = schema_joint_names[idx]
            else:
                mapping[motor_id] = f"_{motor_id}"
        return mapping
    except Exception:
        return {motor.id: f"_{motor.id}" for _, motor in motors.items()}


def build_joint_mappings(
    twin: Any,
    motors: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Build all joint mappings needed for edge scripts (remoteoperate, etc.).

    Returns:
        Dict with:
        - joint_index_to_name: Dict[int, str] (motor_id -> joint_name)
        - joint_name_to_norm_mode: Dict[str, MotorNormMode]
        - motor_id_to_schema_joint: Dict[int, str] (motor_id -> "_1", etc.)
        - schema_joint_to_motor_id: Dict[str, int] ("_1" -> 1, etc.)
    """
    motors = motors or SO101_MOTORS
    joint_index_to_name = {motor.id: name for name, motor in motors.items()}
    joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in motors.items()}
    motor_id_to_schema_joint = build_motor_id_to_schema_joint(twin, motors)
    schema_joint_to_motor_id = {v: k for k, v in motor_id_to_schema_joint.items()}
    return {
        "joint_index_to_name": joint_index_to_name,
        "joint_name_to_norm_mode": joint_name_to_norm_mode,
        "motor_id_to_schema_joint": motor_id_to_schema_joint,
        "schema_joint_to_motor_id": schema_joint_to_motor_id,
    }
