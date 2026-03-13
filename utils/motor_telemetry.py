"""Helpers for publishing detailed SO101 motor telemetry."""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, Optional

from motors.models import MotorNormMode
from motors.registers import BOOLEAN_REGISTER_NAMES, VOLTAGE_REGISTER_NAMES
from utils.utils import convert_position_with_calibration, normalized_to_radians

logger = logging.getLogger(__name__)

DEFAULT_SOURCE_TYPE = "edge"
TELEMETRY_TYPE_MOTOR_STATUS = "motor_status"
STS3215_RADIANS_PER_STEP = (2.0 * 3.141592653589793) / 4095.0
STS3215_SPEED_UNIT_STEPS_PER_SECOND = 50.0
STS3215_SPEED_UNIT_RADIANS_PER_SECOND = (
    STS3215_RADIANS_PER_STEP * STS3215_SPEED_UNIT_STEPS_PER_SECOND
)


def _motor_telemetry_enabled() -> bool:
    return os.getenv("CYBERWAVE_MOTOR_TELEMETRY", "false").lower() in (
        "1",
        "true",
        "yes",
    )


ROBOT_STATUS_REGISTERS = [
    "Firmware_Major_Version",
    "Firmware_Minor_Version",
    "Model_Number",
    "ID",
    "Baud_Rate",
    "Return_Delay_Time",
    "Response_Status_Level",
    "Min_Position_Limit",
    "Max_Position_Limit",
    "Max_Temperature_Limit",
    "Max_Voltage_Limit",
    "Min_Voltage_Limit",
    "Max_Torque_Limit",
    "Phase",
    "Unloading_Condition",
    "LED_Alarm_Condition",
    "P_Coefficient",
    "D_Coefficient",
    "I_Coefficient",
    "Minimum_Startup_Force",
    "CW_Dead_Zone",
    "CCW_Dead_Zone",
    "Protection_Current",
    "Angular_Resolution",
    "Homing_Offset",
    "Operating_Mode",
    "Protective_Torque",
    "Protection_Time",
    "Overload_Torque",
    "Velocity_Closed_Loop_P",
    "Over_Current_Protection_Time",
    "Velocity_Closed_Loop_I",
    "Torque_Enable",
    "Acceleration",
    "Goal_Position",
    "Goal_Time",
    "Goal_Velocity",
    "Torque_Limit",
    "Lock",
    "Present_Position",
    "Present_Velocity",
    "Present_Load",
    "Present_Voltage",
    "Present_Temperature",
    "Status",
    "Moving",
    "Present_Current",
    "Goal_Position_2",
    "Moving_Velocity",
    "DTS",
    "Velocity_Unit_Factor",
    "HTS",
    "Maximum_Velocity_Limit",
    "Maximum_Acceleration",
    "Acceleration_Multiplier",
]


def _calibration_entry_to_dict(calibration_entry: Any, motor_id: int) -> Optional[Dict[str, Any]]:
    """Normalize calibration objects/dicts into the expected conversion shape."""
    if calibration_entry is None:
        return None
    if isinstance(calibration_entry, dict):
        range_min = calibration_entry.get("range_min")
        range_max = calibration_entry.get("range_max")
        if range_min is None or range_max is None:
            return None
        return {
            "id": int(calibration_entry.get("id", motor_id)),
            "drive_mode": int(calibration_entry.get("drive_mode", 0)),
            "range_min": float(range_min),
            "range_max": float(range_max),
        }

    range_min = getattr(calibration_entry, "range_min", None)
    range_max = getattr(calibration_entry, "range_max", None)
    if range_min is None or range_max is None:
        return None
    return {
        "id": int(getattr(calibration_entry, "id", motor_id)),
        "drive_mode": int(getattr(calibration_entry, "drive_mode", 0)),
        "range_min": float(range_min),
        "range_max": float(range_max),
    }


def _normalized_and_radians_from_raw(
    raw_position: int,
    *,
    joint_name: str,
    norm_mode: MotorNormMode,
    calibration_entry: Any,
    motor_id: int,
) -> tuple[Optional[float], Optional[float]]:
    """Convert a raw encoder position into normalized and radian values."""
    calib_dict = _calibration_entry_to_dict(calibration_entry, motor_id)
    if calib_dict is None:
        return None, None

    normalized = convert_position_with_calibration(
        raw_position=float(raw_position),
        joint_name=joint_name,
        calibration_data={joint_name: calib_dict},
        norm_mode=norm_mode,
        use_radians=False,
        drive_mode=int(calib_dict["drive_mode"]),
    )
    calib_like = type(
        "CalibrationLike",
        (),
        {
            "range_min": calib_dict["range_min"],
            "range_max": calib_dict["range_max"],
        },
    )()
    radians = normalized_to_radians(normalized, norm_mode, calib_like)
    return float(normalized), float(radians)


def _derive_register_values(
    *,
    joint_name: str,
    motor_id: int,
    norm_mode: MotorNormMode,
    registers: Dict[str, int],
    calibration_entry: Any,
) -> Dict[str, Any]:
    """Compute higher-level derived fields from the raw register dump."""
    derived: Dict[str, Any] = {}

    present_position = registers.get("Present_Position")
    if present_position is not None:
        normalized, radians = _normalized_and_radians_from_raw(
            int(present_position),
            joint_name=joint_name,
            norm_mode=norm_mode,
            calibration_entry=calibration_entry,
            motor_id=motor_id,
        )
        if normalized is not None:
            derived["present_position_normalized"] = normalized
        if radians is not None:
            derived["present_position_radians"] = radians

    goal_position = registers.get("Goal_Position")
    if goal_position is not None:
        normalized, radians = _normalized_and_radians_from_raw(
            int(goal_position),
            joint_name=joint_name,
            norm_mode=norm_mode,
            calibration_entry=calibration_entry,
            motor_id=motor_id,
        )
        if normalized is not None:
            derived["goal_position_normalized"] = normalized
        if radians is not None:
            derived["goal_position_radians"] = radians

    velocity = registers.get("Present_Velocity")
    if velocity is not None:
        derived["present_velocity_radians_per_second"] = float(
            velocity * STS3215_SPEED_UNIT_RADIANS_PER_SECOND
        )

    goal_velocity = registers.get("Goal_Velocity")
    if goal_velocity is not None:
        derived["goal_velocity_radians_per_second"] = float(
            goal_velocity * STS3215_SPEED_UNIT_RADIANS_PER_SECOND
        )

    for register_name in VOLTAGE_REGISTER_NAMES:
        if register_name in registers:
            derived[f"{register_name.lower()}_volts"] = float(registers[register_name]) / 10.0

    if "Present_Temperature" in registers:
        derived["present_temperature_celsius"] = float(registers["Present_Temperature"])
    if "Torque_Enable" in registers:
        derived["torque_enabled"] = bool(registers["Torque_Enable"])
    if "Moving" in registers:
        derived["moving"] = bool(registers["Moving"])
    if "Lock" in registers:
        derived["locked"] = bool(registers["Lock"])
    if "Present_Load" in registers:
        derived["effort_proxy"] = float(registers["Present_Load"])
    if "Present_Current" in registers:
        derived["present_current_raw"] = float(registers["Present_Current"])

    return derived


def build_device_motor_status_snapshot(
    device: Any,
    *,
    device_label: str,
    motor_id_to_schema_joint: Optional[Dict[int, str]] = None,
    registers: Optional[list[str]] = None,
    use_sequential: bool = False,
) -> Dict[str, Any]:
    """Build a complete register/status snapshot for one device."""
    if device is None:
        return {"connected": False, "motors": {}}

    if not getattr(device, "connected", False):
        return {
            "connected": False,
            "device": device_label,
            "torque_enabled": bool(getattr(device, "torque_enabled", False)),
            "motors": {},
        }

    register_names = registers or ROBOT_STATUS_REGISTERS
    register_snapshot = device.get_motor_register_snapshot(
        register_names,
        decode=True,
        use_sequential=use_sequential,
    )

    motors_payload: Dict[str, Any] = {}
    calibration = getattr(device, "calibration", None) or {}
    for joint_name, registers_by_name in register_snapshot.items():
        motor = device.motors[joint_name]
        coerced_registers: Dict[str, Any] = {}
        for register_name, value in registers_by_name.items():
            if register_name in BOOLEAN_REGISTER_NAMES:
                coerced_registers[register_name] = bool(value)
            else:
                coerced_registers[register_name] = value

        motors_payload[joint_name] = {
            "id": motor.id,
            "model": motor.model,
            "schema_joint": (
                motor_id_to_schema_joint.get(motor.id)
                if motor_id_to_schema_joint is not None
                else None
            ),
            "norm_mode": motor.norm_mode.value,
            "registers": coerced_registers,
            "derived": _derive_register_values(
                joint_name=joint_name,
                motor_id=motor.id,
                norm_mode=motor.norm_mode,
                registers=registers_by_name,
                calibration_entry=calibration.get(joint_name),
            ),
        }

    return {
        "device": device_label,
        "connected": True,
        "torque_enabled": bool(getattr(device, "torque_enabled", False)),
        "port": getattr(getattr(device, "config", None), "port", None),
        "motors": motors_payload,
    }


def build_robot_motor_status_payload(
    *,
    twin_uuid: str,
    leader: Any = None,
    follower: Any = None,
    motor_id_to_schema_joint: Optional[Dict[int, str]] = None,
    mode: str,
    runtime_status: Optional[Dict[str, Any]] = None,
    source_type: str = DEFAULT_SOURCE_TYPE,
) -> Dict[str, Any]:
    """Build the MQTT payload for a full robot motor-status snapshot."""
    runtime = runtime_status or {}
    return {
        "type": TELEMETRY_TYPE_MOTOR_STATUS,
        "timestamp": time.time(),
        "source_type": source_type,
        "twin_uuid": twin_uuid,
        "mode": mode,
        "runtime": {
            "mqtt_connected": runtime.get("mqtt_connected"),
            "camera_enabled": runtime.get("camera_enabled"),
            "messages_produced": runtime.get("messages_produced"),
            "messages_received": runtime.get("messages_received"),
            "messages_processed": runtime.get("messages_processed"),
            "messages_filtered": runtime.get("messages_filtered"),
            "errors_motor": runtime.get("errors_motor"),
            "errors_mqtt": runtime.get("errors_mqtt"),
            "camera_states": runtime.get("camera_states", {}),
        },
        "devices": {
            "leader": build_device_motor_status_snapshot(
                leader,
                device_label="leader",
                motor_id_to_schema_joint=motor_id_to_schema_joint,
            ),
            "follower": build_device_motor_status_snapshot(
                follower,
                device_label="follower",
                motor_id_to_schema_joint=motor_id_to_schema_joint,
            ),
        },
    }


def publish_robot_motor_status(
    mqtt_client: Any,
    *,
    twin_uuid: str,
    leader: Any = None,
    follower: Any = None,
    motor_id_to_schema_joint: Optional[Dict[int, str]] = None,
    mode: str,
    runtime_status: Optional[Dict[str, Any]] = None,
    source_type: str = DEFAULT_SOURCE_TYPE,
) -> bool:
    """Publish a full robot motor-status snapshot over MQTT."""
    if not _motor_telemetry_enabled():
        return False

    if mqtt_client is None or not getattr(mqtt_client, "connected", False):
        return False

    try:
        topic = f"{mqtt_client.topic_prefix}cyberwave/twin/{twin_uuid}/telemetry"
        payload = build_robot_motor_status_payload(
            twin_uuid=twin_uuid,
            leader=leader,
            follower=follower,
            motor_id_to_schema_joint=motor_id_to_schema_joint,
            mode=mode,
            runtime_status=runtime_status,
            source_type=source_type,
        )
        mqtt_client.publish(topic, payload)
        return True
    except Exception:
        logger.exception("Failed to publish robot motor status for twin %s", twin_uuid)
        return False
