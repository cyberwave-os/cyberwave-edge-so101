"""Tests for detailed motor-status telemetry helpers."""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from motors import Motor, MotorCalibration, MotorNormMode
from utils.motor_telemetry import (
    TELEMETRY_TYPE_MOTOR_STATUS,
    build_device_motor_status_snapshot,
    publish_robot_motor_status,
)


def _build_device() -> SimpleNamespace:
    device = SimpleNamespace()
    device.connected = True
    device.torque_enabled = True
    device.config = SimpleNamespace(port="/dev/ttyUSB0")
    device.motors = {
        "shoulder_pan": Motor(id=1, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    }
    device.calibration = {
        "shoulder_pan": MotorCalibration(
            id=1,
            drive_mode=0,
            homing_offset=0.0,
            range_min=1024.0,
            range_max=3072.0,
        )
    }
    device.get_motor_register_snapshot = MagicMock(
        return_value={
            "shoulder_pan": {
                "Present_Position": 2048,
                "Goal_Position": 2300,
                "Present_Velocity": 12,
                "Goal_Velocity": -4,
                "Present_Load": 17,
                "Present_Current": 44,
                "Present_Voltage": 120,
                "Min_Voltage_Limit": 50,
                "Max_Voltage_Limit": 140,
                "Present_Temperature": 43,
                "Torque_Enable": 1,
                "Moving": 1,
                "Lock": 0,
            }
        }
    )
    return device


def test_build_device_motor_status_snapshot_includes_registers_and_derived_values() -> None:
    device = _build_device()

    snapshot = build_device_motor_status_snapshot(
        device,
        device_label="follower",
        motor_id_to_schema_joint={1: "_1"},
    )

    assert snapshot["connected"] is True
    assert snapshot["torque_enabled"] is True
    motor_snapshot = snapshot["motors"]["shoulder_pan"]
    assert motor_snapshot["schema_joint"] == "_1"
    assert motor_snapshot["registers"]["Present_Position"] == 2048
    assert motor_snapshot["registers"]["Torque_Enable"] is True
    assert motor_snapshot["derived"]["present_position_normalized"] == 0.0
    assert "present_position_radians" in motor_snapshot["derived"]
    assert motor_snapshot["derived"]["present_voltage_volts"] == 12.0
    assert motor_snapshot["derived"]["effort_proxy"] == 17.0
    assert motor_snapshot["derived"]["present_current_raw"] == 44.0
    assert motor_snapshot["derived"]["moving"] is True


def test_publish_robot_motor_status_publishes_motor_status_payload() -> None:
    mqtt_client = MagicMock()
    mqtt_client.connected = True
    mqtt_client.topic_prefix = ""
    leader = _build_device()
    follower = _build_device()

    with patch.dict(os.environ, {"CYBERWAVE_MOTOR_TELEMETRY": "true"}):
        ok = publish_robot_motor_status(
            mqtt_client,
            twin_uuid="twin-123",
            leader=leader,
            follower=follower,
            motor_id_to_schema_joint={1: "_1"},
            mode="teleoperate",
            runtime_status={"mqtt_connected": True, "errors_motor": 0},
        )

    assert ok is True
    mqtt_client.publish.assert_called_once()
    topic, payload = mqtt_client.publish.call_args[0]
    assert topic == "cyberwave/twin/twin-123/telemetry"
    assert payload["type"] == TELEMETRY_TYPE_MOTOR_STATUS
    assert payload["mode"] == "teleoperate"
    assert payload["devices"]["leader"]["connected"] is True
    assert payload["devices"]["follower"]["motors"]["shoulder_pan"]["derived"]["present_voltage_volts"] == 12.0


def test_publish_robot_motor_status_disabled_by_default() -> None:
    mqtt_client = MagicMock()
    mqtt_client.connected = True
    mqtt_client.topic_prefix = ""

    with patch.dict(os.environ, {}, clear=True):
        ok = publish_robot_motor_status(
            mqtt_client,
            twin_uuid="twin-123",
            mode="teleoperate",
        )

    assert ok is False
    mqtt_client.publish.assert_not_called()
