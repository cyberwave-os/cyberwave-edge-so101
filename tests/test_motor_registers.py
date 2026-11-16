"""Tests for generic Feetech register helpers and bus register access."""

from __future__ import annotations

from unittest.mock import MagicMock

from motors.feetech_bus import FeetechMotorsBus
from motors.models import Motor, MotorNormMode
from motors.registers import (
    encode_register_value,
    get_register_spec,
)


def _build_bus() -> FeetechMotorsBus:
    bus = FeetechMotorsBus(
        port="/dev/null",
        motors={"joint": Motor(id=1, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100)},
    )
    bus._connected = True
    bus._packet_handler = MagicMock()
    bus._port_handler = object()
    bus._comm_success = 0
    bus._sync_reader = MagicMock()
    bus._sync_writer = MagicMock()
    return bus


def test_get_register_spec_resolves_alias() -> None:
    spec = get_register_spec("Moving_Velocity_Threshold")
    assert spec.name == "Moving_Velocity"
    assert spec.address == 80
    assert spec.writable is True


def test_encode_register_value_handles_signed_velocity() -> None:
    encoded = encode_register_value("Goal_Velocity", -5)
    assert encoded == 0x8005


def test_read_register_decodes_signed_values() -> None:
    bus = _build_bus()
    bus._packet_handler.read2ByteTxRx.return_value = (0x8005, 0, 0)

    value = bus.read_register("Present_Velocity", "joint")

    assert value == -5
    bus._packet_handler.read2ByteTxRx.assert_called_once()


def test_write_register_supports_new_generic_registers() -> None:
    bus = _build_bus()
    bus._packet_handler.writeTxRx.return_value = (0, 0)

    bus.write("Acceleration", "joint", 12)

    spec = get_register_spec("Acceleration")
    bus._packet_handler.writeTxRx.assert_called_once()
    _, motor_id, address, length, data = bus._packet_handler.writeTxRx.call_args[0]
    assert motor_id == 1
    assert address == spec.address
    assert length == spec.length
    assert data == [12]


def test_write_register_rejects_read_only_registers() -> None:
    bus = _build_bus()

    try:
        bus.write("Present_Current", "joint", 10)
    except ValueError as exc:
        assert "not writable" in str(exc)
    else:
        raise AssertionError("Expected write() to reject read-only register")


def test_sync_write_raw_goal_time_routes_through_generic_writer() -> None:
    bus = _build_bus()
    bus.sync_write_register = MagicMock()

    bus.sync_write("Goal_Time", {"joint": 250}, normalize=False)

    bus.sync_write_register.assert_called_once_with("Goal_Time", {1: 250}, num_retry=0)
