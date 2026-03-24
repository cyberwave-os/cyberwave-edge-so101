"""Register metadata and helpers for Feetech STS3215 motors."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

from .encoding import decode_sign_magnitude, encode_sign_magnitude
from .tables import (
    ADDR_ACCELERATION,
    ADDR_ACCELERATION_MULTIPLIER,
    ADDR_ANGULAR_RESOLUTION,
    ADDR_BAUD_RATE,
    ADDR_CCW_DEAD_ZONE,
    ADDR_CW_DEAD_ZONE,
    ADDR_D_COEFFICIENT,
    ADDR_DTS,
    ADDR_FIRMWARE_MAJOR_VERSION,
    ADDR_FIRMWARE_MINOR_VERSION,
    ADDR_GOAL_POSITION,
    ADDR_GOAL_POSITION_2,
    ADDR_GOAL_TIME,
    ADDR_GOAL_VELOCITY,
    ADDR_HOMING_OFFSET,
    ADDR_HTS,
    ADDR_I_COEFFICIENT,
    ADDR_ID,
    ADDR_LED_ALARM_CONDITION,
    ADDR_LOCK,
    ADDR_MAX_POSITION_LIMIT,
    ADDR_MAX_TEMPERATURE_LIMIT,
    ADDR_MAX_TORQUE_LIMIT,
    ADDR_MAX_VOLTAGE_LIMIT,
    ADDR_MAXIMUM_ACCELERATION,
    ADDR_MAXIMUM_VELOCITY_LIMIT,
    ADDR_MIN_POSITION_LIMIT,
    ADDR_MIN_VOLTAGE_LIMIT,
    ADDR_MINIMUM_STARTUP_FORCE,
    ADDR_MODEL_NUMBER,
    ADDR_MOVING,
    ADDR_MOVING_VELOCITY,
    ADDR_OPERATING_MODE,
    ADDR_OVER_CURRENT_PROTECTION_TIME,
    ADDR_OVERLOAD_TORQUE,
    ADDR_P_COEFFICIENT,
    ADDR_PHASE,
    ADDR_PRESENT_CURRENT,
    ADDR_PRESENT_LOAD,
    ADDR_PRESENT_POSITION,
    ADDR_PRESENT_TEMPERATURE,
    ADDR_PRESENT_VELOCITY,
    ADDR_PRESENT_VOLTAGE,
    ADDR_PROTECTION_CURRENT,
    ADDR_PROTECTION_TIME,
    ADDR_PROTECTIVE_TORQUE,
    ADDR_RESPONSE_STATUS_LEVEL,
    ADDR_RETURN_DELAY_TIME,
    ADDR_STATUS,
    ADDR_TORQUE_ENABLE,
    ADDR_TORQUE_LIMIT,
    ADDR_UNLOADING_CONDITION,
    ADDR_VELOCITY_CLOSED_LOOP_I,
    ADDR_VELOCITY_CLOSED_LOOP_P,
    ADDR_VELOCITY_UNIT_FACTOR,
    ENCODING_BIT_HOMING_OFFSET,
    ENCODING_BIT_VELOCITY,
)

AccessMode = Literal["read_only", "read_write"]
StorageKind = Literal["EEPROM", "SRAM"]


@dataclass(frozen=True)
class RegisterSpec:
    """Description of a single STS3215 control-table register."""

    name: str
    address: int
    length: int
    access: AccessMode
    storage: StorageKind
    sign_bit: int | None = None
    aliases: tuple[str, ...] = ()

    @property
    def readable(self) -> bool:
        return True

    @property
    def writable(self) -> bool:
        return self.access == "read_write"


REGISTER_SPECS: tuple[RegisterSpec, ...] = (
    RegisterSpec("Firmware_Major_Version", *ADDR_FIRMWARE_MAJOR_VERSION, "read_only", "EEPROM"),
    RegisterSpec("Firmware_Minor_Version", *ADDR_FIRMWARE_MINOR_VERSION, "read_only", "EEPROM"),
    RegisterSpec("Model_Number", *ADDR_MODEL_NUMBER, "read_only", "EEPROM"),
    RegisterSpec("ID", *ADDR_ID, "read_write", "EEPROM"),
    RegisterSpec("Baud_Rate", *ADDR_BAUD_RATE, "read_write", "EEPROM"),
    RegisterSpec("Return_Delay_Time", *ADDR_RETURN_DELAY_TIME, "read_write", "EEPROM"),
    RegisterSpec("Response_Status_Level", *ADDR_RESPONSE_STATUS_LEVEL, "read_write", "EEPROM"),
    RegisterSpec("Min_Position_Limit", *ADDR_MIN_POSITION_LIMIT, "read_write", "EEPROM"),
    RegisterSpec("Max_Position_Limit", *ADDR_MAX_POSITION_LIMIT, "read_write", "EEPROM"),
    RegisterSpec("Max_Temperature_Limit", *ADDR_MAX_TEMPERATURE_LIMIT, "read_write", "EEPROM"),
    RegisterSpec("Max_Voltage_Limit", *ADDR_MAX_VOLTAGE_LIMIT, "read_write", "EEPROM"),
    RegisterSpec("Min_Voltage_Limit", *ADDR_MIN_VOLTAGE_LIMIT, "read_write", "EEPROM"),
    RegisterSpec("Max_Torque_Limit", *ADDR_MAX_TORQUE_LIMIT, "read_write", "EEPROM"),
    RegisterSpec("Phase", *ADDR_PHASE, "read_write", "EEPROM"),
    RegisterSpec("Unloading_Condition", *ADDR_UNLOADING_CONDITION, "read_write", "EEPROM"),
    RegisterSpec("LED_Alarm_Condition", *ADDR_LED_ALARM_CONDITION, "read_write", "EEPROM"),
    RegisterSpec("P_Coefficient", *ADDR_P_COEFFICIENT, "read_write", "EEPROM"),
    RegisterSpec("D_Coefficient", *ADDR_D_COEFFICIENT, "read_write", "EEPROM"),
    RegisterSpec("I_Coefficient", *ADDR_I_COEFFICIENT, "read_write", "EEPROM"),
    RegisterSpec("Minimum_Startup_Force", *ADDR_MINIMUM_STARTUP_FORCE, "read_write", "EEPROM"),
    RegisterSpec("CW_Dead_Zone", *ADDR_CW_DEAD_ZONE, "read_write", "EEPROM"),
    RegisterSpec("CCW_Dead_Zone", *ADDR_CCW_DEAD_ZONE, "read_write", "EEPROM"),
    RegisterSpec("Protection_Current", *ADDR_PROTECTION_CURRENT, "read_write", "EEPROM"),
    RegisterSpec("Angular_Resolution", *ADDR_ANGULAR_RESOLUTION, "read_write", "EEPROM"),
    RegisterSpec(
        "Homing_Offset",
        *ADDR_HOMING_OFFSET,
        "read_write",
        "EEPROM",
        sign_bit=ENCODING_BIT_HOMING_OFFSET,
    ),
    RegisterSpec("Operating_Mode", *ADDR_OPERATING_MODE, "read_write", "EEPROM"),
    RegisterSpec("Protective_Torque", *ADDR_PROTECTIVE_TORQUE, "read_write", "EEPROM"),
    RegisterSpec("Protection_Time", *ADDR_PROTECTION_TIME, "read_write", "EEPROM"),
    RegisterSpec("Overload_Torque", *ADDR_OVERLOAD_TORQUE, "read_write", "EEPROM"),
    RegisterSpec("Velocity_Closed_Loop_P", *ADDR_VELOCITY_CLOSED_LOOP_P, "read_write", "EEPROM"),
    RegisterSpec(
        "Over_Current_Protection_Time",
        *ADDR_OVER_CURRENT_PROTECTION_TIME,
        "read_write",
        "EEPROM",
    ),
    RegisterSpec("Velocity_Closed_Loop_I", *ADDR_VELOCITY_CLOSED_LOOP_I, "read_write", "EEPROM"),
    RegisterSpec("Torque_Enable", *ADDR_TORQUE_ENABLE, "read_write", "SRAM"),
    RegisterSpec("Acceleration", *ADDR_ACCELERATION, "read_write", "SRAM"),
    RegisterSpec("Goal_Position", *ADDR_GOAL_POSITION, "read_write", "SRAM"),
    RegisterSpec("Goal_Time", *ADDR_GOAL_TIME, "read_write", "SRAM"),
    RegisterSpec(
        "Goal_Velocity",
        *ADDR_GOAL_VELOCITY,
        "read_write",
        "SRAM",
        sign_bit=ENCODING_BIT_VELOCITY,
    ),
    RegisterSpec("Torque_Limit", *ADDR_TORQUE_LIMIT, "read_write", "SRAM"),
    RegisterSpec("Lock", *ADDR_LOCK, "read_write", "SRAM"),
    RegisterSpec("Present_Position", *ADDR_PRESENT_POSITION, "read_only", "SRAM"),
    RegisterSpec(
        "Present_Velocity",
        *ADDR_PRESENT_VELOCITY,
        "read_only",
        "SRAM",
        sign_bit=ENCODING_BIT_VELOCITY,
    ),
    RegisterSpec(
        "Present_Load",
        *ADDR_PRESENT_LOAD,
        "read_only",
        "SRAM",
        sign_bit=ENCODING_BIT_VELOCITY,
    ),
    RegisterSpec("Present_Voltage", *ADDR_PRESENT_VOLTAGE, "read_only", "SRAM"),
    RegisterSpec("Present_Temperature", *ADDR_PRESENT_TEMPERATURE, "read_only", "SRAM"),
    RegisterSpec("Status", *ADDR_STATUS, "read_only", "SRAM"),
    RegisterSpec("Moving", *ADDR_MOVING, "read_only", "SRAM"),
    RegisterSpec("Present_Current", *ADDR_PRESENT_CURRENT, "read_only", "SRAM"),
    RegisterSpec("Goal_Position_2", *ADDR_GOAL_POSITION_2, "read_only", "SRAM"),
    RegisterSpec(
        "Moving_Velocity",
        *ADDR_MOVING_VELOCITY,
        "read_write",
        "SRAM",
        aliases=("Moving_Velocity_Threshold",),
    ),
    RegisterSpec("DTS", *ADDR_DTS, "read_write", "SRAM"),
    RegisterSpec("Velocity_Unit_Factor", *ADDR_VELOCITY_UNIT_FACTOR, "read_write", "SRAM"),
    RegisterSpec("HTS", *ADDR_HTS, "read_write", "SRAM"),
    RegisterSpec("Maximum_Velocity_Limit", *ADDR_MAXIMUM_VELOCITY_LIMIT, "read_write", "SRAM"),
    RegisterSpec("Maximum_Acceleration", *ADDR_MAXIMUM_ACCELERATION, "read_write", "SRAM"),
    RegisterSpec(
        "Acceleration_Multiplier",
        *ADDR_ACCELERATION_MULTIPLIER,
        "read_write",
        "SRAM",
    ),
)

READABLE_REGISTER_NAMES = frozenset(spec.name for spec in REGISTER_SPECS)
WRITABLE_REGISTER_NAMES = frozenset(spec.name for spec in REGISTER_SPECS if spec.writable)
POSITION_REGISTER_NAMES = frozenset(
    {
        "Min_Position_Limit",
        "Max_Position_Limit",
        "Goal_Position",
        "Present_Position",
        "Goal_Position_2",
    }
)
VOLTAGE_REGISTER_NAMES = frozenset({"Present_Voltage", "Min_Voltage_Limit", "Max_Voltage_Limit"})
BOOLEAN_REGISTER_NAMES = frozenset({"Torque_Enable", "Lock", "Moving"})

_REGISTER_MAP: dict[str, RegisterSpec] = {}
for register_spec in REGISTER_SPECS:
    _REGISTER_MAP[register_spec.name] = register_spec
    for alias in register_spec.aliases:
        _REGISTER_MAP[alias] = register_spec


def get_register_spec(register_name: str) -> RegisterSpec:
    """Resolve a register name or alias to its specification."""

    try:
        return _REGISTER_MAP[register_name]
    except KeyError as exc:
        raise KeyError(f"Unknown register: {register_name}") from exc


def iter_register_specs(
    *,
    readable: bool | None = None,
    writable: bool | None = None,
) -> Iterable[RegisterSpec]:
    """Iterate over the canonical register specs."""

    for register_spec in REGISTER_SPECS:
        if readable is not None and register_spec.readable != readable:
            continue
        if writable is not None and register_spec.writable != writable:
            continue
        yield register_spec


def decode_register_value(register_name: str | RegisterSpec, raw_value: int) -> int:
    """Decode a raw register value into the logical register value."""

    register_spec = (
        register_name if isinstance(register_name, RegisterSpec) else get_register_spec(register_name)
    )
    value = int(raw_value)
    if register_spec.name in POSITION_REGISTER_NAMES:
        value &= 0x0FFF
    if register_spec.sign_bit is not None:
        return decode_sign_magnitude(value, sign_bit=register_spec.sign_bit)
    return value


def encode_register_value(register_name: str | RegisterSpec, value: int | float | bool) -> int:
    """Encode a logical register value for transport to the servo."""

    register_spec = (
        register_name if isinstance(register_name, RegisterSpec) else get_register_spec(register_name)
    )
    integer_value = int(value)
    if register_spec.sign_bit is not None:
        return encode_sign_magnitude(integer_value, sign_bit=register_spec.sign_bit)
    if register_spec.name in POSITION_REGISTER_NAMES:
        return max(0, min(4095, integer_value))
    max_value = (1 << (register_spec.length * 8)) - 1
    return max(0, min(max_value, integer_value))
