"""Motor bus and models for so101_lib."""

from .bus import MotorsBus
from .feetech_bus import FeetechMotorsBus
from .models import Motor, MotorCalibration, MotorNormMode, OperatingMode
from .registers import (
    RegisterSpec,
    decode_register_value,
    encode_register_value,
    get_register_spec,
    iter_register_specs,
)

__all__ = [
    "MotorsBus",
    "FeetechMotorsBus",
    "Motor",
    "MotorCalibration",
    "MotorNormMode",
    "OperatingMode",
    "RegisterSpec",
    "get_register_spec",
    "iter_register_specs",
    "encode_register_value",
    "decode_register_value",
]

