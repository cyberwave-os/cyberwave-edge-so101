"""Motor data models and enums."""

from dataclasses import dataclass
from enum import Enum


class MotorNormMode(Enum):
    """Normalization modes for motor position values."""

    RANGE_0_100 = "range_0_100"  # 0 to 100
    RANGE_M100_100 = "range_m100_100"  # -100 to 100
    DEGREES = "degrees"  # Degrees (0-360 or -180 to 180)


class OperatingMode(Enum):
    """Operating modes for motors."""

    POSITION = "position"
    VELOCITY = "velocity"
    PWM = "pwm"
    STEP = "step"


@dataclass
class Motor:
    """Motor configuration data class."""

    id: int
    model: str
    norm_mode: MotorNormMode


@dataclass
class MotorCalibration:
    """Motor calibration data class."""

    id: int
    drive_mode: int
    homing_offset: float
    range_min: float
    range_max: float

