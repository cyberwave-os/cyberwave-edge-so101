"""Configuration dataclasses for SO101 leader and follower."""

from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

# Voltage rating types
VoltageRating = Literal[5, 12]


@dataclass
class LeaderConfig:
    """Configuration for SO101 leader device."""

    port: str
    use_degrees: bool = False
    id: str = "leader1"
    calibration_dir: Optional[Path] = None
    voltage_rating: Optional[VoltageRating] = None  # 5V or 12V, None for auto-detect

    def __post_init__(self):
        """Set default calibration directory if not provided."""
        if self.calibration_dir is None:
            self.calibration_dir = Path.home() / ".so101_lib" / "calibrations"


@dataclass
class FollowerConfig:
    """Configuration for SO101 follower device."""

    port: str
    use_degrees: bool = False
    id: str = "follower1"
    calibration_dir: Optional[Path] = None
    max_relative_target: Optional[float] = (
        None  # Maximum change per update (raw encoder units), None to disable safety limit
    )
    cameras: Optional[int] = None
    voltage_rating: Optional[VoltageRating] = None  # 5V or 12V, None for auto-detect

    def __post_init__(self):
        """Set default calibration directory if not provided."""
        if self.calibration_dir is None:
            self.calibration_dir = Path.home() / ".so101_lib" / "calibrations"


# {
#   "shoulder_pan": {
#     "id": 1,
#     "drive_mode": 0,
#     "homing_offset": 363.0,
#     "range_min": 796.0,
#     "range_max": 3486.0
#   },
#   "shoulder_lift": {
#     "id": 2,
#     "drive_mode": 0,
#     "homing_offset": 382.0,
#     "range_min": 753.0,
#     "range_max": 3113.0
#   },
#   "elbow_flex": {
#     "id": 3,
#     "drive_mode": 0,
#     "homing_offset": -619.0,
#     "range_min": 845.0,
#     "range_max": 3049.0
#   },
#   "wrist_flex": {
#     "id": 4,
#     "drive_mode": 0,
#     "homing_offset": -480.0,
#     "range_min": 962.0,
#     "range_max": 3279.0
#   },
#   "wrist_roll": {
#     "id": 5,
#     "drive_mode": 0,
#     "homing_offset": 1527.0,
#     "range_min": 1180.0,
#     "range_max": 4093.0
#   },
#   "gripper": {
#     "id": 6,
#     "drive_mode": 0,
#     "homing_offset": -1090.0,
#     "range_min": 1953.0,
#     "range_max": 3392.0
#   }
