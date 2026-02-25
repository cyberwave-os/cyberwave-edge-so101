"""Configuration dataclasses for SO101 leader and follower."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Literal, Optional, Union

# Voltage rating types
VoltageRating = Literal[5, 12]


def get_so101_lib_dir() -> Path:
    """Get the SO101 lib directory (same as calibrate saves to).

    When running in edge-core's Docker container, CYBERWAVE_EDGE_CONFIG_DIR
    is set to /app/.cyberwave (mounted from host). Use that so the driver
    finds setup.json written by so101-setup / cw_setup.
    """
    edge_config = os.getenv("CYBERWAVE_EDGE_CONFIG_DIR")
    if edge_config and edge_config.strip():
        return Path(edge_config.strip()) / "so101_lib"
    return Path.home() / ".cyberwave" / "so101_lib"


def get_setup_config_path() -> Path:
    """Get default setup config path (~/.cyberwave/so101_lib/setup.json)."""
    return get_so101_lib_dir() / "setup.json"


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
            self.calibration_dir = get_so101_lib_dir() / "calibrations"


@dataclass
class FollowerConfig:
    """Configuration for SO101 follower device.

    cameras: List of camera device IDs (int or str) for streaming. Order: wrist_camera first
    (if present), then additional cameras. E.g. [0] for wrist only, [0, 1] for wrist + external.
    Loaded from setup.json when using so101-setup, or passed explicitly.
    """

    port: str
    use_degrees: bool = False
    id: str = "follower1"
    calibration_dir: Optional[Path] = None
    max_relative_target: Optional[float] = (
        None  # Maximum change per update (raw encoder units), None to disable safety limit
    )
    cameras: Optional[List[Union[int, str]]] = None
    voltage_rating: Optional[VoltageRating] = None  # 5V or 12V, None for auto-detect

    def __post_init__(self):
        """Set default calibration directory if not provided."""
        if self.calibration_dir is None:
            self.calibration_dir = get_so101_lib_dir() / "calibrations"


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
