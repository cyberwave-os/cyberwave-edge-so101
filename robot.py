"""SO101 Robot base class for leader and follower devices."""

import abc
import logging
from pathlib import Path
from typing import Dict, List, Optional

from motors import (
    FeetechMotorsBus,
    Motor,
    MotorCalibration,
    MotorNormMode,
)

logger = logging.getLogger(__name__)

SO101_MOTORS = {
    "shoulder_pan": Motor(id=1, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "shoulder_lift": Motor(id=2, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "elbow_flex": Motor(id=3, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "wrist_flex": Motor(id=4, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "wrist_roll": Motor(id=5, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "gripper": Motor(id=6, model="STS3215", norm_mode=MotorNormMode.RANGE_0_100),
}


class SO101Robot(abc.ABC):
    """Abstract base class for SO101 leader and follower robots."""

    @abc.abstractmethod
    def __init__(self) -> None:
        """Initialize SO101 Robot."""
        self.motors: Dict[str, Motor] = SO101_MOTORS
        self.bus: Optional[FeetechMotorsBus] = None
        self.calibration: Optional[Dict[str, MotorCalibration]] = None
        self._connected = False
        self._torque_enabled = False

    @abc.abstractmethod
    def connect(self) -> None:
        pass

    @abc.abstractmethod
    def disconnect(self) -> None:
        pass

    @property
    @abc.abstractmethod
    def connected(self) -> bool:
        pass

    @abc.abstractmethod
    def get_action(self) -> Dict[str, float]:
        pass

    @abc.abstractmethod
    def get_observation(self) -> Dict[str, float]:
        pass

    @abc.abstractmethod
    def enable_torque(self, motor_names: Optional[List[str]] = None) -> None:
        pass

    @property
    @abc.abstractmethod
    def torque_enabled(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def calibration_fpath(self) -> Path:
        pass

    @abc.abstractmethod
    def calibrate(self) -> None:
        pass

    @abc.abstractmethod
    def save_calibration(self) -> None:
        pass
