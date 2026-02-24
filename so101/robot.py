"""SO101 Robot base class for leader and follower devices."""

import abc
import json
import logging
import time
from pathlib import Path
from typing import Dict, List, Optional

from motors import (
    FeetechMotorsBus,
    Motor,
    MotorCalibration,
    MotorNormMode,
)
from motors.tables import MODE_POSITION
from utils.errors import DeviceNotConnectedError

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

    def disconnect(self, torque: bool = False) -> None:
        """Disconnect from the robot device."""
        if not self.connected:
            return

        # Disable torque before disconnecting
        if self.bus:
            if not torque:
                self.bus.disable_torque()
            self._torque_enabled = False
            self.bus.disconnect()
            self.bus = None

        self._connected = False
        logger.info(f"{self} disconnected")

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

    def enable_torque(self, motor_names: Optional[List[str]] = None) -> None:
        """
        Enable torque for specified motors.

        Args:
            motor_names: List of motor names. If None, enables all motors.
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected")

        if motor_names is None:
            motor_names = list(self.motors.keys())

        motor_ids = [self.motors[name].id for name in motor_names]
        self.bus.enable_torque(motor_ids)
        self._torque_enabled = True
        logger.info(f"Torque enabled for motors: {motor_names}")

    def disable_torque(self, motor_names: Optional[List[str]] = None) -> None:
        """
        Disable torque for specified motors.

        Args:
            motor_names: List of motor names. If None, disables all motors.
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected")

        if motor_names is None:
            motor_names = list(self.motors.keys())

        motor_ids = [self.motors[name].id for name in motor_names]
        self.bus.disable_torque(motor_ids)
        self._torque_enabled = False
        logger.info(f"Torque disabled for motors: {motor_names}")

    @property
    @abc.abstractmethod
    def torque_enabled(self) -> bool:
        pass

    @property
    @abc.abstractmethod
    def calibration_fpath(self) -> Path:
        pass

    def calibrate(self) -> None:
        """Calibrate motors."""
        logger.info(f"\nRunning calibration of {self}")
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected")

        motor_ids = [motor.id for motor in self.motors.values()]
        # Save torque state before disabling
        was_torque_enabled = self._torque_enabled
        self.bus.disable_torque(motor_ids)
        self._torque_enabled = False

        for motor_name in self.motors:
            self.bus.write("Operating_Mode", motor_name, MODE_POSITION)

        # Reset homing offsets to 0 to ensure we read true raw positions
        # This ensures consistent readings across calibration runs
        logger.info("Resetting homing offsets to ensure consistent readings...")
        self.bus.reset_homing_offsets()
        time.sleep(0.1)  # Brief delay for offsets to take effect

        print(f"\nMove {self} to the middle of its range of motion.")
        print("Current positions:")
        self.bus.display_current_positions()
        input("Press ENTER when ready to continue...")

        homing_offsets = self.bus.set_half_turn_homings()

        print(
            "Move all joints sequentially through their entire ranges "
            "of motion.\nRecording positions. Press ENTER to stop..."
        )
        range_mins, range_maxes = self.bus.record_ranges_of_motion()

        self.calibration = {}
        for motor_name, motor in self.motors.items():
            self.calibration[motor_name] = MotorCalibration(
                id=motor.id,
                drive_mode=0,
                homing_offset=homing_offsets[motor_name],
                range_min=range_mins[motor_name],
                range_max=range_maxes[motor_name],
            )

        # Update bus calibration
        self.bus.calibration = self.calibration
        self.bus.write_calibration(self.calibration)
        self.save_calibration()
        print(f"Calibration saved to {self.calibration_fpath}")

        # Re-enable torque if it was enabled before calibration
        if was_torque_enabled:
            self.bus.enable_torque()  # None = all motors
            self._torque_enabled = True
            logger.info("Torque re-enabled after calibration")

    def save_calibration(self) -> None:
        """Save current calibration to file."""
        if self.calibration is None:
            logger.warning("No calibration to save")
            return

        calib_data = {}
        for name, calib in self.calibration.items():
            # Save only raw values - conversion to degrees/radians happens in teleoperation
            calib_data[name] = {
                "id": calib.id,
                "drive_mode": calib.drive_mode,
                "homing_offset": calib.homing_offset,
                "range_min": calib.range_min,
                "range_max": calib.range_max,
            }

        self.calibration_fpath.parent.mkdir(parents=True, exist_ok=True)

        with open(self.calibration_fpath, "w") as f:
            json.dump(calib_data, f, indent=2)

    def _validate_and_alert_ranges(
        self, range_mins: Dict[str, float], range_maxes: Dict[str, float]
    ) -> None:
        """
        Validate recorded ranges and display red alerts for invalid joints.

        Invalid ranges:
        - Full range (0-4095): Not physically possible
        - Min equals max (0-0 or 4095-4095): Invalid, no range recorded

        Args:
            range_mins: Dictionary mapping motor names to minimum recorded positions
            range_maxes: Dictionary mapping motor names to maximum recorded positions
        """
        invalid_joints = []
        FULL_RANGE_MIN = 0
        FULL_RANGE_MAX = 4095

        for motor_name in self.motors.keys():
            min_val = range_mins.get(motor_name, float("inf"))
            max_val = range_maxes.get(motor_name, float("-inf"))

            # Skip if values are still inf (no data recorded)
            if min_val == float("inf") or max_val == float("-inf"):
                continue

            # Check for invalid ranges
            is_full_range = min_val == FULL_RANGE_MIN and max_val == FULL_RANGE_MAX
            is_zero_range = min_val == max_val

            if is_full_range or is_zero_range:
                invalid_joints.append((motor_name, min_val, max_val, is_full_range, is_zero_range))

        # Display alerts if any invalid joints found
        if invalid_joints:
            print("\n" + "=" * 90)
            print("\033[91m" + "⚠️  CALIBRATION WARNING: Invalid joint ranges detected" + "\033[0m")
            print("=" * 90)
            for motor_name, min_val, max_val, is_full_range, is_zero_range in invalid_joints:
                motor_id = self.motors[motor_name].id
                print(
                    f"\n\033[91m{motor_name:<20} (ID: {motor_id:<3}) - Range: [{min_val:.1f}, {max_val:.1f}]\033[0m"
                )
                if is_full_range:
                    print(
                        "\033[91m  ❌ Full range (0-4095) detected - not physically possible\033[0m"
                    )
                if is_zero_range:
                    print("\033[91m  ❌ Zero range detected - no motion recorded\033[0m")
                print("\033[91m  → Action required:\033[0m")
                print("\033[91m     1. Exit calibration (Ctrl+C)\033[0m")
                print("\033[91m     2. Unplug power and USB cables from the robot\033[0m")
                print("\033[91m     3. Wait 5 seconds\033[0m")
                print("\033[91m     4. Reconnect and retry calibration\033[0m")
            print("\n" + "=" * 90 + "\n")
