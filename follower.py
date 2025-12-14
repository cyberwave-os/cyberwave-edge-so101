"""SO101 Follower class for teleoperation."""

import logging
from pathlib import Path
from typing import Dict, List, Optional
from cyberwave import camera
from config import FollowerConfig
from errors import DeviceNotConnectedError
from motors import (
    FeetechMotorsBus,
    Motor,
    MotorCalibration,
    MotorNormMode,
)
from motors.tables import MODE_POSITION
from utils import load_calibration, save_calibration

logger = logging.getLogger(__name__)

# SO101 Follower motor configuration (same as leader)
SO101_FOLLOWER_MOTORS = {
    "shoulder_pan": Motor(id=1, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "shoulder_lift": Motor(id=2, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "elbow_flex": Motor(id=3, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "wrist_flex": Motor(id=4, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "wrist_roll": Motor(id=5, model="STS3215", norm_mode=MotorNormMode.RANGE_M100_100),
    "gripper": Motor(id=6, model="STS3215", norm_mode=MotorNormMode.RANGE_0_100),
}


class SO101Follower:
    """SO101 Follower device for teleoperation."""

    def __init__(
        self,
        config: Optional[FollowerConfig] = None,
        port: Optional[str] = None,
        use_degrees: bool = False,
        id: str = "follower1",
        calibration_dir: Optional[Path] = None,
        max_relative_target: float = 50.0,
        cameras: Optional[List] = None,
    ):
        """
        Initialize SO101 Follower.

        Args:
            config: FollowerConfig object (if provided, other parameters are ignored)
            port: Serial port path (e.g., "/dev/tty.usbmodem456")
            use_degrees: Whether to use degrees for position values (default: False, uses RANGE_M100_100)
            id: Device identifier for calibration file management
            calibration_dir: Directory for calibration files (default: ~/.so101_lib/calibrations)
            max_relative_target: Maximum allowed change from current position (safety limit)
            cameras: Optional list of camera configurations (not implemented yet)
        """
        if config is not None:
            self.config = config
        else:
            if port is None:
                raise ValueError("Either 'config' or 'port' must be provided")
            self.config = FollowerConfig(
                port=port,
                use_degrees=use_degrees,
                id=id,
                calibration_dir=calibration_dir,
                max_relative_target=max_relative_target,
                cameras=cameras,
            )

        # Determine normalization mode based on config
        norm_mode_body = (
            MotorNormMode.DEGREES if self.config.use_degrees else MotorNormMode.RANGE_M100_100
        )

        # Create motors dict with appropriate normalization modes
        self.motors = {
            "shoulder_pan": Motor(id=1, model="STS3215", norm_mode=norm_mode_body),
            "shoulder_lift": Motor(id=2, model="STS3215", norm_mode=norm_mode_body),
            "elbow_flex": Motor(id=3, model="STS3215", norm_mode=norm_mode_body),
            "wrist_flex": Motor(id=4, model="STS3215", norm_mode=norm_mode_body),
            "wrist_roll": Motor(id=5, model="STS3215", norm_mode=norm_mode_body),
            "gripper": Motor(id=6, model="STS3215", norm_mode=MotorNormMode.RANGE_0_100),
        }

        # Initialize bus with motors and calibration
        # Note: calibration may be None initially, will be set after loading
        self.bus = FeetechMotorsBus(port=self.config.port, motors=self.motors, calibration=None)

        # Load calibration if available (but don't require it)
        self.calibration: Optional[Dict[str, MotorCalibration]] = None
        calibration_path = self.config.calibration_dir / f"{self.config.id}.json"
        if calibration_path.exists():
            logger.info(f"Loading calibration from {calibration_path}")
            calib_data = load_calibration(calibration_path)
            # Convert calibration data to MotorCalibration objects
            # Handle both old format (dict with raw/degrees/radians) and new format (raw values only)
            self.calibration = {}
            for name, data in calib_data.items():
                self.calibration[name] = MotorCalibration(**data)
            # Update bus calibration
            self.bus.calibration = self.calibration
        else:
            logger.info("No calibration file found - follower will work without calibration")

        self._connected = False
        self._current_positions: Dict[str, float] = {}
        self._torque_enabled = False

    def __str__(self) -> str:
        """String representation of the follower."""
        return f"SO101Follower(id={self.config.id}, port={self.config.port})"

    @property
    def connected(self) -> bool:
        """Check if follower is connected."""
        return self._connected and self.bus is not None and self.bus.connected

    @property
    def is_calibrated(self) -> bool:
        """Check if follower is calibrated."""
        return self.calibration is not None

    def connect(self, calibrate: bool = False) -> None:
        """
        Connect to the follower device.

        We assume that at connection time, arm is in a rest position,
        and torque can be safely disabled to run calibration.

        Forces calibration if not already calibrated.

        Args:
            calibrate: Whether to calibrate motors on connection if not already calibrated.
                      If False and not calibrated, will still force calibration
        """
        if self.connected:
            logger.warning("Follower is already connected")
            return

        # Connect to bus
        self.bus.connect()
        self._connected = True

        # Force calibration if not already calibrated
        if not self.is_calibrated:
            logger.info(f"{self} is not calibrated. Running calibration...")
            self.calibrate()
        elif calibrate:
            # If already calibrated but calibrate=True, re-calibrate
            logger.info(f"{self} is already calibrated, but re-calibrating as requested...")
            self.calibrate()

        # Configure motors (set operating mode, PID coefficients)
        self.configure()

        # Enable torque for all motors (follower needs torque to move)
        self.bus.enable_torque()
        self._torque_enabled = True

        # Read initial positions
        self._current_positions = self.get_observation()

        logger.info(f"{self} connected.")

    def configure(self) -> None:
        """
        Configure motors: set operating mode and PID coefficients.

        This is called after connection/calibration to set up motor parameters.
        """
        if not self.connected:
            raise DeviceNotConnectedError("Follower is not connected")

        # Disable torque temporarily to configure motors
        was_torque_enabled = self._torque_enabled
        if was_torque_enabled:
            self.bus.disable_torque()
            self._torque_enabled = False

        try:
            # Set operating mode to position control for all motors
            for motor in self.motors:
                self.bus.write("Operating_Mode", motor, MODE_POSITION)

            # Set PID coefficients to reduce shakiness
            # P_Coefficient: lower value to avoid shakiness (Default is 32, we use 16)
            # I_Coefficient: default 0
            # D_Coefficient: default 32
            for motor in self.motors:
                self.bus.write("P_Coefficient", motor, 16)
                self.bus.write("I_Coefficient", motor, 0)
                self.bus.write("D_Coefficient", motor, 32)

        finally:
            # Re-enable torque if it was enabled before
            if was_torque_enabled:
                self.bus.enable_torque()
                self._torque_enabled = True

    def disconnect(self, torque: bool = False) -> None:
        """Disconnect from the follower device."""
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
        logger.info("Follower disconnected")

    def send_action(self, action: Dict[str, float]) -> Dict[str, float]:
        """
        Command arm to move to a target joint configuration.

        The relative action magnitude may be clipped depending on the configuration parameter
        `max_relative_target`. In this case, the action sent differs from original action.
        Thus, this function always returns the action actually sent.

        Args:
            action: Dictionary mapping motor names (with or without .pos suffix) to normalized positions

        Returns:
            Dictionary mapping motor names to the actual positions sent (normalized)
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected.")

        # Extract goal positions (handle .pos suffix)
        goal_pos = {
            key.removesuffix(".pos"): val for key, val in action.items() if key.endswith(".pos")
        }
        # Also handle keys without .pos suffix
        for key, val in action.items():
            if not key.endswith(".pos") and key in self.motors:
                goal_pos[key] = val

        # Cap goal position when too far away from present position
        # /!\ Slower fps expected due to reading from the follower.
        if self.config.max_relative_target is not None:
            present_pos = self.bus.sync_read("Present_Position", normalize=True)
            goal_present_pos = {key: (g_pos, present_pos[key]) for key, g_pos in goal_pos.items()}
            from utils import ensure_safe_goal_position

            goal_pos = ensure_safe_goal_position(goal_present_pos, self.config.max_relative_target)

        # Send goal position to the arm (bus handles normalization automatically)
        self.bus.sync_write("Goal_Position", goal_pos, normalize=True)

        return {f"{motor}.pos": val for motor, val in goal_pos.items()}

    def get_observation(self) -> Dict[str, float]:
        """
        Get current motor positions as observation dictionary.

        Returns:
            Dictionary mapping motor names (with .pos suffix) to normalized position values
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected.")

        # Read arm position (bus handles normalization automatically)
        obs_dict = self.bus.sync_read("Present_Position", normalize=True)
        obs_dict = {f"{motor}.pos": val for motor, val in obs_dict.items()}

        return obs_dict

    def enable_torque(self, motor_names: Optional[List[str]] = None) -> None:
        """
        Enable torque for specified motors.

        Args:
            motor_names: List of motor names. If None, enables all motors.
        """
        if not self.connected:
            raise DeviceNotConnectedError("Follower is not connected")

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
            raise DeviceNotConnectedError("Follower is not connected")

        if motor_names is None:
            motor_names = list(self.motors.keys())

        motor_ids = [self.motors[name].id for name in motor_names]
        self.bus.disable_torque(motor_ids)
        self._torque_enabled = False
        logger.info(f"Torque disabled for motors: {motor_names}")

    @property
    def torque_enabled(self) -> bool:
        """Check if torque is enabled."""
        return self._torque_enabled

    @property
    def calibration_fpath(self) -> Path:
        """Get calibration file path."""
        return self.config.calibration_dir / f"{self.config.id}.json"

    def calibrate(self) -> None:
        """Calibrate motors."""
        logger.info(f"\nRunning calibration of {self}")
        if not self.connected:
            raise DeviceNotConnectedError("Follower is not connected")

        motor_ids = [motor.id for motor in self.motors.values()]
        # Save torque state before disabling
        was_torque_enabled = self._torque_enabled
        self.bus.disable_torque(motor_ids)
        self._torque_enabled = False

        for motor_name in self.motors:
            self.bus.write("Operating_Mode", motor_name, MODE_POSITION)

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
        self._save_calibration()
        print(f"Calibration saved to {self.calibration_fpath}")

        # Re-enable torque if it was enabled before calibration
        if was_torque_enabled:
            self.bus.enable_torque()  # None = all motors
            self._torque_enabled = True
            logger.info("Torque re-enabled after calibration")

    def _save_calibration(self) -> None:
        """Save calibration to file (internal method)."""
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

        save_calibration(calib_data, self.calibration_fpath)

    def save_calibration(self) -> None:
        """Save current calibration to file."""
        self._save_calibration()
        logger.info(f"Calibration saved to {self.calibration_fpath}")
