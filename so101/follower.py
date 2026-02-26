"""SO101 Follower class for teleoperation."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

from serial.serialutil import SerialException

from motors import (
    FeetechMotorsBus,
    MotorCalibration,
)
from motors.tables import MODE_POSITION
from so101.robot import SO101Robot
from utils.config import FollowerConfig
from utils.errors import DeviceNotConnectedError
from utils.utils import load_calibration

logger = logging.getLogger(__name__)


class SO101Follower(SO101Robot):
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
            calibration_dir: Directory for calibration files (default: ~/.cyberwave/so101_lib/calibrations)
            max_relative_target: Maximum allowed change from current position (safety limit)
            cameras: Optional list of camera configurations (not implemented yet)
        """
        super().__init__()
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

        self._current_positions: Dict[str, float] = {}

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
        elif self.calibration:
            # Restore calibration to motors (homing offset, position limits)
            logger.info("Restoring calibration to follower motors (homing offset, position limits)")
            self.bus.write_calibration(self.calibration)

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

            # Set PID coefficients
            for motor in self.motors:
                self.bus.write("P_Coefficient", motor, 16)
                self.bus.write("I_Coefficient", motor, 0)
                self.bus.write("D_Coefficient", motor, 32)

                if motor == "gripper":
                    self.bus.write("Max_Torque_Limit", motor, 500)
                    self.bus.write("Protection_Current", motor, 250)
                    self.bus.write("Overload_Torque", motor, 25)

        finally:
            # Re-enable torque if it was enabled before
            if was_torque_enabled:
                self.bus.enable_torque()
                self._torque_enabled = True

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
            try:
                present_pos = self.bus.sync_read("Present_Position", normalize=True)
                goal_present_pos = {
                    key: (g_pos, present_pos[key]) for key, g_pos in goal_pos.items()
                }
                from utils.utils import ensure_safe_goal_position

                goal_pos = ensure_safe_goal_position(
                    goal_present_pos, self.config.max_relative_target
                )
            except SerialException as e:
                logger.warning(
                    f"Serial communication error reading follower positions for safety check: {e}. Skipping safety check."
                )
                # Use last known positions if available for safety check
                if self._current_positions:
                    present_pos = {
                        k.removesuffix(".pos"): v for k, v in self._current_positions.items()
                    }
                    goal_present_pos = {
                        key: (g_pos, present_pos.get(key, g_pos)) for key, g_pos in goal_pos.items()
                    }
                    from utils.utils import ensure_safe_goal_position

                    goal_pos = ensure_safe_goal_position(
                        goal_present_pos, self.config.max_relative_target
                    )

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

        try:
            # Read arm position (bus handles normalization automatically)
            obs_dict = self.bus.sync_read("Present_Position", normalize=True)
            obs_dict = {f"{motor}.pos": val for motor, val in obs_dict.items()}
            # Update current positions on successful read
            self._current_positions = obs_dict
            return obs_dict
        except SerialException as e:
            logger.warning(
                f"Serial communication error reading follower positions: {e}. Using last known positions."
            )
            # Return last known positions if available, otherwise empty dict
            # Don't mark as disconnected - this could be a transient error
            return self._current_positions if self._current_positions else {}

    @property
    def torque_enabled(self) -> bool:
        """Check if torque is enabled."""
        return self._torque_enabled

    @property
    def calibration_fpath(self) -> Path:
        """Get calibration file path."""
        return self.config.calibration_dir / f"{self.config.id}.json"
