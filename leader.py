"""SO101 Leader class for teleoperation."""

import logging
from pathlib import Path
from typing import Dict, Optional

from serial.serialutil import SerialException

from config import LeaderConfig
from errors import DeviceNotConnectedError
from motors import (
    FeetechMotorsBus,
    MotorCalibration,
)
from robot import SO101Robot
from utils import load_calibration

logger = logging.getLogger(__name__)


class SO101Leader(SO101Robot):
    """SO101 Leader device for teleoperation."""

    def __init__(
        self,
        config: Optional[LeaderConfig] = None,
        port: Optional[str] = None,
        use_degrees: bool = True,
        id: str = "leader1",
        calibration_dir: Optional[Path] = None,
    ):
        """
        Initialize SO101 Leader.

        Args:
            config: LeaderConfig object (if provided, other parameters are ignored)
            port: Serial port path (e.g., "/dev/tty.usbmodem123")
            use_degrees: Whether to use degrees for position values
            id: Device identifier for calibration file management
            calibration_dir: Directory for calibration files (default: ~/.cyberwave/so101_lib/calibrations)
        """
        super().__init__()
        if config is not None:
            self.config = config
        else:
            if port is None:
                raise ValueError("Either 'config' or 'port' must be provided")
            self.config = LeaderConfig(
                port=port, use_degrees=use_degrees, id=id, calibration_dir=calibration_dir
            )

        self.calibration: Optional[Dict[str, MotorCalibration]] = None
        self._last_known_positions: Dict[str, float] = {}

    @property
    def connected(self) -> bool:
        """Check if leader is connected."""
        return self._connected and self.bus is not None and self.bus.connected

    def connect(self, calibrate: bool = False) -> None:
        """
        Connect to the leader device.

        Args:
            calibrate: Whether to calibrate motors on connection
        """
        if self.connected:
            logger.warning("Leader is already connected")
            return

        # Load calibration if available (before initializing bus)
        calibration_path = self.config.calibration_dir / f"{self.config.id}.json"
        if calibration_path.exists():
            logger.info(f"Loading calibration from {calibration_path}")
            calib_data = load_calibration(calibration_path)
            self.calibration = {}
            for name, data in calib_data.items():
                self.calibration[name] = MotorCalibration(**data)
        else:
            logger.info("No calibration file found, using defaults")
            self.calibration = None

        # Initialize motor bus with calibration
        self.bus = FeetechMotorsBus(
            port=self.config.port, motors=self.motors, calibration=self.calibration
        )
        self.bus.connect()

        # Calibrate if requested
        if calibrate:
            self.calibrate()

        self._connected = True
        # Leader is passive (torque disabled) - user moves it manually
        # Torque remains disabled unless explicitly enabled
        # Read initial positions
        try:
            self._last_known_positions = self.get_action()
        except Exception:
            # If we can't read initial positions, start with empty dict
            self._last_known_positions = {}
        logger.info("Leader connected successfully (torque disabled - passive mode)")

    def get_action(self) -> Dict[str, float]:
        """
        Get current motor positions as action dictionary.

        Returns:
            Dictionary mapping motor names (with .pos suffix) to normalized position values
        """
        if not self.connected:
            raise DeviceNotConnectedError("Leader is not connected")

        try:
            # Read normalized positions (bus handles normalization automatically)
            action = self.bus.sync_read("Present_Position", normalize=True)
            action = {f"{motor}.pos": val for motor, val in action.items()}
            # Update last known positions on successful read
            self._last_known_positions = action
            return action
        except SerialException as e:
            logger.warning(f"Serial communication error reading leader positions: {e}. Using last known positions.")
            # Return last known positions if available, otherwise empty dict
            # Don't mark as disconnected - this could be a transient error
            return self._last_known_positions if self._last_known_positions else {}

    def get_observation(self) -> Dict[str, float]:
        """
        Get current motor positions as observation dictionary.

        Returns:
            Dictionary mapping motor names (with .pos suffix) to normalized position values
        """
        return self.get_action()

    @property
    def torque_enabled(self) -> bool:
        """Check if torque is enabled."""
        return self._torque_enabled

    @property
    def calibration_fpath(self) -> Path:
        """Get calibration file path."""
        return self.config.calibration_dir / f"{self.config.id}.json"
