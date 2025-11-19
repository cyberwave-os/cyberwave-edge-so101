"""Base class for motor bus implementations."""

from abc import ABC, abstractmethod
from typing import Dict, List

from errors import DeviceNotConnectedError


class MotorsBus(ABC):
    """Abstract base class for motor bus implementations."""

    def __init__(self, port: str):
        """
        Initialize motor bus.

        Args:
            port: Serial port path (e.g., "/dev/tty.usbmodem123")
        """
        self.port = port
        self._connected = False

    @property
    def connected(self) -> bool:
        """Check if bus is connected."""
        return self._connected

    @abstractmethod
    def connect(self) -> None:
        """Connect to the motor bus."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the motor bus."""
        pass

    @abstractmethod
    def sync_read_positions(self, motor_ids: List[int]) -> Dict[int, float]:
        """
        Synchronously read positions from multiple motors.

        Args:
            motor_ids: List of motor IDs to read from

        Returns:
            Dictionary mapping motor ID to position value
        """
        pass

    @abstractmethod
    def sync_write_positions(
        self, motor_positions: Dict[int, float]
    ) -> None:
        """
        Synchronously write positions to multiple motors.

        Args:
            motor_positions: Dictionary mapping motor ID to target position
        """
        pass

    def _ensure_connected(self) -> None:
        """Ensure bus is connected, raise error if not."""
        if not self._connected:
            raise DeviceNotConnectedError(
                f"Motor bus on {self.port} is not connected"
            )

