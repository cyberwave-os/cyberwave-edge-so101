"""Custom exceptions for so101_lib."""


class DeviceNotConnectedError(Exception):
    """Raised when attempting to use a device that is not connected."""

    pass


class DeviceAlreadyConnectedError(Exception):
    """Raised when attempting to connect a device that is already connected."""

    pass


class DeviceConnectionError(Exception):
    """Raised when a device connection fails.

    Attributes:
        error_type: Machine-readable error type (e.g., 'device_disconnected', 'preflight_failed')
        port: The port that failed to connect
        description: Human-readable error description
    """

    def __init__(
        self,
        message: str,
        *,
        error_type: str = "connection_failed",
        port: str | None = None,
        description: str | None = None,
    ):
        self.error_type = error_type
        self.port = port
        self.description = description or message
        super().__init__(message)


class InsufficientCalibrationRangeError(Exception):
    """Raised when calibration ranges are insufficient (below 5% or 20% thresholds)."""

    def __init__(self, warnings: list, severity: str):
        self.warnings = warnings
        self.severity = severity
        super().__init__(f"Calibration ranges insufficient: {warnings}")
