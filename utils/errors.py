"""Custom exceptions for so101_lib."""


class DeviceNotConnectedError(Exception):
    """Raised when attempting to use a device that is not connected."""

    pass


class DeviceAlreadyConnectedError(Exception):
    """Raised when attempting to connect a device that is already connected."""

    pass
