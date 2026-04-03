"""Feetech motor bus implementation using scservo_sdk."""

import logging
import math
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

from utils.errors import DeviceAlreadyConnectedError, DeviceConnectionError

from .bus import MotorsBus
from .encoding import decode_sign_magnitude, encode_sign_magnitude
from .models import Motor, MotorCalibration
from .registers import (
    POSITION_REGISTER_NAMES,
    decode_register_value,
    encode_register_value,
    get_register_spec,
    iter_register_specs,
)
from .tables import (
    ADDR_D_COEFFICIENT,
    ADDR_FIRMWARE_MAJOR_VERSION,
    ADDR_FIRMWARE_MINOR_VERSION,
    ADDR_GOAL_POSITION,
    ADDR_HOMING_OFFSET,
    ADDR_I_COEFFICIENT,
    ADDR_MAX_POSITION_LIMIT,
    ADDR_MAX_TORQUE_LIMIT,
    ADDR_MIN_POSITION_LIMIT,
    ADDR_MODEL_NUMBER,
    ADDR_OPERATING_MODE,
    ADDR_OVERLOAD_TORQUE,
    ADDR_P_COEFFICIENT,
    ADDR_PRESENT_LOAD,
    ADDR_PRESENT_POSITION,
    ADDR_PRESENT_VELOCITY,
    ADDR_PROTECTION_CURRENT,
    ADDR_TORQUE_ENABLE,
    ENCODING_BIT_HOMING_OFFSET,
    ENCODING_BIT_VELOCITY,
    TORQUE_DISABLE,
    TORQUE_ENABLE,
)

logger = logging.getLogger(__name__)

EXPECTED_MODEL_NUMBERS = {
    "STS3215": 777,
}


def _split_into_byte_chunks(value: int, length: int) -> list[int]:
    """
    Helper to split an integer into little-endian byte chunks.
    """
    if length == 1:
        data = [value & 0xFF]
    elif length == 2:
        data = [value & 0xFF, (value >> 8) & 0xFF]
    elif length == 4:
        data = [
            value & 0xFF,
            (value >> 8) & 0xFF,
            (value >> 16) & 0xFF,
            (value >> 24) & 0xFF,
        ]
    else:
        raise ValueError(f"Unsupported byte length: {length}")

    return data


class FeetechMotorsBus(MotorsBus):
    """Feetech motor bus implementation using scservo_sdk."""

    def __init__(
        self,
        port: str,
        baudrate: int = 1000000,
        motors: Optional[Dict[str, Motor]] = None,
        calibration: Optional[Dict[str, MotorCalibration]] = None,
    ):
        """
        Initialize Feetech motor bus.

        Args:
            port: Serial port path (e.g., "/dev/tty.usbmodem123")
            baudrate: Serial communication baudrate (default: 1000000)
            motors: Optional dictionary mapping motor names to Motor objects
            calibration: Optional dictionary mapping motor names to MotorCalibration objects
        """
        super().__init__(port)
        self.baudrate = baudrate
        self._port_handler = None
        self._packet_handler = None
        self._sync_reader = None  # GroupSyncRead for batch reads
        self._sync_writer = None  # GroupSyncWrite for batch writes
        self.motors = motors or {}
        self.calibration = calibration if calibration else {}
        self._io_lock = threading.RLock()
        self._calibration_starting_positions: Dict[
            str, float
        ] = {}  # Track starting positions for calibration quality check

    @staticmethod
    def _patch_port_handler(port_handler) -> None:
        """
        Patch PortHandler to fix scservo_sdk bug.

        HACK: This patches the PortHandler behavior to set the correct packet timeouts.
        It fixes https://gitee.com/ftservo/SCServoSDK/issues/IBY2S6
        The bug is fixed on the official Feetech SDK repo (https://gitee.com/ftservo/FTServo_Python)
        but because that version is not published on PyPI, we rely on the (unofficial) one that is, which needs
        patching.

        Args:
            port_handler: PortHandler instance to patch
        """
        from scservo_sdk import PortHandler

        def patch_setPacketTimeout(self, packet_length):  # noqa: N802
            """Patched setPacketTimeout method."""
            self.packet_start_time = self.getCurrentTime()
            self.packet_timeout = (
                (self.tx_time_per_byte * packet_length) + (self.tx_time_per_byte * 3.0) + 50
            )

        port_handler.setPacketTimeout = patch_setPacketTimeout.__get__(port_handler, PortHandler)

    def _clear_serial_buffers(self, port_handler) -> None:
        """
        Clear input and output buffers on the serial port using PortHandler.

        Args:
            port_handler: Already opened PortHandler instance
        """
        try:
            # Try to clear port buffers if method exists
            if hasattr(port_handler, "clearPort"):
                port_handler.clearPort()
                logger.debug(f"Cleared buffers on {self.port}")
            else:
                # If clearPort doesn't exist, just log
                logger.debug("clearPort() not available on PortHandler")
        except Exception as e:
            logger.debug(f"Could not clear buffers: {e}")

    def _resync_motor_protocol(self, port_handler, packet_handler, motor_id: int = 1) -> bool:
        """
        Attempt to resync with motor by sending ping commands.
        This helps clear any misaligned protocol state.

        Uses protocol version 0 (same as FeetechMotorsBus for STS3215 motors).

        Args:
            port_handler: Already opened PortHandler instance
            packet_handler: PacketHandler instance
            motor_id: Motor ID to ping (default: 1)

        Returns:
            True if ping succeeded, False otherwise
        """
        try:
            # Clear buffers first if method exists
            if hasattr(port_handler, "clearPort"):
                port_handler.clearPort()
            time.sleep(0.1)

            # Send ping to motor - this is a minimal command to test communication
            # Ping typically gets a response, helping resync protocol
            ping_result = packet_handler.ping(port_handler, motor_id)
            return ping_result == 0  # Return True if ping succeeded
        except Exception as e:
            logger.debug(f"Resync failed for motor {motor_id}: {e}")
            return False

    def _preflight_check_motors(self, max_retries: int = 3) -> bool:
        """
        Complete pre-flight check before calibration:
        1. Open port and clear buffers
        2. Verify each expected motor responds and has the expected model number
        3. Verify all responding motors report the same firmware version

        This avoids rejecting otherwise reachable motors based on raw position reads.

        Args:
            max_retries: Maximum number of retry attempts per motor

        Returns:
            True if the handshake succeeds for all configured motors, False otherwise
        """
        logger.info("Running pre-flight motor check...")

        if not self.motors:
            logger.warning("No motors configured, skipping preflight check")
            return True

        motor_ids = [motor.id for motor in self.motors.values()]
        if not motor_ids:
            logger.warning("No motor IDs found, skipping preflight check")
            return True

        try:
            import scservo_sdk as scs
            from scservo_sdk import PacketHandler, PortHandler
        except ImportError:
            logger.warning("scservo_sdk not available, skipping motor verification")
            return True

        port_handler = None
        try:
            port_handler = PortHandler(self.port)
            port_handler.setBaudRate(self.baudrate)

            # Apply patch BEFORE opening port (consistent with connect())
            self._patch_port_handler(port_handler)

            if not port_handler.openPort():
                logger.error(f"Could not open port {self.port}")
                return False

            time.sleep(0.3)  # Let port stabilize after opening

            # Clear buffers using PortHandler
            self._clear_serial_buffers(port_handler)
            time.sleep(0.2)

            # Use protocol version 0 (same as FeetechMotorsBus for STS3215 motors)
            packet_handler = PacketHandler(0)
            comm_success = scs.COMM_SUCCESS

            def read_preflight_register(
                motor_id: int,
                addr: int,
                length: int,
                label: str,
            ) -> tuple[Optional[int], str]:
                if length == 1:
                    read_fn = packet_handler.read1ByteTxRx
                elif length == 2:
                    read_fn = packet_handler.read2ByteTxRx
                elif length == 4:
                    read_fn = packet_handler.read4ByteTxRx
                else:
                    return None, f"Unsupported register length for {label}: {length}"

                last_error = f"Unknown read failure for {label}"
                for attempt in range(max_retries):
                    try:
                        value, result, error = read_fn(port_handler, motor_id, addr)
                        if result == comm_success and error == 0:
                            return int(value), ""

                        result_msg = (
                            packet_handler.getTxRxResult(result)
                            if hasattr(packet_handler, "getTxRxResult")
                            else f"result={result}"
                        )
                        if error and hasattr(packet_handler, "getRxPacketError"):
                            packet_msg = packet_handler.getRxPacketError(error)
                            last_error = f"{result_msg}; packet_error={packet_msg}"
                        elif error:
                            last_error = f"{result_msg}; packet_error={error}"
                        else:
                            last_error = result_msg

                        logger.debug(
                            "Motor %s %s read attempt %s/%s failed: %s",
                            motor_id,
                            label,
                            attempt + 1,
                            max_retries,
                            last_error,
                        )
                    except Exception as e:
                        last_error = str(e)
                        logger.debug(
                            "Motor %s %s read attempt %s/%s raised exception: %s",
                            motor_id,
                            label,
                            attempt + 1,
                            max_retries,
                            e,
                        )
                    time.sleep(0.15)

                return None, last_error

            expected_models = {}
            for motor in self.motors.values():
                expected_model = EXPECTED_MODEL_NUMBERS.get(motor.model.upper())
                if expected_model is not None:
                    expected_models[motor.id] = expected_model
                else:
                    logger.warning(
                        "No expected model number configured for motor model %s; "
                        "skipping model-number validation in preflight",
                        motor.model,
                    )

            found_models: Dict[int, int] = {}
            firmware_versions: Dict[int, str] = {}
            missing_ids: List[int] = []
            wrong_models: Dict[int, tuple[int, int]] = {}

            for motor in self.motors.values():
                motor_id = motor.id

                model_number, model_error = read_preflight_register(
                    motor_id,
                    ADDR_MODEL_NUMBER[0],
                    ADDR_MODEL_NUMBER[1],
                    "Model_Number",
                )
                if model_number is None:
                    missing_ids.append(motor_id)
                    logger.error(
                        "Motor %s: ✗ NO RESPONSE while reading Model_Number after %s attempts (%s)",
                        motor_id,
                        max_retries,
                        model_error,
                    )
                    continue

                found_models[motor_id] = model_number
                expected_model = expected_models.get(motor_id)
                if expected_model is not None and model_number != expected_model:
                    wrong_models[motor_id] = (expected_model, model_number)
                    logger.error(
                        "Motor %s: ✗ wrong model number (expected %s, found %s)",
                        motor_id,
                        expected_model,
                        model_number,
                    )
                    continue

                firmware_major, major_error = read_preflight_register(
                    motor_id,
                    ADDR_FIRMWARE_MAJOR_VERSION[0],
                    ADDR_FIRMWARE_MAJOR_VERSION[1],
                    "Firmware_Major_Version",
                )
                firmware_minor, minor_error = read_preflight_register(
                    motor_id,
                    ADDR_FIRMWARE_MINOR_VERSION[0],
                    ADDR_FIRMWARE_MINOR_VERSION[1],
                    "Firmware_Minor_Version",
                )
                if firmware_major is None or firmware_minor is None:
                    missing_ids.append(motor_id)
                    logger.error(
                        "Motor %s: ✗ firmware read failed (%s / %s)",
                        motor_id,
                        major_error,
                        minor_error,
                    )
                    continue

                firmware_versions[motor_id] = f"{firmware_major}.{firmware_minor}"
                logger.debug(
                    "Motor %s: ✓ OK (model=%s firmware=%s)",
                    motor_id,
                    model_number,
                    firmware_versions[motor_id],
                )

            if missing_ids:
                logger.error("Missing motor IDs during preflight: %s", sorted(set(missing_ids)))
                return False

            if wrong_models:
                logger.error("Motors with incorrect model numbers: %s", wrong_models)
                return False

            if firmware_versions and len(set(firmware_versions.values())) != 1:
                logger.error("Firmware version mismatch across motors: %s", firmware_versions)
                return False

            logger.info(
                "All motors responded with expected model number and matching firmware%s",
                f" ({next(iter(firmware_versions.values()))})" if firmware_versions else "",
            )
            return True

        except Exception as e:
            logger.error(f"Preflight check failed: {e}", exc_info=True)
            # Check if this is a "device not found" error (port doesn't exist)
            error_str = str(e).lower()
            if "no such file or directory" in error_str or "could not open port" in error_str:
                raise DeviceConnectionError(
                    f"Device not found on {self.port}",
                    error_type="device_disconnected",
                    port=self.port,
                    description=(
                        f"Device not found on {self.port}. "
                        "Check that the robot is connected and powered on."
                    ),
                ) from e
            # For other errors, return False to let connect() raise preflight_failed
            return False
        finally:
            if port_handler:
                try:
                    port_handler.closePort()
                except Exception:
                    pass

    def connect(self, preflight_check: bool = True) -> None:
        """
        Connect to the motor bus.

        Args:
            preflight_check: If True, run preflight check before connecting.
                            This clears buffers and verifies the expected motors/firmware respond.
                            Default is True.
        """
        if self._connected:
            raise DeviceAlreadyConnectedError(f"Motor bus on {self.port} is already connected")

        # Run preflight check before connecting (clears buffers, resyncs protocol, verifies motors)
        if preflight_check:
            logger.info("Running pre-flight check before connecting...")
            if not self._preflight_check_motors():
                motor_ids = [motor.id for motor in self.motors.values()] if self.motors else []
                raise DeviceConnectionError(
                    f"Pre-flight check failed for motors {motor_ids} on {self.port}. "
                    "Check connections and power.",
                    error_type="preflight_failed",
                    port=self.port,
                    description=(
                        f"Cannot connect to motors on {self.port}. "
                        "Check that all motors are powered and connected."
                    ),
                )
            logger.info("Pre-flight check passed ✓")
            # Small delay to ensure port is fully released before reopening
            time.sleep(0.2)

        try:
            # Import scservo_sdk here to avoid dependency issues if not installed
            import scservo_sdk as scs
            from scservo_sdk import PacketHandler, PortHandler

            # Initialize port handler
            self._port_handler = PortHandler(self.port)
            self._port_handler.setBaudRate(self.baudrate)

            # Patch packet timeout to fix scservo_sdk bug
            self._patch_port_handler(self._port_handler)

            # Open port
            if not self._port_handler.openPort():
                raise DeviceConnectionError(
                    f"Failed to open port: {self.port}",
                    error_type="port_open_failed",
                    port=self.port,
                    description=(
                        f"Cannot open port {self.port}. "
                        "Check that the device is connected and not in use by another application."
                    ),
                )

            time.sleep(0.2)  # Let port stabilize after opening

            # Initialize packet handler
            # STS3215 motors use protocol version 0
            self._packet_handler = PacketHandler(0)

            # Store constants for error checking
            self._comm_success = scs.COMM_SUCCESS
            self._no_error = 0x00

            # Initialize sync reader and writer
            # These are configured per-operation using _setup_sync_reader/_setup_sync_writer
            # Recreate sync reader/writer on each connection to avoid caching issues
            from scservo_sdk import GroupSyncRead, GroupSyncWrite

            self._sync_reader = GroupSyncRead(self._port_handler, self._packet_handler, 0, 0)
            self._sync_writer = GroupSyncWrite(self._port_handler, self._packet_handler, 0, 0)

            # Clear buffers after opening to remove any stale data from preflight check
            self._clear_serial_buffers(self._port_handler)
            time.sleep(0.2)

            # Ensure sync reader is properly initialized
            self._sync_reader.clearParam()

            self._connected = True
        except ImportError as e:
            raise ImportError(
                "scservo_sdk is required. Install it with: pip install scservo_sdk"
            ) from e

    def disconnect(self) -> None:
        """Disconnect from the motor bus."""
        if not self._connected:
            return

        if self._port_handler:
            self._port_handler.closePort()
            self._port_handler = None
            self._packet_handler = None
            self._sync_reader = None
            self._sync_writer = None

        self._connected = False

    def _setup_sync_reader(self, motor_ids: List[int], addr: int, length: int) -> None:
        """
        Setup sync reader for batch read operation

        Args:
            motor_ids: List of motor IDs to read from
            addr: Register address to read from
            length: Number of bytes to read
        """
        # Clear parameters to ensure no cached data is used
        self._sync_reader.clearParam()
        self._sync_reader.start_address = addr
        self._sync_reader.data_length = length
        for motor_id in motor_ids:
            self._sync_reader.addParam(motor_id)

    def _read_function_for_length(self, length: int) -> Callable[..., Any]:
        """Return the packet-handler read function matching a register length."""
        if length == 1:
            return self._packet_handler.read1ByteTxRx
        if length == 2:
            return self._packet_handler.read2ByteTxRx
        if length == 4:
            return self._packet_handler.read4ByteTxRx
        raise ValueError(f"Unsupported register length: {length}")

    def _resolve_motor_id(self, motor_name_or_id: str | int) -> int:
        """Resolve either a motor name or a motor ID into a motor ID."""
        if isinstance(motor_name_or_id, int):
            return motor_name_or_id
        if motor_name_or_id not in self.motors:
            raise ValueError(f"Unknown motor name: {motor_name_or_id}")
        return self.motors[motor_name_or_id].id

    def _resolve_motor_name(self, motor_id: int) -> str:
        """Resolve a motor ID back to the configured motor name."""
        for motor_name, motor in self.motors.items():
            if motor.id == motor_id:
                return motor_name
        raise ValueError(f"Unknown motor ID: {motor_id}")

    def list_registers(
        self,
        *,
        readable: bool | None = None,
        writable: bool | None = None,
    ) -> List[str]:
        """Return canonical register names supported by the bus wrapper."""
        return [spec.name for spec in iter_register_specs(readable=readable, writable=writable)]

    def ping(self, motor_name_or_id: str | int, num_retry: int = 0) -> bool:
        """Ping a motor and return True when communication succeeds."""
        self._ensure_connected()
        motor_id = self._resolve_motor_id(motor_name_or_id)
        with self._io_lock:
            for _ in range(1 + num_retry):
                try:
                    result = self._packet_handler.ping(self._port_handler, motor_id)
                    if result == self._comm_success:
                        return True
                except Exception:
                    continue
        return False

    def read_register(
        self,
        register_name: str,
        motor_name: str,
        *,
        decode: bool = True,
        num_retry: int = 0,
    ) -> int:
        """Read one register from one named motor."""
        return self.read_register_by_id(
            register_name,
            self._resolve_motor_id(motor_name),
            decode=decode,
            num_retry=num_retry,
        )

    def read_register_by_id(
        self,
        register_name: str,
        motor_id: int,
        *,
        decode: bool = True,
        num_retry: int = 0,
    ) -> int:
        """Read one register from one motor ID."""
        self._ensure_connected()
        register_spec = get_register_spec(register_name)
        if not register_spec.readable:
            raise ValueError(f"Register {register_name} is not readable")
        read_fn = self._read_function_for_length(register_spec.length)

        with self._io_lock:
            for n_try in range(1 + num_retry):
                raw_value, result, error = read_fn(
                    self._port_handler, motor_id, register_spec.address
                )
                if result == self._comm_success:
                    return (
                        decode_register_value(register_spec, raw_value) if decode else int(raw_value)
                    )
                if n_try < num_retry:
                    continue

        raise RuntimeError(
            f"Failed to read {register_name} from motor {motor_id}: result={result}, error={error}"
        )

    def sync_read_register(
        self,
        register_name: str,
        motor_ids: List[int],
        *,
        num_retry: int = 0,
        use_sequential: bool = False,
        decode: bool = True,
    ) -> Dict[int, int]:
        """Synchronously read one register from multiple motors."""
        self._ensure_connected()
        if not motor_ids:
            return {}

        register_spec = get_register_spec(register_name)
        if not register_spec.readable:
            raise ValueError(f"Register {register_name} is not readable")

        if use_sequential:
            return self._sync_read_register_sequential(
                register_spec.name, motor_ids, num_retry=num_retry, decode=decode
            )

        with self._io_lock:
            self._sync_reader.clearParam()
            self._setup_sync_reader(motor_ids, register_spec.address, register_spec.length)

            comm = None
            for n_try in range(1 + num_retry):
                comm = self._sync_reader.txRxPacket()
                if comm == self._comm_success:
                    break
                if n_try < num_retry:
                    logger.debug(
                        "sync_read_register(%s) failed (try %d/%d): %s",
                        register_name,
                        n_try + 1,
                        num_retry + 1,
                        comm,
                    )

            if comm != self._comm_success:
                logger.warning(
                    "sync_read_register(%s) failed after %d tries: %s",
                    register_name,
                    num_retry + 1,
                    comm,
                )
                return {}

            values: Dict[int, int] = {}
            for motor_id in motor_ids:
                raw_value = self._sync_reader.getData(
                    motor_id, register_spec.address, register_spec.length
                )
                values[motor_id] = (
                    decode_register_value(register_spec, raw_value) if decode else int(raw_value)
                )
            return values

    def _sync_read_register_sequential(
        self,
        register_name: str,
        motor_ids: List[int],
        *,
        num_retry: int = 0,
        decode: bool = True,
    ) -> Dict[int, int]:
        """Fallback sequential register read implementation."""
        self._ensure_connected()
        register_spec = get_register_spec(register_name)
        read_fn = self._read_function_for_length(register_spec.length)
        values: Dict[int, int] = {}

        with self._io_lock:
            for motor_id in motor_ids:
                for n_try in range(1 + num_retry):
                    raw_value, result, error = read_fn(
                        self._port_handler, motor_id, register_spec.address
                    )
                    if result == self._comm_success:
                        values[motor_id] = (
                            decode_register_value(register_spec, raw_value)
                            if decode
                            else int(raw_value)
                        )
                        break
                    if n_try < num_retry:
                        continue
                    logger.debug(
                        "Sequential read of %s failed for motor %s: result=%s error=%s",
                        register_name,
                        motor_id,
                        result,
                        error,
                    )

        return values

    def read_all_registers(
        self,
        *,
        motors: Optional[Sequence[str]] = None,
        registers: Optional[Sequence[str]] = None,
        decode: bool = True,
        use_sequential: bool = False,
        num_retry: int = 0,
    ) -> Dict[str, Dict[str, int]]:
        """Read a snapshot of multiple registers for multiple motors."""
        self._ensure_connected()

        motor_names = list(motors) if motors is not None else list(self.motors.keys())
        if not motor_names:
            return {}

        register_names = (
            [spec.name for spec in iter_register_specs(readable=True)]
            if registers is None
            else list(registers)
        )
        motor_ids = [self._resolve_motor_id(motor_name) for motor_name in motor_names]
        id_to_name = {self.motors[motor_name].id: motor_name for motor_name in motor_names}
        snapshot: Dict[str, Dict[str, int]] = {motor_name: {} for motor_name in motor_names}

        for register_name in register_names:
            register_values = self.sync_read_register(
                register_name,
                motor_ids,
                num_retry=num_retry,
                use_sequential=use_sequential,
                decode=decode,
            )
            for motor_id, value in register_values.items():
                snapshot[id_to_name[motor_id]][register_name] = value

        return snapshot

    def sync_read_positions(
        self, motor_ids: List[int], num_retry: int = 0, use_sequential: bool = False
    ) -> Dict[int, float]:
        """
        Synchronously read positions from multiple motors using batch read.

        Uses GroupSyncRead for efficient batch reading (single packet for all motors).
        Can fall back to sequential reads to avoid caching issues.

        Args:
            motor_ids: List of motor IDs to read from
            num_retry: Number of retry attempts on communication failure
            use_sequential: If True, use sequential reads instead of sync read (avoids caching)

        Returns:
            Dictionary mapping motor ID to position value
        """
        return {
            motor_id: float(value)
            for motor_id, value in self.sync_read_register(
                "Present_Position",
                motor_ids,
                num_retry=num_retry,
                use_sequential=use_sequential,
                decode=True,
            ).items()
        }

    def _sync_read_positions_sequential(
        self, motor_ids: List[int], num_retry: int = 0
    ) -> Dict[int, float]:
        """Fallback sequential read implementation."""
        return {
            motor_id: float(value)
            for motor_id, value in self._sync_read_register_sequential(
                "Present_Position", motor_ids, num_retry=num_retry, decode=True
            ).items()
        }

    def sync_read_velocities(self, motor_ids: List[int], num_retry: int = 0) -> Dict[int, float]:
        """
        Synchronously read velocities from multiple motors using batch read.

        Args:
            motor_ids: List of motor IDs to read from
            num_retry: Number of retry attempts on communication failure

        Returns:
            Dictionary mapping motor ID to velocity value (RPM, sign-magnitude encoded)
        """
        return {
            motor_id: float(value)
            for motor_id, value in self.sync_read_register(
                "Present_Velocity", motor_ids, num_retry=num_retry, decode=True
            ).items()
        }

    def _sync_read_velocities_sequential(
        self, motor_ids: List[int], num_retry: int = 0
    ) -> Dict[int, float]:
        """Fallback sequential read implementation."""
        return {
            motor_id: float(value)
            for motor_id, value in self._sync_read_register_sequential(
                "Present_Velocity", motor_ids, num_retry=num_retry, decode=True
            ).items()
        }

    def sync_read_loads(self, motor_ids: List[int], num_retry: int = 0) -> Dict[int, float]:
        """
        Synchronously read loads (effort/torque) from multiple motors using batch read.

        Args:
            motor_ids: List of motor IDs to read from
            num_retry: Number of retry attempts on communication failure

        Returns:
            Dictionary mapping motor ID to load value (percentage, sign-magnitude encoded)
        """
        return {
            motor_id: float(value)
            for motor_id, value in self.sync_read_register(
                "Present_Load", motor_ids, num_retry=num_retry, decode=True
            ).items()
        }

    def _sync_read_loads_sequential(
        self, motor_ids: List[int], num_retry: int = 0
    ) -> Dict[int, float]:
        """Fallback sequential read implementation."""
        return {
            motor_id: float(value)
            for motor_id, value in self._sync_read_register_sequential(
                "Present_Load", motor_ids, num_retry=num_retry, decode=True
            ).items()
        }

    def _setup_sync_writer(self, motor_positions: Dict[int, float], addr: int, length: int) -> None:
        """
        Setup sync writer for batch write operation

        Args:
            motor_positions: Dictionary mapping motor ID to value
            addr: Register address to write to
            length: Number of bytes to write
        """
        self._sync_writer.clearParam()
        self._sync_writer.start_address = addr
        self._sync_writer.data_length = length
        for motor_id, value in motor_positions.items():
            # Serialize value as little-endian bytes
            data = _split_into_byte_chunks(int(value), length)
            self._sync_writer.addParam(motor_id, data)

    def sync_write_register(
        self,
        register_name: str,
        motor_values: Dict[int, int | float | bool],
        *,
        num_retry: int = 0,
    ) -> None:
        """Synchronously write one register on multiple motors."""
        self._ensure_connected()
        if not motor_values:
            return

        register_spec = get_register_spec(register_name)
        if not register_spec.writable:
            raise ValueError(f"Register {register_name} is not writable")

        encoded_values = {
            motor_id: encode_register_value(register_spec, value)
            for motor_id, value in motor_values.items()
        }

        with self._io_lock:
            self._setup_sync_writer(
                encoded_values, register_spec.address, register_spec.length
            )

            comm = None
            for n_try in range(1 + num_retry):
                comm = self._sync_writer.txPacket()
                if comm == self._comm_success:
                    return
                if n_try < num_retry:
                    logger.debug(
                        "sync_write_register(%s) failed (try %d/%d): %s",
                        register_name,
                        n_try + 1,
                        num_retry + 1,
                        comm,
                    )

        logger.warning(
            "sync_write_register(%s) failed after %d tries: %s",
            register_name,
            num_retry + 1,
            comm,
        )

    def sync_write_positions(self, motor_positions: Dict[int, float], num_retry: int = 0) -> None:
        """
        Synchronously write positions to multiple motors using batch write.

        Uses GroupSyncWrite for efficient batch writes (single packet for all motors).

        Args:
            motor_positions: Dictionary mapping motor ID to target position (raw encoder values 0-4095)
            num_retry: Number of retry attempts on communication failure
        """
        self.sync_write_register("Goal_Position", motor_positions, num_retry=num_retry)

    def _sync_write_positions_sequential(
        self, motor_positions: Dict[int, float], num_retry: int = 0
    ) -> None:
        """Fallback sequential write implementation."""
        register_spec = get_register_spec("Goal_Position")
        with self._io_lock:
            for motor_id, position in motor_positions.items():
                position_int = encode_register_value(register_spec, position)
                data = _split_into_byte_chunks(position_int, register_spec.length)

                for n_try in range(1 + num_retry):
                    result, error = self._packet_handler.writeTxRx(
                        self._port_handler,
                        motor_id,
                        register_spec.address,
                        len(data),
                        data,
                    )
                    if result == self._comm_success:
                        break
                    if n_try < num_retry:
                        continue

    def enable_torque(self, motor_ids: Optional[List[int]] = None) -> None:
        """
        Enable torque for specified motors.

        Args:
            motor_ids: List of motor IDs. If None, enables all motors in self.motors.
        """
        self._ensure_connected()

        if motor_ids is None:
            motor_ids = [motor.id for motor in self.motors.values()]

        self.sync_write_register(
            "Torque_Enable",
            {motor_id: TORQUE_ENABLE for motor_id in motor_ids},
        )

    def disable_torque(self, motor_ids: Optional[List[int]] = None) -> None:
        """
        Disable torque for specified motors.

        Args:
            motor_ids: List of motor IDs. If None, disables all motors in self.motors.
        """
        self._ensure_connected()

        if motor_ids is None:
            motor_ids = [motor.id for motor in self.motors.values()]

        self.sync_write_register(
            "Torque_Enable",
            {motor_id: TORQUE_DISABLE for motor_id in motor_ids},
        )

    def write(self, register_name: str, motor_name: str, value: int) -> None:
        """
        Write a value to a motor register.

        Args:
            register_name: Name of the register (e.g., "Operating_Mode", "P_Coefficient")
            motor_name: Name of the motor (key in self.motors dict)
            value: Value to write

        Raises:
            ValueError: If motor_name or register_name is invalid
            RuntimeError: If write fails after retries
        """
        self.write_register(register_name, motor_name, value)

    def write_register(
        self,
        register_name: str,
        motor_name: str,
        value: int | float | bool,
        *,
        num_retry: int = 0,
    ) -> None:
        """Write a named register on one motor."""
        self.write_register_by_id(
            register_name,
            self._resolve_motor_id(motor_name),
            value,
            num_retry=num_retry,
        )

    def write_register_by_id(
        self,
        register_name: str,
        motor_id: int,
        value: int | float | bool,
        *,
        num_retry: int = 0,
    ) -> None:
        """Write a named register on one motor identified by ID."""
        self._ensure_connected()
        register_spec = get_register_spec(register_name)
        if not register_spec.writable:
            raise ValueError(f"Register {register_name} is not writable")

        encoded_value = encode_register_value(register_spec, value)
        data = _split_into_byte_chunks(encoded_value, register_spec.length)

        with self._io_lock:
            result = None
            error = None
            for n_try in range(1 + num_retry):
                result, error = self._packet_handler.writeTxRx(
                    self._port_handler,
                    motor_id,
                    register_spec.address,
                    register_spec.length,
                    data,
                )
                if result == self._comm_success:
                    return
                if n_try < num_retry:
                    continue

        logger.warning(
            "Failed to write %s=%s to motor %s: result=%s error=%s",
            register_name,
            value,
            motor_id,
            result,
            error,
        )

    def reset_homing_offsets(self) -> None:
        """
        Reset homing offsets to 0 for all motors.

        This ensures we read true raw positions without any offset applied.
        Should be called at the start of calibration to ensure consistent readings.
        """
        self._ensure_connected()

        for motor_name, motor in self.motors.items():
            try:
                self.write_register_by_id("Homing_Offset", motor.id, 0)
                logger.debug(f"Reset homing offset for {motor_name} (ID: {motor.id})")
            except Exception as e:
                logger.warning(f"Failed to reset homing offset for {motor_name}: {e}")

    def set_half_turn_homings(self) -> Dict[str, float]:
        """
        Set homing offsets based on current position to center at half turn (2048).

        The homing offset is calculated as: 2048 - current_position
        This makes the current position become the "zero" position (2048) after applying the offset.

        Returns:
            Dictionary mapping motor names to homing offset values
        """
        self._ensure_connected()

        # Read current positions
        # Use sequential reads for calibration to avoid GroupSyncRead caching issues
        motor_ids = [motor.id for motor in self.motors.values()]
        current_positions = self.sync_read_positions(motor_ids, use_sequential=True)

        half_turn = 2048  # Half of 4096 (full range) - the desired "zero" position
        homing_offsets = {}

        for motor_name, motor in self.motors.items():
            if motor.id not in current_positions:
                logger.warning(
                    f"Could not read position for {motor_name} (ID: {motor.id}), skipping"
                )
                continue

            current_pos = current_positions[motor.id]

            # Calculate homing offset: offset = current_position - desired_zero
            # This makes current_position - offset = desired_zero (2048)
            # Note: The motor applies offset as: effective_position = raw_position - homing_offset
            # So: current_pos - homing_offset = 2048, therefore: homing_offset = current_pos - 2048
            homing_offset = current_pos - half_turn

            # Encode with sign-magnitude (bit 11 for homing offset)
            encoded = encode_sign_magnitude(int(homing_offset), sign_bit=ENCODING_BIT_HOMING_OFFSET)

            # Write homing offset using length from tuple to determine which write function to use
            addr, length = ADDR_HOMING_OFFSET[0], ADDR_HOMING_OFFSET[1]
            data = _split_into_byte_chunks(encoded, length)
            self._packet_handler.writeTxRx(self._port_handler, motor.id, addr, length, data)

            homing_offsets[motor_name] = float(homing_offset)
            logger.debug(
                f"{motor_name} (ID: {motor.id}): current_pos={current_pos:.1f}, "
                f"homing_offset={homing_offset:.1f}"
            )

        # Store starting positions AFTER homing offset is applied
        # Read the actual displayed positions (raw values after offset)
        time.sleep(0.1)  # Brief delay for offsets to take effect
        self._calibration_starting_positions = {}
        post_offset_positions = self.sync_read_positions(motor_ids, use_sequential=True)
        for motor_name, motor in self.motors.items():
            if motor.id in post_offset_positions:
                # Store the raw position as displayed (after homing offset is applied)
                self._calibration_starting_positions[motor_name] = post_offset_positions[motor.id]

        return homing_offsets

    def _check_calibration_quality(
        self,
        motor_name: str,
        range_min: float,
        range_max: float,
    ) -> bool:
        """
        Check if a motor has been properly calibrated based on range coverage.

        Uses the raw positions as displayed in calibration (after homing offset is applied).

        For RANGE_M100_100 motors: Checks if range covers ±10% of starting position
        For RANGE_0_100 motors: Checks if range covers +10% of starting position

        Args:
            motor_name: Name of the motor
            range_min: Minimum recorded position (raw, as displayed)
            range_max: Maximum recorded position (raw, as displayed)

        Returns:
            True if calibration is adequate, False otherwise
        """
        if motor_name not in self._calibration_starting_positions:
            return False  # No starting position recorded

        motor = self.motors.get(motor_name)
        if not motor:
            return False

        starting_pos = self._calibration_starting_positions[motor_name]  # Raw position as displayed
        full_range = 4095.0
        threshold = full_range * 0.10  # 10% of full range = 409.5

        if motor.norm_mode.value == "range_0_100":
            # For 0_100 range, check if max covers +10% of starting position
            upper_boundary = starting_pos + threshold
            return range_max >= upper_boundary
        elif motor.norm_mode.value == "range_m100_100":
            # For m100_100 range, check if range covers ±10% of starting position
            upper_boundary = starting_pos + threshold
            lower_boundary = starting_pos - threshold
            return range_min <= lower_boundary and range_max >= upper_boundary
        else:
            # For other modes (degrees), use m100_100 logic
            upper_boundary = starting_pos + threshold
            lower_boundary = starting_pos - threshold
            return range_min <= lower_boundary and range_max >= upper_boundary

    def _raw_to_degrees(self, raw_value: float) -> float:
        """
        Convert raw position (0-4095) to degrees (0-360) for display purposes only.

        This is a simple linear mapping for visualization during calibration.
        It does NOT use calibration data and is different from the actual normalization
        used in teleoperation (which uses calibration ranges and normalization modes).

        For actual position conversion in teleoperation, use convert_position_with_calibration()
        in teleoperate.py which applies proper normalization based on calibration data.
        """
        return (raw_value / 4095.0) * 360.0

    def _raw_to_radians(self, raw_value: float) -> float:
        """
        Convert raw position (0-4095) to radians (0-2π) for display purposes only.

        This is a simple linear mapping for visualization during calibration.
        It does NOT use calibration data and is different from the actual normalization
        used in teleoperation (which uses calibration ranges and normalization modes).

        For actual position conversion in teleoperation, use convert_position_with_calibration()
        in teleoperate.py which applies proper normalization based on calibration data.
        """
        return (raw_value / 4095.0) * 2.0 * math.pi

    def _format_calibration_display(
        self,
        motor_names: List[str],
        current_positions: Dict[str, float],
        range_mins: Optional[Dict[str, float]] = None,
        range_maxes: Optional[Dict[str, float]] = None,
        show_min_max: bool = True,
    ) -> str:
        """
        Format calibration display table showing only raw values (0-4095).

        Args:
            motor_names: List of motor names to display
            current_positions: Dictionary of current positions
            range_mins: Optional dictionary of min positions
            range_maxes: Optional dictionary of max positions
            show_min_max: Whether to show min/max columns
        """
        lines = []
        lines.append("\n" + "=" * 90)

        if show_min_max:
            lines.append(
                f"{'Motor':<20} {'ID':<5} {'Current':<12} {'Min':<12} {'Max':<12} {'Status':<8}"
            )
            lines.append("-" * 90)
            lines.append(f"{'':<20} {'':<5} {'Raw':<12} {'Raw':<12} {'Raw':<12} {'':<8}")
        else:
            lines.append(f"{'Motor':<20} {'ID':<5} {'Current Position':<12}")
            lines.append("-" * 80)
            lines.append(f"{'':<20} {'':<5} {'Raw':<12}")

        lines.append("-" * 90)

        for motor_name in motor_names:
            motor = self.motors[motor_name]
            current_raw = current_positions.get(motor_name, 0.0)
            # Clamp to valid range for display
            current_raw = max(0.0, min(4095.0, current_raw))

            if show_min_max and range_mins is not None and range_maxes is not None:
                min_raw = range_mins.get(motor_name, 0.0)
                max_raw = range_maxes.get(motor_name, 0.0)

                # Handle inf values
                if min_raw == float("inf"):
                    min_raw_str = "   ---"
                    is_calibrated = False
                else:
                    min_raw = max(0.0, min(4095.0, min_raw))
                    min_raw_str = f"{min_raw:>9.1f}"

                if max_raw == float("-inf"):
                    max_raw_str = "   ---"
                    is_calibrated = False
                else:
                    max_raw = max(0.0, min(4095.0, max_raw))
                    max_raw_str = f"{max_raw:>9.1f}"

                # Check calibration quality
                if min_raw != float("inf") and max_raw != float("-inf"):
                    is_calibrated = self._check_calibration_quality(motor_name, min_raw, max_raw)
                else:
                    is_calibrated = False

                # Status indicator: 🟡 (yellow) for good calibration, 🔴 (red) for incomplete
                status_indicator = "🟡" if is_calibrated else "🔴"

                # Format values (raw only) with status indicator
                lines.append(
                    f"{motor_name:<20} {motor.id:<5} "
                    f"{current_raw:>9.1f}   {min_raw_str}   {max_raw_str}   {status_indicator}"
                )
            else:
                # Format values (current only)
                lines.append(f"{motor_name:<20} {motor.id:<5} {current_raw:>9.1f}")

        lines.append("=" * 90)
        return "\n".join(lines)

    def display_current_positions(self, motor_names: Optional[List[str]] = None) -> None:
        """
        Display current motor positions in a formatted table.

        Args:
            motor_names: List of motor names to display. If None, displays all motors.
        """
        self._ensure_connected()

        if motor_names is None:
            motor_names = list(self.motors.keys())

        motor_ids = [self.motors[name].id for name in motor_names]
        # Use sequential reads for calibration to avoid GroupSyncRead caching issues
        positions = self.sync_read_positions(motor_ids, use_sequential=True)

        current_positions = {}
        for motor_name, motor_id in zip(motor_names, motor_ids):
            current_positions[motor_name] = positions.get(motor_id, 0.0)

        display_text = self._format_calibration_display(
            motor_names, current_positions, show_min_max=False
        )
        print(display_text)

    def record_ranges_of_motion(
        self,
        motor_names: Optional[List[str]] = None,
        on_progress: Optional[
            Callable[[Dict[str, float], Dict[str, float], Dict[str, float]], None]
        ] = None,
    ) -> tuple:
        """
        Record min and max positions while user moves joints.

        Displays real-time table showing motor names, IDs, and current/min/max
        values in raw, degrees, and radians.

        Detects encoder wrap-around (4095 -> 0) and uses full range (0-4095) for
        motors that wrap.

        Args:
            motor_names: List of motor names to record. If None, records all motors.
            on_progress: Optional callback(current_positions, range_mins, range_maxes)
                called periodically during recording (e.g. for alert updates).

        Returns:
            Tuple of (range_mins, range_maxes) dictionaries mapping motor names to values
        """
        self._ensure_connected()

        if motor_names is None:
            motor_names = list(self.motors.keys())

        # Initialize min/max tracking
        range_mins = dict.fromkeys(motor_names, float("inf"))
        range_maxes = dict.fromkeys(motor_names, float("-inf"))
        current_positions = dict.fromkeys(motor_names, 0.0)
        # Track recent positions for outlier detection (simple median-like filtering)
        # Keep last N positions to detect spurious readings
        recent_positions = {name: [] for name in motor_names}
        RECENT_WINDOW_SIZE = 5  # Keep last 5 readings

        # Thread-safe flag for stopping
        stop_recording = threading.Event()
        display_lock = threading.Lock()

        def read_loop():
            """Continuously read positions and update min/max."""
            motor_ids = {name: self.motors[name].id for name in motor_names}

            while not stop_recording.is_set():
                # Use sequential reads for calibration to avoid GroupSyncRead caching issues
                positions = self.sync_read_positions(list(motor_ids.values()), use_sequential=True)

                with display_lock:
                    for motor_name, motor_id in motor_ids.items():
                        if motor_id in positions:
                            pos = positions[motor_id]

                            # Update recent positions window
                            recent_positions[motor_name].append(pos)
                            if len(recent_positions[motor_name]) > RECENT_WINDOW_SIZE:
                                recent_positions[motor_name].pop(0)

                            current_positions[motor_name] = pos

                            # Update min/max to record the actual range of motion
                            if pos < range_mins[motor_name]:
                                range_mins[motor_name] = pos
                            if pos > range_maxes[motor_name]:
                                range_maxes[motor_name] = pos

                time.sleep(0.01)  # 100 Hz sampling rate

        def display_loop():
            """Continuously update and display calibration table."""
            import sys

            while not stop_recording.is_set():
                with display_lock:
                    display_text = self._format_calibration_display(
                        motor_names, current_positions, range_mins, range_maxes
                    )

                    # Check for invalid ranges and add warnings to display
                    warnings_text = ""
                    try:
                        from utils.utils import (
                            format_calibration_warnings,
                            validate_calibration_ranges,
                        )

                        # Create temporary dicts with current values (replace inf with actual values for validation)
                        temp_mins = {
                            name: range_mins[name] if range_mins[name] != float("inf") else 0.0
                            for name in motor_names
                        }
                        temp_maxes = {
                            name: range_maxes[name]
                            if range_maxes[name] != float("-inf")
                            else 4095.0
                            for name in motor_names
                        }
                        motors_dict = {
                            name: self.motors[name] for name in motor_names if name in self.motors
                        }

                        # Check if there are invalid ranges
                        if motors_dict:
                            invalid_joints = validate_calibration_ranges(
                                temp_mins, temp_maxes, motors_dict
                            )

                            # Format warnings text if invalid joints found (without action required for real-time display)
                            if invalid_joints:
                                warnings_text = format_calibration_warnings(
                                    invalid_joints, motors_dict, include_action_required=False
                                )
                    except ImportError:
                        pass  # Skip validation if utils not available

                # Clear screen and move cursor to top (ANSI escape codes)
                sys.stdout.write("\033[2J\033[H")
                sys.stdout.write(display_text)
                if warnings_text:
                    sys.stdout.write(warnings_text)
                sys.stdout.write("\n\nPress ENTER to stop recording...")
                sys.stdout.flush()

                if on_progress is not None:
                    try:
                        on_progress(
                            dict(current_positions),
                            dict(range_mins),
                            dict(range_maxes),
                        )
                    except Exception:
                        pass  # Don't let callback break calibration

                time.sleep(0.1)  # Update display at 10 Hz

        # Start recording and display threads
        recording_thread = threading.Thread(target=read_loop, daemon=True)
        display_thread = threading.Thread(target=display_loop, daemon=True)

        recording_thread.start()
        display_thread.start()

        # Wait for user to press Enter
        input()

        # Stop recording
        stop_recording.set()
        recording_thread.join(timeout=1.0)
        display_thread.join(timeout=1.0)

        # Clear display
        import sys

        sys.stdout.write("\033[2J\033[H")
        sys.stdout.flush()

        # Replace inf values with defaults if no data was recorded
        for name in motor_names:
            if range_mins[name] == float("inf"):
                range_mins[name] = 10.0
            if range_maxes[name] == float("-inf"):
                range_maxes[name] = 4000.0

        return range_mins, range_maxes

    def sync_read(
        self,
        data_name: str,
        motors: Optional[List[str]] = None,
        normalize: bool = True,
        num_retry: int = 0,
    ) -> Dict[str, float]:
        """
        Read the same register from several motors at once.

        Args:
            data_name: Register name (e.g., "Present_Position")
            motors: List of motor names to read. If None, reads all motors.
            normalize: If True (default), normalize values using calibration
            num_retry: Number of retry attempts on communication failure

        Returns:
            Dictionary mapping motor names to values (normalized if normalize=True)
        """
        self._ensure_connected()

        if motors is None:
            motors = list(self.motors.keys())

        # Read raw values
        motor_ids = [self.motors[motor].id for motor in motors]
        raw_values = self.sync_read_register(data_name, motor_ids, num_retry=num_retry, decode=True)

        # Convert motor IDs to motor names
        id_to_name = {motor.id: name for name, motor in self.motors.items()}
        result = {}
        for motor_id, raw_value in raw_values.items():
            motor_name = id_to_name[motor_id]
            if (
                normalize
                and data_name in POSITION_REGISTER_NAMES
                and self.calibration
                and motor_name in self.calibration
            ):
                # Normalize using calibration
                from utils.utils import convert_position_with_calibration

                motor = self.motors[motor_name]
                calib = self.calibration[motor_name]
                calib_data = {
                    motor_name: {
                        "id": calib.id,
                        "drive_mode": calib.drive_mode,
                        "range_min": calib.range_min,
                        "range_max": calib.range_max,
                    }
                }
                try:
                    normalized = convert_position_with_calibration(
                        raw_position=raw_value,
                        joint_name=motor_name,
                        calibration_data=calib_data,
                        norm_mode=motor.norm_mode,
                        use_radians=False,
                        drive_mode=calib.drive_mode,
                    )
                    result[motor_name] = normalized
                except Exception as e:
                    logger.warning(f"Failed to normalize {motor_name}: {e}, using raw value")
                    result[motor_name] = raw_value
            else:
                result[motor_name] = raw_value

        return result

    def sync_write(
        self,
        data_name: str,
        values: Dict[str, float],
        normalize: bool = True,
        num_retry: int = 0,
    ) -> None:
        """
        Write the same register on multiple motors.

        Args:
            data_name: Register name (e.g., "Goal_Position")
            values: Dictionary mapping motor names to values (normalized if normalize=True)
            normalize: If True (default), unnormalize values using calibration before writing
            num_retry: Number of retry attempts on communication failure
        """
        self._ensure_connected()

        register_spec = get_register_spec(data_name)
        if not register_spec.writable:
            raise NotImplementedError(f"sync_write not implemented for {data_name}")

        motor_positions = {}
        for motor_name, value in values.items():
            if motor_name not in self.motors:
                logger.warning(f"Unknown motor name: {motor_name}")
                continue

            motor = self.motors[motor_name]
            if (
                normalize
                and data_name in POSITION_REGISTER_NAMES
                and self.calibration
                and motor_name in self.calibration
            ):
                from utils.utils import denormalize_position

                calib = self.calibration[motor_name]
                calib_data = {
                    motor_name: {
                        "id": calib.id,
                        "drive_mode": calib.drive_mode,
                        "range_min": calib.range_min,
                        "range_max": calib.range_max,
                    }
                }
                try:
                    value = denormalize_position(
                        normalized_position=float(value),
                        joint_name=motor_name,
                        calibration_data=calib_data,
                        norm_mode=motor.norm_mode,
                        drive_mode=calib.drive_mode,
                    )
                except Exception as e:
                    logger.warning(f"Failed to unnormalize {motor_name}: {e}, skipping")
                    continue

            motor_positions[motor.id] = value

        if motor_positions:
            self.sync_write_register(data_name, motor_positions, num_retry=num_retry)

    def write_calibration(self, calibration: Dict[str, MotorCalibration]) -> None:
        """
        Write calibration data to motor EEPROM.

        Args:
            calibration: Dictionary mapping motor names to MotorCalibration objects
        """
        self._ensure_connected()

        for motor_name, calib in calibration.items():
            if motor_name not in self.motors:
                continue

            motor_id = self.motors[motor_name].id
            self.write_register_by_id("Min_Position_Limit", motor_id, calib.range_min)
            self.write_register_by_id("Max_Position_Limit", motor_id, calib.range_max)
            self.write_register_by_id("Homing_Offset", motor_id, calib.homing_offset)
