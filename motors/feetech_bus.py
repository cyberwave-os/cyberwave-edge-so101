"""Feetech motor bus implementation using scservo_sdk."""

import logging
import math
import threading
import time
from typing import Dict, List, Optional

from errors import DeviceAlreadyConnectedError

from .bus import MotorsBus
from .encoding import decode_sign_magnitude, encode_sign_magnitude
from .models import Motor, MotorCalibration
from .tables import (
    ADDR_D_COEFFICIENT,
    ADDR_GOAL_POSITION,
    ADDR_HOMING_OFFSET,
    ADDR_I_COEFFICIENT,
    ADDR_MAX_POSITION_LIMIT,
    ADDR_MIN_POSITION_LIMIT,
    ADDR_OPERATING_MODE,
    ADDR_P_COEFFICIENT,
    ADDR_PRESENT_LOAD,
    ADDR_PRESENT_POSITION,
    ADDR_PRESENT_VELOCITY,
    ADDR_TORQUE_ENABLE,
    ENCODING_BIT_HOMING_OFFSET,
    ENCODING_BIT_VELOCITY,
    TORQUE_DISABLE,
    TORQUE_ENABLE,
)

logger = logging.getLogger(__name__)


def _split_into_byte_chunks(value: int, length: int) -> list[int]:
    """
    Helper to split an integer into little-endian byte chunks.
    """
    import scservo_sdk as scs

    if length == 1:
        data = [value]
    elif length == 2:
        data = [scs.SCS_LOBYTE(value), scs.SCS_HIBYTE(value)]
    elif length == 4:
        data = [
            scs.SCS_LOBYTE(scs.SCS_LOWORD(value)),
            scs.SCS_HIBYTE(scs.SCS_LOWORD(value)),
            scs.SCS_LOBYTE(scs.SCS_HIWORD(value)),
            scs.SCS_HIBYTE(scs.SCS_HIWORD(value)),
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

    def connect(self) -> None:
        """Connect to the motor bus."""
        if self._connected:
            raise DeviceAlreadyConnectedError(f"Motor bus on {self.port} is already connected")

        try:
            # Import scservo_sdk here to avoid dependency issues if not installed
            import scservo_sdk as scs
            from scservo_sdk import PacketHandler, PortHandler

            # Initialize port handler
            self._port_handler = PortHandler(self.port)
            self._port_handler.setBaudRate(self.baudrate)

            # Patch packet timeout to fix scservo_sdk bug
            def patch_setPacketTimeout(self, packet_length):  # noqa: N802
                """
                HACK: This patches the PortHandler behavior to set the correct packet timeouts.

                It fixes https://gitee.com/ftservo/SCServoSDK/issues/IBY2S6
                The bug is fixed on the official Feetech SDK repo (https://gitee.com/ftservo/FTServo_Python)
                but because that version is not published on PyPI, we rely on the (unofficial) one that is, which needs
                patching.
                """
                self.packet_start_time = self.getCurrentTime()
                self.packet_timeout = (
                    (self.tx_time_per_byte * packet_length) + (self.tx_time_per_byte * 3.0) + 50
                )

            self._port_handler.setPacketTimeout = patch_setPacketTimeout.__get__(
                self._port_handler, PortHandler
            )

            # Open port
            if not self._port_handler.openPort():
                raise ConnectionError(f"Failed to open port: {self.port}")

            # Initialize packet handler
            # STS3215 motors use protocol version 0
            self._packet_handler = PacketHandler(0)

            # Store constants for error checking
            self._comm_success = scs.COMM_SUCCESS
            self._no_error = 0x00

            # Initialize sync reader and writer
            # These are configured per-operation using _setup_sync_reader/_setup_sync_writer
            from scservo_sdk import GroupSyncRead, GroupSyncWrite

            self._sync_reader = GroupSyncRead(self._port_handler, self._packet_handler, 0, 0)
            self._sync_writer = GroupSyncWrite(self._port_handler, self._packet_handler, 0, 0)

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
        self._sync_reader.clearParam()
        self._sync_reader.start_address = addr
        self._sync_reader.data_length = length
        for motor_id in motor_ids:
            self._sync_reader.addParam(motor_id)

    def sync_read_positions(self, motor_ids: List[int], num_retry: int = 0) -> Dict[int, float]:
        """
        Synchronously read positions from multiple motors using batch read.

        Uses GroupSyncRead for efficient batch reading (single packet for all motors).

        Args:
            motor_ids: List of motor IDs to read from
            num_retry: Number of retry attempts on communication failure

        Returns:
            Dictionary mapping motor ID to position value
        """
        self._ensure_connected()

        if not motor_ids:
            return {}

        # Setup sync reader
        # Extract address and length from tuple
        addr, length = ADDR_PRESENT_POSITION[0], ADDR_PRESENT_POSITION[1]
        self._setup_sync_reader(motor_ids, addr, length)

        # Execute batch read with retries
        comm = None
        for n_try in range(1 + num_retry):
            comm = self._sync_reader.txRxPacket()
            if comm == self._comm_success:
                break
            logger.info(
                f"Failed to sync read @{addr=} ({length=}) on {motor_ids=} ({n_try=}): "
                + self.packet_handler.getTxRxResult(comm)
            )
            if n_try < num_retry:
                logger.debug(
                    f"sync_read_positions failed (try {n_try + 1}/{num_retry + 1}): {comm}"
                )

        positions: Dict[int, float] = {}
        if comm == self._comm_success:
            addr, length = ADDR_PRESENT_POSITION[0], ADDR_PRESENT_POSITION[1]
            for motor_id in motor_ids:
                position_raw = self._sync_reader.getData(motor_id, addr, length)
                position_value = float(position_raw)
                positions[motor_id] = position_value

        return positions

    def _sync_read_positions_sequential(
        self, motor_ids: List[int], num_retry: int = 0
    ) -> Dict[int, float]:
        """Fallback sequential read implementation."""
        positions: Dict[int, float] = {}
        for motor_id in motor_ids:
            addr, length = ADDR_PRESENT_POSITION[0], ADDR_PRESENT_POSITION[1]
            for n_try in range(1 + num_retry):
                # Read position using length from tuple to determine which read function to use
                if length == 1:
                    read_fn = self._packet_handler.read1ByteTxRx
                elif length == 2:
                    read_fn = self._packet_handler.read2ByteTxRx
                elif length == 4:
                    read_fn = self._packet_handler.read4ByteTxRx
                else:
                    raise ValueError(f"Unsupported length: {length}")

                position_raw, result, error = read_fn(self._port_handler, motor_id, addr)
                if result != self._comm_success:
                    if n_try < num_retry:
                        continue
                    break

                # Present_Position is UNSIGNED (0-4095) - NO sign-magnitude decoding
                # Only velocity and homing_offset use sign-magnitude encoding
                # Trust read2ByteTxRx value
                position_value = float(position_raw)
                positions[motor_id] = position_value
                break

        return positions

    def sync_read_velocities(self, motor_ids: List[int], num_retry: int = 0) -> Dict[int, float]:
        """
        Synchronously read velocities from multiple motors using batch read.

        Args:
            motor_ids: List of motor IDs to read from
            num_retry: Number of retry attempts on communication failure

        Returns:
            Dictionary mapping motor ID to velocity value (RPM, sign-magnitude encoded)
        """
        self._ensure_connected()

        if not motor_ids:
            return {}

        # Setup sync reader
        # Extract address and length from tuple
        addr, length = ADDR_PRESENT_VELOCITY[0], ADDR_PRESENT_VELOCITY[1]
        self._setup_sync_reader(motor_ids, addr, length)

        # Execute batch read with retries
        comm = None
        for n_try in range(1 + num_retry):
            comm = self._sync_reader.txRxPacket()
            if comm == self._comm_success:
                break
            if n_try < num_retry:
                logger.debug(
                    f"sync_read_velocities failed (try {n_try + 1}/{num_retry + 1}): {comm}"
                )

        velocities = {}
        if comm == self._comm_success:
            # Extract values from batch read
            # Just calls getData directly without checking isAvailable
            addr, length = ADDR_PRESENT_VELOCITY[0], ADDR_PRESENT_VELOCITY[1]
            for motor_id in motor_ids:
                velocity_raw = self._sync_reader.getData(motor_id, addr, length)
                # Decode sign-magnitude (bit 15 for velocity)
                velocity = decode_sign_magnitude(velocity_raw, sign_bit=ENCODING_BIT_VELOCITY)
                velocities[motor_id] = float(velocity)
        else:
            logger.warning(f"sync_read_velocities failed after {num_retry + 1} tries: {comm}")

        return velocities

    def _sync_read_velocities_sequential(
        self, motor_ids: List[int], num_retry: int = 0
    ) -> Dict[int, float]:
        """Fallback sequential read implementation."""
        velocities = {}
        for motor_id in motor_ids:
            addr, length = ADDR_PRESENT_VELOCITY[0], ADDR_PRESENT_VELOCITY[1]
            for n_try in range(1 + num_retry):
                # Read velocity using length from tuple to determine which read function to use
                if length == 1:
                    read_fn = self._packet_handler.read1ByteTxRx
                elif length == 2:
                    read_fn = self._packet_handler.read2ByteTxRx
                elif length == 4:
                    read_fn = self._packet_handler.read4ByteTxRx
                else:
                    raise ValueError(f"Unsupported length: {length}")

                velocity_raw, result, error = read_fn(self._port_handler, motor_id, addr)
                if result != self._comm_success:
                    if n_try < num_retry:
                        continue
                    break

                # Decode sign-magnitude (bit 15 for velocity)
                velocity = decode_sign_magnitude(velocity_raw, sign_bit=ENCODING_BIT_VELOCITY)
                velocities[motor_id] = float(velocity)
                break

        return velocities

    def sync_read_loads(self, motor_ids: List[int], num_retry: int = 0) -> Dict[int, float]:
        """
        Synchronously read loads (effort/torque) from multiple motors using batch read.

        Args:
            motor_ids: List of motor IDs to read from
            num_retry: Number of retry attempts on communication failure

        Returns:
            Dictionary mapping motor ID to load value (percentage, sign-magnitude encoded)
        """
        self._ensure_connected()

        if not motor_ids:
            return {}

        # Setup sync reader
        # Extract address and length from tuple
        addr, length = ADDR_PRESENT_LOAD[0], ADDR_PRESENT_LOAD[1]
        self._setup_sync_reader(motor_ids, addr, length)

        # Execute batch read with retries
        comm = None
        for n_try in range(1 + num_retry):
            comm = self._sync_reader.txRxPacket()
            if comm == self._comm_success:
                break
            if n_try < num_retry:
                logger.debug(f"sync_read_loads failed (try {n_try + 1}/{num_retry + 1}): {comm}")

        loads = {}
        if comm == self._comm_success:
            # Extract values from batch read
            # Just calls getData directly without checking isAvailable
            addr, length = ADDR_PRESENT_LOAD[0], ADDR_PRESENT_LOAD[1]
            for motor_id in motor_ids:
                load_raw = self._sync_reader.getData(motor_id, addr, length)
                # Decode sign-magnitude (bit 15 for load)
                # Load represents percentage of max torque, with sign indicating direction
                load = decode_sign_magnitude(load_raw, sign_bit=ENCODING_BIT_VELOCITY)
                loads[motor_id] = float(load)
        else:
            logger.warning(f"sync_read_loads failed after {num_retry + 1} tries: {comm}")

        return loads

    def _sync_read_loads_sequential(
        self, motor_ids: List[int], num_retry: int = 0
    ) -> Dict[int, float]:
        """Fallback sequential read implementation."""
        loads = {}
        for motor_id in motor_ids:
            addr, length = ADDR_PRESENT_LOAD[0], ADDR_PRESENT_LOAD[1]
            for n_try in range(1 + num_retry):
                # Read load using length from tuple to determine which read function to use
                if length == 1:
                    read_fn = self._packet_handler.read1ByteTxRx
                elif length == 2:
                    read_fn = self._packet_handler.read2ByteTxRx
                elif length == 4:
                    read_fn = self._packet_handler.read4ByteTxRx
                else:
                    raise ValueError(f"Unsupported length: {length}")

                load_raw, result, error = read_fn(self._port_handler, motor_id, addr)
                if result != self._comm_success:
                    if n_try < num_retry:
                        continue
                    break

                # Decode sign-magnitude (bit 15 for load)
                load = decode_sign_magnitude(load_raw, sign_bit=ENCODING_BIT_VELOCITY)
                loads[motor_id] = float(load)
                break

        return loads

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

    def sync_write_positions(self, motor_positions: Dict[int, float], num_retry: int = 0) -> None:
        """
        Synchronously write positions to multiple motors using batch write.

        Uses GroupSyncWrite for efficient batch writes (single packet for all motors).

        Args:
            motor_positions: Dictionary mapping motor ID to target position (raw encoder values 0-4095)
            num_retry: Number of retry attempts on communication failure
        """
        self._ensure_connected()

        if not motor_positions:
            return

        # Clamp positions to valid range (0-4095 for STS3215) before writing
        clamped_positions = {
            motor_id: max(0, min(4095, int(position)))
            for motor_id, position in motor_positions.items()
        }

        # Setup sync writer
        # Extract address and length from tuple
        addr, length = ADDR_GOAL_POSITION[0], ADDR_GOAL_POSITION[1]
        self._setup_sync_writer(clamped_positions, addr, length)

        # Execute batch write with retries
        comm = None
        for n_try in range(1 + num_retry):
            comm = self._sync_writer.txPacket()
            if comm == self._comm_success:
                break
            if n_try < num_retry:
                logger.debug(
                    f"sync_write_positions failed (try {n_try + 1}/{num_retry + 1}): {comm}"
                )

        if comm != self._comm_success:
            logger.warning(f"sync_write_positions failed after {num_retry + 1} tries: {comm}")

    def _sync_write_positions_sequential(
        self, motor_positions: Dict[int, float], num_retry: int = 0
    ) -> None:
        """Fallback sequential write implementation."""
        addr, length = ADDR_GOAL_POSITION[0], ADDR_GOAL_POSITION[1]
        for motor_id, position in motor_positions.items():
            position_int = int(max(0, min(4095, position)))
            data = _split_into_byte_chunks(position_int, length)

            for n_try in range(1 + num_retry):
                result, error = self._packet_handler.writeTxRx(
                    self._port_handler,
                    motor_id,
                    addr,
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

        addr, length = ADDR_TORQUE_ENABLE[0], ADDR_TORQUE_ENABLE[1]
        for motor_id in motor_ids:
            data = _split_into_byte_chunks(TORQUE_ENABLE, length)
            self._packet_handler.writeTxRx(self._port_handler, motor_id, addr, length, data)

    def disable_torque(self, motor_ids: Optional[List[int]] = None) -> None:
        """
        Disable torque for specified motors.

        Args:
            motor_ids: List of motor IDs. If None, disables all motors in self.motors.
        """
        self._ensure_connected()

        if motor_ids is None:
            motor_ids = [motor.id for motor in self.motors.values()]

        addr, length = ADDR_TORQUE_ENABLE[0], ADDR_TORQUE_ENABLE[1]
        for motor_id in motor_ids:
            data = _split_into_byte_chunks(TORQUE_DISABLE, length)
            self._packet_handler.writeTxRx(self._port_handler, motor_id, addr, length, data)

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
        self._ensure_connected()

        if motor_name not in self.motors:
            raise ValueError(f"Unknown motor name: {motor_name}")

        motor_id = self.motors[motor_name].id

        # Map register names to address tuples
        register_map = {
            "Operating_Mode": ADDR_OPERATING_MODE,
            "P_Coefficient": ADDR_P_COEFFICIENT,
            "I_Coefficient": ADDR_I_COEFFICIENT,
            "D_Coefficient": ADDR_D_COEFFICIENT,
        }

        if register_name not in register_map:
            raise ValueError(f"Unknown register: {register_name}")

        addr, length = register_map[register_name][0], register_map[register_name][1]
        data = _split_into_byte_chunks(value, length)
        result, error = self._packet_handler.writeTxRx(
            self._port_handler, motor_id, addr, length, data
        )

        # Check for communication errors
        if result != self._comm_success:
            logger.warning(
                f"Failed to write {register_name}={value} to {motor_name} (ID: {motor_id}): "
                f"result={result}, error={error}"
            )
            # Don't raise exception - allow operation to continue, but log the error

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
        motor_ids = [motor.id for motor in self.motors.values()]
        current_positions = self.sync_read_positions(motor_ids)

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

        return homing_offsets

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
        lines.append("\n" + "=" * 80)

        if show_min_max:
            lines.append(f"{'Motor':<20} {'ID':<5} {'Current':<12} {'Min':<12} {'Max':<12}")
            lines.append("-" * 80)
            lines.append(f"{'':<20} {'':<5} {'Raw':<12} {'Raw':<12} {'Raw':<12}")
        else:
            lines.append(f"{'Motor':<20} {'ID':<5} {'Current Position':<12}")
            lines.append("-" * 80)
            lines.append(f"{'':<20} {'':<5} {'Raw':<12}")

        lines.append("-" * 80)

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
                else:
                    min_raw = max(0.0, min(4095.0, min_raw))
                    min_raw_str = f"{min_raw:>9.1f}"

                if max_raw == float("-inf"):
                    max_raw_str = "   ---"
                else:
                    max_raw = max(0.0, min(4095.0, max_raw))
                    max_raw_str = f"{max_raw:>9.1f}"

                # Format values (raw only)
                lines.append(
                    f"{motor_name:<20} {motor.id:<5} "
                    f"{current_raw:>9.1f}   {min_raw_str}   {max_raw_str}"
                )
            else:
                # Format values (current only)
                lines.append(f"{motor_name:<20} {motor.id:<5} {current_raw:>9.1f}")

        lines.append("=" * 80)
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
        positions = self.sync_read_positions(motor_ids)

        current_positions = {}
        for motor_name, motor_id in zip(motor_names, motor_ids):
            current_positions[motor_name] = positions.get(motor_id, 0.0)

        display_text = self._format_calibration_display(
            motor_names, current_positions, show_min_max=False
        )
        print(display_text)

    def record_ranges_of_motion(self, motor_names: Optional[List[str]] = None) -> tuple:
        """
        Record min and max positions while user moves joints.

        Displays real-time table showing motor names, IDs, and current/min/max
        values in raw, degrees, and radians.

        Detects encoder wrap-around (4095 -> 0) and uses full range (0-4095) for
        motors that wrap.

        Args:
            motor_names: List of motor names to record. If None, records all motors.

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
        # Outlier detection: if a value is more than 1000 units away from median of recent values,
        # it's likely a communication error (especially for spurious low values like 0-10)
        OUTLIER_THRESHOLD = 1000.0

        # Thread-safe flag for stopping
        stop_recording = threading.Event()
        display_lock = threading.Lock()

        def read_loop():
            """Continuously read positions and update min/max."""
            motor_ids = {name: self.motors[name].id for name in motor_names}

            while not stop_recording.is_set():
                positions = self.sync_read_positions(list(motor_ids.values()))

                with display_lock:
                    for motor_name, motor_id in motor_ids.items():
                        if motor_id in positions:
                            pos = positions[motor_id]

                            # Simple outlier detection: compare with recent values
                            # This filters spurious readings (like 0-10) when motor is actually at 2000+
                            # if len(recent_positions[motor_name]) >= 3:
                            #     # Calculate median of recent positions
                            #     recent_sorted = sorted(recent_positions[motor_name])
                            #     median_pos = recent_sorted[len(recent_sorted) // 2]
                            #     # If new value is way off from median, it's likely a communication error
                            #     if abs(pos - median_pos) > OUTLIER_THRESHOLD:
                            #         # Skip this reading - it's likely noise
                            #         continue

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

                # Clear screen and move cursor to top (ANSI escape codes)
                sys.stdout.write("\033[2J\033[H")
                sys.stdout.write(display_text)
                sys.stdout.write("\n\nPress ENTER to stop recording...")
                sys.stdout.flush()

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
                range_mins[name] = 0.0
            if range_maxes[name] == float("-inf"):
                range_maxes[name] = 4095.0

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
        if data_name == "Present_Position":
            raw_values = self.sync_read_positions(motor_ids, num_retry=num_retry)
        else:
            raise NotImplementedError(f"sync_read not implemented for {data_name}")

        # Convert motor IDs to motor names
        id_to_name = {motor.id: name for name, motor in self.motors.items()}
        result = {}
        for motor_id, raw_value in raw_values.items():
            motor_name = id_to_name[motor_id]
            if (
                normalize
                and data_name == "Present_Position"
                and self.calibration
                and motor_name in self.calibration
            ):
                # Normalize using calibration
                from utils import convert_position_with_calibration

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

        if data_name != "Goal_Position":
            raise NotImplementedError(f"sync_write not implemented for {data_name}")

        # Convert normalized values to raw positions
        motor_positions = {}
        for motor_name, value in values.items():
            if motor_name not in self.motors:
                logger.warning(f"Unknown motor name: {motor_name}")
                continue

            motor = self.motors[motor_name]
            if normalize and self.calibration and motor_name in self.calibration:
                # Unnormalize using calibration
                from utils import denormalize_position

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
                    raw_position = denormalize_position(
                        normalized_position=value,
                        joint_name=motor_name,
                        calibration_data=calib_data,
                        norm_mode=motor.norm_mode,
                        drive_mode=calib.drive_mode,
                    )
                    motor_positions[motor.id] = raw_position
                except Exception as e:
                    logger.warning(f"Failed to unnormalize {motor_name}: {e}, skipping")
            else:
                # Use value as-is (assumed to be raw)
                motor_positions[motor.id] = value

        # Write raw positions
        if motor_positions:
            self.sync_write_positions(motor_positions, num_retry=num_retry)

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

            # Write min position limit using length from tuple
            min_addr, min_length = ADDR_MIN_POSITION_LIMIT[0], ADDR_MIN_POSITION_LIMIT[1]
            min_pos = int(calib.range_min)
            min_data = _split_into_byte_chunks(min_pos, min_length)
            self._packet_handler.writeTxRx(
                self._port_handler, motor_id, min_addr, min_length, min_data
            )

            # Write max position limit using length from tuple
            max_addr, max_length = ADDR_MAX_POSITION_LIMIT[0], ADDR_MAX_POSITION_LIMIT[1]
            max_pos = int(calib.range_max)
            max_data = _split_into_byte_chunks(max_pos, max_length)
            self._packet_handler.writeTxRx(
                self._port_handler, motor_id, max_addr, max_length, max_data
            )

            # Write homing offset using length from tuple (sign-magnitude encoded)
            encoded = encode_sign_magnitude(
                int(calib.homing_offset), sign_bit=ENCODING_BIT_HOMING_OFFSET
            )
            offset_addr, offset_length = ADDR_HOMING_OFFSET[0], ADDR_HOMING_OFFSET[1]
            offset_data = _split_into_byte_chunks(encoded, offset_length)
            self._packet_handler.writeTxRx(
                self._port_handler, motor_id, offset_addr, offset_length, offset_data
            )
