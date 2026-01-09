"""Teleoperation loop for SO101 leader and follower."""

import argparse
import asyncio
import logging
import math
import queue
import select
import sys
import threading
import time
from typing import Dict, Optional

from dotenv import load_dotenv

from cyberwave import Cyberwave, Twin
from cyberwave.utils import TimeReference

from follower import SO101Follower
from leader import SO101Leader
from motors import MotorNormMode

logger = logging.getLogger(__name__)


class StatusTracker:
    """Thread-safe status tracker for teleoperation system."""

    def __init__(self):
        self.lock = threading.Lock()
        self.script_started = False
        self.mqtt_connected = False
        self.camera_detected = False
        self.camera_started = False
        # WebRTC states: "idle" (red), "connecting" (yellow), "streaming" (green)
        self.webrtc_state = "idle"
        self.fps = 0
        self.camera_fps = 0
        self.messages_produced = 0
        self.messages_filtered = 0
        self.errors = 0
        self.joint_states: Dict[str, float] = {}
        self.joint_index_to_name: Dict[str, str] = {}
        self.robot_uuid: str = ""
        self.robot_name: str = ""
        self.camera_uuid: str = ""
        self.camera_name: str = ""

    def update_mqtt_status(self, connected: bool):
        with self.lock:
            self.mqtt_connected = connected

    def update_camera_status(self, detected: bool, started: bool = False):
        with self.lock:
            self.camera_detected = detected
            self.camera_started = started

    def update_webrtc_state(self, state: str):
        """Update WebRTC state: 'idle', 'connecting', or 'streaming'."""
        with self.lock:
            self.webrtc_state = state

    def increment_produced(self):
        with self.lock:
            self.messages_produced += 1

    def increment_filtered(self):
        with self.lock:
            self.messages_filtered += 1

    def increment_errors(self):
        with self.lock:
            self.errors += 1

    def update_joint_states(self, states: Dict[str, float]):
        """Merge new joint states with existing ones (doesn't replace)."""
        with self.lock:
            self.joint_states.update(states)

    def set_joint_index_to_name(self, mapping: Dict[str, str]):
        """Set mapping from joint index to joint name."""
        with self.lock:
            self.joint_index_to_name = mapping.copy()

    def set_twin_info(self, robot_uuid: str, robot_name: str, camera_uuid: str, camera_name: str):
        """Set twin information for display."""
        with self.lock:
            self.robot_uuid = robot_uuid
            self.robot_name = robot_name
            self.camera_uuid = camera_uuid
            self.camera_name = camera_name

    def get_status(self) -> Dict:
        """Get a snapshot of current status."""
        with self.lock:
            return {
                "script_started": self.script_started,
                "mqtt_connected": self.mqtt_connected,
                "camera_detected": self.camera_detected,
                "camera_started": self.camera_started,
                "webrtc_state": self.webrtc_state,
                "fps": self.fps,
                "camera_fps": self.camera_fps,
                "messages_produced": self.messages_produced,
                "messages_filtered": self.messages_filtered,
                "errors": self.errors,
                "joint_states": self.joint_states.copy(),
                "robot_uuid": self.robot_uuid,
                "robot_name": self.robot_name,
                "camera_uuid": self.camera_uuid,
                "camera_name": self.camera_name,
            }


def _camera_worker_thread(
    client: Cyberwave,
    camera_id: int,
    fps: int,
    twin_uuid: str,
    stop_event: threading.Event,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
) -> None:
    """
    Worker thread that handles camera streaming.

    Runs an async event loop in a separate thread to handle camera streaming.
    Uses CameraStreamer.run_with_auto_reconnect() for automatic command handling
    and reconnection.

    Args:
        client: Cyberwave client instance
        camera_id: Camera index to stream
        fps: Frames per second for camera stream
        twin_uuid: UUID of the twin to stream to
        stop_event: Event to signal thread to stop
        time_reference: TimeReference instance
        status_tracker: Optional status tracker for camera status updates
    Returns:
        None
    """
    if status_tracker:
        status_tracker.update_camera_status(detected=True, started=False)

    async def _run_camera_streamer():
        """Async function that runs the camera streamer with auto-reconnect."""
        # Create async stop event from threading.Event
        async_stop_event = asyncio.Event()

        # Create camera streamer using the SDK API
        streamer = client.video_stream(
            twin_uuid=twin_uuid,
            camera_id=camera_id,
            fps=fps,
            time_reference=time_reference,
            sensor_type="rgb",
        )

        # Monitor the threading stop_event and set async_stop_event
        async def monitor_stop():
            while not stop_event.is_set():
                await asyncio.sleep(0.1)
            async_stop_event.set()

        # Start the monitor task
        monitor_task = asyncio.create_task(monitor_stop())

        def command_callback(status: str, msg: str):
            """Callback to track WebRTC state from commands."""
            if status_tracker:
                if "started" in msg.lower() or status == "ok":
                    status_tracker.update_webrtc_state("streaming")
                elif "stopped" in msg.lower():
                    status_tracker.update_webrtc_state("idle")

        try:
            # Update status when camera starts
            if status_tracker:
                status_tracker.update_camera_status(detected=True, started=True)
                # WebRTC starts in connecting state when camera worker begins
                status_tracker.update_webrtc_state("connecting")

            # Run with auto-reconnect - this handles all command subscriptions internally
            await streamer.run_with_auto_reconnect(
                stop_event=async_stop_event,
                command_callback=command_callback,
            )
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

    # Run async function in event loop
    loop = None
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(_run_camera_streamer())
    except Exception:
        if status_tracker:
            status_tracker.increment_errors()
    finally:
        if loop is not None:
            try:
                loop.close()
            except Exception:
                pass


def cyberwave_update_worker(
    action_queue: queue.Queue,
    joint_name_to_index: Dict[str, int],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    use_radians: bool,
    stop_event: threading.Event,
    twin: Optional[Twin] = None,
    time_reference: TimeReference = None,
    status_tracker: Optional[StatusTracker] = None,
) -> None:
    """
    Worker thread that processes actions from the queue and updates Cyberwave twin.

    Batches multiple joint updates together and skips updates where position is 0.0.
    Velocity and effort are hardcoded to 0.0 to avoid issues.

    Args:
        action_queue: Queue containing (joint_name, action_data) tuples where action_data
                     is a dict with 'position', 'velocity', 'load', and 'timestamp' keys
        joint_name_to_index: Dictionary mapping joint names to joint indexes (1-6)
        joint_name_to_norm_mode: Dictionary mapping joint names to normalization modes
        use_radians: Whether to convert positions to radians (only for DEGREES mode)
        stop_event: Event to signal thread to stop
        twin: Optional Twin instance for updating joint states
        time_reference: TimeReference instance
        status_tracker: Optional status tracker for statistics
    Returns:
        None
    """
    processed_count = 0
    error_count = 0
    batch_timeout = 0.01  # 10ms - collect updates for batching

    while not stop_event.is_set():
        try:
            # Collect multiple joint updates for batching
            batch_updates = {}  # joint_index -> (position, velocity, effort, timestamp)
            batch_start_time = time.time()

            # Collect updates until timeout or queue is empty
            while True:
                try:
                    # Try to get an update with short timeout
                    remaining_time = batch_timeout - (time.time() - batch_start_time)
                    if remaining_time <= 0:
                        break

                    joint_name, action_data = action_queue.get(timeout=min(remaining_time, 0.001))
                except queue.Empty:
                    # No more items available, process what we have
                    break

                try:
                    # Get joint index from joint name
                    joint_index = joint_name_to_index.get(joint_name)
                    if joint_index is None:
                        action_queue.task_done()
                        continue

                    # Extract normalized position from action_data (velocity and effort are hardcoded to 0.0)
                    normalized_position = action_data.get("position", 0.0)
                    timestamp = action_data.get("timestamp")  # Extract timestamp from action_data

                    # Get normalization mode for this joint
                    norm_mode = joint_name_to_norm_mode.get(joint_name, MotorNormMode.DEGREES)

                    # Convert normalized position to degrees/radians for Cyberwave
                    # The position is already normalized, so we need to convert it to degrees/radians
                    # based on the normalization mode
                    if norm_mode == MotorNormMode.DEGREES:
                        # Already in degrees, just convert to radians if needed
                        position = normalized_position
                        if use_radians:
                            import math

                            position = normalized_position * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_M100_100:
                        # Convert from -100 to 100 range to degrees
                        # Assuming full range maps to 360 degrees
                        position_degrees = (
                            normalized_position / 100.0
                        ) * 180.0  # -100 to 100 -> -180 to 180 degrees
                        if use_radians:
                            import math

                            position = position_degrees * math.pi / 180.0
                        else:
                            position = position_degrees
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        # Convert from 0 to 100 range to degrees
                        # Assuming full range maps to 360 degrees
                        position_degrees = (
                            normalized_position / 100.0
                        ) * 360.0  # 0 to 100 -> 0 to 360 degrees
                        if use_radians:
                            import math

                            position = position_degrees * math.pi / 180.0
                        else:
                            position = position_degrees
                    else:
                        # Default: use as-is
                        position = normalized_position

                    # Hardcode velocity and effort to 0.0 to avoid issues
                    velocity = 0.0
                    effort = 0.0

                    # Store in batch (overwrite if same joint appears multiple times)
                    batch_updates[joint_index] = (position, velocity, effort, timestamp)
                    action_queue.task_done()

                except Exception:
                    error_count += 1
                    if status_tracker:
                        status_tracker.increment_errors()
                    action_queue.task_done()

            # Send batched updates
            if batch_updates:
                try:
                    # Send all joints in the batch
                    joint_states = {}
                    for joint_index, (position, _, _, timestamp) in batch_updates.items():
                        twin.joints.set(
                            joint_name=str(joint_index),
                            position=position,
                            degrees=False,
                            timestamp=timestamp,
                        )
                        joint_states[str(joint_index)] = position

                    processed_count += len(batch_updates)
                    if status_tracker:
                        status_tracker.increment_produced()
                        status_tracker.update_joint_states(joint_states)
                except Exception:
                    error_count += len(batch_updates)
                    if status_tracker:
                        status_tracker.increment_errors()

        except Exception:
            error_count += 1
            if status_tracker:
                status_tracker.increment_errors()


def _process_cyberwave_updates(
    action: Dict[str, float],
    last_observation: Dict[str, float],
    action_queue: queue.Queue,
    position_threshold: float,
    velocity_threshold: float,
    effort_threshold: float,
    timestamp: float,
    status_tracker: Optional[StatusTracker] = None,
    last_send_times: Optional[Dict[str, float]] = None,
    heartbeat_interval: float = 1.0,
) -> tuple[int, int]:
    """
    Process follower observation and queue Cyberwave updates for changed joints.

    Follower observation contains normalized positions. If a joint hasn't been sent
    for heartbeat_interval seconds, it will be sent anyway as a heartbeat.

    Args:
        action: Follower observation dictionary with normalized positions (keys have .pos suffix)
        last_observation: Dictionary tracking last sent observation state (normalized positions)
        action_queue: Queue for Cyberwave updates
        position_threshold: Minimum change in normalized position to trigger update
        velocity_threshold: Unused (kept for compatibility)
        effort_threshold: Unused (kept for compatibility)
        timestamp: Timestamp to associate with this update (generated in teleop loop)
        status_tracker: Optional status tracker for statistics
        last_send_times: Dictionary tracking last send time per joint (for heartbeat)
        heartbeat_interval: Interval in seconds to send heartbeat if no changes (default 1.0)
    Returns:
        Tuple of (update_count, skip_count)
    """
    update_count = 0
    skip_count = 0
    current_time = time.time()

    for joint_key, normalized_pos in action.items():
        # Extract joint name from key (remove .pos suffix if present)
        joint_name = joint_key.removesuffix(".pos") if joint_key.endswith(".pos") else joint_key

        if joint_name not in last_observation:
            # New joint, initialize and send
            last_observation[joint_name] = float("inf")

        if last_send_times is not None and joint_name not in last_send_times:
            last_send_times[joint_name] = 0.0

        last_obs = last_observation[joint_name]

        # Check if position has changed beyond threshold (using normalized values)
        pos_changed = abs(normalized_pos - last_obs) >= position_threshold

        # Force first update (when last_obs is inf)
        is_first_update = last_obs == float("inf")

        # Check if heartbeat is needed (no update sent for heartbeat_interval)
        needs_heartbeat = False
        if last_send_times is not None:
            time_since_last_send = current_time - last_send_times.get(joint_name, 0.0)
            needs_heartbeat = time_since_last_send >= heartbeat_interval

        if is_first_update or pos_changed or needs_heartbeat:
            # Update last observation (store normalized position)
            last_observation[joint_name] = normalized_pos

            # Update last send time
            if last_send_times is not None:
                last_send_times[joint_name] = current_time

            # Queue action for Cyberwave update (non-blocking)
            # Format: (joint_name, {"position": normalized_pos, "velocity": 0.0, "load": 0.0, "timestamp": timestamp})
            # Worker thread will handle conversion to degrees/radians
            action_data = {
                "position": normalized_pos,
                "velocity": 0.0,  # Hardcoded to 0.0
                "load": 0.0,  # Hardcoded to 0.0
                "timestamp": timestamp,  # Add timestamp from teleop loop
            }
            try:
                action_queue.put_nowait((joint_name, action_data))
                update_count += 1
            except queue.Full:
                if status_tracker:
                    status_tracker.increment_errors()
                continue
        else:
            skip_count += 1
            if status_tracker:
                status_tracker.increment_filtered()

    return update_count, skip_count


def _status_logging_thread(
    status_tracker: StatusTracker,
    stop_event: threading.Event,
    fps: int,
    camera_fps: int,
) -> None:
    """
    Thread that logs status information at 1 fps.

    Args:
        status_tracker: StatusTracker instance
        stop_event: Event to signal thread to stop
        fps: Target frames per second for teleoperation loop
        camera_fps: Frames per second for camera streaming
    """
    status_tracker.fps = fps
    status_tracker.camera_fps = camera_fps
    status_interval = 1.0  # Update status at 1 fps

    # Hide cursor and save position
    sys.stdout.write("\033[?25l")  # Hide cursor
    sys.stdout.flush()

    try:
        while not stop_event.is_set():
            status = status_tracker.get_status()

            # Build status display with fixed width lines
            lines = []
            lines.append("=" * 70)
            lines.append("SO101 Teleoperation Status".center(70))
            lines.append("=" * 70)

            # Twin info
            robot_name = status["robot_name"] or "N/A"
            camera_name = status["camera_name"] or "N/A"
            lines.append(f"Robot:  {robot_name} ({status['robot_uuid']})"[:70].ljust(70))
            lines.append(f"Camera: {camera_name} ({status['camera_uuid']})"[:70].ljust(70))
            lines.append("-" * 70)

            # Status indicators
            script_icon = "🟢" if status["script_started"] else "🟡"
            mqtt_icon = "🟢" if status["mqtt_connected"] else "🔴"
            if not status["camera_detected"]:
                camera_icon = "🔴"
            elif not status["camera_started"]:
                camera_icon = "🟡"
            else:
                camera_icon = "🟢"
            # WebRTC: idle=red, connecting=yellow, streaming=green
            webrtc_state = status["webrtc_state"]
            if webrtc_state == "streaming":
                webrtc_icon = "🟢"
            elif webrtc_state == "connecting":
                webrtc_icon = "🟡"
            else:
                webrtc_icon = "🔴"

            lines.append(f"Script:{script_icon} MQTT:{mqtt_icon} Camera:{camera_icon} WebRTC:{webrtc_icon}".ljust(70))
            lines.append("-" * 70)

            # Statistics
            stats = f"FPS:{status['fps']} Cam:{status['camera_fps']} Prod:{status['messages_produced']} Filt:{status['messages_filtered']} Err:{status['errors']}"
            lines.append(stats.ljust(70))
            lines.append("-" * 70)

            # Joint states
            if status["joint_states"]:
                index_to_name = status_tracker.joint_index_to_name
                joint_parts = []
                for joint_index in sorted(status["joint_states"].keys()):
                    position = status["joint_states"][joint_index]
                    joint_name = index_to_name.get(joint_index, joint_index)
                    short_name = joint_name[:3]
                    joint_parts.append(f"{short_name}:{position:5.1f}")
                lines.append(" ".join(joint_parts).ljust(70))
            else:
                lines.append("Joints: (waiting)".ljust(70))

            lines.append("=" * 70)
            lines.append("Press 'q' to stop".ljust(70))

            # Clear screen and move to top, then write all lines
            # Use \r\n for proper line breaks in raw terminal mode
            output = "\033[2J\033[H"  # Clear screen and move to home
            output += "\r\n".join(lines)

            try:
                sys.stdout.write(output)
                sys.stdout.flush()
            except (IOError, OSError):
                pass

            # Wait for next update
            time.sleep(status_interval)
    finally:
        # Show cursor again when done
        sys.stdout.write("\033[?25h")  # Show cursor
        sys.stdout.flush()


def _log_leader_follower_states(
    leader_observation: Dict[str, float],
    follower_observation: Optional[Dict[str, float]],
    leader: SO101Leader,
    follower: Optional[SO101Follower] = None,
) -> None:
    """
    Log leader and follower states in a readable format matching read_device.py style.

    Args:
        leader_observation: Leader observation (normalized positions)
        follower_observation: Optional follower observation (normalized or raw positions)
        leader: Leader instance to get raw values if needed
        follower: Optional follower instance to check calibration status
    """
    lines = []
    lines.append("Leader/Follower States\n")

    if follower_observation is not None:
        follower_calibrated = follower.is_calibrated if follower is not None else False

        if follower_calibrated:
            lines.append("Comparison (normalized values):")
            for name in sorted(leader_observation.keys()):
                leader_val = leader_observation.get(name, 0.0)
                follower_val = follower_observation.get(name, 0.0)
                diff = abs(leader_val - follower_val)
                diff_marker = "⚠️" if diff > 5.0 else "  "
                lines.append(
                    f"  {diff_marker} {name:16s} | Leader: {int(leader_val):4d} | Follower: {int(follower_val):4d} | Diff: {int(diff):4d}\n"
                )
        else:
            lines.append("Comparison (raw values) - Follower NOT calibrated:")
            # When follower is not calibrated, get raw positions from bus
            motor_ids = [motor.id for motor in leader.motors.values()]
            leader_raw_positions = leader.bus.sync_read_positions(motor_ids)
            for name in sorted(leader_observation.keys()):
                # Get raw position for this motor
                motor_id = leader.motors[name].id
                leader_raw = leader_raw_positions.get(motor_id, 0.0)
                # Follower observation is raw when not calibrated
                follower_raw = follower_observation.get(name, 0.0)
                diff = abs(leader_raw - follower_raw)
                diff_marker = "⚠️" if diff > 100.0 else "  "
                lines.append(
                    f"  {diff_marker} {name:16s} | Leader: {int(leader_raw):4d} | Follower: {int(follower_raw):4d} | Diff: {int(diff):4d}\n"
                )
            lines.append("  Note: Calibrate the follower to get normalized values for comparison\n")
    else:
        lines.append("Leader States (normalized):")
        for name in sorted(leader_observation.keys()):
            leader_val = leader_observation.get(name, 0.0)
            lines.append(f"  {name:16s} | Position: {int(leader_val):4d}")
    lines.append("\n")
    # Logging disabled for clean status display
    pass


def _keyboard_input_thread(stop_event: threading.Event) -> None:
    """
    Thread to monitor keyboard input for 'q' key to stop the loop gracefully.

    Args:
        stop_event: Event to set when 'q' is pressed
    """
    if sys.stdin.isatty():
        # Only enable keyboard input if running in a terminal
        try:
            import termios
            import tty

            old_settings = termios.tcgetattr(sys.stdin)
            tty.setraw(sys.stdin.fileno())

            try:
                while not stop_event.is_set():
                    if select.select([sys.stdin], [], [], 0.1)[0]:
                        char = sys.stdin.read(1)
                        if char == "q" or char == "Q":
                            stop_event.set()
                            break
            finally:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except (ImportError, OSError):
            # If termios is not available (e.g., Windows), keyboard input disabled
            pass


def _teleop_loop(
    leader: SO101Leader,
    follower: Optional[SO101Follower],
    action_queue: queue.Queue,
    stop_event: threading.Event,
    last_observation: Dict[str, Dict[str, float]],
    position_threshold: float,
    velocity_threshold: float,
    effort_threshold: float,
    log_states: bool,
    log_states_interval: int,
    frame_time: float,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    heartbeat_interval: float = 1.0,
) -> tuple[int, int]:
    """
    Main teleoperation loop: read from leader, send to follower, and send follower data to Cyberwave.

    Args:
        leader: SO101Leader instance
        follower: Optional SO101Follower instance (required when sending to Cyberwave)
        action_queue: Queue for Cyberwave updates
        stop_event: Event to signal loop to stop
        last_observation: Dictionary tracking last sent observation state (normalized positions)
        position_threshold: Minimum change in normalized position to trigger update
        velocity_threshold: Unused (kept for compatibility)
        effort_threshold: Unused (kept for compatibility)
        log_states: Whether to log leader/follower states
        log_states_interval: Interval for logging states
        frame_time: Target time per frame (1/fps)
        time_reference: TimeReference instance
        heartbeat_interval: Interval in seconds to send heartbeat if no changes (default 1.0)
    Returns:
        Tuple of (update_count, skip_count)
    """
    total_update_count = 0
    total_skip_count = 0
    iteration_count = 0

    # Track last send time per joint for heartbeat
    last_send_times: Dict[str, float] = {}

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            # Generate timestamp for this iteration (before reading action)
            timestamp, timestamp_monotonic = time_reference.update()

            # Read action from leader (for sending to follower)
            leader_action = leader.get_action() if leader is not None else {}

            # Read follower observation (for sending to Cyberwave - this is the actual robot state)
            follower_action = None
            if follower is not None:
                follower_action = follower.get_observation()
            else:
                # Fallback to leader if no follower (shouldn't happen when robot twin is provided)
                follower_action = leader_action

            # Process Cyberwave updates (queue changed joints from follower)
            # Worker thread handles all conversion
            update_count, skip_count = _process_cyberwave_updates(
                action=follower_action,
                last_observation=last_observation,
                action_queue=action_queue,
                position_threshold=position_threshold,
                velocity_threshold=velocity_threshold,
                effort_threshold=effort_threshold,
                timestamp=timestamp,  # Pass timestamp to processing function
                status_tracker=status_tracker,
                last_send_times=last_send_times,
                heartbeat_interval=heartbeat_interval,
            )
            total_update_count += update_count
            total_skip_count += skip_count

            # Send action to follower if provided
            leader_observation = None
            follower_observation = None
            if follower is not None and leader is not None:
                try:
                    # Send leader action to follower (follower handles safety limits and normalization)
                    follower.send_action(leader_action)

                    # Get observations for logging
                    if log_states:
                        leader_observation = leader.get_observation()
                        follower_observation = follower.get_observation()

                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors()

            # Log states if enabled
            if log_states:
                iteration_count += 1
                should_log = iteration_count <= 10 or iteration_count % log_states_interval == 0

                if should_log:
                    if leader_observation is None:
                        leader_observation = leader.get_observation()
                    if follower is not None and follower_observation is None:
                        follower_observation = follower.get_observation()
                    _log_leader_follower_states(
                        leader_observation=leader_observation,
                        follower_observation=follower_observation,
                        leader=leader,
                        follower=follower,
                    )

            # Rate limiting
            elapsed = time.time() - loop_start
            sleep_time = frame_time - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        pass
    except Exception:
        if status_tracker:
            status_tracker.increment_errors()
        raise

    return total_update_count, total_skip_count


def teleoperate(
    leader: Optional[SO101Leader],
    cyberwave_client: Optional[Cyberwave] = None,
    follower: Optional[SO101Follower] = None,
    fps: int = 30,
    camera_fps: int = 30,
    use_radians: bool = True,
    position_threshold: float = 0.1,
    velocity_threshold: float = 100.0,
    effort_threshold: float = 0.1,
    log_states: bool = False,
    log_states_interval: int = 30,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
) -> None:
    """
    Run teleoperation loop: read from leader, send to follower, and send follower data to Cyberwave.

    Uses a separate thread with a FIFO queue to send updates to Cyberwave,
    keeping the main loop responsive. Only sends updates when values change.
    Follower data (actual robot state) is sent to Cyberwave, not leader data.

    Args:
        leader: SO101Leader instance (optional if camera_only=True)
        cyberwave_client: Cyberwave client instance (required)
        follower: SO101Follower instance (required when robot twin is provided)
        fps: Target frames per second for the teleoperation loop
        camera_fps: Frames per second for camera streaming
        use_radians: If True, convert positions to radians; if False, convert to degrees (default)
        position_threshold: Minimum change in position to trigger an update (in degrees/radians)
        velocity_threshold: Minimum change in velocity to trigger an update
        effort_threshold: Minimum change in effort to trigger an update
        log_states: Whether to log leader/follower states
        log_states_interval: Interval for logging states
        robot: Robot twin instance
        camera: Camera twin instance
    """
    time_reference = TimeReference()

    # Disable all logging to avoid interfering with status display
    logging.disable(logging.CRITICAL)

    # Create status tracker
    status_tracker = StatusTracker()
    status_tracker.script_started = True

    # Set twin info for status display
    robot_uuid = robot.uuid if robot else ""
    robot_name = robot.name if robot and hasattr(robot, 'name') else "so101-teleop"
    camera_uuid_val = camera.uuid if camera else ""
    camera_name = camera.name if camera and hasattr(camera, 'name') else "camera-teleop"
    status_tracker.set_twin_info(robot_uuid, robot_name, camera_uuid_val, camera_name)

    # Ensure MQTT client is connected
    if cyberwave_client is None:
        raise RuntimeError("Cyberwave client is required")

    mqtt_client = cyberwave_client.mqtt
    if mqtt_client is not None and not mqtt_client.connected:
        mqtt_client.connect()

        # Wait for connection with timeout
        max_wait_time = 10.0  # seconds
        wait_start = time.time()
        while not mqtt_client.connected:
            if time.time() - wait_start > max_wait_time:
                status_tracker.update_mqtt_status(False)
                raise RuntimeError(
                    f"Failed to connect to Cyberwave MQTT broker within {max_wait_time} seconds"
                )
            time.sleep(0.1)
        status_tracker.update_mqtt_status(True)
    else:
        status_tracker.update_mqtt_status(mqtt_client.connected if mqtt_client else False)

    if leader is not None and not leader.connected:
        raise RuntimeError("Leader is not connected")

    # Require follower when robot twin is provided (we send follower data to Cyberwave)
    if robot is not None and follower is None:
        raise RuntimeError("Follower is required when robot twin is provided (follower data is sent to Cyberwave)")

    if follower is not None and not follower.connected:
        raise RuntimeError("Follower is not connected")

    # Verify follower has torque enabled (required for movement)
    if follower is not None and not follower.torque_enabled:
        follower.enable_torque()

    # Get calibration data from leader (leader handles its own calibration loading)
    if leader is not None:
        if leader.calibration is None:
            raise RuntimeError(
                "Leader is not calibrated. Please calibrate the leader first using the calibration script."
            )

    # Use follower motors for mappings when sending to Cyberwave (follower data is what we send)
    # Fall back to leader motors if follower not available (for camera-only mode)
    motors_for_mapping = follower.motors if follower is not None else (leader.motors if leader is not None else {})

    if motors_for_mapping:
        # Create mapping from joint names to joint indexes (motor IDs: 1-6)
        joint_name_to_index = {name: motor.id for name, motor in motors_for_mapping.items()}

        # Create mapping from joint indexes to joint names (for status display)
        joint_index_to_name = {str(motor.id): name for name, motor in motors_for_mapping.items()}
        status_tracker.set_joint_index_to_name(joint_index_to_name)

        # Create mapping from joint names to normalization modes
        joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in motors_for_mapping.items()}

        # Initialize last observation state (track normalized positions)
        # Follower returns normalized positions, worker thread handles conversion to degrees/radians
        last_observation: Dict[str, float] = {}
        for joint_name in motors_for_mapping.keys():
            last_observation[joint_name] = float("inf")  # Use inf to force first update
    else:
        joint_name_to_index = {}
        joint_name_to_norm_mode = {}
        last_observation: Dict[str, float] = {}

    # Create queue and worker thread for Cyberwave updates
    num_joints = len(motors_for_mapping) if motors_for_mapping else 0
    sampling_rate = 100  # Hz
    seconds = 60  # seconds
    queue_size = num_joints * sampling_rate * seconds if num_joints > 0 else 1000
    action_queue = queue.Queue(maxsize=queue_size)  # Limit queue size to prevent memory issues
    stop_event = threading.Event()

    # Start keyboard input thread for 'q' key to stop gracefully
    keyboard_thread = threading.Thread(
        target=_keyboard_input_thread,
        args=(stop_event,),
        daemon=True,
    )
    keyboard_thread.start()

    # Start status logging thread
    status_thread = threading.Thread(
        target=_status_logging_thread,
        args=(status_tracker, stop_event, fps, camera_fps),
        daemon=True,
    )
    status_thread.start()

    # Start MQTT update worker thread
    worker_thread = None
    if robot is not None:
        worker_thread = threading.Thread(
            target=cyberwave_update_worker,
            args=(
                action_queue,
                joint_name_to_index,
                joint_name_to_norm_mode,
                use_radians,
                stop_event,
                robot,
                time_reference,
                status_tracker,
            ),
            daemon=True,
        )
        worker_thread.start()

    # Start camera streaming thread if follower has cameras configured
    camera_thread = None
    if follower is not None and cyberwave_client is not None:
        if follower.config.cameras and len(follower.config.cameras) > 0:
            camera_id = follower.config.cameras[0]
            camera_thread = threading.Thread(
                target=_camera_worker_thread,
                args=(
                    cyberwave_client,
                    camera_id,
                    camera_fps,
                    camera.uuid,
                    stop_event,
                    time_reference,
                    status_tracker,
                ),
                daemon=True,
            )
            camera_thread.start()
        else:
            status_tracker.update_camera_status(detected=False, started=False)

    # TimeReference synchronization: The teleop loop updates TimeReference, and the camera reads from it.
    # This ensures that when an action is performed, the camera frame is synchronized with that action.
    # The camera calls time_reference.read() to get the timestamp that was set by the teleop loop.
    # We require fps >= camera_fps to ensure the teleop loop updates TimeReference at least as fast
    # as the camera captures frames.
    if follower is not None and follower.config.cameras and len(follower.config.cameras) > 0:
        if fps < camera_fps:
            raise ValueError(
                f"fps ({fps}) must be >= camera_fps ({camera_fps}) for proper synchronization. "
                "The teleop loop must update TimeReference at least as fast as the camera captures frames."
            )

    frame_time = 1.0 / fps
    status_tracker.fps = fps

    try:
        if (
            follower is not None
            and robot is not None
            and mqtt_client is not None
            and time_reference is not None
        ):
            # Get follower observation (this is what we send to Cyberwave)
            follower_obs = follower.get_observation()
            observations = {}
            for joint_key, normalized_pos in follower_obs.items():
                # Remove .pos suffix if present
                name = joint_key.removesuffix(".pos")
                if name in follower.motors:
                    joint_index = follower.motors[name].id
                    # Convert normalized position to radians for Cyberwave
                    norm_mode = joint_name_to_norm_mode[name]
                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        degrees = (normalized_pos / 100.0) * 180.0
                        radians = degrees * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        degrees = (normalized_pos / 100.0) * 360.0
                        radians = degrees * math.pi / 180.0
                    else:
                        radians = normalized_pos * math.pi / 180.0  # Assume degrees
                    observations[joint_index] = radians
            # Send follower observations to Cyberwave as single update
            mqtt_client.publish_initial_observation(
                twin_uuid=robot.uuid,
                observations=observations,
            )

    except Exception:
        if status_tracker:
            status_tracker.increment_errors()

    try:
        if leader is not None:
            _teleop_loop(
                leader=leader,
                follower=follower,
                action_queue=action_queue,
                stop_event=stop_event,
                last_observation=last_observation,
                position_threshold=position_threshold,
                velocity_threshold=velocity_threshold,
                effort_threshold=effort_threshold,
                log_states=log_states,
                log_states_interval=log_states_interval,
                frame_time=frame_time,
                time_reference=time_reference,
                status_tracker=status_tracker,
            )
        else:
            # No leader, just wait for stop event
            while not stop_event.is_set():
                time.sleep(0.1)
    finally:
        # Signal all threads to stop
        stop_event.set()

        # Wait for threads to finish
        if worker_thread is not None:
            try:
                action_queue.join(timeout=2.0)
            except Exception:
                pass
            worker_thread.join(timeout=1.0)

        if camera_thread is not None:
            camera_thread.join(timeout=5.0)

        if status_thread is not None:
            status_thread.join(timeout=1.0)


def main():
    """Main entry point for teleoperation script."""
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Teleoperate SO101 leader and update Cyberwave twin"
    )
    parser.add_argument(
        "--twin-uuid",
        type=str,
        required=False,
        help="UUID of the twin to update",
    )
    parser.add_argument(
        "--leader-port",
        type=str,
        required=False,
        help="Serial port for leader device (optional, will try to find if not provided). "
        "Not required when using --camera-only.",
    )
    parser.add_argument(
        "--follower-port",
        type=str,
        help="Serial port for follower device (optional)",
    )
    parser.add_argument(
        "--fps",
        type=int,
        default=30,
        help="Target frames per second (default: 30)",
    )
    parser.add_argument(
        "--use-radians",
        action="store_true",
        default=True,
        help="Convert positions to radians (default: degrees)",
    )
    parser.add_argument(
        "--max-relative-target",
        type=float,
        default=None,
        help="Maximum change per update for follower (raw encoder units, default: None = no limit). "
        "Set to a value to enable safety limit (e.g., 50.0 for slower/safer movement).",
    )
    parser.add_argument(
        "--log-states",
        action="store_true",
        help="Enable logging of leader and follower states",
    )
    parser.add_argument(
        "--log-states-interval",
        type=int,
        default=30,
        help="Log states every N updates (default: 30, ~1 second at 30fps). "
        "Only used if --log-states is enabled.",
    )
    parser.add_argument(
        "--camera-only",
        action="store_true",
        help="Only stream camera (skip teleoperation loop). Requires --follower-port.",
    )
    parser.add_argument(
        "--camera-uuid",
        type=str,
        required=False,
        help="UUID of the twin to stream camera to (default: same as --twin-uuid)",
    )
    parser.add_argument(
        "--camera-fps",
        type=int,
        required=False,
        default=30,
        help="FPS to use for the camera (default: 30)",
    )

    args = parser.parse_args()
    if args.camera_uuid is None:
        args.camera_uuid = args.twin_uuid
    # Initialize Cyberwave client and get MQTT client
    cyberwave_client = Cyberwave()
    robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id=args.twin_uuid, name="robot")
    camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id=args.camera_uuid, name="camera")
    mqtt_client = cyberwave_client.mqtt

    # Initialize leader (optional if camera-only mode)
    leader = None
    if not args.camera_only:
        from config import LeaderConfig
        from utils import find_port

        leader_port = args.leader_port
        if not leader_port:
            leader_port = find_port(device_name="SO101 Leader")

        leader_config = LeaderConfig(port=leader_port)
        leader = SO101Leader(config=leader_config)
        leader.connect()

    # Initialize follower (required for camera-only mode, optional otherwise)
    follower = None
    if args.follower_port or args.camera_only:
        if not args.follower_port:
            raise RuntimeError("--follower-port is required when using --camera-only")
        from config import FollowerConfig

        follower_config = FollowerConfig(
            port=args.follower_port, max_relative_target=args.max_relative_target, cameras=[0]
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()

    try:
        teleoperate(
            leader=leader,
            cyberwave_client=cyberwave_client,
            follower=follower,
            fps=args.fps,
            camera_fps=args.camera_fps,
            use_radians=args.use_radians,
            log_states=args.log_states,
            log_states_interval=args.log_states_interval,
            robot=robot,
            camera=camera,
        )
    finally:
        if leader is not None:
            leader.disconnect()
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()
