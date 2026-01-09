"""Remote operation loop for SO101 follower via Cyberwave MQTT."""

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

from cyberwave import Cyberwave, Twin
from cyberwave.utils import TimeReference

from dotenv import load_dotenv
from follower import SO101Follower
from motors import MotorNormMode
from write_position import validate_position

logger = logging.getLogger(__name__)


class StatusTracker:
    """Thread-safe status tracker for remote operation system."""

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
        self.messages_received = 0
        self.messages_processed = 0
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

    def increment_received(self):
        with self.lock:
            self.messages_received += 1

    def increment_processed(self):
        with self.lock:
            self.messages_processed += 1

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
                "messages_received": self.messages_received,
                "messages_processed": self.messages_processed,
                "messages_filtered": self.messages_filtered,
                "errors": self.errors,
                "joint_states": self.joint_states.copy(),
                "robot_uuid": self.robot_uuid,
                "robot_name": self.robot_name,
                "camera_uuid": self.camera_uuid,
                "camera_name": self.camera_name,
            }


def _status_logging_thread(
    status_tracker: StatusTracker,
    stop_event: threading.Event,
    camera_fps: int,
) -> None:
    """
    Thread that logs status information at 1 fps.

    Args:
        status_tracker: StatusTracker instance
        stop_event: Event to signal thread to stop
        camera_fps: Frames per second for camera streaming
    """
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
            lines.append("SO101 Remote Operation Status".center(70))
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
            stats = f"Cam:{status['camera_fps']} Recv:{status['messages_received']} Proc:{status['messages_processed']} Filt:{status['messages_filtered']} Err:{status['errors']}"
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


def _camera_worker_thread(
    client: Cyberwave,
    camera_id: int,
    fps: int,
    twin_uuid: str,
    stop_event: threading.Event,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    camera_type: str = "rgb",
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
        camera_type: Camera sensor type ("rgb" or "depth", default: "rgb")
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
            sensor_type=camera_type,
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


def _radians_to_normalized(radians: float, norm_mode: MotorNormMode) -> float:
    """
    Convert radians to normalized position based on motor normalization mode.

    Args:
        radians: Position in radians
        norm_mode: Motor normalization mode

    Returns:
        Normalized position value
    """
    if norm_mode == MotorNormMode.DEGREES:
        # Convert radians to degrees
        degrees = radians * 180.0 / math.pi
        return degrees
    elif norm_mode == MotorNormMode.RANGE_M100_100:
        # Convert radians -> degrees -> normalized (-100 to 100)
        # Reverse of: normalized / 100.0 * 180.0 = degrees
        degrees = radians * 180.0 / math.pi
        normalized = (degrees / 180.0) * 100.0
        return normalized
    elif norm_mode == MotorNormMode.RANGE_0_100:
        # Convert radians -> degrees -> normalized (0 to 100)
        # Reverse of: normalized / 100.0 * 360.0 = degrees
        degrees = radians * 180.0 / math.pi
        # Handle negative angles by converting to 0-360 range
        if degrees < 0:
            degrees = degrees + 360.0
        normalized = (degrees / 360.0) * 100.0
        return normalized
    else:
        return radians


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
                            logger.info("\n'q' key pressed - stopping remote operation loop...")
                            stop_event.set()
                            break
            finally:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except (ImportError, OSError):
            # If termios is not available (e.g., Windows), fall back to simple input
            logger.debug("Termios not available, keyboard input disabled")
            pass


def _motor_writer_worker(
    action_queue: queue.Queue,
    follower: SO101Follower,
    stop_event: threading.Event,
    status_tracker: Optional[StatusTracker] = None,
) -> None:
    """
    Worker thread that reads actions from queue and writes to follower motors.

    Splits large position changes into multiple smaller steps based on max_relative_target.

    Args:
        action_queue: Queue containing action dictionaries (normalized positions)
        follower: SO101Follower instance
        stop_event: Event to signal thread to stop
        status_tracker: Optional status tracker for statistics
    """
    processed_count = 0
    error_count = 0
    actions_received = 0
    from utils import ensure_safe_goal_position

    while not stop_event.is_set():
        try:
            # Get action from queue with timeout
            try:
                action = action_queue.get(timeout=0.1)
                actions_received += 1
            except queue.Empty:
                continue

            try:
                # If max_relative_target is set, split large movements into smaller steps
                if follower.config.max_relative_target is not None:
                    # Get initial current positions (only once, at the start)
                    present_pos = follower.get_observation()
                    current_pos = {
                        key.removesuffix(".pos"): val for key, val in present_pos.items()
                    }

                    # Extract goal positions from action
                    goal_pos = {
                        key.removesuffix(".pos"): val
                        for key, val in action.items()
                        if key.endswith(".pos")
                    }
                    # Also handle keys without .pos suffix
                    for key, val in action.items():
                        if not key.endswith(".pos") and key in follower.motors:
                            goal_pos[key] = val
                            # Initialize current_pos if not present
                            if key not in current_pos:
                                current_pos[key] = 0.0

                    # Calculate how many steps are needed for each joint
                    max_steps = 1
                    for joint_name, goal_val in goal_pos.items():
                        current_val = current_pos.get(joint_name, 0.0)
                        diff = abs(goal_val - current_val)
                        if diff > 0.01:  # Only count if there's actual movement needed
                            steps_needed = int(diff / follower.config.max_relative_target) + 1
                            max_steps = max(max_steps, steps_needed)

                    # Send steps quickly without waiting for physical movement
                    # This allows the queue to process new actions faster
                    for step in range(max_steps):
                        # Check which joints still need to move
                        remaining_movements = {}
                        for joint_name, goal_val in goal_pos.items():
                            current_val = current_pos.get(joint_name, 0.0)
                            diff = abs(goal_val - current_val)
                            if diff > 0.01:  # Small threshold
                                remaining_movements[joint_name] = goal_val

                        if not remaining_movements:
                            # All joints have reached their targets
                            break

                        # Prepare goal_present_pos dict for ensure_safe_goal_position
                        # Format: {joint_name: (goal_pos, current_pos)}
                        goal_present_pos = {
                            key: (goal_pos[key], current_pos.get(key, 0.0))
                            for key in remaining_movements.keys()
                        }

                        # Get safe goal positions (clamped to max_relative_target)
                        safe_goal_pos = ensure_safe_goal_position(
                            goal_present_pos, follower.config.max_relative_target
                        )

                        # Update current_pos with what we're about to send
                        # (for next iteration calculation, not actual motor position)
                        for name, pos in safe_goal_pos.items():
                            current_pos[name] = pos

                        # Convert back to action format with .pos suffix
                        safe_action = {f"{name}.pos": pos for name, pos in safe_goal_pos.items()}

                        # Temporarily disable max_relative_target in follower to avoid double-clamping
                        original_max_relative = follower.config.max_relative_target
                        follower.config.max_relative_target = None

                        try:
                            # Send the safe action directly to bus
                            goal_pos_for_bus = {
                                key.removesuffix(".pos"): val for key, val in safe_action.items()
                            }
                            follower.bus.sync_write(
                                "Goal_Position", goal_pos_for_bus, normalize=True
                            )
                            processed_count += 1
                            if status_tracker:
                                status_tracker.increment_processed()
                        finally:
                            # Restore original max_relative_target
                            follower.config.max_relative_target = original_max_relative

                        # No sleep - send steps as fast as possible
                        # The motors will catch up naturally
                else:
                    # No max_relative_target limit, send action directly
                    follower.send_action(action)
                    processed_count += 1
                    if status_tracker:
                        status_tracker.increment_processed()

            except Exception:
                error_count += 1
                if status_tracker:
                    status_tracker.increment_errors()
            finally:
                action_queue.task_done()

        except Exception:
            error_count += 1
            if status_tracker:
                status_tracker.increment_errors()


def _create_joint_state_callback(
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    status_tracker: Optional[StatusTracker] = None,
) -> callable:
    """
    Create a callback function for MQTT joint state updates.

    Args:
        current_state: Dictionary to maintain current state (joint_name -> normalized position)
        action_queue: Queue to put validated actions
        joint_index_to_name: Mapping from joint index (1-6) to joint name
        joint_name_to_norm_mode: Mapping from joint name to normalization mode
        follower: Follower instance for validation
        status_tracker: Optional status tracker for statistics

    Returns:
        Callback function for MQTT joint state updates
    """

    def callback(topic: str, data: Dict) -> None:
        """
        Callback function for MQTT joint state updates.

        Args:
            topic: MQTT topic string
            data: Dictionary containing joint state update
                  Expected format: {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}
        """
        if status_tracker:
            status_tracker.increment_received()

        try:
            # Check if topic ends with "update" - only process update messages
            if not topic.endswith("/update"):
                if status_tracker:
                    status_tracker.increment_filtered()
                return

            # Extract joint index/name and position from data
            # Message format:
            # {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}

            # Extract joint_name and joint_state
            if "joint_name" not in data or "joint_state" not in data:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            joint_name_str = data.get("joint_name")
            joint_state = data.get("joint_state", {})
            position_radians = joint_state.get("position")

            if position_radians is None:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Convert joint_name to joint_index
            # joint_name is a string like "5" (joint index) or could be a joint name like "shoulder_pan"
            joint_index = None  # Initialize before try block
            try:
                joint_index = int(joint_name_str)
            except (ValueError, TypeError):
                # Try to find joint by name (reverse lookup)
                for idx, name in joint_index_to_name.items():
                    if name == joint_name_str:
                        joint_index = idx
                        break
                if joint_index is None:
                    if status_tracker:
                        status_tracker.increment_errors()
                    return

            # Get joint name from index
            joint_name = joint_index_to_name.get(joint_index)
            if joint_name is None:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Convert position_radians to float
            try:
                position_radians = float(position_radians)
            except (ValueError, TypeError):
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Get normalization mode for this joint
            norm_mode = joint_name_to_norm_mode.get(joint_name)
            if norm_mode is None:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Convert radians to normalized position
            normalized_position = _radians_to_normalized(position_radians, norm_mode)

            # Validate position
            motor_id = follower.motors[joint_name].id
            is_valid, error_msg = validate_position(motor_id, normalized_position)
            if not is_valid:
                if status_tracker:
                    status_tracker.increment_filtered()
                return

            # Update current state (merge with previous state)
            # Create full action state by taking current state and updating the changed joint
            action = current_state.copy()
            action[f"{joint_name}.pos"] = normalized_position
            # Also update the current_state dictionary
            current_state[f"{joint_name}.pos"] = normalized_position

            # Update joint states in status tracker
            if status_tracker:
                joint_states = {str(joint_index): normalized_position}
                status_tracker.update_joint_states(joint_states)

            # Put action in queue (non-blocking)
            try:
                action_queue.put_nowait(action)
            except queue.Full:
                if status_tracker:
                    status_tracker.increment_errors()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors()

    return callback


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
    fps: int = 30,
    camera_fps: int = 30,
    camera_type: str = "rgb",
) -> None:
    """
    Run remote operation loop: receive joint states via MQTT and write to follower motors.

    Args:
        client: Cyberwave client instance
        follower: SO101Follower instance
        robot: Robot twin instance
        camera: Camera twin instance
        fps: Target frames per second (not used directly, but kept for compatibility)
        camera_fps: Frames per second for camera streaming
        camera_type: Camera sensor type ("rgb" or "depth", default: "rgb")
    """
    time_reference = TimeReference()

    # Disable all logging to avoid interfering with status display
    logging.disable(logging.CRITICAL)

    # Create status tracker
    status_tracker = StatusTracker()
    status_tracker.script_started = True

    # Set twin info for status display
    robot_uuid = robot.uuid if robot else ""
    robot_name = robot.name if robot and hasattr(robot, 'name') else "so101-remote"
    camera_uuid_val = camera.uuid if camera else ""
    camera_name = camera.name if camera and hasattr(camera, 'name') else "camera-remote"
    status_tracker.set_twin_info(robot_uuid, robot_name, camera_uuid_val, camera_name)

    # Get twin_uuid from robot twin
    if robot is None:
        raise RuntimeError("Robot twin is required")
    twin_uuid = robot.uuid

    # Ensure follower is connected
    if not follower.connected:
        raise RuntimeError("Follower is not connected")

    # Verify follower has torque enabled (required for movement)
    if not follower.torque_enabled:
        follower.enable_torque()

    # Create mapping from joint index (motor ID) to joint name
    joint_index_to_name = {motor.id: name for name, motor in follower.motors.items()}

    # Create mapping from joint indexes to joint names (for status display, string keys)
    joint_index_to_name_str = {str(motor.id): name for name, motor in follower.motors.items()}
    status_tracker.set_joint_index_to_name(joint_index_to_name_str)

    # Create mapping from joint names to normalization modes
    joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in follower.motors.items()}

    # Initialize current state with follower's current observation
    current_state: Dict[str, float] = {}
    try:
        initial_obs = follower.get_observation()
        current_state.update(initial_obs)
    except Exception:
        # Initialize with empty state
        for name in follower.motors.keys():
            current_state[f"{name}.pos"] = 0.0

    # Ensure MQTT client is connected
    mqtt_client = client.mqtt
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

    # Send initial observation to Cyberwave using publish_initial_observation
    try:
        # Get follower's current observation (normalized positions)
        follower_obs = follower.get_observation()

        # Convert to joint index format for initial observation
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

        # Send initial observation as single update
        mqtt_client.publish_initial_observation(
            twin_uuid=twin_uuid,
            observations=observations,
        )

    except Exception:
        if status_tracker:
            status_tracker.increment_errors()

    # Create queue for actions
    queue_size = 1000  # Reasonable queue size
    action_queue = queue.Queue(maxsize=queue_size)
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
        args=(status_tracker, stop_event, camera_fps),
        daemon=True,
    )
    status_thread.start()

    # Start camera streaming thread if follower has cameras configured and camera twin is provided
    camera_thread = None
    if follower.config.cameras and len(follower.config.cameras) > 0 and camera is not None:
        camera_id = follower.config.cameras[0]
        camera_thread = threading.Thread(
            target=_camera_worker_thread,
            args=(client, camera_id, camera_fps, camera.uuid, stop_event, time_reference, status_tracker, camera_type),
            daemon=True,
        )
        camera_thread.start()
    else:
        status_tracker.update_camera_status(detected=False, started=False)

    # Create callback for joint state updates
    joint_state_callback = _create_joint_state_callback(
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
        status_tracker=status_tracker,
    )

    # Subscribe to joint states
    mqtt_client.subscribe_joint_states(twin_uuid, joint_state_callback)

    # Start motor writer worker thread
    writer_thread = threading.Thread(
        target=_motor_writer_worker,
        args=(action_queue, follower, stop_event, status_tracker),
        daemon=True,
    )
    writer_thread.start()

    try:
        # Main loop: just wait for stop event
        while not stop_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        # Signal all threads to stop
        stop_event.set()

        # Wait for queue to drain (with timeout)
        try:
            action_queue.join(timeout=2.0)
        except Exception:
            pass

        # Wait for writer thread to finish
        writer_thread.join(timeout=1.0)

        # Stop camera streaming thread
        if camera_thread is not None:
            camera_thread.join(timeout=5.0)

        # Stop status thread
        if status_thread is not None:
            status_thread.join(timeout=1.0)


def main():
    """Main entry point for remote operation script."""
    load_dotenv()
    parser = argparse.ArgumentParser(description="Remote operate SO101 follower via Cyberwave MQTT")
    parser.add_argument(
        "--twin-uuid",
        type=str,
        required=False,
        help="UUID of the twin to subscribe to",
    )
    parser.add_argument(
        "--follower-port",
        type=str,
        required=True,
        help="Serial port for follower device",
    )
    parser.add_argument(
        "--max-relative-target",
        type=float,
        default=None,
        help="Maximum change per update for follower (raw encoder units, default: None = no limit). "
        "Set to a value to enable safety limit (e.g., 50.0 for slower/safer movement).",
    )
    parser.add_argument(
        "--follower-id",
        type=str,
        default="follower1",
        help="Device identifier for calibration file (default: 'follower1')",
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
    parser.add_argument(
        "--camera-type",
        type=str,
        choices=["rgb", "depth"],
        default="rgb",
        help="Camera sensor type: 'rgb' or 'depth' (default: 'rgb')",
    )

    args = parser.parse_args()

    # Default camera-uuid to twin-uuid if not provided
    if args.camera_uuid is None:
        args.camera_uuid = args.twin_uuid

    # Initialize Cyberwave client and create twins
    cyberwave_client = Cyberwave()
    robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id=args.twin_uuid, name="robot")
    camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id=args.camera_uuid, name="camera")

    # Initialize follower
    from config import FollowerConfig

    follower_config = FollowerConfig(
        port=args.follower_port,
        max_relative_target=args.max_relative_target,
        id=args.follower_id,
        cameras=[0],  # Enable camera support
    )
    follower = SO101Follower(config=follower_config)
    follower.connect()

    try:
        remoteoperate(
            client=cyberwave_client,
            follower=follower,
            robot=robot,
            camera=camera,
            camera_fps=args.camera_fps,
            camera_type=args.camera_type,
        )
    finally:
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        mqtt_client = cyberwave_client.mqtt
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
