"""Teleoperation loop for SO101 leader and follower."""

import argparse
import asyncio
import json
import logging
import math
import os
import queue
import select
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from cyberwave import Cyberwave, Twin

# Import camera configuration and stream manager from cyberwave SDK
from cyberwave.sensor import (
    CameraStreamManager,
    CV2CameraStreamer,
    RealSenseStreamer,
    Resolution,
)
from cyberwave.utils import TimeReference
from dotenv import load_dotenv

# RealSense is optional - only import if available
try:
    from cyberwave.sensor import (
        RealSenseConfig,
        RealSenseDiscovery,
    )

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None
    RealSenseDiscovery = None

from motors import MotorNormMode
from scripts.cw_setup import load_setup_config
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import get_setup_config_path
from utils.trackers import StatusTracker, run_status_logging_thread
from utils.utils import load_calibration

logger = logging.getLogger(__name__)


def cyberwave_update_worker(
    action_queue: queue.Queue,
    joint_name_to_index: Dict[str, int],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    stop_event: threading.Event,
    twin: Optional[Twin] = None,
    time_reference: TimeReference = None,
    status_tracker: Optional[StatusTracker] = None,
    follower_calibration: Optional[Dict[str, Any]] = None,
    motor_id_to_schema_joint: Optional[Dict[int, str]] = None,
) -> None:
    """
    Worker thread that processes actions from the queue and updates Cyberwave twin.

    Batches multiple joint updates together and skips updates where position is 0.0.
    Velocity and effort are hardcoded to 0.0 to avoid issues.
    Always converts positions to radians.
    Uses schema joint names (e.g. "_1", "_2") for MQTT updates, matching the twin's
    universal schema.

    Args:
        action_queue: Queue containing (joint_name, action_data) tuples where action_data
                     is a dict with 'position', 'velocity', 'load', and 'timestamp' keys
        joint_name_to_index: Dictionary mapping joint names to joint indexes (1-6)
        joint_name_to_norm_mode: Dictionary mapping joint names to normalization modes
        stop_event: Event to signal thread to stop
        twin: Optional Twin instance for updating joint states
        time_reference: TimeReference instance
        status_tracker: Optional status tracker for statistics
        follower_calibration: Calibration data for converting normalized positions
        motor_id_to_schema_joint: Mapping from motor ID to schema joint name (e.g. 1 -> "_1")
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

                    # Convert normalized position to radians using follower calibration
                    calib = follower_calibration[joint_name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    # Get normalization mode for this joint
                    norm_mode = joint_name_to_norm_mode.get(
                        joint_name, MotorNormMode.RANGE_M100_100
                    )

                    # Convert normalized -> radians using calibration ranges
                    # Normalized value represents percentage of calibrated range
                    match norm_mode:
                        case MotorNormMode.RANGE_M100_100:
                            raw_offset = (normalized_position / 100.0) * delta_r
                            position = raw_offset * (2.0 * math.pi / 4095.0)
                        case MotorNormMode.RANGE_0_100:
                            delta_r = r_max - r_min
                            raw_value = r_min + (normalized_position / 100.0) * delta_r
                            position = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                        case _:
                            position = normalized_position * math.pi / 180.0

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
                    # Send all joints in the batch using schema joint names (e.g. "_1", "_2")
                    joint_states = {}
                    for joint_index, (position, _, _, timestamp) in batch_updates.items():
                        schema_joint = (
                            motor_id_to_schema_joint.get(joint_index, str(joint_index))
                            if motor_id_to_schema_joint
                            else str(joint_index)
                        )
                        twin.joints.set(
                            joint_name=schema_joint,
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


CONTROL_RATE_HZ = 100


def _teleop_loop(
    leader: SO101Leader,
    follower: Optional[SO101Follower],
    action_queue: queue.Queue,
    stop_event: threading.Event,
    last_observation: Dict[str, Dict[str, float]],
    position_threshold: float,
    velocity_threshold: float,
    effort_threshold: float,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    heartbeat_interval: float = 1.0,
    control_rate_hz: int = CONTROL_RATE_HZ,
) -> tuple[int, int]:
    """
    Main teleoperation loop: read from leader, send to follower, send data to Cyberwave.

    The loop always runs at 100Hz for responsive robot control and MQTT updates.

    Args:
        leader: SO101Leader instance
        follower: Optional SO101Follower instance (required when sending to Cyberwave)
        action_queue: Queue for Cyberwave updates
        stop_event: Event to signal loop to stop
        last_observation: Dictionary tracking last sent observation state (normalized positions)
        position_threshold: Minimum change in normalized position to trigger update
        velocity_threshold: Unused (kept for compatibility)
        effort_threshold: Unused (kept for compatibility)
        time_reference: TimeReference instance
        heartbeat_interval: Interval in seconds to send heartbeat if no changes (default 1.0)
        control_rate_hz: Control loop frequency in Hz (always 100 for SO101)
    Returns:
        Tuple of (update_count, skip_count)
    """
    total_update_count = 0
    total_skip_count = 0

    # Track last send time per joint for heartbeat
    last_send_times: Dict[str, float] = {}

    # Control loop timing - always run at control_rate_hz for responsive control
    control_frame_time = 1.0 / control_rate_hz

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            # Generate timestamp for this iteration (before reading action)
            # This runs at 100Hz to provide fresh timestamps for camera sync
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

            # Send action to follower if provided - always do this at control_rate_hz
            if follower is not None and leader is not None:
                try:
                    # Send leader action to follower (follower handles safety limits and normalization)
                    follower.send_action(leader_action)
                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors()

            # Process Cyberwave updates every loop (100Hz)
            update_count, skip_count = _process_cyberwave_updates(
                action=follower_action,
                last_observation=last_observation,
                action_queue=action_queue,
                position_threshold=position_threshold,
                velocity_threshold=velocity_threshold,
                effort_threshold=effort_threshold,
                timestamp=timestamp,
                status_tracker=status_tracker,
                last_send_times=last_send_times,
                heartbeat_interval=heartbeat_interval,
            )
            total_update_count += update_count
            total_skip_count += skip_count

            # Rate limiting - maintain control_rate_hz
            elapsed = time.time() - loop_start
            sleep_time = control_frame_time - elapsed
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
    camera_fps: int = 30,
    position_threshold: float = 0.1,
    velocity_threshold: float = 100.0,
    effort_threshold: float = 0.1,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
    cameras: Optional[List[Dict[str, Any]]] = None,
    camera_type: str = "cv2",
    camera_id: Union[int, str] = 0,
    camera_resolution: Resolution = Resolution.VGA,
    enable_depth: bool = False,
    depth_fps: int = 30,
    depth_resolution: Optional[Resolution] = None,
    depth_publish_interval: int = 30,
) -> None:
    """
    Run teleoperation loop: read from leader, send to follower, and send follower data to Cyberwave.

    Uses a separate thread with a FIFO queue to send updates to Cyberwave,
    keeping the main loop responsive. Only sends updates when values change.
    Follower data (actual robot state) is sent to Cyberwave, not leader data.
    Always converts positions to radians.

    The control loop always runs at 100Hz for responsive robot control and MQTT updates.

    Supports both CV2 (USB/webcam/IP) cameras and Intel RealSense cameras.

    Args:
        leader: SO101Leader instance (optional if camera_only=True)
        cyberwave_client: Cyberwave client instance (required)
        follower: SO101Follower instance (required when robot twin is provided)
        camera_fps: Frames per second for camera streaming
        position_threshold: Minimum change in position to trigger an update (in normalized units)
        velocity_threshold: Minimum change in velocity to trigger an update
        effort_threshold: Minimum change in effort to trigger an update
        robot: Robot twin instance
        camera: Camera twin instance (single camera, backward compat). Ignored if cameras is set.
        cameras: List of camera configs for multi-stream. Each dict: twin, camera_id, camera_name
            (optional, from sensor id), camera_type, camera_resolution, enable_depth, etc.
            Each entry gets its own stream manager to avoid cascading failures.
        camera_type: Camera type - "cv2" for USB/webcam/IP, "realsense" for Intel RealSense
        camera_id: Camera device ID (int) or stream URL (str) for CV2 cameras (default: 0)
        camera_resolution: Video resolution (default: VGA 640x480)
        enable_depth: Enable depth streaming for RealSense (default: False)
        depth_fps: Depth stream FPS for RealSense (default: 30)
        depth_resolution: Depth resolution for RealSense (default: same as color)
        depth_publish_interval: Publish depth every N frames for RealSense (default: 30)
    """
    time_reference = TimeReference()

    # Disable all logging to avoid interfering with status display
    logging.disable(logging.CRITICAL)

    # Build camera twins for CameraStreamManager (twin + optional overrides)
    camera_twins: List[Any] = []
    if cameras is not None:
        # cameras: list of dicts with "twin" and overrides -> (twin, overrides)
        for cfg in cameras:
            twin = cfg["twin"]
            overrides = {k: v for k, v in cfg.items() if k != "twin"}
            camera_twins.append((twin, overrides) if overrides else twin)
    elif camera is not None:
        actual_camera_id = camera_id
        if follower is not None and follower.config.cameras and len(follower.config.cameras) > 0:
            actual_camera_id = follower.config.cameras[0]
        overrides = {
            "camera_id": actual_camera_id,
            "camera_type": camera_type,
            "camera_resolution": camera_resolution,
            "enable_depth": enable_depth,
            "depth_fps": depth_fps,
            "depth_resolution": depth_resolution,
            "depth_publish_interval": depth_publish_interval,
        }
        camera_twins.append((camera, overrides))

    # Create status tracker
    status_tracker = StatusTracker()
    status_tracker.script_started = True
    status_tracker.camera_enabled = len(camera_twins) > 0

    # Set twin info for status display (use first camera for display)
    robot_uuid = robot.uuid if robot else ""
    robot_name = robot.name if robot and hasattr(robot, "name") else "so101-teleop"

    def _first_twin(twins: List[Any]):
        if not twins:
            return None
        first = twins[0]
        return first[0] if isinstance(first, tuple) else first

    first_camera_twin = _first_twin(camera_twins)
    camera_uuid_val = first_camera_twin.uuid if first_camera_twin else ""
    camera_display_name = (
        first_camera_twin.name if first_camera_twin and hasattr(first_camera_twin, "name") else ""
    )
    status_tracker.set_twin_info(robot_uuid, robot_name, camera_uuid_val, camera_display_name)

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
        raise RuntimeError(
            "Follower is required when robot twin is provided (follower data is sent to Cyberwave)"
        )

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
        # Upload leader calibration to twin if robot twin is provided
        if robot is not None:
            _upload_calibration_to_twin(leader, robot, "leader")

    # Upload follower calibration to twin if available
    if follower is not None and follower.calibration is not None and robot is not None:
        _upload_calibration_to_twin(follower, robot, "follower")

    # Use follower motors for mappings when sending to Cyberwave (follower data is what we send)
    # Fall back to leader motors if follower not available (for camera-only mode)
    motors_for_mapping = (
        follower.motors if follower is not None else (leader.motors if leader is not None else {})
    )

    # Build motor_id -> schema joint name mapping from twin's universal schema
    # Schema uses names like "_1", "_2" (SO101); matches backend get_controllable_joints
    motor_id_to_schema_joint: Dict[int, str] = {}
    if robot is not None and motors_for_mapping:
        try:
            schema_joint_names = robot.get_controllable_joint_names()
            for _, motor in motors_for_mapping.items():
                motor_id = motor.id
                idx = motor_id - 1  # 0-based index
                if idx < len(schema_joint_names):
                    motor_id_to_schema_joint[motor_id] = schema_joint_names[idx]
                else:
                    motor_id_to_schema_joint[motor_id] = f"_{motor_id}"  # fallback
        except Exception:
            # Fallback to _n naming if schema fetch fails
            motor_id_to_schema_joint = {
                motor.id: f"_{motor.id}" for _, motor in motors_for_mapping.items()
            }

    if motors_for_mapping:
        # Create mapping from joint names to joint indexes (motor IDs: 1-6)
        joint_name_to_index = {name: motor.id for name, motor in motors_for_mapping.items()}

        # Create mapping from joint indexes to joint names (for status display)
        joint_index_to_name = {str(motor.id): name for name, motor in motors_for_mapping.items()}
        status_tracker.set_joint_index_to_name(joint_index_to_name)

        # Create mapping from joint names to normalization modes
        joint_name_to_norm_mode = {
            name: motor.norm_mode for name, motor in motors_for_mapping.items()
        }

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
        target=run_status_logging_thread,
        args=(status_tracker, stop_event, CONTROL_RATE_HZ, camera_fps),
        kwargs={"leader": leader, "follower": follower, "mode": "teleoperate"},
        daemon=True,
    )
    status_thread.start()

    # Get follower calibration for proper conversion to radians
    follower_calibration = None
    if follower is not None and follower.calibration is not None:
        follower_calibration = follower.calibration
        assert follower_calibration.keys() == follower.motors.keys()
        for joint_name, calibration in follower_calibration.items():
            assert joint_name in follower.motors
            assert calibration.range_min is not None
            assert calibration.range_max is not None
            assert calibration.range_min < calibration.range_max

    # Start MQTT update worker thread
    worker_thread = None
    if robot is not None:
        worker_thread = threading.Thread(
            target=cyberwave_update_worker,
            args=(
                action_queue,
                joint_name_to_index,
                joint_name_to_norm_mode,
                stop_event,
                robot,
                time_reference,
                status_tracker,
                follower_calibration,
                motor_id_to_schema_joint,
            ),
            daemon=True,
        )
        worker_thread.start()

    # Start camera streaming via SDK CameraStreamManager (one stream per twin, each with own thread)
    camera_manager: Optional[CameraStreamManager] = None
    if camera_twins and cyberwave_client is not None:
        # Enrich overrides with camera_id from follower and fps where not set
        follower_cameras = (
            follower.config.cameras if follower is not None and follower.config.cameras else []
        )
        enriched_twins: List[Any] = []
        for idx, item in enumerate(camera_twins):
            if isinstance(item, tuple):
                twin, overrides = item
                overrides = dict(overrides)
            else:
                twin = item
                overrides = {}
            if "camera_id" not in overrides:
                overrides["camera_id"] = (
                    follower_cameras[idx] if idx < len(follower_cameras) else 0
                )
            overrides.setdefault("fps", camera_fps)
            enriched_twins.append((twin, overrides))

        # Build camera infos for per-camera status display
        camera_infos = []
        for item in enriched_twins:
            if isinstance(item, tuple):
                twin, overrides = item
            else:
                twin, overrides = item, {}
            cam_name = overrides.get("camera_name")
            if not cam_name:
                sensors = getattr(twin, "capabilities", {}).get("sensors", [])
                cam_name = (
                    sensors[0].get("id", "default")
                    if sensors and isinstance(sensors[0], dict)
                    else "default"
                )
            camera_infos.append({"uuid": str(twin.uuid), "name": cam_name})
        status_tracker.set_camera_infos(camera_infos)

        def command_callback(status: str, msg: str, camera_name: str = "default"):
            if status_tracker:
                msg_lower = msg.lower()
                if (
                    "started" in msg_lower
                    or (status == "ok" and ("streaming" in msg_lower or "running" in msg_lower))
                ):
                    status_tracker.update_webrtc_state(camera_name, "streaming")
                    status_tracker.update_camera_status(camera_name, detected=True, started=True)
                elif status == "connecting" or "starting" in msg_lower:
                    status_tracker.update_webrtc_state(camera_name, "connecting")
                elif status == "error":
                    status_tracker.update_webrtc_state(camera_name, "idle")
                elif "stopped" in msg_lower:
                    status_tracker.update_webrtc_state(camera_name, "idle")
                    status_tracker.update_camera_status(camera_name, detected=True, started=False)

        for info in camera_infos:
            cam_name = info["name"]
            status_tracker.update_camera_status(cam_name, detected=True, started=False)
            status_tracker.update_webrtc_state(cam_name, "connecting")

        camera_manager = CameraStreamManager(
            client=cyberwave_client,
            twins=enriched_twins,
            stop_event=stop_event,
            time_reference=time_reference,
            command_callback=command_callback,
        )
        camera_manager.start()
    else:
        status_tracker.camera_states.clear()

    # TimeReference synchronization: teleop loop runs at 100Hz for control and MQTT updates.
    status_tracker.fps = CONTROL_RATE_HZ

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
                    # Convert normalized position to radians using calibration
                    if follower_calibration and name in follower_calibration:
                        calib = follower_calibration[name]
                        r_min = calib.range_min
                        r_max = calib.range_max
                        delta_r = (r_max - r_min) / 2.0

                        norm_mode = joint_name_to_norm_mode[name]
                        if norm_mode == MotorNormMode.RANGE_M100_100:
                            # Normalized is in [-100, 100], convert to radians
                            raw_offset = (normalized_pos / 100.0) * delta_r
                            radians = raw_offset * (2.0 * math.pi / 4095.0)
                        elif norm_mode == MotorNormMode.RANGE_0_100:
                            # Normalized is in [0, 100], center at 50
                            center_normalized = 50.0
                            offset_normalized = normalized_pos - center_normalized
                            raw_offset = (offset_normalized / 100.0) * delta_r
                            radians = raw_offset * (2.0 * math.pi / 4095.0)
                        else:  # DEGREES
                            radians = normalized_pos * math.pi / 180.0
                    else:
                        # Fallback if no calibration
                        norm_mode = joint_name_to_norm_mode[name]
                        if norm_mode == MotorNormMode.RANGE_M100_100:
                            degrees = (normalized_pos / 100.0) * 180.0
                            radians = degrees * math.pi / 180.0
                        elif norm_mode == MotorNormMode.RANGE_0_100:
                            degrees = (normalized_pos / 100.0) * 360.0
                            radians = degrees * math.pi / 180.0
                        else:
                            radians = normalized_pos * math.pi / 180.0
                    # Use schema joint name (e.g. "_1", "_2") for MQTT
                    schema_joint = motor_id_to_schema_joint.get(
                        joint_index, f"_{joint_index}"
                    )
                    observations[schema_joint] = radians
            # Send follower observations to Cyberwave as single update
            # together with the desired actual frequency
            mqtt_client.publish_initial_observation(
                twin_uuid=robot.uuid,
                observations=observations,
                fps=CONTROL_RATE_HZ,
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

        if camera_manager is not None:
            camera_manager.join(timeout=5.0)

        if status_thread is not None:
            status_thread.join(timeout=1.0)


def _upload_calibration_to_twin(
    device: Union[SO101Leader, SO101Follower],
    twin: Twin,
    robot_type: str,
) -> None:
    """
    Upload calibration data from device to Cyberwave twin.

    Args:
        device: SO101Leader or SO101Follower instance with calibration
        twin: Twin instance to upload calibration to
        robot_type: Robot type ("leader" or "follower")
    """
    try:
        # Get calibration path from device config
        calibration_path = device.config.calibration_dir / f"{device.config.id}.json"

        if not calibration_path.exists():
            logger.debug(f"No calibration file found at {calibration_path}, skipping upload")
            return

        # Load calibration file
        calib_data = load_calibration(calibration_path)

        # Convert calibration format to backend format
        # Backend expects: joint_calibration dict with JointCalibration objects
        # Keys should be motor IDs as strings (e.g., "1", "2", "3") not joint names
        # Note: drive_mode and id must be strings per the schema
        joint_calibration = {}
        for joint_name, calib in calib_data.items():
            # Get motor ID from device motors mapping
            if joint_name not in device.motors:
                logger.warning(f"Joint '{joint_name}' not found in device motors, skipping")
                continue

            motor_id = device.motors[joint_name].id
            motor_id_str = str(motor_id)

            joint_calibration[motor_id_str] = {
                "range_min": calib["range_min"],
                "range_max": calib["range_max"],
                "homing_offset": calib["homing_offset"],
                "drive_mode": str(calib["drive_mode"]),
                "id": str(calib["id"]),
            }

        # Upload calibration to twin
        logger.info(f"Uploading {robot_type} calibration to twin {twin.uuid}...")
        twin.update_calibration(joint_calibration, robot_type=robot_type)
        logger.info(f"Calibration uploaded successfully to twin {twin.uuid}")
    except ImportError:
        logger.warning(
            "Cyberwave SDK not installed. Skipping calibration upload. "
            "Install with: pip install cyberwave"
        )
    except Exception as e:
        logger.warning(f"Failed to upload calibration: {e}")
        logger.debug("Calibration upload failed, continuing without upload", exc_info=True)


def _parse_resolution(resolution_str: str) -> Resolution:
    """Parse resolution string to Resolution enum.

    Args:
        resolution_str: Resolution string like "VGA", "HD", "FULL_HD", or "WIDTHxHEIGHT"

    Returns:
        Resolution enum value
    """
    resolution_str = resolution_str.upper().strip()

    # Try to match enum name
    resolution_map = {
        "QVGA": Resolution.QVGA,
        "VGA": Resolution.VGA,
        "SVGA": Resolution.SVGA,
        "HD": Resolution.HD,
        "720P": Resolution.HD,
        "FULL_HD": Resolution.FULL_HD,
        "1080P": Resolution.FULL_HD,
    }

    if resolution_str in resolution_map:
        return resolution_map[resolution_str]

    # Try to parse WIDTHxHEIGHT format
    if "x" in resolution_str.lower():
        try:
            width, height = resolution_str.lower().split("x")
            width, height = int(width), int(height)
            # Try to find matching resolution enum
            match = Resolution.from_size(width, height)
            if match:
                return match
            # Return closest resolution
            return Resolution.closest(width, height)
        except ValueError:
            pass

    raise ValueError(
        f"Invalid resolution: {resolution_str}. "
        "Use QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT format."
    )


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
        default=os.getenv("CYBERWAVE_TWIN_UUID"),
        help="SO101 twin UUID (override from setup.json)",
    )
    parser.add_argument(
        "--leader-port",
        type=str,
        default=os.getenv("CYBERWAVE_METADATA_LEADER_PORT"),
        help="Leader serial port (override from setup.json)",
    )
    parser.add_argument(
        "--follower-port",
        type=str,
        default=os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT"),
        help="Follower serial port (override from setup.json)",
    )
    parser.add_argument(
        "--list-realsense",
        action="store_true",
        help="List available RealSense devices and exit",
    )
    parser.add_argument(
        "--setup-path",
        type=str,
        default=None,
        help="Path to setup.json (default: ~/.cyberwave/so101_lib/setup.json)",
    )

    args = parser.parse_args()

    # Handle --list-realsense
    if args.list_realsense:
        if not _has_realsense:
            print("RealSense support not available. Install with: pip install pyrealsense2")
            sys.exit(1)

        print("Discovering RealSense devices...")
        devices = RealSenseDiscovery.list_devices()
        if not devices:
            print("No RealSense devices found.")
        else:
            print(f"Found {len(devices)} RealSense device(s):\n")
            for i, dev in enumerate(devices):
                print(f"Device {i}:")
                print(f"  Name: {dev.name}")
                print(f"  Serial: {dev.serial_number}")
                print(f"  Firmware: {dev.firmware_version}")
                print(f"  USB Type: {dev.usb_type}")
                print(f"  Sensors: {', '.join(dev.sensors)}")

                # Get detailed info for color resolutions
                detailed = RealSenseDiscovery.get_device_info(dev.serial_number)
                if detailed:
                    color_res = detailed.get_color_resolutions()
                    depth_res = detailed.get_depth_resolutions()
                    print(f"  Color Resolutions: {color_res}")
                    print(f"  Depth Resolutions: {depth_res}")
                print()
        sys.exit(0)

    # Initialize Cyberwave client
    cyberwave_client = Cyberwave()

    camera_twin = None
    cameras_list: List[Dict[str, Any]] = []
    setup_config: Dict[str, Any] = {}

    # Load setup.json by default when it exists (used for cameras, twin_uuid, ports)
    setup_path = Path(args.setup_path) if args.setup_path else get_setup_config_path()
    if setup_path.exists():
        setup_config = load_setup_config(setup_path)
        if setup_config:
            print(f"Loaded setup from: {setup_path}")

    # All camera/teleop config comes from setup.json (so101-setup)
    camera_only = setup_config.get("camera_only", False)
    max_relative_target = setup_config.get("max_relative_target")

    # Validate: camera-only requires setup with cameras
    if camera_only:
        has_setup_cameras = setup_config.get("wrist_camera") or len(
            setup_config.get("additional_cameras", [])
        ) > 0
        if not has_setup_cameras:
            print(
                "Error: camera_only in setup requires wrist_camera or additional_cameras. "
                "Run so101-setup with --wrist-camera or --additional-camera."
            )
            sys.exit(1)

    # Default camera values (used when cameras_list is empty - no camera streaming)
    camera_type = "cv2"
    camera_fps = setup_config.get("camera_fps", 30)
    enable_depth = False
    depth_fps = 30
    depth_publish_interval = 30
    camera_id: Union[int, str] = 0
    camera_resolution = Resolution.VGA
    depth_resolution = None

    # Use setup for cameras (always from setup)
    if setup_config:
        cameras_list = []

        if setup_config.get("wrist_camera"):
            uuid = setup_config.get("wrist_camera_twin_uuid")
            if not uuid:
                print("Error: wrist_camera_twin_uuid missing in setup config")
                sys.exit(1)
            twin = cyberwave_client.twin(twin_id=uuid)
            wrist_fps = setup_config.get("camera_fps", 30)
            wrist_res = setup_config.get("wrist_camera_resolution", "VGA")
            wrist_res_enum = _parse_resolution(wrist_res)
            cameras_list.append({
                "twin": twin,
                "camera_id": setup_config.get("wrist_camera_id", 0),
                "camera_type": "cv2",
                "camera_resolution": wrist_res_enum,
                "camera_name": setup_config.get("wrist_camera_name", "wrist_camera"),
                "fps": wrist_fps,
                "fourcc": setup_config.get("wrist_camera_fourcc"),
                "keyframe_interval": setup_config.get("wrist_camera_keyframe_interval"),
                "enable_depth": False,
                "depth_fps": 30,
                "depth_resolution": None,
                "depth_publish_interval": 30,
            })

        for add in setup_config.get("additional_cameras", []):
            uuid = add.get("twin_uuid")
            if not uuid:
                print("Error: twin_uuid missing in additional_cameras entry")
                sys.exit(1)
            twin = cyberwave_client.twin(twin_id=uuid)
            res = add.get("resolution", [640, 480])
            cam_res = Resolution.from_size(res[0], res[1]) if len(res) >= 2 else Resolution.VGA
            if cam_res is None:
                cam_res = Resolution.closest(res[0], res[1]) if len(res) >= 2 else Resolution.VGA
            depth_res = add.get("depth_resolution")
            depth_res_enum = None
            if depth_res and len(depth_res) >= 2:
                depth_res_enum = Resolution.from_size(depth_res[0], depth_res[1]) or Resolution.closest(depth_res[0], depth_res[1])
            cameras_list.append({
                "twin": twin,
                "camera_id": add.get("camera_id", 1),
                "camera_type": add.get("camera_type", "cv2"),
                "camera_resolution": cam_res,
                "camera_name": add.get("camera_name", "external"),
                "fps": add.get("fps", 30),
                "fourcc": add.get("fourcc"),
                "enable_depth": add.get("enable_depth", False),
                "depth_fps": add.get("depth_fps", 30),
                "depth_resolution": depth_res_enum,
                "depth_publish_interval": add.get("depth_publish_interval", 30),
            })

        if cameras_list:
            camera_twin = cameras_list[0]["twin"]
            camera_fps = cameras_list[0].get("fps", 30)

    # Resolve twin UUID and ports: CLI/env > setup.json (so teleoperate works with no args when setup exists)
    effective_twin_uuid = args.twin_uuid or setup_config.get("twin_uuid") or setup_config.get(
        "wrist_camera_twin_uuid"
    )
    effective_leader_port = args.leader_port or setup_config.get("leader_port")
    effective_follower_port = args.follower_port or setup_config.get("follower_port")
    if not effective_twin_uuid:
        print(
            "Error: Twin UUID required. Use --twin-uuid, set CYBERWAVE_TWIN_UUID, "
            "or run so101-setup with --twin-uuid"
        )
        sys.exit(1)

    # Follower required for teleop (sends data to Cyberwave) or camera-only (streaming)
    if not effective_follower_port and (not camera_only or cameras_list):
        print(
            "Error: Follower port required. Calibrate follower first (so101-calibrate) to save port, "
            "or pass --follower-port / set CYBERWAVE_METADATA_FOLLOWER_PORT"
        )
        sys.exit(1)

    robot = cyberwave_client.twin(
        asset_key="the-robot-studio/so101", twin_id=effective_twin_uuid, name="robot"
    )
    # Pass cameras list when we have configs (single or multi), else single camera for backward compat
    camera = camera_twin if not cameras_list else None
    cameras = cameras_list if cameras_list else None
    mqtt_client = cyberwave_client.mqtt

    # Initialize leader (optional if camera-only mode)
    leader = None
    if not camera_only:
        from utils.config import LeaderConfig
        from utils.utils import find_port

        leader_port = effective_leader_port
        if not leader_port:
            leader_port = find_port(device_name="SO101 Leader")

        leader_config = LeaderConfig(port=leader_port)
        leader = SO101Leader(config=leader_config)
        leader.connect()

    # Initialize follower (required for camera-only mode, optional otherwise)
    follower = None
    if effective_follower_port or camera_only:
        if not effective_follower_port:
            raise RuntimeError(
                "--follower-port is required when using --camera-only. "
                "Calibrate the follower first (so101-calibrate) or pass --follower-port."
            )
        from utils.config import FollowerConfig

        # Only configure cameras on the follower if camera(s) are being used
        follower_cameras = None
        if cameras_list:
            # Support both int (device index) and str (URL) for camera_id
            follower_cameras = [cfg["camera_id"] for cfg in cameras_list]

        follower_config = FollowerConfig(
            port=effective_follower_port,
            max_relative_target=max_relative_target,
            cameras=follower_cameras,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()

    try:
        teleoperate(
            leader=leader,
            cyberwave_client=cyberwave_client,
            follower=follower,
            camera_fps=camera_fps,
            robot=robot,
            camera=camera,
            cameras=cameras,
            camera_type=camera_type,
            camera_id=camera_id,
            camera_resolution=camera_resolution,
            enable_depth=enable_depth,
            depth_fps=depth_fps,
            depth_resolution=depth_resolution,
            depth_publish_interval=depth_publish_interval,
        )
    finally:
        if leader is not None:
            leader.disconnect()
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
