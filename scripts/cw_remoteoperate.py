"""Remote operation loop for SO101 follower via Cyberwave MQTT."""

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
from scripts.cw_write_position import validate_position
from so101.camera import CameraConfig
from so101.follower import SO101Follower
from utils.config import get_setup_config_path
from utils.trackers import StatusTracker, run_status_logging_thread
from utils.utils import load_calibration

logger = logging.getLogger(__name__)

def _radians_to_normalized(
    radians: float,
    norm_mode: MotorNormMode,
    calib: Optional[Any] = None,
) -> float:
    """
    Convert radians to normalized position based on motor normalization mode and calibration.

    This is the inverse of teleoperate's normalized->radians conversion.
    Uses calibration data when available for accurate conversion.

    Args:
        radians: Position in radians
        norm_mode: Motor normalization mode
        calib: Calibration data with range_min and range_max (optional)

    Returns:
        Normalized position value
    """
    if calib is not None:
        # Use calibration-based conversion (inverse of teleoperate's conversion)
        r_min = calib.range_min
        r_max = calib.range_max
        delta_r = (r_max - r_min) / 2.0

        if norm_mode == MotorNormMode.RANGE_M100_100:
            # Inverse of: raw_offset = (normalized / 100.0) * delta_r
            #             radians = raw_offset * (2.0 * math.pi / 4095.0)
            # So: raw_offset = radians / (2.0 * math.pi / 4095.0)
            #     normalized = (raw_offset / delta_r) * 100.0
            raw_offset = radians / (2.0 * math.pi / 4095.0)
            normalized = (raw_offset / delta_r) * 100.0
            return normalized
        elif norm_mode == MotorNormMode.RANGE_0_100:
            # Inverse of teleoperate's conversion for RANGE_0_100
            delta_r_full = r_max - r_min
            # radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
            # raw_value = radians / (2.0 * math.pi / 4095.0) + r_min
            # normalized = ((raw_value - r_min) / delta_r_full) * 100.0
            raw_value = radians / (2.0 * math.pi / 4095.0) + r_min
            normalized = ((raw_value - r_min) / delta_r_full) * 100.0
            return normalized
        else:  # DEGREES
            return radians * 180.0 / math.pi
    else:
        # Fallback without calibration
        if norm_mode == MotorNormMode.DEGREES:
            degrees = radians * 180.0 / math.pi
            return degrees
        elif norm_mode == MotorNormMode.RANGE_M100_100:
            degrees = radians * 180.0 / math.pi
            normalized = (degrees / 180.0) * 100.0
            return normalized
        elif norm_mode == MotorNormMode.RANGE_0_100:
            degrees = radians * 180.0 / math.pi
            if degrees < 0:
                degrees = degrees + 360.0
            normalized = (degrees / 360.0) * 100.0
            return normalized
        else:
            return radians


def _joint_position_heartbeat_thread(
    follower: SO101Follower,
    mqtt_client: Any,
    twin_uuid: str,
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower_calibration: Optional[Dict[str, Any]],
    motor_id_to_schema_joint: Dict[int, str],
    stop_event: threading.Event,
    interval: float = 1.0,
) -> None:
    """
    Thread that publishes the follower's current joint positions via MQTT every second.

    Keeps the remote operator in sync with the actual follower position to avoid mismatches.

    Args:
        follower: SO101Follower instance
        mqtt_client: MQTT client for publishing
        twin_uuid: Twin UUID
        joint_name_to_norm_mode: Mapping from joint name to normalization mode
        follower_calibration: Calibration data for converting normalized to radians
        motor_id_to_schema_joint: Mapping from motor ID to schema joint name (e.g. 1 -> "_1")
        stop_event: Event to signal thread to stop
        interval: Publish interval in seconds (default: 1.0)
    """
    while not stop_event.is_set():
        try:
            if not mqtt_client or not mqtt_client.connected:
                time.sleep(interval)
                continue

            follower_obs = follower.get_observation()
            if not follower_obs:
                time.sleep(interval)
                continue

            joint_positions: Dict[str, float] = {}
            for joint_key, normalized_pos in follower_obs.items():
                name = joint_key.removesuffix(".pos")
                if name not in follower.motors:
                    continue
                joint_index = follower.motors[name].id
                norm_mode = joint_name_to_norm_mode.get(name)
                if norm_mode is None:
                    continue

                if follower_calibration and name in follower_calibration:
                    calib = follower_calibration[name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        raw_offset = (normalized_pos / 100.0) * delta_r
                        radians = raw_offset * (2.0 * math.pi / 4095.0)
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        delta_r_full = r_max - r_min
                        raw_value = r_min + (normalized_pos / 100.0) * delta_r_full
                        radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                    else:
                        radians = normalized_pos * math.pi / 180.0
                else:
                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        degrees = (normalized_pos / 100.0) * 180.0
                        radians = degrees * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        degrees = (normalized_pos / 100.0) * 360.0
                        radians = degrees * math.pi / 180.0
                    else:
                        radians = normalized_pos * math.pi / 180.0

                schema_joint = motor_id_to_schema_joint.get(
                    joint_index, f"_{joint_index}"
                )
                joint_positions[schema_joint] = radians

            if joint_positions:
                timestamp = time.time()
                for schema_joint, radians in joint_positions.items():
                    mqtt_client.update_joint_state(
                        twin_uuid=twin_uuid,
                        joint_name=schema_joint,
                        position=radians,
                        timestamp=timestamp,
                    )
        except Exception:
            pass
        time.sleep(interval)


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
    from utils.utils import ensure_safe_goal_position

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
                    # get_observation() handles SerialException internally and returns last known positions
                    present_pos = follower.get_observation()
                    # Handle empty dict gracefully (shouldn't happen, but be safe)
                    if not present_pos:
                        # Skip this action if we can't get current positions
                        continue
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
                    for _ in range(max_steps):
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
    follower_calibration: Optional[Dict[str, Any]] = None,
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
        follower_calibration: Calibration data for converting radians to normalized positions
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
            # Supported formats:
            # 1) Single joint: {"joint_name": "5", "joint_state": {"position": -1.22}, "timestamp": ...}
            # 2) Multi-joint: {"source_type": "sim", "shoulder_pan": 1.5, "shoulder_lift": 0.3, ...}

            if "joint_name" in data and "joint_state" in data:
                # Format 1: single joint
                _process_single_joint_update(
                    data, current_state, action_queue, joint_index_to_name,
                    joint_name_to_norm_mode, follower, follower_calibration, status_tracker,
                )
                return

            # Format 2: multi-joint (direct joint name -> position)
            # Build one action with all joints, then put in queue
            action = current_state.copy()
            joint_states_for_status = {}
            any_valid = False
            for joint_name, position_val in data.items():
                if joint_name in ("source_type", "timestamp", "session_id", "type"):
                    continue
                if joint_name not in follower.motors:
                    continue
                try:
                    position_radians = float(position_val)
                except (ValueError, TypeError):
                    continue

                norm_mode = joint_name_to_norm_mode.get(joint_name)
                if norm_mode is None:
                    continue
                calib = follower_calibration.get(joint_name) if follower_calibration else None
                normalized_position = _radians_to_normalized(position_radians, norm_mode, calib)
                motor_id = follower.motors[joint_name].id
                is_valid, _ = validate_position(motor_id, normalized_position)
                if not is_valid:
                    if status_tracker:
                        status_tracker.increment_filtered()
                    continue

                action[f"{joint_name}.pos"] = normalized_position
                current_state[f"{joint_name}.pos"] = normalized_position
                joint_states_for_status[str(motor_id)] = position_radians
                any_valid = True

            if any_valid:
                if status_tracker:
                    status_tracker.update_joint_states(joint_states_for_status)
                try:
                    action_queue.put_nowait(action)
                except queue.Full:
                    if status_tracker:
                        status_tracker.increment_errors()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors()

    return callback


def _apply_joint_position(
    joint_name: str,
    position_radians: float,
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]],
    status_tracker: Optional[StatusTracker],
) -> None:
    """Apply a single joint position to the action queue."""
    norm_mode = joint_name_to_norm_mode.get(joint_name)
    if norm_mode is None:
        return
    calib = follower_calibration.get(joint_name) if follower_calibration else None
    normalized_position = _radians_to_normalized(position_radians, norm_mode, calib)
    motor_id = follower.motors[joint_name].id
    is_valid, _ = validate_position(motor_id, normalized_position)
    if not is_valid:
        if status_tracker:
            status_tracker.increment_filtered()
        return
    action = current_state.copy()
    action[f"{joint_name}.pos"] = normalized_position
    current_state[f"{joint_name}.pos"] = normalized_position
    if status_tracker:
        joint_states = {str(motor_id): position_radians}
        status_tracker.update_joint_states(joint_states)
    try:
        action_queue.put_nowait(action)
    except queue.Full:
        if status_tracker:
            status_tracker.increment_errors()


def _process_single_joint_update(
    data: Dict,
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]],
    status_tracker: Optional[StatusTracker],
) -> None:
    """Process single-joint format: joint_name + joint_state."""
    joint_name_str = data.get("joint_name")
    joint_state = data.get("joint_state", {})
    position_radians = joint_state.get("position")

    if position_radians is None:
        if status_tracker:
            status_tracker.increment_errors()
        return

    # Convert joint_name to joint_index (e.g. "5" or "shoulder_pan")
    joint_index = None
    try:
        joint_index = int(joint_name_str)
    except (ValueError, TypeError):
        for idx, name in joint_index_to_name.items():
            if name == joint_name_str:
                joint_index = idx
                break
        if joint_index is None:
            if status_tracker:
                status_tracker.increment_errors()
            return

    joint_name = joint_index_to_name.get(joint_index)
    if joint_name is None:
        if status_tracker:
            status_tracker.increment_errors()
        return

    try:
        position_radians = float(position_radians)
    except (ValueError, TypeError):
        if status_tracker:
            status_tracker.increment_errors()
        return

    _apply_joint_position(
        joint_name=joint_name,
        position_radians=position_radians,
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
        follower_calibration=follower_calibration,
        status_tracker=status_tracker,
    )


CONTROL_RATE_HZ = 100


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
    cameras: Optional[List[Dict[str, Any]]] = None,
    fps: int = 30,
    camera_fps: int = 30,
    camera_type: str = "cv2",
    camera_id: Union[int, str] = 0,
    camera_resolution: Resolution = Resolution.VGA,
    enable_depth: bool = False,
    depth_fps: int = 30,
    depth_resolution: Optional[Resolution] = None,
    depth_publish_interval: int = 30,
) -> None:
    """
    Run remote operation loop: receive joint states via MQTT and write to follower motors.

    Supports both CV2 (USB/webcam/IP) cameras and Intel RealSense cameras.

    Args:
        client: Cyberwave client instance
        follower: SO101Follower instance
        robot: Robot twin instance
        camera: Camera twin instance (single camera, backward compat). Ignored if cameras is set.
        cameras: List of camera configs for multi-stream. Each dict: twin, camera_id, camera_name
            (optional, from sensor id), camera_type, camera_resolution, enable_depth, etc.
            Each entry gets its own stream manager to avoid cascading failures.
        fps: Target frames per second (not used directly, but kept for compatibility)
        camera_fps: Frames per second for camera streaming
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
    robot_name = robot.name if robot and hasattr(robot, "name") else "so101-remote"

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

    # Upload follower calibration to twin if available
    if follower.calibration is not None:
        _upload_calibration_to_twin(follower, robot, "follower")

    # Get follower calibration for proper conversion to/from radians
    follower_calibration = None
    if follower is not None and follower.calibration is not None:
        follower_calibration = follower.calibration
        assert follower_calibration.keys() == follower.motors.keys()
        for joint_name, calibration in follower_calibration.items():
            assert joint_name in follower.motors
            assert calibration.range_min is not None
            assert calibration.range_max is not None
            assert calibration.range_min < calibration.range_max

    # Create mapping from joint index (motor ID) to joint name
    joint_index_to_name = {motor.id: name for name, motor in follower.motors.items()}

    # Create mapping from joint indexes to joint names (for status display, string keys)
    joint_index_to_name_str = {str(motor.id): name for name, motor in follower.motors.items()}
    status_tracker.set_joint_index_to_name(joint_index_to_name_str)

    # Create mapping from joint names to normalization modes
    joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in follower.motors.items()}

    # Build motor_id -> schema joint name mapping (e.g. 1 -> "_1") for MQTT publish
    motor_id_to_schema_joint: Dict[int, str] = {}
    try:
        schema_joint_names = robot.get_controllable_joint_names()
        for _, motor in follower.motors.items():
            motor_id = motor.id
            idx = motor_id - 1
            if idx < len(schema_joint_names):
                motor_id_to_schema_joint[motor_id] = schema_joint_names[idx]
            else:
                motor_id_to_schema_joint[motor_id] = f"_{motor_id}"
    except Exception:
        motor_id_to_schema_joint = {
            motor.id: f"_{motor.id}" for _, motor in follower.motors.items()
        }

    # Initialize current state with follower's current observation
    current_state: Dict[str, float] = {}
    initial_joint_states_radians: Dict[str, float] = {}  # For status display
    try:
        # get_observation() handles SerialException internally and returns last known positions
        initial_obs = follower.get_observation()
        current_state.update(initial_obs)

        # Convert to radians for status display
        for joint_key, normalized_pos in initial_obs.items():
            name = joint_key.removesuffix(".pos")
            if name in follower.motors:
                joint_index = follower.motors[name].id
                norm_mode = joint_name_to_norm_mode[name]

                # Convert normalized position to radians using calibration
                if follower_calibration and name in follower_calibration:
                    calib = follower_calibration[name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        raw_offset = (normalized_pos / 100.0) * delta_r
                        radians = raw_offset * (2.0 * math.pi / 4095.0)
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        delta_r_full = r_max - r_min
                        raw_value = r_min + (normalized_pos / 100.0) * delta_r_full
                        radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                    else:
                        radians = normalized_pos * math.pi / 180.0
                else:
                    # Fallback without calibration
                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        degrees = (normalized_pos / 100.0) * 180.0
                        radians = degrees * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        degrees = (normalized_pos / 100.0) * 360.0
                        radians = degrees * math.pi / 180.0
                    else:
                        radians = normalized_pos * math.pi / 180.0

                initial_joint_states_radians[str(joint_index)] = radians
    except Exception:
        # Initialize with empty state if get_observation() fails for other reasons
        for name in follower.motors.keys():
            current_state[f"{name}.pos"] = 0.0
            joint_index = follower.motors[name].id
            initial_joint_states_radians[str(joint_index)] = 0.0

    # Populate initial joint states in status tracker (so we never show "waiting")
    status_tracker.update_joint_states(initial_joint_states_radians)

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

        # Convert to joint index format for initial observation using calibration
        observations = {}
        for joint_key, normalized_pos in follower_obs.items():
            name = joint_key.removesuffix(".pos")
            if name in follower.motors:
                joint_index = follower.motors[name].id
                norm_mode = joint_name_to_norm_mode[name]

                # Convert normalized position to radians using calibration (like teleoperate)
                if follower_calibration and name in follower_calibration:
                    calib = follower_calibration[name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        raw_offset = (normalized_pos / 100.0) * delta_r
                        radians = raw_offset * (2.0 * math.pi / 4095.0)
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        delta_r_full = r_max - r_min
                        raw_value = r_min + (normalized_pos / 100.0) * delta_r_full
                        radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                    else:
                        radians = normalized_pos * math.pi / 180.0
                else:
                    # Fallback without calibration
                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        degrees = (normalized_pos / 100.0) * 180.0
                        radians = degrees * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        degrees = (normalized_pos / 100.0) * 360.0
                        radians = degrees * math.pi / 180.0
                    else:
                        radians = normalized_pos * math.pi / 180.0

                observations[joint_index] = radians

        # Send initial observation
        mqtt_client.publish_initial_observation(
            twin_uuid=twin_uuid,
            observations=observations,
            fps=CONTROL_RATE_HZ,
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
        target=run_status_logging_thread,
        args=(status_tracker, stop_event, CONTROL_RATE_HZ, camera_fps),
        kwargs={"follower": follower, "mode": "remoteoperate"},
        daemon=True,
    )
    status_thread.start()

    # Start joint position heartbeat thread - publish follower position every second to avoid mismatches
    heartbeat_thread = threading.Thread(
        target=_joint_position_heartbeat_thread,
        args=(
            follower,
            mqtt_client,
            twin_uuid,
            joint_name_to_norm_mode,
            follower_calibration,
            motor_id_to_schema_joint,
            stop_event,
        ),
        kwargs={"interval": 1.0},
        daemon=True,
    )
    heartbeat_thread.start()

    # Start camera streaming via SDK CameraStreamManager (one stream per twin, each with own thread)
    camera_manager: Optional[CameraStreamManager] = None
    if camera_twins and client is not None:
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
            client=client,
            twins=enriched_twins,
            stop_event=stop_event,
            time_reference=time_reference,
            command_callback=command_callback,
        )
        camera_manager.start()
    else:
        status_tracker.camera_states.clear()

    # Create callback for joint state updates
    joint_state_callback = _create_joint_state_callback(
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
        follower_calibration=follower_calibration,
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

        # Wait for heartbeat thread
        heartbeat_thread.join(timeout=1.0)

        # Stop camera streaming
        if camera_manager is not None:
            camera_manager.join(timeout=5.0)

        # Stop status thread
        if status_thread is not None:
            status_thread.join(timeout=1.0)


def _upload_calibration_to_twin(
    follower: SO101Follower,
    twin: Twin,
    robot_type: str = "follower",
) -> None:
    """
    Upload calibration data from follower device to Cyberwave twin.

    Args:
        follower: SO101Follower instance with calibration
        twin: Twin instance to upload calibration to
        robot_type: Robot type (default: "follower")
    """
    try:
        # Get calibration path from follower config
        calibration_path = follower.config.calibration_dir / f"{follower.config.id}.json"

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
            # Get motor ID from follower motors mapping
            if joint_name not in follower.motors:
                logger.warning(f"Joint '{joint_name}' not found in follower motors, skipping")
                continue

            motor_id = follower.motors[joint_name].id
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
    """Parse resolution string to Resolution enum."""
    resolution_str = resolution_str.upper().strip()

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

    if "x" in resolution_str.lower():
        try:
            width, height = resolution_str.lower().split("x")
            width, height = int(width), int(height)
            match = Resolution.from_size(width, height)
            if match:
                return match
            return Resolution.closest(width, height)
        except ValueError:
            pass

    raise ValueError(
        f"Invalid resolution: {resolution_str}. "
        "Use QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT format."
    )


def main():
    """Main entry point for remote operation script."""
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Remote operate SO101 follower via Cyberwave MQTT"
    )
    parser.add_argument(
        "--twin-uuid",
        type=str,
        default=os.getenv("CYBERWAVE_TWIN_UUID"),
        help="SO101 twin UUID (override from setup.json)",
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

    # All camera/remote config comes from setup.json (so101-setup)
    max_relative_target = setup_config.get("max_relative_target")

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

    # Resolve twin UUID and ports: CLI/env > setup.json (so remoteoperate works with no args when setup exists)
    effective_twin_uuid = args.twin_uuid or setup_config.get("twin_uuid") or setup_config.get(
        "wrist_camera_twin_uuid"
    )
    effective_follower_port = args.follower_port or setup_config.get("follower_port")
    if not effective_twin_uuid:
        print(
            "Error: Twin UUID required. Use --twin-uuid, set CYBERWAVE_TWIN_UUID, "
            "or run so101-setup with --twin-uuid"
        )
        sys.exit(1)

    # Follower required for remote operation
    if not effective_follower_port:
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

    # Initialize follower
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
        remoteoperate(
            client=cyberwave_client,
            follower=follower,
            robot=robot,
            camera=camera,
            cameras=cameras,
            camera_fps=camera_fps,
            camera_type=camera_type,
            camera_id=camera_id,
            camera_resolution=camera_resolution,
            enable_depth=enable_depth,
            depth_fps=depth_fps,
            depth_resolution=depth_resolution,
            depth_publish_interval=depth_publish_interval,
        )
    finally:
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
