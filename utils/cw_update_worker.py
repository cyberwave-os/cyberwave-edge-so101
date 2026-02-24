"""MQTT update worker for publishing joint states to Cyberwave twin."""

import math
import queue
import threading
import time
from typing import Any, Dict, Optional

from cyberwave import Twin

from motors import MotorNormMode

from utils.trackers import StatusTracker


def cyberwave_update_worker(
    action_queue: queue.Queue,
    joint_name_to_index: Dict[str, int],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    stop_event: threading.Event,
    twin: Optional[Twin] = None,
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
    """
    batch_timeout = 0.01  # 10ms - collect updates for batching

    while not stop_event.is_set():
        try:
            batch_updates: Dict[int, tuple] = {}
            batch_start_time = time.time()

            while True:
                try:
                    remaining_time = batch_timeout - (time.time() - batch_start_time)
                    if remaining_time <= 0:
                        break

                    joint_name, action_data = action_queue.get(timeout=min(remaining_time, 0.001))
                except queue.Empty:
                    break

                try:
                    joint_index = joint_name_to_index.get(joint_name)
                    if joint_index is None:
                        action_queue.task_done()
                        continue

                    normalized_position = action_data.get("position", 0.0)
                    timestamp = action_data.get("timestamp")

                    calib = follower_calibration[joint_name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    norm_mode = joint_name_to_norm_mode.get(
                        joint_name, MotorNormMode.RANGE_M100_100
                    )

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

                    batch_updates[joint_index] = (position, 0.0, 0.0, timestamp)
                    action_queue.task_done()

                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors()
                    action_queue.task_done()

            if batch_updates:
                try:
                    joint_states: Dict[str, float] = {}
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

                    if status_tracker:
                        status_tracker.increment_produced()
                        status_tracker.update_joint_states(joint_states)
                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors()


def process_cyberwave_updates(
    action: Dict[str, float],
    last_observation: Dict[str, float],
    action_queue: queue.Queue,
    position_threshold: float,
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
        joint_name = joint_key.removesuffix(".pos") if joint_key.endswith(".pos") else joint_key

        if joint_name not in last_observation:
            last_observation[joint_name] = float("inf")

        if last_send_times is not None and joint_name not in last_send_times:
            last_send_times[joint_name] = 0.0

        last_obs = last_observation[joint_name]
        pos_changed = abs(normalized_pos - last_obs) >= position_threshold
        is_first_update = last_obs == float("inf")

        needs_heartbeat = False
        if last_send_times is not None:
            time_since_last_send = current_time - last_send_times.get(joint_name, 0.0)
            needs_heartbeat = time_since_last_send >= heartbeat_interval

        if is_first_update or pos_changed or needs_heartbeat:
            last_observation[joint_name] = normalized_pos

            if last_send_times is not None:
                last_send_times[joint_name] = current_time

            action_data = {
                "position": normalized_pos,
                "velocity": 0.0,
                "load": 0.0,
                "timestamp": timestamp,
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
