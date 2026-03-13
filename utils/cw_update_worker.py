"""MQTT update worker for publishing joint states to Cyberwave twin."""

import queue
import threading
import time
from typing import Any, Dict, Optional

from cyberwave import Twin

from motors import MotorNormMode
from utils.trackers import StatusTracker
from utils.utils import normalized_to_radians


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

    Sends aggregated joint updates with all joints in a single MQTT message using the SDK's
    update_joints_state method, which produces the format:
    {"positions": {...}, "velocities": {...}, "efforts": {...}, "timestamp": ..., "source_type": ...}

    Args:
        action_queue: Queue containing (source_type, observation, timestamp) tuples where
                     observation is a dict with joint positions
        joint_name_to_index: Dictionary mapping joint names to joint indexes (1-6)
        joint_name_to_norm_mode: Dictionary mapping joint names to normalization modes
        stop_event: Event to signal thread to stop
        twin: Optional Twin instance for updating joint states
        status_tracker: Optional status tracker for statistics
        follower_calibration: Calibration data for converting normalized positions
        motor_id_to_schema_joint: Mapping from motor ID to schema joint name (e.g. 1 -> "_1")
    """
    batch_timeout = 0.01  # 10ms - collect updates for batching

    while not stop_event.is_set():
        try:
            # Collect all updates within batch window, keyed by source_type
            batch_by_source: Dict[str, tuple] = {}
            batch_start_time = time.time()

            while True:
                try:
                    remaining_time = batch_timeout - (time.time() - batch_start_time)
                    if remaining_time <= 0:
                        break

                    source_type, observation, timestamp = action_queue.get(
                        timeout=min(remaining_time, 0.001)
                    )
                    # Keep latest update per source_type
                    batch_by_source[source_type or "edge"] = (observation, timestamp, source_type)
                    action_queue.task_done()
                except queue.Empty:
                    break

            # Publish aggregated message for each source_type
            for _source_key, (observation, timestamp, source_type) in batch_by_source.items():
                try:
                    positions: Dict[str, float] = {}
                    velocities: Dict[str, float] = {}
                    efforts: Dict[str, float] = {}
                    joint_states: Dict[str, float] = {}

                    for joint_key, normalized_pos in observation.items():
                        joint_name = (
                            joint_key.removesuffix(".pos")
                            if joint_key.endswith(".pos")
                            else joint_key
                        )
                        joint_index = joint_name_to_index.get(joint_name)
                        if joint_index is None:
                            continue

                        norm_mode = joint_name_to_norm_mode.get(
                            joint_name, MotorNormMode.RANGE_M100_100
                        )
                        calib = (
                            follower_calibration.get(joint_name) if follower_calibration else None
                        )
                        position = normalized_to_radians(normalized_pos, norm_mode, calib)

                        schema_joint = (
                            motor_id_to_schema_joint.get(joint_index, str(joint_index))
                            if motor_id_to_schema_joint
                            else str(joint_index)
                        )

                        positions[schema_joint] = position
                        velocities[schema_joint] = 0.0
                        efforts[schema_joint] = 0.0
                        joint_states[str(joint_index)] = position

                    if positions and twin is not None:
                        twin.client.mqtt.update_joints_state(
                            twin_uuid=str(twin.uuid),
                            joint_positions=positions,
                            source_type=source_type,
                            velocities=velocities,
                            efforts=efforts,
                            timestamp=timestamp,
                        )

                        if status_tracker:
                            status_tracker.increment_produced()
                            status_tracker.update_joint_states(joint_states)

                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors_mqtt()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors_mqtt()


# Frame counters for downsampling (100Hz control loop -> 30Hz MQTT updates)
_FRAME_COUNTERS: Dict[str, int] = {}
PUBLISH_EVERY_N_FRAMES = 3  # 100Hz / 3 ≈ 33Hz


def process_cyberwave_updates(
    action: Dict[str, float],
    action_queue: queue.Queue,
    timestamp: float,
    status_tracker: Optional[StatusTracker] = None,
    source_type: Optional[str] = None,
) -> int:
    """
    Queue entire observation for Cyberwave update (no per-joint filtering).

    Sends all joints together in a single aggregated message.
    Downsamples from 100Hz to ~30Hz by only sending every 3rd frame.

    Args:
        action: Observation dictionary with normalized positions
        action_queue: Queue for Cyberwave updates
        timestamp: Timestamp to associate with this update
        status_tracker: Optional status tracker for statistics
        source_type: SOURCE_TYPE_EDGE_LEADER or SOURCE_TYPE_EDGE_FOLLOWER for MQTT
    Returns:
        Number of joints queued (0 if queue full or skipped due to sampling)
    """
    if not action:
        return 0

    # Downsample: only send every Nth frame per source_type
    key = source_type or "edge"
    frame_count = _FRAME_COUNTERS.get(key, 0)
    _FRAME_COUNTERS[key] = frame_count + 1

    if frame_count % PUBLISH_EVERY_N_FRAMES != 0:
        if status_tracker:
            status_tracker.increment_filtered()
        return 0

    try:
        action_queue.put_nowait((source_type, action, timestamp))
        return len(action)
    except queue.Full:
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return 0
