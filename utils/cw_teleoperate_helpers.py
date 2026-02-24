"""Helper functions for cw_teleoperate script."""

import argparse
import logging
import os
import queue
import threading
import time
from typing import Any, Dict, Optional

from cyberwave.utils import TimeReference

from motors import MotorNormMode
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.cw_update_worker import process_cyberwave_updates
from utils.trackers import StatusTracker
from utils.utils import normalized_to_radians

logger = logging.getLogger(__name__)

CONTROL_RATE_HZ = 100


def get_teleoperate_parser() -> argparse.ArgumentParser:
    """Create argument parser for cw_teleoperate script."""
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
    return parser


def teleop_loop(
    leader: SO101Leader,
    follower: SO101Follower,
    action_queue: queue.Queue,
    stop_event: threading.Event,
    last_observation: Dict[str, float],
    position_threshold: float,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    heartbeat_interval: float = 1.0,
    control_rate_hz: int = CONTROL_RATE_HZ,
) -> tuple[int, int]:
    """
    Main teleoperation loop: read from leader, send to follower, send data to Cyberwave.

    The loop always runs at 100Hz for responsive robot control and MQTT updates.
    """
    total_update_count = 0
    total_skip_count = 0
    last_send_times: Dict[str, float] = {}
    control_frame_time = 1.0 / control_rate_hz

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            timestamp, timestamp_monotonic = time_reference.update()

            leader_action = leader.get_action()
            follower_action = follower.get_observation()

            try:
                follower.send_action(leader_action)
            except Exception:
                if status_tracker:
                    status_tracker.increment_errors()

            update_count, skip_count = process_cyberwave_updates(
                action=follower_action,
                last_observation=last_observation,
                action_queue=action_queue,
                position_threshold=position_threshold,
                timestamp=timestamp,
                status_tracker=status_tracker,
                last_send_times=last_send_times,
                heartbeat_interval=heartbeat_interval,
            )
            total_update_count += update_count
            total_skip_count += skip_count

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


def publish_initial_follower_observation(
    follower: SO101Follower,
    robot: Any,
    mqtt_client: Any,
    follower_calibration: Optional[Dict[str, Any]],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    motor_id_to_schema_joint: Dict[int, str],
    fps: int = 100,
) -> None:
    """
    Convert follower observation to schema joint format and publish to Cyberwave.

    Uses calibration to convert normalized positions to radians, and motor_id_to_schema_joint
    to map SO101 joints to twin schema names (e.g. "_1", "_2").
    """
    follower_obs = follower.get_observation()
    observations = {}
    for joint_key, normalized_pos in follower_obs.items():
        name = joint_key.removesuffix(".pos")
        if name not in follower.motors:
            continue
        joint_index = follower.motors[name].id
        norm_mode = joint_name_to_norm_mode.get(name, MotorNormMode.RANGE_M100_100)

        calib = follower_calibration.get(name) if follower_calibration else None
        radians = normalized_to_radians(normalized_pos, norm_mode, calib)
        schema_joint = motor_id_to_schema_joint.get(joint_index, f"_{joint_index}")
        observations[schema_joint] = radians

    mqtt_client.publish_initial_observation(
        twin_uuid=robot.uuid,
        observations=observations,
        fps=fps,
    )
