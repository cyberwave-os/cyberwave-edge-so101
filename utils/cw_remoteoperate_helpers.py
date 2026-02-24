"""Helper functions for cw_remoteoperate and cw_teleoperate scripts."""

import argparse
import logging
import os
import queue
import threading
import time
from typing import Any, Callable, Dict, Optional, Union

from cyberwave import Twin
from cyberwave.constants import SOURCE_TYPE_EDGE_FOLLOWER

from motors import MotorNormMode
from scripts.cw_write_position import validate_position
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.cw_alerts import (
    create_calibration_needed_alert,
    create_calibration_upload_failed_alert,
)
from utils.trackers import StatusTracker
from utils.utils import (
    calibration_range_to_radians,
    ensure_safe_goal_position,
    load_calibration,
    normalized_to_radians,
    radians_to_normalized,
)

logger = logging.getLogger(__name__)


def get_remoteoperate_parser() -> argparse.ArgumentParser:
    """Create argument parser for cw_remoteoperate script."""
    parser = argparse.ArgumentParser(description="Remote operate SO101 follower via Cyberwave MQTT")
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
    return parser


def joint_position_heartbeat_thread(
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

            timestamp = time.time()
            for joint_key, normalized_pos in follower_obs.items():
                name = joint_key.removesuffix(".pos")
                if name not in follower.motors:
                    continue
                joint_index = follower.motors[name].id
                norm_mode = joint_name_to_norm_mode.get(name)
                if norm_mode is None:
                    continue

                calib = follower_calibration.get(name) if follower_calibration else None
                radians = normalized_to_radians(normalized_pos, norm_mode, calib)
                schema_joint = motor_id_to_schema_joint.get(joint_index, f"_{joint_index}")
                mqtt_client.update_joint_state(
                    twin_uuid=twin_uuid,
                    joint_name=schema_joint,
                    position=radians,
                    timestamp=timestamp,
                    source_type=SOURCE_TYPE_EDGE_FOLLOWER,
                )
        except Exception:
            pass
        time.sleep(interval)


def motor_writer_worker(
    action_queue: queue.Queue,
    follower: SO101Follower,
    stop_event: threading.Event,
    status_tracker: Optional[StatusTracker] = None,
) -> None:
    """
    Worker thread that reads actions from queue and writes to follower motors.

    Splits large position changes into multiple smaller steps based on max_relative_target.
    """
    while not stop_event.is_set():
        try:
            try:
                action = action_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                if follower.config.max_relative_target is not None:
                    present_pos = follower.get_observation()
                    if not present_pos:
                        continue
                    current_pos = {
                        key.removesuffix(".pos"): val for key, val in present_pos.items()
                    }

                    goal_pos = {
                        key.removesuffix(".pos"): val
                        for key, val in action.items()
                        if key.endswith(".pos")
                    }
                    for key, val in action.items():
                        if not key.endswith(".pos") and key in follower.motors:
                            goal_pos[key] = val
                            if key not in current_pos:
                                current_pos[key] = 0.0

                    max_steps = 1
                    for joint_name, goal_val in goal_pos.items():
                        current_val = current_pos.get(joint_name, 0.0)
                        diff = abs(goal_val - current_val)
                        if diff > 0.01:
                            steps_needed = int(diff / follower.config.max_relative_target) + 1
                            max_steps = max(max_steps, steps_needed)

                    for _ in range(max_steps):
                        remaining_movements = {}
                        for joint_name, goal_val in goal_pos.items():
                            current_val = current_pos.get(joint_name, 0.0)
                            diff = abs(goal_val - current_val)
                            if diff > 0.01:
                                remaining_movements[joint_name] = goal_val

                        if not remaining_movements:
                            break

                        goal_present_pos = {
                            key: (goal_pos[key], current_pos.get(key, 0.0))
                            for key in remaining_movements.keys()
                        }

                        safe_goal_pos = ensure_safe_goal_position(
                            goal_present_pos, follower.config.max_relative_target
                        )

                        for name, pos in safe_goal_pos.items():
                            current_pos[name] = pos

                        safe_action = {f"{name}.pos": pos for name, pos in safe_goal_pos.items()}

                        original_max_relative = follower.config.max_relative_target
                        follower.config.max_relative_target = None

                        try:
                            goal_pos_for_bus = {
                                key.removesuffix(".pos"): val for key, val in safe_action.items()
                            }
                            follower.bus.sync_write(
                                "Goal_Position", goal_pos_for_bus, normalize=True
                            )
                            if status_tracker:
                                status_tracker.increment_processed()
                        finally:
                            follower.config.max_relative_target = original_max_relative
                else:
                    follower.send_action(action)
                    if status_tracker:
                        status_tracker.increment_processed()

            except Exception:
                if status_tracker:
                    status_tracker.increment_errors()
            finally:
                action_queue.task_done()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors()


def create_joint_state_callback(
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]] = None,
    status_tracker: Optional[StatusTracker] = None,
    schema_joint_to_motor_id: Optional[Dict[str, int]] = None,
) -> Callable[[str, Dict], None]:
    """Create a callback function for MQTT joint state updates."""

    def callback(topic: str, data: Dict) -> None:
        if status_tracker:
            status_tracker.increment_received()

        try:
            if not topic.endswith("/update"):
                if status_tracker:
                    status_tracker.increment_filtered()
                return

            source_type = data.get("source_type")
            if source_type != "tele":
                return

            if "joint_name" in data and "joint_state" in data:
                process_single_joint_update(
                    data,
                    current_state,
                    action_queue,
                    joint_index_to_name,
                    joint_name_to_norm_mode,
                    follower,
                    follower_calibration,
                    status_tracker,
                    schema_joint_to_motor_id=schema_joint_to_motor_id,
                )
                return

            action = current_state.copy()
            joint_states_for_status = {}
            any_valid = False
            for joint_name, position_val in data.items():
                if joint_name in ("source_type", "timestamp", "session_id", "type"):
                    continue
                # Resolve schema joint names (e.g. "_1") to SO101 joint names
                resolved_name = joint_name
                if joint_name not in follower.motors and schema_joint_to_motor_id:
                    motor_id = schema_joint_to_motor_id.get(joint_name)
                    if motor_id is not None:
                        resolved_name = joint_index_to_name.get(motor_id)
                if resolved_name is None or resolved_name not in follower.motors:
                    continue
                joint_name = resolved_name
                try:
                    position_radians = float(position_val)
                except (ValueError, TypeError):
                    continue

                norm_mode = joint_name_to_norm_mode.get(joint_name)
                if norm_mode is None:
                    continue
                calib = follower_calibration.get(joint_name) if follower_calibration else None
                normalized_position = radians_to_normalized(position_radians, norm_mode, calib)
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
    normalized_position = radians_to_normalized(position_radians, norm_mode, calib)
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


def process_single_joint_update(
    data: Dict,
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]],
    status_tracker: Optional[StatusTracker],
    schema_joint_to_motor_id: Optional[Dict[str, int]] = None,
) -> None:
    """Process single-joint format: joint_name + joint_state."""
    joint_name_str = data.get("joint_name")
    joint_state = data.get("joint_state", {})
    position_radians = joint_state.get("position")

    if position_radians is None:
        if status_tracker:
            status_tracker.increment_errors()
        return

    joint_index = None
    try:
        joint_index = int(joint_name_str)
    except (ValueError, TypeError):
        if schema_joint_to_motor_id and joint_name_str in schema_joint_to_motor_id:
            joint_index = schema_joint_to_motor_id[joint_name_str]
        else:
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


def upload_calibration_to_twin(
    device: Union[SO101Leader, SO101Follower],
    twin: Twin,
    robot_type: str = "follower",
) -> None:
    """Upload calibration data from leader or follower device to Cyberwave twin.

    Maps device motor IDs to the twin's schema joint names (from get_controllable_joint_names).
    The backend expects joint names matching the asset kinematics, not motor IDs.

    API endpoint: POST /api/v1/twins/{uuid}/calibration
    """
    try:
        calibration_path = device.config.calibration_dir / f"{device.config.id}.json"

        if not calibration_path.exists():
            msg = (
                f"No calibration file at {calibration_path}, skipping upload. "
                "Run so101-calibrate first."
            )
            logger.warning(msg)
            print(f"[calibration] {msg}")
            return

        calib_data = load_calibration(calibration_path)

        # Get twin schema joint names (e.g. "_1", "_2", etc. for so101)
        try:
            schema_joint_names = twin.get_controllable_joint_names()
        except Exception as e:
            logger.warning(f"Could not get twin joint names: {e}, using motor IDs")
            schema_joint_names = None

        joint_calibration = {}
        for joint_name, calib in calib_data.items():
            if joint_name not in device.motors:
                logger.warning(f"Joint '{joint_name}' not found in device motors, skipping")
                continue

            motor_id = device.motors[joint_name].id

            # Map motor ID to schema joint name (motor ID 1 -> index 0 in schema)
            if schema_joint_names is not None:
                idx = motor_id - 1
                if idx < len(schema_joint_names):
                    schema_joint = schema_joint_names[idx]
                else:
                    # Fallback to motor ID if index out of range
                    schema_joint = f"_{motor_id}"
            else:
                # Fallback if we couldn't get schema joint names
                schema_joint = f"_{motor_id}"

            norm_mode = device.motors[joint_name].norm_mode
            lower_rad, upper_rad = calibration_range_to_radians(
                calib["range_min"], calib["range_max"], norm_mode
            )

            joint_calibration[schema_joint] = {
                "range_min": calib["range_min"],
                "range_max": calib["range_max"],
                "homing_offset": calib["homing_offset"],
                "drive_mode": str(calib["drive_mode"]),
                "id": str(calib["id"]),
                "lower": lower_rad,
                "upper": upper_rad,
            }

        base_url = getattr(getattr(twin.client, "config", None), "base_url", "?")
        endpoint = f"{base_url}/api/v1/twins/{twin.uuid}/calibration"
        logger.info(
            "Uploading %s calibration to twin %s via POST %s",
            robot_type,
            twin.uuid,
            endpoint,
        )
        print(f"[calibration] POST {endpoint} ({len(joint_calibration)} joints)")
        twin.update_calibration(joint_calibration, robot_type=robot_type)
        logger.info("Calibration uploaded successfully to twin %s", twin.uuid)
        print(f"[calibration] Uploaded successfully to twin {twin.uuid}")
    except ImportError as e:
        msg = (
            "Cyberwave SDK not installed. Skipping calibration upload. "
            "Install with: pip install cyberwave"
        )
        logger.warning(msg)
        print(f"[calibration] {msg}")
        create_calibration_upload_failed_alert(twin, robot_type, e)
    except Exception as e:
        logger.warning(
            "Failed to upload calibration: %s. Check logs for details.", e
        )
        logger.exception("Calibration upload failed, continuing without upload")
        print(
            f"[calibration] failed: {e} "
            "(check API key, base URL, and joint count matches twin schema)"
        )
        create_calibration_upload_failed_alert(twin, robot_type, e)


def check_calibration_required(
    device: Union[SO101Leader, SO101Follower],
    device_type: str,
    twin: Optional[Twin] = None,
    require_calibration: bool = True,
) -> bool:
    """Check if calibration exists and optionally raise if missing.

    Args:
        device: Leader or Follower device
        device_type: "leader" or "follower" for error messages
        twin: Optional twin for creating alerts
        require_calibration: If True, raise RuntimeError when calibration is missing

    Returns:
        True if calibration exists, False otherwise

    Raises:
        RuntimeError: If require_calibration is True and calibration is missing
    """
    if device.calibration is not None:
        return True

    msg = (
        f"{device_type.capitalize()} is not calibrated. "
        f"Please calibrate using: python scripts/cw_calibrate.py --type {device_type}"
    )

    if twin is not None:
        create_calibration_needed_alert(
            twin,
            device_type,
            description=f"Please calibrate the {device_type} using the calibration script.",
        )

    if require_calibration:
        raise RuntimeError(msg)
    else:
        logger.warning(msg)
        return False
