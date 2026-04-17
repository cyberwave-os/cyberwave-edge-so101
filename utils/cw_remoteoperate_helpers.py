"""Helper functions for cw_remoteoperate and cw_teleoperate scripts."""

import argparse
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union

from cyberwave import Twin
from cyberwave.constants import SOURCE_TYPE_EDGE_FOLLOWER

from motors import MotorNormMode
from scripts.cw_write_position import validate_position
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import MAX_CHUNKS
from utils.cw_alerts import (
    create_calibration_needed_alert,
    create_calibration_upload_failed_alert,
    create_high_load_alert,
)
from utils.trackers import StatusTracker
from utils.utils import (
    calibration_range_to_radians,
    load_calibration,
    normalized_to_radians,
    radians_to_normalized,
    validate_calibration_ranges_sufficient,
)

logger = logging.getLogger(__name__)

# Keys used for delta metadata in actions (stripped before sending to motors)
DELTA_META_KEY = "_delta_meta"
DEFAULT_DELTA_TOLERANCE_RAD = 0.005  # ~0.3 degrees
CHUNKED_DELTA_FEED_UNTIL_REACH_MAX_RETRIES = 2

# High load safety threshold (percentage of max torque)
# A value of 900 means 90% of max torque - indicates potential collision or obstruction
HIGH_LOAD_THRESHOLD = 900

# High load debouncing: require N consecutive checks above threshold before triggering shutdown
HIGH_LOAD_CONSECUTIVE_THRESHOLD = 3


@dataclass
class DeltaResidualStore:
    """
    Thread-safe store for residual deltas per joint.

    When a delta is applied but the motor doesn't reach the target (drift, lag, limits),
    the residual (target - actual) is accumulated and added to the next delta.
    """

    _residuals: Dict[str, float] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def get(self, joint_name: str) -> float:
        """Get residual for a joint (radians)."""
        with self._lock:
            return self._residuals.get(joint_name, 0.0)

    def get_all(self, joint_names: List[str]) -> Dict[str, float]:
        """Get residuals for multiple joints."""
        with self._lock:
            return {j: self._residuals.get(j, 0.0) for j in joint_names}

    def add(
        self,
        joint_name: str,
        residual_rad: float,
        tolerance_rad: float = DEFAULT_DELTA_TOLERANCE_RAD,
    ) -> None:
        """Add residual for a joint; only accumulates if above tolerance."""
        if abs(residual_rad) <= tolerance_rad:
            return
        with self._lock:
            self._residuals[joint_name] = self._residuals.get(joint_name, 0.0) + residual_rad

    def clear(self, joint_names: Optional[List[str]] = None) -> None:
        """Clear residuals for given joints, or all if None."""
        with self._lock:
            if joint_names is None:
                self._residuals.clear()
            else:
                for j in joint_names:
                    self._residuals.pop(j, None)


@dataclass
class HighLoadDebouncer:
    """
    Track high load occurrences per motor to prevent false positives from transient spikes.

    Only triggers shutdown after sustained high load across multiple consecutive checks.
    """

    _counts: Dict[int, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock)
    _consecutive_threshold: int = HIGH_LOAD_CONSECUTIVE_THRESHOLD

    def update(self, high_loads: Optional[Dict[int, float]]) -> Optional[Dict[int, float]]:
        """
        Update high load counts and return motors that exceed consecutive threshold.

        Args:
            high_loads: Dict of motor_id -> load for motors currently above threshold, or None

        Returns:
            Dict of motor_id -> load for motors that have been high for consecutive_threshold checks,
            or None if no motors have sustained high load
        """
        with self._lock:
            if high_loads:
                # Increment count for motors with high load
                for motor_id, load in high_loads.items():
                    self._counts[motor_id] = self._counts.get(motor_id, 0) + 1

                # Reset count for motors no longer in high_loads
                motor_ids_to_clear = [mid for mid in self._counts if mid not in high_loads]
                for motor_id in motor_ids_to_clear:
                    self._counts.pop(motor_id, None)

                # Check which motors have sustained high load
                sustained = {
                    motor_id: load
                    for motor_id, load in high_loads.items()
                    if self._counts.get(motor_id, 0) >= self._consecutive_threshold
                }
                return sustained if sustained else None
            else:
                # No high loads - reset all counts
                self._counts.clear()
                return None

    def reset(self) -> None:
        """Clear all high load counts."""
        with self._lock:
            self._counts.clear()


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
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Disable status display; only log warnings and errors (for edge-core/main.py)",
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


@dataclass
class DeltaVerificationContext:
    """Context for verifying delta application and updating residuals."""

    residual_store: DeltaResidualStore
    joint_name_to_norm_mode: Dict[str, MotorNormMode]
    follower_calibration: Optional[Dict[str, Any]]
    tolerance_rad: float = DEFAULT_DELTA_TOLERANCE_RAD


def _verify_delta_and_update_residuals(
    follower: SO101Follower,
    delta_meta: Dict[str, Any],
    ctx: DeltaVerificationContext,
) -> None:
    """Read actual positions, compute residuals, and accumulate in store."""
    try:
        actual = follower.get_observation()
        if not actual:
            return
        for joint_name, expected_rad in delta_meta.get("expected_rad", {}).items():
            if expected_rad is None:
                continue
            actual_norm = actual.get(f"{joint_name}.pos")
            if actual_norm is None:
                continue
            norm_mode = ctx.joint_name_to_norm_mode.get(joint_name)
            calib = ctx.follower_calibration.get(joint_name) if ctx.follower_calibration else None
            if norm_mode is None:
                continue
            actual_rad = normalized_to_radians(actual_norm, norm_mode, calib)
            residual_rad = float(expected_rad) - actual_rad
            ctx.residual_store.add(joint_name, residual_rad, ctx.tolerance_rad)
    except Exception as e:
        logger.debug("Delta verification failed: %s", e)


def _feed_until_reach(
    follower: SO101Follower,
    expected_final_norm: Dict[str, float],
    expected_final_rad: Dict[str, float],
    ctx: Optional[DeltaVerificationContext],
    tolerance_rad: float = DEFAULT_DELTA_TOLERANCE_RAD,
    max_retries: int = CHUNKED_DELTA_FEED_UNTIL_REACH_MAX_RETRIES,
    retry_delay_sec: float = 0.05,
) -> None:
    """
    Keep sending expected_final until the robot reaches the target pose.

    Used after chunked_delta last chunk: if we don't match expected final,
    feed corrective actions until we reach it (or max retries).
    """
    if not expected_final_norm or not expected_final_rad or not ctx:
        return
    for _ in range(max_retries):
        actual = follower.get_observation()
        if not actual:
            time.sleep(retry_delay_sec)
            continue
        at_target = True
        for joint_name, expected_rad in expected_final_rad.items():
            actual_norm = actual.get(f"{joint_name}.pos")
            if actual_norm is None:
                at_target = False
                break
            norm_mode = ctx.joint_name_to_norm_mode.get(joint_name)
            calib = ctx.follower_calibration.get(joint_name) if ctx.follower_calibration else None
            if norm_mode is None:
                at_target = False
                break
            actual_rad = normalized_to_radians(actual_norm, norm_mode, calib)
            if abs(actual_rad - expected_rad) > tolerance_rad:
                at_target = False
                break
        if at_target:
            return
        corrective_action = {f"{j}.pos": v for j, v in expected_final_norm.items()}
        follower.send_action(corrective_action)
        time.sleep(retry_delay_sec)


def check_high_load(
    follower: SO101Follower,
    threshold: float = HIGH_LOAD_THRESHOLD,
) -> Optional[Dict[int, float]]:
    """
    Check if any motor has a load exceeding the threshold.

    Args:
        follower: SO101Follower instance with connected bus
        threshold: Load threshold (absolute value). Default is HIGH_LOAD_THRESHOLD.

    Returns:
        Dictionary of motor_id -> load for motors exceeding threshold, or None if all loads are safe.
    """
    try:
        motor_ids = [motor.id for motor in follower.motors.values()]
        loads = follower.bus.sync_read_loads(motor_ids, num_retry=1)
        if not loads:
            return None

        high_loads = {motor_id: load for motor_id, load in loads.items() if abs(load) > threshold}
        return high_loads if high_loads else None
    except Exception as e:
        logger.debug(f"Failed to read loads for safety check: {e}")
        return None


def motor_writer_worker(
    action_queue: queue.Queue,
    follower: SO101Follower,
    stop_event: threading.Event,
    status_tracker: Optional[StatusTracker] = None,
    delta_verification: Optional[DeltaVerificationContext] = None,
    enable_load_safety: bool = True,
    load_threshold: float = HIGH_LOAD_THRESHOLD,
    twin: Optional[Any] = None,
    load_check_interval: int = 10,
    use_fast_collision: bool = False,
) -> None:
    """
    Worker thread that reads actions from queue and writes to follower motors.

    Optimized for high-frequency remote operation (e.g. OpenVLA at 5-10Hz):
    - Uses send_action() directly like teleoperate for minimal serial bus traffic
    - Periodic load safety checks (every N actions) instead of every action
    - Pinocchio collision checks still run per-action (CPU only, no serial I/O)

    ST3215 serial protocol: sync_write to Goal_Position is ~2-3ms per call.
    Previous implementation did 4-6 serial ops per action (load read, position read,
    position write, another load read). Now we do 1 write + periodic load read,
    matching the efficient pattern in cw_teleoperate.py.

    Args:
        action_queue: Queue of motor actions to process
        follower: SO101Follower instance
        stop_event: Threading event to signal shutdown
        status_tracker: Optional status tracker for metrics
        delta_verification: Optional context for delta mode verification
        enable_load_safety: Enable periodic load safety checks (default True)
        load_threshold: Load threshold for safety intervention (default 900)
        twin: Optional twin for creating alerts
        load_check_interval: Check motor loads every N actions (default 10)
        use_fast_collision: If True, use boolean computeCollisions instead of
            computeDistances with margin (faster, no proximity margin).
    """
    load_debouncer = HighLoadDebouncer()
    action_count = 0

    while not stop_event.is_set():
        try:
            try:
                action = action_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            delta_meta = action.pop(DELTA_META_KEY, None)

            try:
                # Validate with Pinocchio (CPU only, no serial I/O) if initialized
                if follower._pin_model is not None:
                    is_safe, msg = follower.validate_action_kinematics(
                        action, use_distance_check=not use_fast_collision
                    )
                    if not is_safe:
                        logger.warning("Rejected action (collision): %s", msg)
                        if status_tracker:
                            status_tracker.increment_filtered()
                        action_queue.task_done()
                        continue

                # Send action directly like teleoperate — single sync_write, minimal bus traffic
                follower.send_action(action)
                action_count += 1

                if status_tracker:
                    status_tracker.increment_processed()

                # Periodic load safety check (every N actions) to reduce serial bus contention.
                # ST3215 motors report Present_Load; we sample periodically, not every frame.
                if enable_load_safety and action_count % load_check_interval == 0:
                    high_loads = check_high_load(follower, threshold=load_threshold)
                    sustained_high_loads = load_debouncer.update(high_loads)

                    if sustained_high_loads:
                        motor_names_map = {
                            motor.id: name for name, motor in follower.motors.items()
                        }
                        high_loads_named = {
                            motor_names_map.get(mid, f"motor_{mid}"): load
                            for mid, load in sustained_high_loads.items()
                        }
                        high_load_info = ", ".join(
                            f"{name}={load:.1f}" for name, load in high_loads_named.items()
                        )
                        overloaded_motor_names = list(high_loads_named.keys())

                        logger.warning(
                            f"Sustained high motor load ({HIGH_LOAD_CONSECUTIVE_THRESHOLD} checks): "
                            f"{high_load_info}. Releasing torque on affected motors."
                        )

                        if twin is not None:
                            try:
                                create_high_load_alert(twin, high_loads_named, load_threshold)
                            except Exception as e:
                                logger.debug(f"Failed to create high load alert: {e}")

                        try:
                            logger.info(f"Disabling torque on motors: {overloaded_motor_names}")
                            follower.disable_torque(motor_names=overloaded_motor_names)
                            time.sleep(1.0)
                            logger.info(f"Re-enabling torque on motors: {overloaded_motor_names}")
                            follower.enable_torque(motor_names=overloaded_motor_names)
                            load_debouncer.reset()
                            logger.info("Torque recovery complete")
                        except Exception as e:
                            logger.error(f"Torque recovery failed: {e}")
                            try:
                                follower.disable_torque()
                            except Exception as e2:
                                logger.error(f"Failed to disable all torque: {e2}")
                            stop_event.set()
                            action_queue.task_done()
                            return

                    elif high_loads:
                        motor_names_map = {
                            motor.id: name for name, motor in follower.motors.items()
                        }
                        high_loads_named = {
                            motor_names_map.get(mid, f"motor_{mid}"): load
                            for mid, load in high_loads.items()
                        }
                        high_load_info = ", ".join(
                            f"{name}={load:.1f}" for name, load in high_loads_named.items()
                        )
                        logger.debug(
                            f"Transient high load: {high_load_info} "
                            f"(count: {max(load_debouncer._counts.get(mid, 0) for mid in high_loads.keys())}/{HIGH_LOAD_CONSECUTIVE_THRESHOLD})"
                        )
                else:
                    # No load check this cycle — update debouncer with None to decay counts
                    load_debouncer.update(None)

                # Feed until reach: for chunked_delta last chunk
                expected_final_norm = delta_meta.get("expected_final_norm") if delta_meta else None
                expected_final_rad = delta_meta.get("expected_final_rad") if delta_meta else None
                if expected_final_norm and expected_final_rad and delta_verification:
                    _feed_until_reach(
                        follower=follower,
                        expected_final_norm=expected_final_norm,
                        expected_final_rad=expected_final_rad,
                        ctx=delta_verification,
                    )

                # Inter-chunk delay for chunked_delta
                delay = delta_meta.get("inter_chunk_delay_sec") if delta_meta else 0
                if delay and delay > 0:
                    time.sleep(delay)

            except Exception:
                if status_tracker:
                    status_tracker.increment_errors_motor()
            finally:
                action_queue.task_done()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors_motor()


SKIP_KEYS = frozenset(
    {
        "source_type",
        "timestamp",
        "session_id",
        "type",
        "positions",
        "velocities",
        "efforts",
        "source_subtype",
        "workload_uuid",
        "update_mode",
        "chunks",
        "deltas",
    }
)


def _resolve_schema_joint_to_so101(
    schema_joint: str,
    follower: SO101Follower,
    joint_index_to_name: Dict[int, str],
    schema_joint_to_motor_id: Optional[Dict[str, int]] = None,
) -> Optional[str]:
    """Resolve schema joint name (e.g. '_1') to SO101 joint name."""
    if schema_joint in follower.motors:
        return schema_joint
    if schema_joint_to_motor_id:
        motor_id = schema_joint_to_motor_id.get(schema_joint)
        if motor_id is not None:
            return joint_index_to_name.get(motor_id)
    for name in joint_index_to_name.values():
        if name == schema_joint:
            return name
    return None


def process_multi_joint_update(
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
    """Process multi-joint absolute positions (flat or nested 'positions' format)."""
    if "positions" in data and isinstance(data["positions"], dict):
        joint_items = data["positions"].items()
    else:
        joint_items = data.items()

    action = current_state.copy()
    joint_states_for_status: Dict[str, float] = {}
    any_valid = False

    for joint_name, position_val in joint_items:
        if joint_name in SKIP_KEYS:
            continue
        resolved = _resolve_schema_joint_to_so101(
            joint_name, follower, joint_index_to_name, schema_joint_to_motor_id
        )
        if resolved is None:
            continue
        joint_name = resolved

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
                status_tracker.increment_errors_mqtt()


def process_delta_update(
    data: Dict,
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]],
    status_tracker: Optional[StatusTracker],
    schema_joint_to_motor_id: Optional[Dict[str, int]] = None,
    residual_store: Optional[DeltaResidualStore] = None,
) -> None:
    """
    Process delta update: target = current_observation + delta + residual.

    Uses follower.get_observation() as base (robot ground truth). Accumulates
    residuals when verification detects motors didn't reach target.
    """
    deltas = data.get("positions") or data.get("deltas")
    if not deltas or not isinstance(deltas, dict):
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return

    follower_obs = follower.get_observation()
    if not follower_obs:
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return

    residuals = residual_store.get_all(list(follower.motors)) if residual_store else {}

    action = current_state.copy()
    joint_states_for_status: Dict[str, float] = {}
    expected_rad: Dict[str, float] = {}
    any_valid = False

    for schema_joint, delta_val in deltas.items():
        if schema_joint in SKIP_KEYS:
            continue
        joint_name = _resolve_schema_joint_to_so101(
            schema_joint, follower, joint_index_to_name, schema_joint_to_motor_id
        )
        if joint_name is None:
            continue

        try:
            delta_rad = float(delta_val)
        except (ValueError, TypeError):
            continue

        norm_mode = joint_name_to_norm_mode.get(joint_name)
        if norm_mode is None:
            continue
        calib = follower_calibration.get(joint_name) if follower_calibration else None

        current_norm = follower_obs.get(f"{joint_name}.pos", 0.0)
        current_rad = normalized_to_radians(current_norm, norm_mode, calib)
        residual_rad = residuals.get(joint_name, 0.0)
        effective_delta = delta_rad + residual_rad
        target_rad = current_rad + effective_delta

        target_norm = radians_to_normalized(target_rad, norm_mode, calib)
        motor_id = follower.motors[joint_name].id
        is_valid, _ = validate_position(motor_id, target_norm)
        if not is_valid:
            if status_tracker:
                status_tracker.increment_filtered()
            continue

        action[f"{joint_name}.pos"] = target_norm
        current_state[f"{joint_name}.pos"] = target_norm
        joint_states_for_status[str(motor_id)] = target_rad
        expected_rad[joint_name] = target_rad
        any_valid = True

    if any_valid:
        if residual_store:
            residual_store.clear(list(expected_rad.keys()))
        if status_tracker:
            status_tracker.update_joint_states(joint_states_for_status)
        action[DELTA_META_KEY] = {"expected_rad": expected_rad}
        try:
            action_queue.put_nowait(action)
        except queue.Full:
            if status_tracker:
                status_tracker.increment_errors_mqtt()


def process_chunked_delta_update(
    data: Dict,
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]],
    status_tracker: Optional[StatusTracker],
    schema_joint_to_motor_id: Optional[Dict[str, int]] = None,
    residual_store: Optional[DeltaResidualStore] = None,
) -> None:
    """
    Process chunked delta update: each chunk is delta relative to previous chunk's target.

    Chunks form a chain: target_i = target_{i-1} + delta_i. Uses get_observation() for
    the first chunk; subsequent chunks use the previous target as base.

    After all chunks, computes expected final pose = current + sum(deltas). If actual
    pose doesn't match, sends corrective actions until it does.
    """
    chunks = data.get("chunks", [])
    if not chunks:
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return

    # Security: reject oversized payloads (DoS protection)
    if len(chunks) > MAX_CHUNKS:
        logger.warning(
            "Rejected chunked_delta with %d chunks (max=%d) - potential DoS attack",
            len(chunks),
            MAX_CHUNKS,
        )
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return

    follower_obs = follower.get_observation()
    if not follower_obs:
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return

    base_rad: Dict[str, float] = {}

    for joint_name in follower.motors:
        norm_mode = joint_name_to_norm_mode.get(joint_name)
        calib = follower_calibration.get(joint_name) if follower_calibration else None
        if norm_mode is None:
            continue
        current_norm = follower_obs.get(f"{joint_name}.pos", 0.0)
        base_rad[joint_name] = normalized_to_radians(current_norm, norm_mode, calib)

    # Pre-compute expected final pose: current + sum of deltas per joint across all chunks
    sum_deltas: Dict[str, float] = dict.fromkeys(base_rad, 0.0)
    for chunk in chunks:
        deltas = chunk.get("positions") or chunk.get("deltas")
        if not deltas or not isinstance(deltas, dict):
            continue
        for schema_joint, delta_val in deltas.items():
            if schema_joint in SKIP_KEYS:
                continue
            joint_name = _resolve_schema_joint_to_so101(
                schema_joint, follower, joint_index_to_name, schema_joint_to_motor_id
            )
            if joint_name is None or joint_name not in base_rad:
                continue
            try:
                sum_deltas[joint_name] += float(delta_val)
            except (ValueError, TypeError):
                pass

    # Expected final only for joints with non-zero total delta
    expected_final_rad: Dict[str, float] = {}
    for joint_name in base_rad:
        total_delta = sum_deltas.get(joint_name, 0.0)
        if abs(total_delta) > 1e-9:
            expected_final_rad[joint_name] = base_rad[joint_name] + total_delta

    for chunk_idx, chunk in enumerate(chunks):
        deltas = chunk.get("positions") or chunk.get("deltas")
        if not deltas or not isinstance(deltas, dict):
            continue

        # Only include joints with non-zero delta in the action.
        # Joints with delta=0 should hold naturally, not be commanded to their
        # (noisy) current position which causes jitter.
        action: Dict[str, float] = {}
        joint_states_for_status: Dict[str, float] = {}
        any_valid = False

        for schema_joint, delta_val in deltas.items():
            if schema_joint in SKIP_KEYS:
                continue
            joint_name = _resolve_schema_joint_to_so101(
                schema_joint, follower, joint_index_to_name, schema_joint_to_motor_id
            )
            if joint_name is None or joint_name not in base_rad:
                continue

            try:
                delta_rad = float(delta_val)
            except (ValueError, TypeError):
                continue

            # Skip joints with zero delta - let them hold position naturally
            if abs(delta_rad) < 1e-9:
                continue

            norm_mode = joint_name_to_norm_mode.get(joint_name)
            if norm_mode is None:
                continue
            calib = follower_calibration.get(joint_name) if follower_calibration else None

            current_rad = base_rad[joint_name]
            target_rad = current_rad + delta_rad

            target_norm = radians_to_normalized(target_rad, norm_mode, calib)
            motor_id = follower.motors[joint_name].id
            is_valid, _ = validate_position(motor_id, target_norm)
            if not is_valid:
                if status_tracker:
                    status_tracker.increment_filtered()
                continue

            action[f"{joint_name}.pos"] = target_norm
            current_state[f"{joint_name}.pos"] = target_norm
            base_rad[joint_name] = target_rad
            joint_states_for_status[str(motor_id)] = target_rad
            any_valid = True

        if any_valid:
            if status_tracker:
                status_tracker.update_joint_states(joint_states_for_status)
            delta_meta: Dict[str, Any] = {
                "inter_chunk_delay_sec": 0.1,
            }
            is_last_chunk = chunk_idx == len(chunks) - 1
            if is_last_chunk and expected_final_rad:
                delta_meta["expected_final_rad"] = expected_final_rad
                expected_final_norm: Dict[str, float] = {}
                for jname, rad_val in expected_final_rad.items():
                    nmode = joint_name_to_norm_mode.get(jname)
                    c = follower_calibration.get(jname) if follower_calibration else None
                    if nmode is not None:
                        expected_final_norm[jname] = radians_to_normalized(rad_val, nmode, c)
                delta_meta["expected_final_norm"] = expected_final_norm
            action[DELTA_META_KEY] = delta_meta
            try:
                action_queue.put_nowait(action)
            except queue.Full:
                if status_tracker:
                    status_tracker.increment_errors_mqtt()


def create_joint_state_callback(
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]] = None,
    status_tracker: Optional[StatusTracker] = None,
    schema_joint_to_motor_id: Optional[Dict[str, int]] = None,
    residual_store: Optional[DeltaResidualStore] = None,
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

            if data.get("source_type") != "tele":
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

            update_mode = data.get("update_mode", "absolute")

            if update_mode == "delta":
                process_delta_update(
                    data,
                    current_state,
                    action_queue,
                    joint_index_to_name,
                    joint_name_to_norm_mode,
                    follower,
                    follower_calibration,
                    status_tracker,
                    schema_joint_to_motor_id=schema_joint_to_motor_id,
                    residual_store=residual_store,
                )
                return

            if update_mode == "chunked_delta":
                process_chunked_delta_update(
                    data,
                    current_state,
                    action_queue,
                    joint_index_to_name,
                    joint_name_to_norm_mode,
                    follower,
                    follower_calibration,
                    status_tracker,
                    schema_joint_to_motor_id=schema_joint_to_motor_id,
                    residual_store=residual_store,
                )
                return

            process_multi_joint_update(
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

        except Exception:
            if status_tracker:
                status_tracker.increment_errors_mqtt()

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
            status_tracker.increment_errors_mqtt()


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
            status_tracker.increment_errors_mqtt()
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
                status_tracker.increment_errors_mqtt()
            return

    joint_name = joint_index_to_name.get(joint_index)
    if joint_name is None:
        if status_tracker:
            status_tracker.increment_errors_mqtt()
        return

    try:
        position_radians = float(position_radians)
    except (ValueError, TypeError):
        if status_tracker:
            status_tracker.increment_errors_mqtt()
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
        logger.warning("Failed to upload calibration: %s. Check logs for details.", e)
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
    """Check if calibration exists, is valid, and optionally raise if missing/invalid.

    Args:
        device: Leader or Follower device
        device_type: "leader" or "follower" for error messages
        twin: Optional twin for creating alerts
        require_calibration: If True, raise RuntimeError when calibration is missing or invalid

    Returns:
        True if calibration exists and is valid, False otherwise

    Raises:
        RuntimeError: If require_calibration is True and calibration is missing or invalid
    """
    # Check if calibration exists
    if device.calibration is None:
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

    # Validate calibration ranges are sufficient
    calib_data = {}
    for name, calib in device.calibration.items():
        calib_data[name] = {
            "range_min": calib.range_min,
            "range_max": calib.range_max,
        }

    is_valid, warnings, _severity = validate_calibration_ranges_sufficient(calib_data)
    if not is_valid:
        msg = (
            f"{device_type.capitalize()} calibration has insufficient ranges. "
            f"Please recalibrate using: python scripts/cw_calibrate.py --type {device_type}\n"
            f"Issues:\n" + "\n".join(f"  - {w}" for w in warnings)
        )

        if twin is not None:
            create_calibration_needed_alert(
                twin,
                device_type,
                description=(
                    f"The {device_type} calibration is invalid. "
                    f"Please recalibrate and move all joints through their full ranges."
                ),
            )

        if require_calibration:
            raise RuntimeError(msg)
        else:
            logger.warning(msg)
            return False

    return True
