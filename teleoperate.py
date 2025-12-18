"""Teleoperation loop for SO101 leader and follower."""

import argparse
import asyncio
import logging
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
from utils import setup_logging

logger = logging.getLogger(__name__)


def _camera_worker_thread(
    client: Cyberwave,
    camera_id: int,
    fps: int,
    twin_uuid: str,
    stop_event: threading.Event,
    time_reference: TimeReference,
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
    Returns:
        None
    """
    logger.debug("Camera worker thread started")

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
        )

        # Monitor the threading stop_event and set async_stop_event
        async def monitor_stop():
            while not stop_event.is_set():
                await asyncio.sleep(0.1)
            async_stop_event.set()

        # Start the monitor task
        monitor_task = asyncio.create_task(monitor_stop())

        try:
            # Run with auto-reconnect - this handles all command subscriptions internally
            await streamer.run_with_auto_reconnect(
                stop_event=async_stop_event,
                command_callback=lambda status, msg: logger.info(
                    f"Camera command: {status} - {msg}"
                ),
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
    except Exception as e:
        logger.error(f"Error in camera worker thread: {e}", exc_info=True)
    finally:
        if loop is not None:
            try:
                loop.close()
            except Exception as e:
                logger.error(f"Error closing event loop: {e}", exc_info=True)


def cyberwave_update_worker(
    action_queue: queue.Queue,
    joint_name_to_index: Dict[str, int],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    use_radians: bool,
    stop_event: threading.Event,
    twin: Optional[Twin] = None,
    time_reference: TimeReference = None,
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
    Returns:
        None
    """
    logger.debug("Cyberwave update worker thread started")
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
                        logger.warning(
                            f"Joint '{joint_name}' not found in joint_name_to_index mapping"
                        )
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

                    # Skip if position is 0.0 (or very close to 0)
                    if abs(position) < 1e-6:
                        action_queue.task_done()
                        continue

                    # Store in batch (overwrite if same joint appears multiple times)
                    batch_updates[joint_index] = (position, velocity, effort, timestamp)
                    action_queue.task_done()

                except Exception as e:
                    error_count += 1
                    joint_index_str = str(joint_name_to_index.get(joint_name, "unknown"))
                    logger.warning(
                        f"Failed to process joint {joint_name} (index {joint_index_str}): {e}",
                        exc_info=True,
                    )
                    action_queue.task_done()

            # Send batched updates
            if batch_updates:
                try:
                    # Send all joints in the batch
                    for joint_index, (position, _, _, timestamp) in batch_updates.items():
                        twin.joints.set(
                            joint_name=str(joint_index),
                            position=position,
                            degrees=False,
                            timestamp=timestamp,
                        )
                    processed_count += len(batch_updates)
                except Exception as e:
                    error_count += len(batch_updates)
                    logger.warning(
                        f"Failed to send batch update for {len(batch_updates)} joints: {e}",
                        exc_info=True,
                    )

        except Exception as e:
            logger.error(f"Error in cyberwave update worker: {e}", exc_info=True)

    logger.info(
        f"Cyberwave update worker stopped. Processed: {processed_count}, Errors: {error_count}"
    )


def _process_cyberwave_updates(
    action: Dict[str, float],
    last_observation: Dict[str, float],
    action_queue: queue.Queue,
    position_threshold: float,
    velocity_threshold: float,
    effort_threshold: float,
    timestamp: float,
) -> tuple[int, int]:
    """
    Process leader action and queue Cyberwave updates for changed joints.

    Leader action is now normalized positions

    Args:
        action: Leader action dictionary with normalized positions (keys have .pos suffix)
        last_observation: Dictionary tracking last sent observation state (normalized positions)
        action_queue: Queue for Cyberwave updates
        position_threshold: Minimum change in normalized position to trigger update
        velocity_threshold: Unused (kept for compatibility)
        effort_threshold: Unused (kept for compatibility)
        timestamp: Timestamp to associate with this update (generated in teleop loop)
    Returns:
        Tuple of (update_count, skip_count)
    """
    update_count = 0
    skip_count = 0

    for joint_key, normalized_pos in action.items():
        # Extract joint name from key (remove .pos suffix if present)
        joint_name = joint_key.removesuffix(".pos") if joint_key.endswith(".pos") else joint_key

        if joint_name not in last_observation:
            # New joint, initialize and send
            last_observation[joint_name] = float("inf")

        last_obs = last_observation[joint_name]

        # Check if position has changed beyond threshold (using normalized values)
        pos_changed = abs(normalized_pos - last_obs) >= position_threshold

        # Force first update (when last_obs is inf)
        is_first_update = last_obs == float("inf")

        if is_first_update or pos_changed:
            # Update last observation (store normalized position)
            last_observation[joint_name] = normalized_pos

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
                logger.warning(
                    f"Action queue full, dropping update for {joint_name}. "
                    "Consider increasing queue size or reducing fps."
                )
        else:
            skip_count += 1

    return update_count, skip_count


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
    message = "\n".join(lines)
    logger.info(message)


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
                            logger.info("\n'q' key pressed - stopping teleoperation loop...")
                            stop_event.set()
                            break
            finally:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except (ImportError, OSError):
            # If termios is not available (e.g., Windows), fall back to simple input
            logger.debug("Termios not available, keyboard input disabled")
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
) -> tuple[int, int]:
    """
    Main teleoperation loop: read from leader, send to follower, and queue Cyberwave updates.

    Args:
        leader: SO101Leader instance
        follower: Optional SO101Follower instance
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
    Returns:
        Tuple of (update_count, skip_count)
    """
    total_update_count = 0
    total_skip_count = 0
    iteration_count = 0

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            # Generate timestamp for this iteration (before reading action)
            timestamp, timestamp_monotonic = time_reference.update()

            # Read action from leader (normalized positions with .pos suffix)
            action = leader.get_action()
            logger.debug(f"Leader action: {action}")

            # Process Cyberwave updates (queue changed joints)
            # Worker thread handles all conversion
            update_count, skip_count = _process_cyberwave_updates(
                action=action,
                last_observation=last_observation,
                action_queue=action_queue,
                position_threshold=position_threshold,
                velocity_threshold=velocity_threshold,
                effort_threshold=effort_threshold,
                timestamp=timestamp,  # Pass timestamp to processing function
            )
            total_update_count += update_count
            total_skip_count += skip_count

            # Send action to follower if provided
            leader_observation = None
            follower_observation = None
            if follower is not None:
                try:
                    # Get normalized positions from leader
                    # Leader's get_action() returns normalized positions with .pos suffix
                    leader_action = leader.get_action()
                    # Send directly to follower (follower handles safety limits and normalization)
                    follower.send_action(leader_action)

                    # Get observations for logging
                    if log_states:
                        if leader_observation is None:
                            leader_observation = leader.get_observation()
                        follower_observation = follower.get_observation()

                except Exception as e:
                    logger.error(f"Error sending action to follower: {e}", exc_info=True)

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
            logger.debug(
                f"Elapsed time: {elapsed:.4f}s, target: {frame_time:.4f}s, sleep time: {sleep_time:.4f}s"
            )
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        logger.info("Teleoperation loop interrupted by user")
    except Exception as e:
        logger.error(f"Error in teleoperation loop: {e}", exc_info=True)
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
    Run teleoperation loop: read from leader and update Cyberwave twin joint states.

    Uses a separate thread with a FIFO queue to send updates to Cyberwave,
    keeping the main loop responsive. Only sends updates when values change.

    Args:
        leader: SO101Leader instance (optional if camera_only=True)
        cyberwave_client: Cyberwave client instance (required)
        follower: Optional SO101Follower instance (required for camera streaming)
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
    setup_logging()
    time_reference = TimeReference()
    # Ensure MQTT client is connected
    if cyberwave_client is None:
        raise RuntimeError("Cyberwave client is required")

    mqtt_client = cyberwave_client.mqtt
    if mqtt_client is not None and not mqtt_client.connected:
        logger.info("Connecting to Cyberwave MQTT broker...")
        mqtt_client.connect()

        # Wait for connection with timeout
        max_wait_time = 10.0  # seconds
        wait_start = time.time()
        while not mqtt_client.connected:
            if time.time() - wait_start > max_wait_time:
                raise RuntimeError(
                    f"Failed to connect to Cyberwave MQTT broker within {max_wait_time} seconds"
                )
            time.sleep(0.1)
        logger.info("Connected to Cyberwave MQTT broker")

    if leader is not None and not leader.connected:
        raise RuntimeError("Leader is not connected")

    if follower is not None and not follower.connected:
        raise RuntimeError("Follower is not connected")

    # Verify follower has torque enabled (required for movement)
    if follower is not None and not follower.torque_enabled:
        logger.warning("Follower torque is not enabled! Motors will not move.")
        logger.info("Enabling follower torque...")
        follower.enable_torque()

    # Log follower configuration for debugging
    if follower is not None:
        logger.info(
            f"Follower configured with max_relative_target={follower.config.max_relative_target} "
            f"(this limits movement speed - increase if follower moves too slowly)"
        )

    # Get calibration data from leader (leader handles its own calibration loading)
    if leader is not None:
        if leader.calibration is None:
            raise RuntimeError(
                "Leader is not calibrated. Please calibrate the leader first using the calibration script."
            )

        # Create mapping from joint names to joint indexes (motor IDs: 1-6)
        joint_name_to_index = {name: motor.id for name, motor in leader.motors.items()}
        logger.debug(f"Joint name to index mapping: {joint_name_to_index}")

        # Create mapping from joint names to normalization modes
        joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in leader.motors.items()}
        logger.debug(f"Joint name to norm mode mapping: {joint_name_to_norm_mode}")

        # Initialize last observation state (track normalized positions)
        # Leader returns normalized positions, worker thread handles conversion to degrees/radians
        last_observation: Dict[str, float] = {}
        for joint_name in leader.motors.keys():
            last_observation[joint_name] = float("inf")  # Use inf to force first update
    else:
        joint_name_to_index = {}
        joint_name_to_norm_mode = {}
        last_observation: Dict[str, float] = {}

    # Create queue and worker thread for Cyberwave updates
    num_joints = len(leader.motors) if leader is not None else 0
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
    logger.info("Press 'q' to stop teleoperation")

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
            ),
            daemon=True,
        )
        worker_thread.start()
        logger.info("Started Cyberwave update worker thread")

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
                ),
                daemon=True,
            )
            camera_thread.start()
            logger.info(
                f"Started camera streaming thread (camera ID: {camera_id}, FPS: {camera_fps})"
            )
        else:
            logger.debug("Follower has no cameras configured, skipping camera streaming")

    frame_time = 1.0 / fps
    # Thresholds: position is in normalized units (e.g., 0.1 for normalized position change)
    # Worker thread handles conversion to degrees/radians for Cyberwave
    logger.info(f"Starting teleoperation loop at {fps} fps")
    logger.info(
        f"Change thresholds: position={position_threshold} (normalized), "
        f"velocity={velocity_threshold}, effort={effort_threshold}"
    )
    try:
        if (
            leader is not None
            and robot is not None
            and mqtt_client is not None
            and time_reference is not None
        ):
            actions = leader.get_action()
            actions = {
                key.removesuffix(".pos"): val
                for key, val in actions.items()
                if key.endswith(".pos")
            }
            # Use joint_name_to_index to convert actions to joint indexes
            actions = {joint_name_to_index[key]: val for key, val in actions.items()}
            # Send actions to Cyberwave as single update
            mqtt_client.publish_initial_observation(
                twin_uuid=robot.uuid,
                observations=actions,
            )
            logger.info(
                f"Initial observation sent to Cyberwave twin {robot.uuid}, {len(actions)} joints updated"
            )

    except Exception as e:
        logger.error(f"Error getting leader actions: {e}", exc_info=True)

    try:
        if leader is not None:
            update_count, skip_count = _teleop_loop(
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
            )
        else:
            # No leader, just wait for stop event
            update_count = 0
            skip_count = 0
            while not stop_event.is_set():
                time.sleep(0.1)
    finally:
        # Signal all threads to stop
        logger.info("Stopping all worker threads...")
        stop_event.set()

        # Stop MQTT update worker thread
        if worker_thread is not None:
            logger.info("Stopping Cyberwave update worker thread...")
            # Wait for queue to drain (with timeout)
            try:
                action_queue.join(timeout=2.0)
            except Exception:
                pass

            # Wait for thread to finish
            worker_thread.join(timeout=1.0)
            if worker_thread.is_alive():
                logger.warning("MQTT update worker thread did not stop in time")
            else:
                logger.info("MQTT update worker thread stopped successfully")

        # Stop camera streaming thread
        if camera_thread is not None:
            logger.info("Stopping camera streaming thread...")
            camera_thread.join(timeout=5.0)
            if camera_thread.is_alive():
                logger.warning("Camera streaming thread did not stop in time")
            else:
                logger.info("Camera streaming thread stopped successfully")

        logger.info("Teleoperation loop ended")
        try:
            logger.info(f"Total updates sent: {update_count}, skipped: {skip_count}")
        except NameError:
            # Handle case where loop failed before setting update_count
            logger.info("Teleoperation loop ended (no statistics available)")


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
    robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id=args.twin_uuid)
    camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id=args.camera_uuid)
    mqtt_client = cyberwave_client.mqtt
    logger.info("Connected to Cyberwave MQTT broker and created twin")

    # Initialize leader (optional if camera-only mode)
    leader = None
    if not args.camera_only:
        from config import LeaderConfig
        from utils import find_port

        leader_port = args.leader_port
        if not leader_port:
            logger.info("Finding leader port...")
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
