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
from utils import setup_logging
from write_position import validate_position

logger = logging.getLogger(__name__)


def _camera_worker_thread(
    client: Cyberwave,
    camera_id: int,
    fps: int,
    twin_uuid: str,
    stop_event: threading.Event,
    time_reference: TimeReference
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
            sensor_type="rgb",
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
                command_callback=lambda status, msg: logger.info(f"Camera command: {status} - {msg}"),
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
) -> None:
    """
    Worker thread that reads actions from queue and writes to follower motors.

    Splits large position changes into multiple smaller steps based on max_relative_target.

    Args:
        action_queue: Queue containing action dictionaries (normalized positions)
        follower: SO101Follower instance
        stop_event: Event to signal thread to stop
    """
    logger.info("Motor writer worker thread started")
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
                logger.debug(
                    f"[MOTOR WRITER] Received action #{actions_received} from queue "
                    f"(queue size now: {action_queue.qsize()})"
                )
                logger.debug(f"[MOTOR WRITER] Action data: {action}")
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
                            logger.debug(
                                f"Sent step {step + 1}/{max_steps} to follower: {safe_action}"
                            )
                        finally:
                            # Restore original max_relative_target
                            follower.config.max_relative_target = original_max_relative

                        # No sleep - send steps as fast as possible
                        # The motors will catch up naturally
                else:
                    # No max_relative_target limit, send action directly
                    logger.debug(f"[MOTOR WRITER] Sending action directly (no max_relative_target): {action}")
                    sent_action = follower.send_action(action)
                    processed_count += 1
                    logger.debug(
                        f"[MOTOR WRITER] Action sent successfully #{processed_count}. "
                        f"Sent: {sent_action}"
                    )

            except Exception as e:
                error_count += 1
                logger.error(
                    f"[MOTOR WRITER] Error sending action to follower (error #{error_count}): {e}",
                    exc_info=True
                )
            finally:
                action_queue.task_done()
                # Periodic summary
                if actions_received % 5 == 0:
                    logger.info(
                        f"[MOTOR WRITER STATS] Received: {actions_received}, "
                        f"Processed: {processed_count}, Errors: {error_count}, "
                        f"Queue size: {action_queue.qsize()}"
                    )

        except Exception as e:
            logger.error(f"[MOTOR WRITER] Error in worker loop: {e}", exc_info=True)

    logger.info(
        f"[MOTOR WRITER] Worker stopped. Total received: {actions_received}, "
        f"Processed: {processed_count}, Errors: {error_count}"
    )


def _create_joint_state_callback(
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
) -> callable:
    """
    Create a callback function for MQTT joint state updates.

    Args:
        current_state: Dictionary to maintain current state (joint_name -> normalized position)
        action_queue: Queue to put validated actions
        joint_index_to_name: Mapping from joint index (1-6) to joint name
        joint_name_to_norm_mode: Mapping from joint name to normalization mode
        follower: Follower instance for validation

    Returns:
        Callback function for MQTT joint state updates
    """
    # Track message count for debugging
    message_count = {"total": 0, "processed": 0, "filtered": 0, "invalid": 0, "queued": 0}

    def callback(topic: str, data: Dict) -> None:
        """
        Callback function for MQTT joint state updates.

        Args:
            topic: MQTT topic string
            data: Dictionary containing joint state update
                  Expected format: {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}
        """
        message_count["total"] += 1
        try:
            # Log every message received (even if filtered)
            logger.debug(f"[MSG #{message_count['total']}] Received on topic: {topic}")
            logger.debug(f"[MSG #{message_count['total']}] Raw data: {data}")

            # Check if topic ends with "update" - only process update messages
            if not topic.endswith("/update"):
                message_count["filtered"] += 1
                logger.debug(f"[MSG #{message_count['total']}] FILTERED - topic does not end with '/update'")
                return

            message_count["processed"] += 1
            logger.debug(f"[MSG #{message_count['total']}] Processing message (processed: {message_count['processed']})")

            # Extract joint index/name and position from data
            # Message format:
            # {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}

            # Extract joint_name and joint_state
            if "joint_name" not in data or "joint_state" not in data:
                logger.warning(
                    f"Joint state update missing required fields (joint_name or joint_state): {data}"
                )
                return

            joint_name_str = data.get("joint_name")
            joint_state = data.get("joint_state", {})
            position_radians = joint_state.get("position")

            if position_radians is None:
                logger.warning(
                    f"Joint state update missing 'position' field in joint_state: {data}"
                )
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
                    logger.warning(f"Unknown joint name/index: {joint_name_str}")
                    return

            # Get joint name from index
            joint_name = joint_index_to_name.get(joint_index)
            if joint_name is None:
                logger.warning(f"Unknown joint index: {joint_index}")
                return

            # Convert position_radians to float
            try:
                position_radians = float(position_radians)
            except (ValueError, TypeError):
                logger.warning(f"Invalid position value: {position_radians}")
                return

            # Get normalization mode for this joint
            norm_mode = joint_name_to_norm_mode.get(joint_name)
            if norm_mode is None:
                logger.warning(f"Unknown normalization mode for joint: {joint_name}")
                return

            # Convert radians to normalized position
            normalized_position = _radians_to_normalized(position_radians, norm_mode)
            logger.debug(
                f"[MSG #{message_count['total']}] Conversion: {position_radians:.4f} rad -> "
                f"{normalized_position:.2f} normalized (mode: {norm_mode})"
            )

            # Validate position
            motor_id = follower.motors[joint_name].id
            is_valid, error_msg = validate_position(motor_id, normalized_position)
            if not is_valid:
                message_count["invalid"] += 1
                logger.warning(
                    f"[MSG #{message_count['total']}] INVALID position for {joint_name} (motor {motor_id}): "
                    f"{error_msg} (invalid count: {message_count['invalid']})"
                )
                return
            logger.debug(f"[MSG #{message_count['total']}] Position validated OK for {joint_name}")

            # Update current state (merge with previous state)
            # Create full action state by taking current state and updating the changed joint
            action = current_state.copy()
            action[f"{joint_name}.pos"] = normalized_position
            # Also update the current_state dictionary
            current_state[f"{joint_name}.pos"] = normalized_position

            # Put action in queue (non-blocking)
            try:
                queue_size_before = action_queue.qsize()
                action_queue.put_nowait(action)
                message_count["queued"] += 1
                logger.debug(
                    f"[MSG #{message_count['total']}] QUEUED action for {joint_name}: "
                    f"{normalized_position:.2f} (from {position_radians:.4f} rad) "
                    f"[queue size: {queue_size_before} -> {action_queue.qsize()}, total queued: {message_count['queued']}]"
                )
            except queue.Full:
                logger.warning(
                    f"[MSG #{message_count['total']}] QUEUE FULL - dropping update for {joint_name}. "
                    f"Queue size: {action_queue.qsize()}"
                )

        except Exception as e:
            logger.error(f"[MSG #{message_count['total']}] Error processing joint state update: {e}", exc_info=True)

        # Periodic summary logging
        if message_count["total"] % 10 == 0:
            logger.info(
                f"[CALLBACK STATS] Total: {message_count['total']}, Processed: {message_count['processed']}, "
                f"Filtered: {message_count['filtered']}, Invalid: {message_count['invalid']}, "
                f"Queued: {message_count['queued']}, Queue size: {action_queue.qsize()}"
            )

    return callback


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
    fps: int = 30,
    camera_fps: int = 30,
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
    """
    setup_logging()
    time_reference = TimeReference()

    # Get twin_uuid from robot twin
    if robot is None:
        raise RuntimeError("Robot twin is required")
    twin_uuid = robot.uuid

    # Ensure follower is connected
    if not follower.connected:
        raise RuntimeError("Follower is not connected")

    # Verify follower has torque enabled (required for movement)
    if not follower.torque_enabled:
        logger.warning("Follower torque is not enabled! Motors will not move.")
        logger.info("Enabling follower torque...")
        follower.enable_torque()

    # Log follower configuration for debugging
    logger.info(
        f"Follower configured with max_relative_target={follower.config.max_relative_target} "
        f"(this limits movement speed - increase if follower moves too slowly)"
    )

    # Create mapping from joint index (motor ID) to joint name
    joint_index_to_name = {motor.id: name for name, motor in follower.motors.items()}
    logger.debug(f"Joint index to name mapping: {joint_index_to_name}")

    # Create mapping from joint names to normalization modes
    joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in follower.motors.items()}
    logger.debug(f"Joint name to norm mode mapping: {joint_name_to_norm_mode}")

    # Initialize current state with follower's current observation
    current_state: Dict[str, float] = {}
    try:
        initial_obs = follower.get_observation()
        current_state.update(initial_obs)
        logger.info(f"Initial follower state: {current_state}")
    except Exception as e:
        logger.warning(f"Could not get initial follower observation: {e}")
        # Initialize with empty state
        for name in follower.motors.keys():
            current_state[f"{name}.pos"] = 0.0

    # Ensure MQTT client is connected
    mqtt_client = client.mqtt
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
        logger.info(f"Initial observation sent to Cyberwave twin {twin_uuid}, {len(observations)} joints updated")
        logger.info(
            f"Initial observation sent to Cyberwave twin {twin_uuid}, {len(follower_obs)} joints updated"
        )

    except Exception as e:
        logger.error(f"Error sending initial observation: {e}", exc_info=True)

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
    logger.info("Press 'q' to stop remote operation")

    # Start camera streaming thread if follower has cameras configured and camera twin is provided
    camera_thread = None
    if follower.config.cameras and len(follower.config.cameras) > 0 and camera is not None:
        camera_id = follower.config.cameras[0]
        camera_thread = threading.Thread(
            target=_camera_worker_thread,
            args=(client, camera_id, camera_fps, camera.uuid, stop_event, time_reference),
            daemon=True,
        )
        camera_thread.start()
        logger.info(f"Started camera streaming thread (camera ID: {camera_id}, FPS: {camera_fps})")
    elif camera is not None:
        logger.debug("Follower has no cameras configured, skipping camera streaming")

    # Create callback for joint state updates
    logger.info("Creating joint state callback with mappings:")
    logger.info(f"  joint_index_to_name: {joint_index_to_name}")
    logger.info(f"  joint_name_to_norm_mode: {joint_name_to_norm_mode}")
    joint_state_callback = _create_joint_state_callback(
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
    )

    # Subscribe to joint states
    # The topic pattern is: {prefix}cyberwave/joint/{twin_uuid}/+
    # Messages should come on topics like: cyberwave/joint/{twin_uuid}/update
    logger.info(f"Subscribing to joint states for twin {twin_uuid}...")
    logger.info(f"  Expected topic pattern: cyberwave/joint/{twin_uuid}/+")
    mqtt_client.subscribe_joint_states(twin_uuid, joint_state_callback)
    logger.info("Subscribed to joint states - waiting for messages...")

    # Start motor writer worker thread
    writer_thread = threading.Thread(
        target=_motor_writer_worker,
        args=(action_queue, follower, stop_event),
        daemon=True,
    )
    writer_thread.start()
    logger.info("Started motor writer worker thread")

    try:
        # Main loop: just wait for stop event
        logger.info("Remote operation active. Waiting for joint state updates...")
        while not stop_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Remote operation interrupted by user")
        stop_event.set()
    finally:
        # Signal all threads to stop
        logger.info("Stopping all worker threads...")
        stop_event.set()

        # Wait for queue to drain (with timeout)
        try:
            action_queue.join(timeout=2.0)
        except Exception:
            pass

        # Wait for writer thread to finish
        writer_thread.join(timeout=1.0)
        if writer_thread.is_alive():
            logger.warning("Motor writer thread did not stop in time")
        else:
            logger.info("Motor writer thread stopped successfully")

        # Stop camera streaming thread
        if camera_thread is not None:
            logger.info("Stopping camera streaming thread...")
            camera_thread.join(timeout=5.0)
            if camera_thread.is_alive():
                logger.warning("Camera streaming thread did not stop in time")
            else:
                logger.info("Camera streaming thread stopped successfully")

        logger.info("Remote operation loop ended")


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
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Default camera-uuid to twin-uuid if not provided
    if args.camera_uuid is None:
        args.camera_uuid = args.twin_uuid

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Initialize Cyberwave client and create twins
    cyberwave_client = Cyberwave()
    robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id=args.twin_uuid)
    camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id=args.camera_uuid)
    logger.info("Connected to Cyberwave MQTT broker and created twins")

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
        )
    except Exception as e:
        logger.error(f"Error in remoteoperate: {e}", exc_info=True)
    finally:
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        mqtt_client = cyberwave_client.mqtt
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()
        logger.info("Disconnected from Cyberwave MQTT broker")


if __name__ == "__main__":
    main()
