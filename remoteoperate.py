"""Remote operation loop for SO101 follower via Cyberwave MQTT."""

import argparse
import logging
import math
import queue
import select
import sys
import threading
import time
from typing import Dict

from cyberwave import Cyberwave

from follower import SO101Follower
from motors import MotorNormMode
from utils import setup_logging
from write_position import validate_position

logger = logging.getLogger(__name__)


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
                        if char == 'q' or char == 'Q':
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
    logger.debug("Motor writer worker thread started")
    processed_count = 0
    error_count = 0
    from utils import ensure_safe_goal_position

    while not stop_event.is_set():
        try:
            # Get action from queue with timeout
            try:
                action = action_queue.get(timeout=0.1)
            except queue.Empty:
                continue

            try:
                # If max_relative_target is set, split large movements into smaller steps
                if follower.config.max_relative_target is not None:
                    # Get initial current positions (only once, at the start)
                    present_pos = follower.get_observation()
                    current_pos = {
                        key.removesuffix(".pos"): val
                        for key, val in present_pos.items()
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
                        safe_action = {
                            f"{name}.pos": pos for name, pos in safe_goal_pos.items()
                        }

                        # Temporarily disable max_relative_target in follower to avoid double-clamping
                        original_max_relative = follower.config.max_relative_target
                        follower.config.max_relative_target = None

                        try:
                            # Send the safe action directly to bus
                            goal_pos_for_bus = {
                                key.removesuffix(".pos"): val
                                for key, val in safe_action.items()
                            }
                            follower.bus.sync_write("Goal_Position", goal_pos_for_bus, normalize=True)
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
                    follower.send_action(action)
                    processed_count += 1
                    logger.debug(f"Sent action to follower: {action}")

            except Exception as e:
                error_count += 1
                logger.error(f"Error sending action to follower: {e}", exc_info=True)
            finally:
                action_queue.task_done()

        except Exception as e:
            logger.error(f"Error in motor writer worker: {e}", exc_info=True)

    logger.info(
        f"Motor writer worker stopped. Processed: {processed_count}, Errors: {error_count}"
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
    def callback(topic: str, data: Dict) -> None:
        """
        Callback function for MQTT joint state updates.

        Args:
            topic: MQTT topic string
            data: Dictionary containing joint state update
                  Expected format: {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}
        """
        try:
            # Check if topic ends with "update" - only process update messages
            if not topic.endswith("/update"):
                logger.debug(f"Ignoring message on topic {topic} (not an update message)")
                return

            logger.debug(f"Received joint state update on topic {topic}: {data}")

            # Extract joint index/name and position from data
            # Message format:
            # {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}

            # Extract joint_name and joint_state
            if "joint_name" not in data or "joint_state" not in data:
                logger.warning(f"Joint state update missing required fields (joint_name or joint_state): {data}")
                return

            joint_name_str = data.get("joint_name")
            joint_state = data.get("joint_state", {})
            position_radians = joint_state.get("position")

            if position_radians is None:
                logger.warning(f"Joint state update missing 'position' field in joint_state: {data}")
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

            # Validate position
            motor_id = follower.motors[joint_name].id
            is_valid, error_msg = validate_position(motor_id, normalized_position)
            if not is_valid:
                logger.warning(f"Invalid position for {joint_name}: {error_msg}")
                return

            # Update current state (merge with previous state)
            # Create full action state by taking current state and updating the changed joint
            action = current_state.copy()
            action[f"{joint_name}.pos"] = normalized_position
            # Also update the current_state dictionary
            current_state[f"{joint_name}.pos"] = normalized_position

            # Put action in queue (non-blocking)
            try:
                action_queue.put_nowait(action)
                logger.debug(
                    f"Queued action for {joint_name}: {normalized_position:.2f} "
                    f"(from {position_radians:.4f} radians)"
                )
            except queue.Full:
                logger.warning(
                    f"Action queue full, dropping update for {joint_name}. "
                    "Consider increasing queue size."
                )

        except Exception as e:
            logger.error(f"Error processing joint state update: {e}", exc_info=True)

    return callback


def remoteoperate(
    twin_uuid: str,
    client: Cyberwave,
    follower: SO101Follower,
    fps: int = 30,
) -> None:
    """
    Run remote operation loop: receive joint states via MQTT and write to follower motors.

    Args:
        twin_uuid: UUID of the twin to subscribe to
        client: Cyberwave client instance
        follower: SO101Follower instance
        fps: Target frames per second (not used directly, but kept for compatibility)
    """
    setup_logging()

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

    # Send initial observation to Cyberwave as individual MQTT messages
    try:
        # Get follower's current observation (normalized positions)
        follower_obs = follower.get_observation()

        # Convert to joint index format and send each as individual MQTT message
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
                # Send individual joint state update
                mqtt_client.update_joint_state(
                    twin_uuid=twin_uuid,
                    joint_name=str(joint_index),
                    position=radians,
                    velocity=0.0,
                    effort=0.0,
                )

        logger.info(f"Initial observation sent to Cyberwave twin {twin_uuid}, {len(follower_obs)} joints updated")

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

    # Create callback for joint state updates
    joint_state_callback = _create_joint_state_callback(
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
    )

    # Subscribe to joint states
    logger.info(f"Subscribing to joint states for twin {twin_uuid}...")
    mqtt_client.subscribe_joint_states(twin_uuid, joint_state_callback)
    logger.info("Subscribed to joint states")

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

        logger.info("Remote operation loop ended")


def main():
    """Main entry point for remote operation script."""
    parser = argparse.ArgumentParser(
        description="Remote operate SO101 follower via Cyberwave MQTT"
    )
    parser.add_argument(
        "--token",
        type=str,
        required=True,
        help="Cyberwave API token",
    )
    parser.add_argument(
        "--twin-uuid",
        type=str,
        required=True,
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
        "--environment-uuid",
        type=str,
        required=False,
        help="Environment UUID to use for the twin",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Initialize Cyberwave client
    cyberwave_client = Cyberwave(token=args.token)
    logger.info("Initialized Cyberwave client")

    # Initialize follower
    from config import FollowerConfig

    follower_config = FollowerConfig(
        port=args.follower_port,
        max_relative_target=args.max_relative_target,
        id=args.follower_id,
    )
    follower = SO101Follower(config=follower_config)
    follower.connect()

    try:
        remoteoperate(
            twin_uuid=args.twin_uuid,
            client=cyberwave_client,
            follower=follower,
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
