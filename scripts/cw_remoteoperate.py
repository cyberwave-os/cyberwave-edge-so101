"""Remote operation loop for SO101 follower via Cyberwave MQTT."""

import logging
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from cyberwave import Cyberwave, Twin
from cyberwave.constants import SOURCE_TYPE_EDGE_FOLLOWER
from cyberwave.sensor import CameraStreamManager
from cyberwave.utils import TimeReference
from dotenv import load_dotenv

try:
    from cyberwave.sensor import RealSenseDiscovery

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None
    RealSenseDiscovery = None

from scripts.cw_setup import (
    load_setup_config,
    load_so101_config,
    materialize_camera_entries_for_cli,
)
from so101.follower import GRIPPER_HOME_OPEN_NORM, SO101Follower
from utils.config import (
    get_default_mqtt_port,
    get_mute_temperature_notifications,
    get_setup_config_path,
    get_so101_urdf_path,
)
from utils.cw_alerts import (
    create_collision_alert,
    create_robot_setup_done_alert,
    resolve_alert_by_uuid,
    schedule_robot_setup_done_resolve,
)
from utils.cw_remoteoperate_helpers import (
    DeltaResidualStore,
    DeltaVerificationContext,
    check_calibration_required,
    create_joint_state_callback,
    get_remoteoperate_parser,
    motor_writer_worker,
    upload_calibration_to_twin,
)
from utils.cw_teleoperate_helpers import publish_initial_observations
from utils.cw_update_worker import cyberwave_update_worker
from utils.cw_utils import build_joint_mappings, resolve_calibration_for_edge
from utils.keyboard import keyboard_input_thread
from utils.trackers import StatusTracker, run_status_logging_thread
from utils.utils import normalized_to_radians

logger = logging.getLogger(__name__)


OBSERVATION_PUBLISH_HZ = 30


def remoteoperate_loop(
    follower: SO101Follower,
    observation_queue: queue.Queue,
    stop_event: threading.Event,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    observation_rate_hz: int = OBSERVATION_PUBLISH_HZ,
) -> int:
    """
    Read follower observations at a fixed rate and publish via MQTT.

    Unlike teleoperate which runs at 100Hz for responsive leader-follower control,
    remoteoperate only needs to publish observations for UI feedback. A lower rate
    (10Hz) reduces serial bus contention with the motor_writer_worker thread.

    No downsampling needed — we read and publish at the same rate.
    """
    total_update_count = 0
    observation_frame_time = 1.0 / observation_rate_hz

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            timestamp, _ = time_reference.update()

            follower_obs = follower.get_observation() if follower is not None else {}

            if follower_obs:
                # Queue every observation for MQTT publish (no downsampling at OBSERVATION_PUBLISH_HZ)
                try:
                    observation_queue.put_nowait(
                        (SOURCE_TYPE_EDGE_FOLLOWER, follower_obs, timestamp)
                    )
                    total_update_count += len(follower_obs)
                except queue.Full:
                    if status_tracker:
                        status_tracker.increment_errors_mqtt()

            elapsed = time.time() - loop_start
            sleep_time = observation_frame_time - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    except KeyboardInterrupt:
        pass
    except Exception:
        if status_tracker:
            status_tracker.increment_errors_motor()
        raise

    return total_update_count


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    cameras: Optional[List[Dict[str, Any]]] = None,
    stop_event: Optional[threading.Event] = None,
    quiet: bool = False,
    setup_alert_uuid: Optional[str] = None,
) -> None:
    """
    Run remote operation loop: receive joint states via MQTT and write to follower motors.

    Camera config is loaded from setup.json via so101-setup. Pass cameras list built from
    setup (each dict: twin, camera_id, camera_type, camera_resolution, fps, etc.).

    Args:
        client: Cyberwave client instance
        follower: SO101Follower instance
        robot: Robot twin instance (required)
        cameras: List of camera configs from setup.json. Each dict has twin + overrides
            (camera_id, camera_type, camera_resolution, fps, enable_depth, etc.).
            CameraStreamManager uses these directly.
        stop_event: Optional event to signal loop exit. When provided by main.py (edge-core),
            setting it stops the loop. When None (standalone script), creates one internally.
        quiet: If True, disable status display and keep logging at WARNING+ (for edge-core).
        setup_alert_uuid: If set, resolve this alert after MQTT and workers are ready (edge-core).
    """
    time_reference = TimeReference()

    # Initialize variables that need cleanup tracking (set before try block)
    mqtt_client = client.mqtt
    status_tracker = StatusTracker()
    camera_manager: Optional[CameraStreamManager] = None
    status_thread: Optional[threading.Thread] = None
    writer_thread: Optional[threading.Thread] = None
    cyberwave_worker_thread: Optional[threading.Thread] = None
    action_queue: Optional[queue.Queue] = None
    observation_queue: Optional[queue.Queue] = None
    telemetry_started = False
    setup_done_alert_uuid: Optional[str] = None
    setup_done_resolve_timer: Optional[threading.Timer] = None

    # Disable all logging to avoid interfering with status display (unless quiet mode)
    if not quiet:
        logging.disable(logging.CRITICAL)

    try:
        # Build camera twins for CameraStreamManager from cameras (loaded from setup.json)
        camera_twins: List[Any] = []
        camera_fps = 30
        if cameras:
            for cfg in cameras:
                twin = cfg["twin"]
                overrides = {
                    k: v for k, v in cfg.items() if k != "twin" and v is not None
                }
                camera_twins.append((twin, overrides) if overrides else twin)
            camera_fps = cameras[0].get("fps", 30)

        # Create status tracker
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
            first_camera_twin.name
            if first_camera_twin and hasattr(first_camera_twin, "name")
            else ""
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

        # Check calibration - required for accurate positioning
        check_calibration_required(
            follower,
            "follower",
            twin=robot,
            require_calibration=True,
        )

        # Upload follower calibration to twin if available
        if follower.calibration is not None:
            upload_calibration_to_twin(follower, robot, "follower")

        # Move follower to zero pose after connecting (bypass collision checks)
        logger.info("Moving follower to zero pose...")
        zero_pose_action = {
            "shoulder_pan.pos": 0.0,
            "shoulder_lift.pos": 0.0,
            "elbow_flex.pos": 0.0,
            "wrist_flex.pos": 0.0,
            "wrist_roll.pos": 0.0,
            "gripper.pos": GRIPPER_HOME_OPEN_NORM,
        }
        success, msg = follower.send_action_safe(
            zero_pose_action,
            check_collision=False,
            check_load_before=False,
        )
        if success:
            logger.info("Follower moved to zero pose successfully")
        else:
            logger.warning(f"Failed to move follower to zero pose: {msg}")

        # Initialize Pinocchio for kinematics safety checks (load URDF once)
        # floor_z=0.0 means the floor is at the base level (Z=0)
        # collision_margin=0.01 means reject poses within 1cm of collision
        urdf_path = get_so101_urdf_path()
        if urdf_path:
            follower.init_pin(urdf_path)

            # Set up collision alert callback
            def on_collision(
                collision_type: str,
                message: str,
                joint_angles: Optional[Dict[str, float]],
            ) -> None:
                if robot is not None:
                    try:
                        create_collision_alert(robot, collision_type, message, joint_angles=joint_angles)
                    except Exception as e:
                        logger.debug("Failed to create collision alert: %s", e)

            follower.set_collision_callback(on_collision)
        else:
            logger.debug("No URDF found; kinematics safety checks disabled")

        # Resolve calibration: prefer local device, fallback to twin API
        follower_calibration = resolve_calibration_for_edge(
            robot,
            follower.calibration if follower is not None else None,
            "follower",
        )

        # Build joint mappings from twin schema and SO101 motors
        mappings = build_joint_mappings(robot, follower.motors if follower else None)
        joint_index_to_name = mappings["joint_index_to_name"]
        joint_name_to_norm_mode = mappings["joint_name_to_norm_mode"]
        motor_id_to_schema_joint = mappings["motor_id_to_schema_joint"]
        schema_joint_to_motor_id = mappings["schema_joint_to_motor_id"]

        joint_index_to_name_str = {str(motor.id): name for name, motor in follower.motors.items()}
        status_tracker.set_joint_index_to_name(joint_index_to_name_str)

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
                    calib = follower_calibration.get(name) if follower_calibration else None
                    radians = normalized_to_radians(normalized_pos, norm_mode, calib)
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

        # Send telemetry_start with follower observations before any joint updates.
        # fps reflects actual MQTT publish rate (10Hz observation loop, no downsampling needed)
        if not telemetry_started:
            publish_initial_observations(
                leader=None,
                follower=follower,
                robot=robot,
                mqtt_client=mqtt_client,
                leader_calibration=None,
                follower_calibration=follower_calibration,
                joint_name_to_norm_mode=joint_name_to_norm_mode,
                motor_id_to_schema_joint=motor_id_to_schema_joint,
                fps=OBSERVATION_PUBLISH_HZ,
            )
            telemetry_started = True

        # Queue for motor commands from MQTT (motor_writer_worker)
        action_queue = queue.Queue(maxsize=1000)
        # Queue for follower observations -> cyberwave_update_worker (aggregated joint MQTT)
        joint_name_to_index = {name: mid for mid, name in joint_index_to_name.items()}
        num_joints = len(follower.motors)
        seconds = 60
        observation_queue = queue.Queue(maxsize=max(num_joints * OBSERVATION_PUBLISH_HZ * seconds, 1000))
        if stop_event is None:
            stop_event = threading.Event()

        # Start keyboard input thread for 'q' key to stop gracefully (no-op when run by main.py in Docker)
        keyboard_thread = threading.Thread(
            target=keyboard_input_thread,
            args=(stop_event,),
            daemon=True,
        )
        keyboard_thread.start()

        # Start status logging thread
        _setup = load_setup_config()
        _mute_temp = get_mute_temperature_notifications(_setup)
        status_thread = threading.Thread(
            target=run_status_logging_thread,
            args=(status_tracker, stop_event, OBSERVATION_PUBLISH_HZ, camera_fps),
            kwargs={
                "follower": follower,
                "robot": robot,
                "mode": "remoteoperate",
                "mqtt_client": mqtt_client,
                "motor_id_to_schema_joint": motor_id_to_schema_joint,
                "mute_temperature_notifications": _mute_temp,
            },
            daemon=True,
        )
        status_thread.start()

        # Note: No separate heartbeat thread needed — the observation loop at 10Hz
        # continuously publishes follower positions via cyberwave_update_worker.

        # Start camera streaming via SDK CameraStreamManager (one stream per twin, each with own thread)
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
                    if "started" in msg_lower or (
                        status == "ok" and ("streaming" in msg_lower or "running" in msg_lower)
                    ):
                        status_tracker.update_webrtc_state(camera_name, "streaming")
                        status_tracker.update_camera_status(
                            camera_name, detected=True, started=True
                        )
                    elif status == "connecting" or "starting" in msg_lower:
                        status_tracker.update_webrtc_state(camera_name, "connecting")
                    elif status == "error":
                        status_tracker.update_webrtc_state(camera_name, "idle")
                    elif "stopped" in msg_lower:
                        status_tracker.update_webrtc_state(camera_name, "idle")
                        status_tracker.update_camera_status(
                            camera_name, detected=True, started=False
                        )

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

        # Delta residual store for drift compensation (delta/chunked_delta modalities)
        delta_residual_store = DeltaResidualStore()
        delta_verification = DeltaVerificationContext(
            residual_store=delta_residual_store,
            joint_name_to_norm_mode=joint_name_to_norm_mode,
            follower_calibration=follower_calibration,
        )

        # Create callback for joint state updates
        joint_state_callback = create_joint_state_callback(
            current_state=current_state,
            action_queue=action_queue,
            joint_index_to_name=joint_index_to_name,
            joint_name_to_norm_mode=joint_name_to_norm_mode,
            follower=follower,
            follower_calibration=follower_calibration,
            status_tracker=status_tracker,
            schema_joint_to_motor_id=schema_joint_to_motor_id,
            residual_store=delta_residual_store,
        )

        # Subscribe to joint states
        mqtt_client.subscribe_joint_states(twin_uuid, joint_state_callback)

        # Start motor writer worker thread
        writer_thread = threading.Thread(
            target=motor_writer_worker,
            args=(action_queue, follower, stop_event, status_tracker),
            kwargs={
                "delta_verification": delta_verification,
                "twin": robot,
                "use_fast_collision": True,
            },
            daemon=True,
        )
        writer_thread.start()

        # Publish follower joint states from observation loop (same path as cw_teleoperate)
        if robot is not None:
            cyberwave_worker_thread = threading.Thread(
                target=cyberwave_update_worker,
                args=(
                    observation_queue,
                    joint_name_to_index,
                    joint_name_to_norm_mode,
                    stop_event,
                    robot,
                    status_tracker,
                    follower_calibration,
                    motor_id_to_schema_joint,
                ),
                daemon=True,
            )
            cyberwave_worker_thread.start()

        if setup_alert_uuid and robot is not None:
            try:
                alert = robot.alerts.get(setup_alert_uuid)
                if alert is not None:
                    alert.resolve()
            except Exception:
                logger.debug(
                    "Could not resolve setup alert %s",
                    setup_alert_uuid,
                    exc_info=True,
                )

            setup_done_alert_uuid = create_robot_setup_done_alert(robot)
            if setup_done_alert_uuid:
                setup_done_resolve_timer = schedule_robot_setup_done_resolve(
                    robot,
                    setup_done_alert_uuid,
                )

        # Main loop: read hardware observations and queue MQTT publishes (10Hz, no downsampling)
        status_tracker.fps = OBSERVATION_PUBLISH_HZ
        remoteoperate_loop(
            follower=follower,
            observation_queue=observation_queue,
            stop_event=stop_event,
            time_reference=time_reference,
            status_tracker=status_tracker,
        )

    except KeyboardInterrupt:
        pass  # Allow normal cleanup in finally block

    finally:
        # Re-enable logging for cleanup diagnostics
        logging.disable(logging.NOTSET)
        cleanup_logger = logging.getLogger(__name__)

        if setup_done_resolve_timer is not None:
            setup_done_resolve_timer.cancel()
        if setup_done_alert_uuid and robot is not None:
            resolve_alert_by_uuid(robot, setup_done_alert_uuid)

        # Signal all threads to stop
        if stop_event is not None:
            stop_event.set()

        # Wait for motor action queue to drain so pending commands are delivered
        if action_queue is not None:
            try:
                action_queue.join()
            except Exception:
                pass

        # Wait for writer thread to finish
        if writer_thread is not None:
            writer_thread.join(timeout=2.0)

        # Wait for observation queue to drain so pending MQTT joint updates are delivered
        if observation_queue is not None:
            try:
                observation_queue.join()
            except Exception:
                pass

        if cyberwave_worker_thread is not None:
            cyberwave_worker_thread.join(timeout=2.0)

        # Stop camera streaming
        if camera_manager is not None:
            camera_manager.join(timeout=5.0)

        # Stop status thread
        if status_thread is not None:
            status_thread.join(timeout=1.0)

        # ALWAYS publish telemetry_end, regardless of whether telemetry_start succeeded
        # This ensures the backend knows the session ended
        twin_uuid_str = str(robot.uuid)
        cleanup_logger.info(
            "Publishing telemetry_end for twin %s (mqtt connected: %s, telemetry_started: %s)",
            twin_uuid_str,
            mqtt_client.connected if mqtt_client else "N/A",
            telemetry_started,
        )
        try:
            if hasattr(mqtt_client, "publish_telemetry_end"):
                mqtt_client.publish_telemetry_end(twin_uuid_str)
            else:
                # Fallback for older SDK versions without publish_telemetry_end
                topic = f"{mqtt_client.topic_prefix}cyberwave/twin/{twin_uuid_str}/telemetry"
                message = {"type": "telemetry_end", "timestamp": time.time()}
                mqtt_client.publish(topic, message)
            cleanup_logger.info("telemetry_end published successfully")
        except Exception:
            cleanup_logger.exception("Failed to publish telemetry_end")
            if status_tracker:
                status_tracker.increment_errors_mqtt()


def main():
    """Main entry point for remote operation script."""
    load_dotenv()

    parser = get_remoteoperate_parser()
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
    cyberwave_client = Cyberwave(mqtt_port=get_default_mqtt_port())

    cameras_list: List[Dict[str, Any]] = []
    setup_config: Dict[str, Any] = {}

    # Load setup.json by default when it exists (used for cameras, twin_uuid, ports)
    setup_path = Path(args.setup_path) if args.setup_path else get_setup_config_path()
    so101_loaded = None
    if setup_path.exists():
        setup_config = load_setup_config(setup_path)
        if setup_config:
            print(f"Loaded setup from: {setup_path}")
        so101_loaded = load_so101_config(setup_path)

    # Remote operation: always use max_relative_target=None to avoid position reads in send_action.
    # The ST3215 motors' internal Vmax/Amax (configured in follower.connect()) handle safe motion.
    # This reduces serial bus contention between the observation loop and motor writer thread.

    if so101_loaded is not None and setup_config:
        cameras_list = materialize_camera_entries_for_cli(cyberwave_client, so101_loaded.cameras)

    # Resolve twin UUID and ports: CLI/env > setup.json (so remoteoperate works with no args when setup exists)
    effective_twin_uuid = (
        args.twin_uuid
        or setup_config.get("twin_uuid")
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
        max_relative_target=None,  # Rely on motor Vmax/Amax; avoid serial reads in send_action
        cameras=follower_cameras,
    )
    follower = SO101Follower(config=follower_config)
    follower.connect()

    try:
        remoteoperate(
            client=cyberwave_client,
            follower=follower,
            robot=robot,
            cameras=cameras_list if cameras_list else None,
            quiet=args.quiet,
        )
    finally:
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
