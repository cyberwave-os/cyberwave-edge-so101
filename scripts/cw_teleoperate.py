"""Teleoperation loop for SO101 leader and follower."""

import logging
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from cyberwave import Cyberwave, Twin
from cyberwave.constants import SOURCE_TYPE_EDGE_FOLLOWER, SOURCE_TYPE_EDGE_LEADER
from cyberwave.sensor import CameraStreamManager, Resolution
from cyberwave.utils import TimeReference
from dotenv import load_dotenv

try:
    from cyberwave.sensor import RealSenseDiscovery

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None
    RealSenseDiscovery = None

from scripts.cw_setup import load_setup_config
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import get_setup_config_path
from utils.cw_remoteoperate_helpers import (
    check_calibration_required,
    upload_calibration_to_twin,
)
from utils.cw_teleoperate_helpers import (
    get_teleoperate_parser,
    publish_initial_follower_observation,
)
from utils.cw_update_worker import cyberwave_update_worker, process_cyberwave_updates
from utils.cw_utils import build_joint_mappings, resolve_calibration_for_edge
from utils.keyboard import keyboard_input_thread
from utils.trackers import StatusTracker, run_status_logging_thread
from utils.utils import parse_resolution_to_enum

logger = logging.getLogger(__name__)

CONTROL_RATE_HZ = 100


def teleop_loop(
    leader: SO101Leader,
    follower: Optional[SO101Follower],
    action_queue: queue.Queue,
    stop_event: threading.Event,
    last_observation_leader: Dict[str, float],
    last_observation_follower: Dict[str, float],
    position_threshold: float,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    heartbeat_interval: float = 1.0,
    control_rate_hz: int = CONTROL_RATE_HZ,
) -> tuple[int, int]:
    """
    Main teleoperation loop: read from leader, send to follower, send data to Cyberwave.

    The loop always runs at 100Hz for responsive robot control and MQTT updates.
    Leader and follower observations are each threshold-filtered against their own
    previous values before sending to Cyberwave.
    """
    total_update_count = 0
    total_skip_count = 0
    last_send_times_leader: Dict[str, float] = {}
    last_send_times_follower: Dict[str, float] = {}
    control_frame_time = 1.0 / control_rate_hz

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            timestamp, timestamp_monotonic = time_reference.update()

            leader_obs = leader.get_observation() if leader is not None else {}
            follower_obs = follower.get_observation() if follower is not None else leader_obs

            if follower is not None and leader is not None:
                try:
                    follower.send_action(leader_obs)
                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors()

            # Send both leader and follower observations to Cyberwave (when both exist).
            # Each is threshold-filtered against its own previous values.
            update_count, skip_count = 0, 0
            if leader_obs:
                uc, sc = process_cyberwave_updates(
                    action=leader_obs,
                    last_observation=last_observation_leader,
                    action_queue=action_queue,
                    position_threshold=position_threshold,
                    timestamp=timestamp,
                    status_tracker=status_tracker,
                    last_send_times=last_send_times_leader,
                    heartbeat_interval=heartbeat_interval,
                    source_type=SOURCE_TYPE_EDGE_LEADER,
                )
                update_count += uc
                skip_count += sc
            if follower_obs and follower is not None and follower_obs is not leader_obs:
                uc, sc = process_cyberwave_updates(
                    action=follower_obs,
                    last_observation=last_observation_follower,
                    action_queue=action_queue,
                    position_threshold=position_threshold,
                    timestamp=timestamp,
                    status_tracker=status_tracker,
                    last_send_times=last_send_times_follower,
                    heartbeat_interval=heartbeat_interval,
                    source_type=SOURCE_TYPE_EDGE_FOLLOWER,
                )
                update_count += uc
                skip_count += sc
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


def teleoperate(
    leader: SO101Leader,
    cyberwave_client: Cyberwave,
    follower: SO101Follower,
    robot: Twin,
    cameras: Optional[List[Dict[str, Any]]] = None,
    position_threshold: float = 0.1,
) -> None:
    """
    Run teleoperation loop: read from leader, send to follower, and send follower data to Cyberwave.

    Uses a separate thread with a FIFO queue to send updates to Cyberwave,
    keeping the main loop responsive. Only sends updates when values change.
    Follower data (actual robot state) is sent to Cyberwave, not leader data.
    Always converts positions to radians.

    The control loop always runs at 100Hz for responsive robot control and MQTT updates.

    Camera config is loaded from setup.json via so101-setup. Pass cameras list built from
    setup (each dict: twin, camera_id, camera_type, camera_resolution, fps, etc.).

    Args:
        leader: SO101Leader instance (required)
        cyberwave_client: Cyberwave client instance (required)
        follower: SO101Follower instance (required)
        robot: Robot twin instance (required)
        cameras: List of camera configs from setup.json. Each dict has twin + overrides
            (camera_id, camera_type, camera_resolution, fps, enable_depth, etc.).
            CameraStreamManager uses these directly.
        position_threshold: Minimum change in position to trigger an update (in normalized units)
    """
    time_reference = TimeReference()

    # Disable all logging to avoid interfering with status display
    logging.disable(logging.CRITICAL)

    # Build camera twins for CameraStreamManager from cameras (loaded from setup.json)
    camera_twins: List[Any] = []
    camera_fps = 30
    if cameras:
        for cfg in cameras:
            twin = cfg["twin"]
            overrides = {k: v for k, v in cfg.items() if k != "twin"}
            camera_twins.append((twin, overrides) if overrides else twin)
        camera_fps = cameras[0].get("fps", 30)

    # Create status tracker
    status_tracker = StatusTracker()
    status_tracker.script_started = True
    status_tracker.camera_enabled = len(camera_twins) > 0

    # Set twin info for status display (use first camera for display)
    robot_uuid = robot.uuid
    robot_name = robot.name if hasattr(robot, "name") else "so101-teleop"

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

    # Ensure MQTT client is connected
    mqtt_client = cyberwave_client.mqtt
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

    if not leader.connected:
        raise RuntimeError("Leader is not connected")

    if not follower.connected:
        raise RuntimeError("Follower is not connected")

    # Verify follower has torque enabled (required for movement)
    if not follower.torque_enabled:
        follower.enable_torque()

    # Check calibration for leader (required)
    check_calibration_required(
        leader,
        "leader",
        twin=robot,
        require_calibration=True,
    )
    # Upload leader calibration to twin
    upload_calibration_to_twin(leader, robot, "leader")

    # Check calibration for follower (required)
    check_calibration_required(
        follower,
        "follower",
        twin=robot,
        require_calibration=True,
    )

    # Upload follower calibration to twin if available
    if follower.calibration is not None:
        upload_calibration_to_twin(follower, robot, "follower")

    # Use follower motors for mappings when sending to Cyberwave (follower data is what we send)
    motors_for_mapping = follower.motors

    # Resolve calibration: prefer local device, fallback to twin API
    follower_calibration = resolve_calibration_for_edge(
        robot,
        follower.calibration,
        "follower",
    )

    # Build joint mappings from twin schema and SO101 motors
    mappings = build_joint_mappings(robot, motors_for_mapping)
    motor_id_to_schema_joint = mappings["motor_id_to_schema_joint"]
    joint_index_to_name = mappings["joint_index_to_name"]
    joint_name_to_norm_mode = mappings["joint_name_to_norm_mode"]
    joint_name_to_index = {name: mid for mid, name in joint_index_to_name.items()}

    joint_index_to_name_str = {str(mid): name for mid, name in joint_index_to_name.items()}
    status_tracker.set_joint_index_to_name(joint_index_to_name_str)

    # Initialize last observation state per source (track normalized positions for threshold filtering)
    last_observation_leader: Dict[str, float] = {}
    last_observation_follower: Dict[str, float] = {}
    for joint_name in motors_for_mapping.keys():
        last_observation_leader[joint_name] = float("inf")  # Use inf to force first update
        last_observation_follower[joint_name] = float("inf")

    # Create queue and worker thread for Cyberwave updates
    num_joints = len(motors_for_mapping)
    sampling_rate = 100  # Hz
    seconds = 60  # seconds
    queue_size = num_joints * sampling_rate * seconds
    action_queue = queue.Queue(maxsize=queue_size)  # Limit queue size to prevent memory issues
    stop_event = threading.Event()

    # Start keyboard input thread for 'q' key to stop gracefully
    keyboard_thread = threading.Thread(
        target=keyboard_input_thread,
        args=(stop_event,),
        daemon=True,
    )
    keyboard_thread.start()

    # Start status logging thread
    status_thread = threading.Thread(
        target=run_status_logging_thread,
        args=(status_tracker, stop_event, CONTROL_RATE_HZ, camera_fps),
        kwargs={
            "leader": leader,
            "follower": follower,
            "robot": robot,
            "mode": "teleoperate",
        },
        daemon=True,
    )
    status_thread.start()

    # Start MQTT update worker thread
    worker_thread = None
    if robot is not None:
        worker_thread = threading.Thread(
            target=cyberwave_update_worker,
            args=(
                action_queue,
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
        worker_thread.start()

    # Start camera streaming via SDK CameraStreamManager (one stream per twin, each with own thread)
    camera_manager: Optional[CameraStreamManager] = None
    if camera_twins and cyberwave_client is not None:
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
                overrides["camera_id"] = follower_cameras[idx] if idx < len(follower_cameras) else 0
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
            client=cyberwave_client,
            twins=enriched_twins,
            stop_event=stop_event,
            time_reference=time_reference,
            command_callback=command_callback,
        )
        camera_manager.start()
    else:
        status_tracker.camera_states.clear()

    # TimeReference synchronization: teleop loop runs at 100Hz for control and MQTT updates.
    status_tracker.fps = CONTROL_RATE_HZ

    try:
        if (
            follower is not None
            and robot is not None
            and mqtt_client is not None
            and time_reference is not None
        ):
            publish_initial_follower_observation(
                follower=follower,
                robot=robot,
                mqtt_client=mqtt_client,
                follower_calibration=follower_calibration,
                joint_name_to_norm_mode=joint_name_to_norm_mode,
                motor_id_to_schema_joint=motor_id_to_schema_joint,
                fps=CONTROL_RATE_HZ,
            )
    except Exception:
        if status_tracker:
            status_tracker.increment_errors()

    try:
        if leader is not None:
            teleop_loop(
                leader=leader,
                follower=follower,
                action_queue=action_queue,
                stop_event=stop_event,
                last_observation_leader=last_observation_leader,
                last_observation_follower=last_observation_follower,
                position_threshold=position_threshold,
                time_reference=time_reference,
                status_tracker=status_tracker,
            )
        else:
            # No leader, just wait for stop event
            while not stop_event.is_set():
                time.sleep(0.1)
    finally:
        # Signal all threads to stop
        stop_event.set()

        # Wait for threads to finish
        if worker_thread is not None:
            try:
                action_queue.join(timeout=2.0)
            except Exception:
                pass
            worker_thread.join(timeout=1.0)

        if camera_manager is not None:
            camera_manager.join(timeout=5.0)

        if status_thread is not None:
            status_thread.join(timeout=1.0)


def main():
    """Main entry point for teleoperation script."""
    load_dotenv()

    parser = get_teleoperate_parser()
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

    cameras_list: List[Dict[str, Any]] = []
    setup_config: Dict[str, Any] = {}

    # Load setup.json by default when it exists (used for cameras, twin_uuid, ports)
    setup_path = Path(args.setup_path) if args.setup_path else get_setup_config_path()
    if setup_path.exists():
        setup_config = load_setup_config(setup_path)
        if setup_config:
            print(f"Loaded setup from: {setup_path}")

    # All camera/teleop config comes from setup.json (so101-setup)
    max_relative_target = setup_config.get("max_relative_target")

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
            wrist_res_enum = parse_resolution_to_enum(wrist_res)
            cameras_list.append(
                {
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
                }
            )

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
                depth_res_enum = Resolution.from_size(
                    depth_res[0], depth_res[1]
                ) or Resolution.closest(depth_res[0], depth_res[1])
            cameras_list.append(
                {
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
                }
            )

    # Resolve twin UUID and ports: CLI/env > setup.json (so teleoperate works with no args when setup exists)
    effective_twin_uuid = (
        args.twin_uuid
        or setup_config.get("twin_uuid")
        or setup_config.get("wrist_camera_twin_uuid")
    )
    effective_leader_port = args.leader_port or setup_config.get("leader_port")
    effective_follower_port = args.follower_port or setup_config.get("follower_port")
    if not effective_twin_uuid:
        print(
            "Error: Twin UUID required. Use --twin-uuid, set CYBERWAVE_TWIN_UUID, "
            "or run so101-setup with --twin-uuid"
        )
        sys.exit(1)

    # Follower and leader are both required for teleoperation
    if not effective_follower_port:
        print(
            "Error: Follower port required. Calibrate follower first (so101-calibrate) to save port, "
            "or pass --follower-port / set CYBERWAVE_METADATA_FOLLOWER_PORT"
        )
        sys.exit(1)

    if not effective_leader_port:
        print(
            "Error: Leader port required. Calibrate leader first (so101-calibrate) to save port, "
            "or pass --leader-port / set CYBERWAVE_METADATA_LEADER_PORT"
        )
        sys.exit(1)

    robot = cyberwave_client.twin(
        asset_key="the-robot-studio/so101", twin_id=effective_twin_uuid, name="robot"
    )
    mqtt_client = cyberwave_client.mqtt

    # Initialize leader (required)
    from utils.config import LeaderConfig

    leader_config = LeaderConfig(port=effective_leader_port)
    leader = SO101Leader(config=leader_config)
    leader.connect()

    # Initialize follower (required)
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
        teleoperate(
            leader=leader,
            cyberwave_client=cyberwave_client,
            follower=follower,
            robot=robot,
            cameras=cameras_list if cameras_list else None,
        )
    finally:
        leader.disconnect()
        follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
