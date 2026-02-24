"""Remote operation loop for SO101 follower via Cyberwave MQTT."""

import logging
import queue
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from cyberwave import Cyberwave, Twin
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
from utils.config import get_setup_config_path
from utils.cw_alerts import create_calibration_needed_alert
from utils.cw_remoteoperate_helpers import (
    create_joint_state_callback,
    get_remoteoperate_parser,
    joint_position_heartbeat_thread,
    motor_writer_worker,
    upload_calibration_to_twin,
)
from utils.cw_utils import build_joint_mappings, resolve_calibration_for_edge
from utils.keyboard import keyboard_input_thread
from utils.trackers import StatusTracker, run_status_logging_thread
from utils.utils import normalized_to_radians, parse_resolution_to_enum

logger = logging.getLogger(__name__)


CONTROL_RATE_HZ = 100


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    cameras: Optional[List[Dict[str, Any]]] = None,
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
        first_camera_twin.name if first_camera_twin and hasattr(first_camera_twin, "name") else ""
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

    # Alert when follower is not calibrated (can still run with fallback conversion)
    if follower.calibration is None:
        create_calibration_needed_alert(
            robot,
            "follower",
            description="Please calibrate the follower using the calibration script for accurate positioning.",
        )

    # Upload follower calibration to twin if available
    if follower.calibration is not None:
        upload_calibration_to_twin(follower, robot, "follower")

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
    mqtt_client = client.mqtt
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

    # Send initial observation to Cyberwave using publish_initial_observation
    try:
        # Get follower's current observation (normalized positions)
        follower_obs = follower.get_observation()

        # Convert to joint index format for initial observation using calibration
        observations = {}
        for joint_key, normalized_pos in follower_obs.items():
            name = joint_key.removesuffix(".pos")
            if name in follower.motors:
                joint_index = follower.motors[name].id
                norm_mode = joint_name_to_norm_mode[name]

                calib = follower_calibration.get(name) if follower_calibration else None
                radians = normalized_to_radians(normalized_pos, norm_mode, calib)
                observations[joint_index] = radians

        # Send initial observation
        mqtt_client.publish_initial_observation(
            twin_uuid=twin_uuid,
            observations=observations,
            fps=CONTROL_RATE_HZ,
        )

    except Exception:
        if status_tracker:
            status_tracker.increment_errors()

    # Create queue for actions
    queue_size = 1000  # Reasonable queue size
    action_queue = queue.Queue(maxsize=queue_size)
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
            "follower": follower,
            "robot": robot,
            "mode": "remoteoperate",
        },
        daemon=True,
    )
    status_thread.start()

    # Start joint position heartbeat thread - publish follower position every second to avoid mismatches
    heartbeat_thread = threading.Thread(
        target=joint_position_heartbeat_thread,
        args=(
            follower,
            mqtt_client,
            twin_uuid,
            joint_name_to_norm_mode,
            follower_calibration,
            motor_id_to_schema_joint,
            stop_event,
        ),
        kwargs={"interval": 1.0},
        daemon=True,
    )
    heartbeat_thread.start()

    # Start camera streaming via SDK CameraStreamManager (one stream per twin, each with own thread)
    camera_manager: Optional[CameraStreamManager] = None
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
            client=client,
            twins=enriched_twins,
            stop_event=stop_event,
            time_reference=time_reference,
            command_callback=command_callback,
        )
        camera_manager.start()
    else:
        status_tracker.camera_states.clear()

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
    )

    # Subscribe to joint states
    mqtt_client.subscribe_joint_states(twin_uuid, joint_state_callback)

    # Start motor writer worker thread
    writer_thread = threading.Thread(
        target=motor_writer_worker,
        args=(action_queue, follower, stop_event, status_tracker),
        daemon=True,
    )
    writer_thread.start()

    try:
        # Main loop: just wait for stop event
        while not stop_event.is_set():
            time.sleep(0.1)
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        # Signal all threads to stop
        stop_event.set()

        # Wait for queue to drain (with timeout)
        try:
            action_queue.join(timeout=2.0)
        except Exception:
            pass

        # Wait for writer thread to finish
        writer_thread.join(timeout=1.0)

        # Wait for heartbeat thread
        heartbeat_thread.join(timeout=1.0)

        # Stop camera streaming
        if camera_manager is not None:
            camera_manager.join(timeout=5.0)

        # Stop status thread
        if status_thread is not None:
            status_thread.join(timeout=1.0)


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
    cyberwave_client = Cyberwave()

    cameras_list: List[Dict[str, Any]] = []
    setup_config: Dict[str, Any] = {}

    # Load setup.json by default when it exists (used for cameras, twin_uuid, ports)
    setup_path = Path(args.setup_path) if args.setup_path else get_setup_config_path()
    if setup_path.exists():
        setup_config = load_setup_config(setup_path)
        if setup_config:
            print(f"Loaded setup from: {setup_path}")

    # All camera/remote config comes from setup.json (so101-setup)
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

    # Resolve twin UUID and ports: CLI/env > setup.json (so remoteoperate works with no args when setup exists)
    effective_twin_uuid = (
        args.twin_uuid
        or setup_config.get("twin_uuid")
        or setup_config.get("wrist_camera_twin_uuid")
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
        max_relative_target=max_relative_target,
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
        )
    finally:
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
