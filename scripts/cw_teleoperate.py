"""Teleoperation loop for SO101 leader and follower."""

import argparse
import logging
import math
import os
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

from motors import MotorNormMode
from scripts.cw_setup import load_setup_config
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import get_setup_config_path
from utils.cw_alerts import create_calibration_needed_alert
from utils.cw_remoteoperate_helpers import upload_calibration_to_twin
from utils.cw_teleoperate_helpers import CONTROL_RATE_HZ, teleop_loop
from utils.cw_update_worker import cyberwave_update_worker
from utils.keyboard import keyboard_input_thread
from utils.trackers import StatusTracker, run_status_logging_thread
from utils.utils import parse_resolution_to_enum

logger = logging.getLogger(__name__)


def teleoperate(
    leader: Optional[SO101Leader],
    cyberwave_client: Optional[Cyberwave] = None,
    follower: Optional[SO101Follower] = None,
    robot: Optional[Twin] = None,
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
        leader: SO101Leader instance (optional if camera_only=True)
        cyberwave_client: Cyberwave client instance (required)
        follower: SO101Follower instance (required when robot twin is provided)
        robot: Robot twin instance
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
    robot_uuid = robot.uuid if robot else ""
    robot_name = robot.name if robot and hasattr(robot, "name") else "so101-teleop"

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
    if cyberwave_client is None:
        raise RuntimeError("Cyberwave client is required")

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

    if leader is not None and not leader.connected:
        raise RuntimeError("Leader is not connected")

    # Require follower when robot twin is provided (we send follower data to Cyberwave)
    if robot is not None and follower is None:
        raise RuntimeError(
            "Follower is required when robot twin is provided (follower data is sent to Cyberwave)"
        )

    if follower is not None and not follower.connected:
        raise RuntimeError("Follower is not connected")

    # Verify follower has torque enabled (required for movement)
    if follower is not None and not follower.torque_enabled:
        follower.enable_torque()

    # Get calibration data from leader (leader handles its own calibration loading)
    if leader is not None:
        if leader.calibration is None:
            if robot is not None:
                create_calibration_needed_alert(
                    robot,
                    "leader",
                    description="Please calibrate the leader using the calibration script.",
                )
            raise RuntimeError(
                "Leader is not calibrated. Please calibrate the leader first using the calibration script."
            )
        # Upload leader calibration to twin if robot twin is provided
        if robot is not None:
            upload_calibration_to_twin(leader, robot, "leader")

    # Require follower calibration when sending to Cyberwave (robot twin)
    if follower is not None and robot is not None and follower.calibration is None:
        create_calibration_needed_alert(
            robot,
            "follower",
            description="Please calibrate the follower using the calibration script.",
        )
        raise RuntimeError(
            "Follower is not calibrated. Please calibrate the follower first using the calibration script."
        )

    # Upload follower calibration to twin if available
    if follower is not None and follower.calibration is not None and robot is not None:
        upload_calibration_to_twin(follower, robot, "follower")

    # Use follower motors for mappings when sending to Cyberwave (follower data is what we send)
    # Fall back to leader motors if follower not available (for camera-only mode)
    motors_for_mapping = (
        follower.motors if follower is not None else (leader.motors if leader is not None else {})
    )

    # Build motor_id -> schema joint name mapping from twin's universal schema
    # Schema uses names like "_1", "_2" (SO101); matches backend get_controllable_joints
    motor_id_to_schema_joint: Dict[int, str] = {}
    if robot is not None and motors_for_mapping:
        try:
            schema_joint_names = robot.get_controllable_joint_names()
            for _, motor in motors_for_mapping.items():
                motor_id = motor.id
                idx = motor_id - 1  # 0-based index
                if idx < len(schema_joint_names):
                    motor_id_to_schema_joint[motor_id] = schema_joint_names[idx]
                else:
                    motor_id_to_schema_joint[motor_id] = f"_{motor_id}"  # fallback
        except Exception:
            # Fallback to _n naming if schema fetch fails
            motor_id_to_schema_joint = {
                motor.id: f"_{motor.id}" for _, motor in motors_for_mapping.items()
            }

    if motors_for_mapping:
        # Create mapping from joint names to joint indexes (motor IDs: 1-6)
        joint_name_to_index = {name: motor.id for name, motor in motors_for_mapping.items()}

        # Create mapping from joint indexes to joint names (for status display)
        joint_index_to_name = {str(motor.id): name for name, motor in motors_for_mapping.items()}
        status_tracker.set_joint_index_to_name(joint_index_to_name)

        # Create mapping from joint names to normalization modes
        joint_name_to_norm_mode = {
            name: motor.norm_mode for name, motor in motors_for_mapping.items()
        }

        # Initialize last observation state (track normalized positions)
        # Follower returns normalized positions, worker thread handles conversion to degrees/radians
        last_observation: Dict[str, float] = {}
        for joint_name in motors_for_mapping.keys():
            last_observation[joint_name] = float("inf")  # Use inf to force first update
    else:
        joint_name_to_index = {}
        joint_name_to_norm_mode = {}
        last_observation: Dict[str, float] = {}

    # Create queue and worker thread for Cyberwave updates
    num_joints = len(motors_for_mapping) if motors_for_mapping else 0
    sampling_rate = 100  # Hz
    seconds = 60  # seconds
    queue_size = num_joints * sampling_rate * seconds if num_joints > 0 else 1000
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

    # Get follower calibration for proper conversion to radians
    follower_calibration = None
    if follower is not None and follower.calibration is not None:
        follower_calibration = follower.calibration
        assert follower_calibration.keys() == follower.motors.keys()
        for joint_name, calibration in follower_calibration.items():
            assert joint_name in follower.motors
            assert calibration.range_min is not None
            assert calibration.range_max is not None
            assert calibration.range_min < calibration.range_max

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
                if (
                    "started" in msg_lower
                    or (status == "ok" and ("streaming" in msg_lower or "running" in msg_lower))
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
            # Get follower observation (this is what we send to Cyberwave)
            follower_obs = follower.get_observation()
            observations = {}
            for joint_key, normalized_pos in follower_obs.items():
                # Remove .pos suffix if present
                name = joint_key.removesuffix(".pos")
                if name in follower.motors:
                    joint_index = follower.motors[name].id
                    # Convert normalized position to radians using calibration
                    if follower_calibration and name in follower_calibration:
                        calib = follower_calibration[name]
                        r_min = calib.range_min
                        r_max = calib.range_max
                        delta_r = (r_max - r_min) / 2.0

                        norm_mode = joint_name_to_norm_mode[name]
                        if norm_mode == MotorNormMode.RANGE_M100_100:
                            # Normalized is in [-100, 100], convert to radians
                            raw_offset = (normalized_pos / 100.0) * delta_r
                            radians = raw_offset * (2.0 * math.pi / 4095.0)
                        elif norm_mode == MotorNormMode.RANGE_0_100:
                            # Normalized is in [0, 100], center at 50
                            center_normalized = 50.0
                            offset_normalized = normalized_pos - center_normalized
                            raw_offset = (offset_normalized / 100.0) * delta_r
                            radians = raw_offset * (2.0 * math.pi / 4095.0)
                        else:  # DEGREES
                            radians = normalized_pos * math.pi / 180.0
                    else:
                        raise Exception(f"No calibration found for joint: {name}")
                    # Use schema joint name (e.g. "_1", "_2") for MQTT
                    schema_joint = motor_id_to_schema_joint.get(
                        joint_index, f"_{joint_index}"
                    )
                    observations[schema_joint] = radians
            # Send follower observations to Cyberwave as single update
            # together with the desired actual frequency
            mqtt_client.publish_initial_observation(
                twin_uuid=robot.uuid,
                observations=observations,
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
                last_observation=last_observation,
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
    # Load environment variables from .env file
    load_dotenv()

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
    camera_only = setup_config.get("camera_only", False)
    max_relative_target = setup_config.get("max_relative_target")

    # Validate: camera-only requires setup with cameras
    if camera_only:
        has_setup_cameras = setup_config.get("wrist_camera") or len(
            setup_config.get("additional_cameras", [])
        ) > 0
        if not has_setup_cameras:
            print(
                "Error: camera_only in setup requires wrist_camera or additional_cameras. "
                "Run so101-setup with --wrist-camera or --additional-camera."
            )
            sys.exit(1)

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
            cameras_list.append({
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
            })

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
                depth_res_enum = Resolution.from_size(depth_res[0], depth_res[1]) or Resolution.closest(depth_res[0], depth_res[1])
            cameras_list.append({
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
            })

    # Resolve twin UUID and ports: CLI/env > setup.json (so teleoperate works with no args when setup exists)
    effective_twin_uuid = args.twin_uuid or setup_config.get("twin_uuid") or setup_config.get(
        "wrist_camera_twin_uuid"
    )
    effective_leader_port = args.leader_port or setup_config.get("leader_port")
    effective_follower_port = args.follower_port or setup_config.get("follower_port")
    if not effective_twin_uuid:
        print(
            "Error: Twin UUID required. Use --twin-uuid, set CYBERWAVE_TWIN_UUID, "
            "or run so101-setup with --twin-uuid"
        )
        sys.exit(1)

    # Follower required for teleop (sends data to Cyberwave) or camera-only (streaming)
    if not effective_follower_port and (not camera_only or cameras_list):
        print(
            "Error: Follower port required. Calibrate follower first (so101-calibrate) to save port, "
            "or pass --follower-port / set CYBERWAVE_METADATA_FOLLOWER_PORT"
        )
        sys.exit(1)

    robot = cyberwave_client.twin(
        asset_key="the-robot-studio/so101", twin_id=effective_twin_uuid, name="robot"
    )
    mqtt_client = cyberwave_client.mqtt

    # Initialize leader (optional if camera-only mode)
    leader = None
    if not camera_only:
        from utils.config import LeaderConfig
        from utils.utils import find_port

        leader_port = effective_leader_port
        if not leader_port:
            leader_port = find_port(device_name="SO101 Leader")

        leader_config = LeaderConfig(port=leader_port)
        leader = SO101Leader(config=leader_config)
        leader.connect()

    # Initialize follower (required for camera-only mode, optional otherwise)
    follower = None
    if effective_follower_port or camera_only:
        if not effective_follower_port:
            raise RuntimeError(
                "--follower-port is required when using --camera-only. "
                "Calibrate the follower first (so101-calibrate) or pass --follower-port."
            )
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
        if leader is not None:
            leader.disconnect()
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
