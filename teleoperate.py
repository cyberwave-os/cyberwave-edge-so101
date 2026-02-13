"""Teleoperation loop for SO101 leader and follower."""

import argparse
import asyncio
import json
import logging
import math
import os
import queue
import select
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional, Union

from cyberwave import Cyberwave, Twin
from cyberwave.twin import DepthCameraTwin, CameraTwin

# Import camera configuration and streamers from cyberwave SDK
from cyberwave.sensor import (
    CV2CameraStreamer,
    Resolution,
)
from cyberwave.utils import TimeReference
from dotenv import load_dotenv

# RealSense is optional - only import if available
try:
    from cyberwave.sensor import (
        RealSenseConfig,
        RealSenseDiscovery,
        RealSenseStreamer,
    )

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseStreamer = None
    RealSenseConfig = None
    RealSenseDiscovery = None

from follower import SO101Follower
from leader import SO101Leader
from motors import MotorNormMode

logger = logging.getLogger(__name__)

# Default camera config file path
DEFAULT_CAMERA_CONFIG_PATH = "camera_config.json"


# Default camera config directory (same structure as calibration)
def get_default_camera_config_path() -> str:
    """Get default camera config path in ~/.cyberwave/so101_lib/camera/camera_config.json"""
    from pathlib import Path

    config_dir = Path.home() / ".cyberwave" / "so101_lib" / "camera"
    return str(config_dir / "camera_config.json")


@dataclass
class TeleoperateCameraConfig:
    """Camera configuration for teleoperation.

    This configuration can be saved to and loaded from a JSON file,
    making it easy to share camera settings across different setups.

    Attributes:
        camera_type: Camera type - "cv2" for USB/webcam/IP, "realsense" for Intel RealSense
        camera_id: Camera device ID (int) or stream URL (str) for CV2 cameras
        fps: Frames per second for camera streaming
        resolution: Video resolution as [width, height] list
        enable_depth: Enable depth streaming for RealSense cameras
        depth_fps: Depth stream FPS for RealSense cameras
        depth_resolution: Depth resolution as [width, height] list (optional)
        depth_publish_interval: Publish depth every N frames for RealSense
        serial_number: RealSense device serial number (optional)

    Example JSON file (camera_config.json):
        {
            "camera_type": "cv2",
            "camera_id": 0,
            "fps": 30,
            "resolution": [640, 480]
        }

    Example for RealSense:
        {
            "camera_type": "realsense",
            "fps": 30,
            "resolution": [1280, 720],
            "enable_depth": true,
            "depth_fps": 15,
            "depth_resolution": [640, 480],
            "depth_publish_interval": 30
        }

    Example for IP camera:
        {
            "camera_type": "cv2",
            "camera_id": "rtsp://192.168.1.100:554/stream",
            "fps": 15,
            "resolution": [640, 480]
        }
    """

    camera_type: str = "cv2"
    camera_id: Union[int, str] = 0
    fps: int = 30
    resolution: list = field(default_factory=lambda: [640, 480])
    enable_depth: bool = False
    depth_fps: int = 30
    depth_resolution: Optional[list] = None
    depth_publish_interval: int = 30
    serial_number: Optional[str] = None

    def get_resolution(self) -> Resolution:
        """Get resolution as Resolution enum."""
        width, height = self.resolution
        match = Resolution.from_size(width, height)
        if match:
            return match
        return Resolution.closest(width, height)

    def get_depth_resolution(self) -> Optional[Resolution]:
        """Get depth resolution as Resolution enum."""
        if self.depth_resolution is None:
            return None
        width, height = self.depth_resolution
        match = Resolution.from_size(width, height)
        if match:
            return match
        return Resolution.closest(width, height)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return asdict(self)

    def save(self, path: str = DEFAULT_CAMERA_CONFIG_PATH) -> None:
        """Save configuration to JSON file.

        Args:
            path: Path to save the configuration file
        """
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Camera configuration saved to {path}")

    @classmethod
    def load(cls, path: str = DEFAULT_CAMERA_CONFIG_PATH) -> "TeleoperateCameraConfig":
        """Load configuration from JSON file.

        Args:
            path: Path to the configuration file

        Returns:
            TeleoperateCameraConfig instance

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            json.JSONDecodeError: If the file is not valid JSON
        """
        with open(path, "r") as f:
            data = json.load(f)

        # Handle resolution conversion
        if "resolution" in data and isinstance(data["resolution"], str):
            # Parse "WIDTHxHEIGHT" format
            width, height = map(int, data["resolution"].lower().split("x"))
            data["resolution"] = [width, height]

        if "depth_resolution" in data and isinstance(data["depth_resolution"], str):
            width, height = map(int, data["depth_resolution"].lower().split("x"))
            data["depth_resolution"] = [width, height]

        # Normalize enable_depth: handle string "true"/"false" from JSON
        if "enable_depth" in data and isinstance(data["enable_depth"], str):
            data["enable_depth"] = data["enable_depth"].lower() in ("true", "1", "yes")

        logger.info(f"Camera configuration loaded from {path}")
        return cls(**data)

    @classmethod
    def from_realsense_device(
        cls,
        prefer_resolution: Resolution = Resolution.VGA,
        prefer_fps: int = 30,
        enable_depth: bool = True,
        serial_number: Optional[str] = None,
    ) -> "TeleoperateCameraConfig":
        """Create configuration from connected RealSense device.

        Auto-detects device capabilities and creates optimal configuration.

        Args:
            prefer_resolution: Preferred resolution (will find closest match)
            prefer_fps: Preferred FPS (will find closest match)
            enable_depth: Whether to enable depth streaming
            serial_number: Target device serial number (None = first device)

        Returns:
            TeleoperateCameraConfig configured for the device
        """
        if not _has_realsense:
            raise ImportError(
                "RealSense support requires pyrealsense2. Install with: pip install pyrealsense2"
            )

        # Use SDK's RealSenseConfig.from_device() for auto-detection
        rs_config = RealSenseConfig.from_device(
            serial_number=serial_number,
            prefer_resolution=prefer_resolution,
            prefer_fps=prefer_fps,
            enable_depth=enable_depth,
        )

        return cls(
            camera_type="realsense",
            fps=rs_config.color_fps,
            resolution=[rs_config.color_width, rs_config.color_height],
            enable_depth=rs_config.enable_depth,
            depth_fps=rs_config.depth_fps,
            depth_resolution=[rs_config.depth_width, rs_config.depth_height],
            depth_publish_interval=rs_config.depth_publish_interval,
            serial_number=rs_config.serial_number,
        )

    @classmethod
    def create_default_cv2(
        cls,
        camera_id: Union[int, str] = 0,
        fps: int = 30,
        resolution: Resolution = Resolution.VGA,
    ) -> "TeleoperateCameraConfig":
        """Create default CV2 camera configuration.

        Args:
            camera_id: Camera device ID or stream URL
            fps: Frames per second
            resolution: Video resolution

        Returns:
            TeleoperateCameraConfig for CV2 camera
        """
        return cls(
            camera_type="cv2",
            camera_id=camera_id,
            fps=fps,
            resolution=[resolution.width, resolution.height],
        )

    def __str__(self) -> str:
        res_str = f"{self.resolution[0]}x{self.resolution[1]}"
        if self.camera_type == "realsense" and self.enable_depth:
            depth_res = self.depth_resolution or self.resolution
            return (
                f"TeleoperateCameraConfig(type={self.camera_type}, "
                f"color={res_str}@{self.fps}fps, "
                f"depth={depth_res[0]}x{depth_res[1]}@{self.depth_fps}fps)"
            )
        return f"TeleoperateCameraConfig(type={self.camera_type}, id={self.camera_id}, {res_str}@{self.fps}fps)"


def _detect_camera_type_from_asset(asset_key: str) -> str:
    """
    Detect camera type from asset key.

    Args:
        asset_key: Asset key (e.g., "intel/realsensed455", "cyberwave/standard-cam")

    Returns:
        Camera type: "realsense" or "cv2"
    """
    asset_lower = asset_key.lower()
    if "realsense" in asset_lower or "intel" in asset_lower:
        return "realsense"
    return "cv2"


def _get_default_camera_config(camera_type: str) -> TeleoperateCameraConfig:
    """
    Get default camera configuration based on camera type.

    Args:
        camera_type: Camera type ("cv2" or "realsense")

    Returns:
        TeleoperateCameraConfig with default settings
    """
    if camera_type == "realsense":
        return TeleoperateCameraConfig(
            camera_type="realsense",
            fps=30,
            resolution=[640, 480],
            enable_depth=True,
            depth_fps=15,
            depth_resolution=[640, 480],
            depth_publish_interval=30,
        )
    else:
        return TeleoperateCameraConfig.create_default_cv2()


def generate_camera_config(
    output_path: str = DEFAULT_CAMERA_CONFIG_PATH,
    camera_type: str = "cv2",
    auto_detect: bool = False,
) -> TeleoperateCameraConfig:
    """Generate a camera configuration file.

    Args:
        output_path: Path to save the configuration file
        camera_type: Camera type ("cv2" or "realsense")
        auto_detect: Auto-detect RealSense device capabilities

    Returns:
        Generated TeleoperateCameraConfig
    """
    if camera_type == "realsense" and auto_detect:
        config = TeleoperateCameraConfig.from_realsense_device()
    elif camera_type == "realsense":
        config = TeleoperateCameraConfig(
            camera_type="realsense",
            fps=30,
            resolution=[640, 480],
            enable_depth=True,
            depth_fps=30,
            depth_resolution=[640, 480],
            depth_publish_interval=30,
        )
    else:
        config = TeleoperateCameraConfig.create_default_cv2()

    config.save(output_path)
    return config


class StatusTracker:
    """Thread-safe status tracker for teleoperation system."""

    def __init__(self):
        self.lock = threading.Lock()
        self.script_started = False
        self.mqtt_connected = False
        self.camera_enabled = False  # Whether camera streaming was configured at all
        self.camera_detected = False
        self.camera_started = False
        # WebRTC states: "idle" (red), "connecting" (yellow), "streaming" (green)
        self.webrtc_state = "idle"
        self.fps = 0
        self.camera_fps = 0
        self.messages_produced = 0
        self.messages_filtered = 0
        self.errors = 0
        self.joint_states: Dict[str, float] = {}
        self.joint_temperatures: Dict[
            str, float
        ] = {}  # Format: "leader_1" or "follower_1" -> temperature
        self.joint_index_to_name: Dict[str, str] = {}
        self.robot_uuid: str = ""
        self.robot_name: str = ""
        self.camera_uuid: str = ""
        self.camera_name: str = ""

    def update_mqtt_status(self, connected: bool):
        with self.lock:
            self.mqtt_connected = connected

    def update_camera_status(self, detected: bool, started: bool = False):
        with self.lock:
            self.camera_detected = detected
            self.camera_started = started

    def update_webrtc_state(self, state: str):
        """Update WebRTC state: 'idle', 'connecting', or 'streaming'."""
        with self.lock:
            self.webrtc_state = state

    def increment_produced(self):
        with self.lock:
            self.messages_produced += 1

    def increment_filtered(self):
        with self.lock:
            self.messages_filtered += 1

    def increment_errors(self):
        with self.lock:
            self.errors += 1

    def update_joint_states(self, states: Dict[str, float]):
        """Merge new joint states with existing ones (doesn't replace)."""
        with self.lock:
            self.joint_states.update(states)

    def update_joint_temperatures(self, temperatures: Dict[str, float]):
        """Merge new joint temperatures with existing ones (doesn't replace)."""
        with self.lock:
            self.joint_temperatures.update(temperatures)

    def set_joint_index_to_name(self, mapping: Dict[str, str]):
        """Set mapping from joint index to joint name."""
        with self.lock:
            self.joint_index_to_name = mapping.copy()

    def set_twin_info(self, robot_uuid: str, robot_name: str, camera_uuid: str, camera_name: str):
        """Set twin information for display."""
        with self.lock:
            self.robot_uuid = robot_uuid
            self.robot_name = robot_name
            self.camera_uuid = camera_uuid
            self.camera_name = camera_name

    def get_status(self) -> Dict:
        """Get a snapshot of current status."""
        with self.lock:
            return {
                "script_started": self.script_started,
                "mqtt_connected": self.mqtt_connected,
                "camera_enabled": self.camera_enabled,
                "camera_detected": self.camera_detected,
                "camera_started": self.camera_started,
                "webrtc_state": self.webrtc_state,
                "fps": self.fps,
                "camera_fps": self.camera_fps,
                "messages_produced": self.messages_produced,
                "messages_filtered": self.messages_filtered,
                "errors": self.errors,
                "joint_states": self.joint_states.copy(),
                "joint_temperatures": self.joint_temperatures.copy(),
                "robot_uuid": self.robot_uuid,
                "robot_name": self.robot_name,
                "camera_uuid": self.camera_uuid,
                "camera_name": self.camera_name,
            }


def _camera_worker_thread(
    client: Cyberwave,
    camera_id: Union[int, str],
    fps: int,
    twin_uuid: str,
    stop_event: threading.Event,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    camera_type: str = "cv2",
    resolution: Resolution = Resolution.VGA,
    enable_depth: bool = False,
    depth_fps: int = 30,
    depth_resolution: Optional[Resolution] = None,
    depth_publish_interval: int = 30,
) -> None:
    """
    Worker thread that handles camera streaming.

    Runs an async event loop in a separate thread to handle camera streaming.
    Supports both CV2 (USB/webcam/IP) cameras and Intel RealSense cameras.
    Uses CameraStreamer.run_with_auto_reconnect() for automatic command handling
    and reconnection.

    Args:
        client: Cyberwave client instance
        camera_id: Camera index (int) or URL (str) to stream
        fps: Frames per second for camera stream
        twin_uuid: UUID of the twin to stream to
        stop_event: Event to signal thread to stop
        time_reference: TimeReference instance
        status_tracker: Optional status tracker for camera status updates
        camera_type: Camera type - "cv2" for USB/webcam/IP, "realsense" for Intel RealSense
        resolution: Video resolution (default: VGA 640x480)
        enable_depth: Enable depth streaming for RealSense (default: False)
        depth_fps: Depth stream FPS for RealSense (default: 30)
        depth_resolution: Depth resolution for RealSense (default: same as color)
        depth_publish_interval: Publish depth every N frames for RealSense (default: 30)
    Returns:
        None
    """
    if status_tracker:
        status_tracker.update_camera_status(detected=True, started=False)

    async def _run_camera_streamer():
        """Async function that runs the camera streamer with auto-reconnect."""
        # Create async stop event from threading.Event
        async_stop_event = asyncio.Event()

        # Ensure MQTT is connected and wait for connection (required for WebRTC offer/answer)
        if not client.mqtt.connected:
            client.mqtt.connect()
        max_wait = 10.0
        wait_start = time.time()
        while not client.mqtt.connected:
            if time.time() - wait_start > max_wait:
                raise RuntimeError("Failed to connect to MQTT broker - cannot send WebRTC offer")
            await asyncio.sleep(0.1)

        # Create camera streamer based on camera type
        streamer = None
        camera_type_lower = camera_type.lower()

        if camera_type_lower == "cv2":
            # Create CV2 camera streamer
            streamer = CV2CameraStreamer(
                client=client.mqtt,
                camera_id=camera_id,
                fps=fps,
                resolution=resolution,
                twin_uuid=twin_uuid,
                time_reference=time_reference,
                auto_reconnect=True,
            )
        elif camera_type_lower == "realsense":
            if not _has_realsense:
                raise ImportError(
                    "RealSense camera support requires pyrealsense2. "
                    "Install with: pip install pyrealsense2"
                )

            # Use depth_resolution if provided, otherwise use color resolution
            actual_depth_resolution = depth_resolution if depth_resolution else resolution

            # Create RealSense streamer
            streamer = RealSenseStreamer(
                client=client.mqtt,
                color_fps=fps,
                depth_fps=depth_fps,
                color_resolution=resolution,
                depth_resolution=actual_depth_resolution,
                enable_depth=enable_depth,
                depth_publish_interval=depth_publish_interval,
                twin_uuid=twin_uuid,
                time_reference=time_reference,
                auto_reconnect=True,
            )
        else:
            raise ValueError(f"Unsupported camera type: {camera_type}. Use 'cv2' or 'realsense'.")

        # Monitor the threading stop_event and set async_stop_event
        async def monitor_stop():
            while not stop_event.is_set():
                await asyncio.sleep(0.1)
            async_stop_event.set()

        # Start the monitor task
        monitor_task = asyncio.create_task(monitor_stop())

        def command_callback(status: str, msg: str):
            """Callback to track WebRTC state from commands."""
            if status_tracker:
                if "started" in msg.lower() or status == "ok":
                    status_tracker.update_webrtc_state("streaming")
                elif "stopped" in msg.lower():
                    status_tracker.update_webrtc_state("idle")

        try:
            # Update status when camera starts
            if status_tracker:
                status_tracker.update_camera_status(detected=True, started=True)
                # WebRTC starts in idle state, waiting for start_video command
                status_tracker.update_webrtc_state("idle")

            # Run with auto-reconnect - handles command subscriptions and connection monitoring
            # Waits for start_video command before starting streaming
            await streamer.run_with_auto_reconnect(
                stop_event=async_stop_event,
                command_callback=command_callback,
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
        if status_tracker:
            status_tracker.increment_errors()
        # Surface camera/WebRTC errors so user can diagnose (logging is disabled)
        print(f"Camera streaming error: {e}", file=sys.stderr)
    finally:
        if loop is not None:
            try:
                loop.close()
            except Exception:
                pass


def cyberwave_update_worker(
    action_queue: queue.Queue,
    joint_name_to_index: Dict[str, int],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    stop_event: threading.Event,
    twin: Optional[Twin] = None,
    time_reference: TimeReference = None,
    status_tracker: Optional[StatusTracker] = None,
    follower_calibration: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Worker thread that processes actions from the queue and updates Cyberwave twin.

    Batches multiple joint updates together and skips updates where position is 0.0.
    Velocity and effort are hardcoded to 0.0 to avoid issues.
    Always converts positions to radians.

    Args:
        action_queue: Queue containing (joint_name, action_data) tuples where action_data
                     is a dict with 'position', 'velocity', 'load', and 'timestamp' keys
        joint_name_to_index: Dictionary mapping joint names to joint indexes (1-6)
        joint_name_to_norm_mode: Dictionary mapping joint names to normalization modes
        stop_event: Event to signal thread to stop
        twin: Optional Twin instance for updating joint states
        time_reference: TimeReference instance
        status_tracker: Optional status tracker for statistics
    Returns:
        None
    """
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
                        action_queue.task_done()
                        continue

                    # Extract normalized position from action_data (velocity and effort are hardcoded to 0.0)
                    normalized_position = action_data.get("position", 0.0)
                    timestamp = action_data.get("timestamp")  # Extract timestamp from action_data

                    # Convert normalized position to radians using follower calibration
                    calib = follower_calibration[joint_name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    # Get normalization mode for this joint
                    norm_mode = joint_name_to_norm_mode.get(
                        joint_name, MotorNormMode.RANGE_M100_100
                    )

                    # Convert normalized -> radians using calibration ranges
                    # Normalized value represents percentage of calibrated range
                    match norm_mode:
                        case MotorNormMode.RANGE_M100_100:
                            raw_offset = (normalized_position / 100.0) * delta_r
                            position = raw_offset * (2.0 * math.pi / 4095.0)
                        case MotorNormMode.RANGE_0_100:
                            delta_r = r_max - r_min
                            raw_value = r_min + (normalized_position / 100.0) * delta_r
                            position = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                        case _:
                            position = normalized_position * math.pi / 180.0

                    # Hardcode velocity and effort to 0.0 to avoid issues
                    velocity = 0.0
                    effort = 0.0

                    # Store in batch (overwrite if same joint appears multiple times)
                    batch_updates[joint_index] = (position, velocity, effort, timestamp)
                    action_queue.task_done()

                except Exception:
                    error_count += 1
                    if status_tracker:
                        status_tracker.increment_errors()
                    action_queue.task_done()

            # Send batched updates
            if batch_updates:
                try:
                    # Send all joints in the batch
                    joint_states = {}
                    for joint_index, (position, _, _, timestamp) in batch_updates.items():
                        twin.joints.set(
                            joint_name=str(joint_index),
                            position=position,
                            degrees=False,
                            timestamp=timestamp,
                        )
                        joint_states[str(joint_index)] = position

                    processed_count += len(batch_updates)
                    if status_tracker:
                        status_tracker.increment_produced()
                        status_tracker.update_joint_states(joint_states)
                except Exception:
                    error_count += len(batch_updates)
                    if status_tracker:
                        status_tracker.increment_errors()

        except Exception:
            error_count += 1
            if status_tracker:
                status_tracker.increment_errors()


def _process_cyberwave_updates(
    action: Dict[str, float],
    last_observation: Dict[str, float],
    action_queue: queue.Queue,
    position_threshold: float,
    velocity_threshold: float,
    effort_threshold: float,
    timestamp: float,
    status_tracker: Optional[StatusTracker] = None,
    last_send_times: Optional[Dict[str, float]] = None,
    heartbeat_interval: float = 1.0,
) -> tuple[int, int]:
    """
    Process follower observation and queue Cyberwave updates for changed joints.

    Follower observation contains normalized positions. If a joint hasn't been sent
    for heartbeat_interval seconds, it will be sent anyway as a heartbeat.

    Args:
        action: Follower observation dictionary with normalized positions (keys have .pos suffix)
        last_observation: Dictionary tracking last sent observation state (normalized positions)
        action_queue: Queue for Cyberwave updates
        position_threshold: Minimum change in normalized position to trigger update
        velocity_threshold: Unused (kept for compatibility)
        effort_threshold: Unused (kept for compatibility)
        timestamp: Timestamp to associate with this update (generated in teleop loop)
        status_tracker: Optional status tracker for statistics
        last_send_times: Dictionary tracking last send time per joint (for heartbeat)
        heartbeat_interval: Interval in seconds to send heartbeat if no changes (default 1.0)
    Returns:
        Tuple of (update_count, skip_count)
    """
    update_count = 0
    skip_count = 0
    current_time = time.time()

    for joint_key, normalized_pos in action.items():
        # Extract joint name from key (remove .pos suffix if present)
        joint_name = joint_key.removesuffix(".pos") if joint_key.endswith(".pos") else joint_key

        if joint_name not in last_observation:
            # New joint, initialize and send
            last_observation[joint_name] = float("inf")

        if last_send_times is not None and joint_name not in last_send_times:
            last_send_times[joint_name] = 0.0

        last_obs = last_observation[joint_name]

        # Check if position has changed beyond threshold (using normalized values)
        pos_changed = abs(normalized_pos - last_obs) >= position_threshold

        # Force first update (when last_obs is inf)
        is_first_update = last_obs == float("inf")

        # Check if heartbeat is needed (no update sent for heartbeat_interval)
        needs_heartbeat = False
        if last_send_times is not None:
            time_since_last_send = current_time - last_send_times.get(joint_name, 0.0)
            needs_heartbeat = time_since_last_send >= heartbeat_interval

        if is_first_update or pos_changed or needs_heartbeat:
            # Update last observation (store normalized position)
            last_observation[joint_name] = normalized_pos

            # Update last send time
            if last_send_times is not None:
                last_send_times[joint_name] = current_time

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
                if status_tracker:
                    status_tracker.increment_errors()
                continue
        else:
            skip_count += 1
            if status_tracker:
                status_tracker.increment_filtered()

    return update_count, skip_count


def _read_temperatures(
    leader: Optional[SO101Leader],
    follower: Optional[SO101Follower],
    joint_index_to_name: Dict[str, str],
) -> Dict[str, float]:
    """
    Read temperatures from leader and follower motors.

    Args:
        leader: SO101Leader instance (optional)
        follower: SO101Follower instance (optional)
        joint_index_to_name: Mapping from joint index to joint name

    Returns:
        Dictionary mapping "leader_{motor_id}" or "follower_{motor_id}" to temperature in Celsius
    """
    temperatures = {}

    try:
        from motors.tables import ADDR_PRESENT_TEMPERATURE

        addr = ADDR_PRESENT_TEMPERATURE[0]  # Temperature is 1 byte

        # Read temperatures from leader
        if leader is not None and leader.connected:
            motor_ids = [motor.id for motor in leader.motors.values()]
            for motor_id in motor_ids:
                try:
                    temperature, result, error = leader.bus._packet_handler.read1ByteTxRx(
                        leader.bus._port_handler, motor_id, addr
                    )
                    if result == 0:  # COMM_SUCCESS
                        temperatures[f"leader_{motor_id}"] = float(temperature)
                except Exception:
                    pass

        # Read temperatures from follower
        if follower is not None and follower.connected:
            motor_ids = [motor.id for motor in follower.motors.values()]
            for motor_id in motor_ids:
                try:
                    temperature, result, error = follower.bus._packet_handler.read1ByteTxRx(
                        follower.bus._port_handler, motor_id, addr
                    )
                    if result == 0:  # COMM_SUCCESS
                        temperatures[f"follower_{motor_id}"] = float(temperature)
                except Exception:
                    pass
    except Exception:
        pass

    return temperatures


def _status_logging_thread(
    status_tracker: StatusTracker,
    stop_event: threading.Event,
    fps: int,
    camera_fps: int,
    leader: Optional[SO101Leader] = None,
    follower: Optional[SO101Follower] = None,
) -> None:
    """
    Thread that logs status information at 1 fps.

    Args:
        status_tracker: StatusTracker instance
        stop_event: Event to signal thread to stop
        fps: Target frames per second for teleoperation loop
        camera_fps: Frames per second for camera streaming
        follower: Optional SO101Follower instance for reading temperatures
    """
    status_tracker.fps = fps
    status_tracker.camera_fps = camera_fps
    status_interval = 1.0  # Update status at 1 fps

    # Hide cursor and save position
    sys.stdout.write("\033[?25l")  # Hide cursor
    sys.stdout.flush()

    try:
        while not stop_event.is_set():
            # Read temperatures from both leader and follower if available
            joint_index_to_name = status_tracker.joint_index_to_name
            temperatures = _read_temperatures(leader, follower, joint_index_to_name)
            if temperatures:
                status_tracker.update_joint_temperatures(temperatures)

            status = status_tracker.get_status()

            # Build status display with fixed width lines
            lines = []
            lines.append("=" * 70)
            lines.append("SO101 Teleoperation Status".center(70))
            lines.append("=" * 70)

            # Twin info
            robot_name = status["robot_name"] or "N/A"
            lines.append(f"Robot:  {robot_name} ({status['robot_uuid']})"[:70].ljust(70))
            if status["camera_enabled"]:
                camera_name = status["camera_name"] or "N/A"
                lines.append(f"Camera: {camera_name} ({status['camera_uuid']})"[:70].ljust(70))
            else:
                lines.append("Camera: not configured".ljust(70))
            lines.append("-" * 70)

            # Status indicators
            script_icon = "🟢" if status["script_started"] else "🟡"
            mqtt_icon = "🟢" if status["mqtt_connected"] else "🔴"
            if not status["camera_enabled"]:
                camera_icon = "⚫"  # Not configured
                webrtc_icon = "⚫"
            else:
                if not status["camera_detected"]:
                    camera_icon = "🔴"
                elif not status["camera_started"]:
                    camera_icon = "🟡"
                else:
                    camera_icon = "🟢"
                # WebRTC: idle=red, connecting=yellow, streaming=green
                webrtc_state = status["webrtc_state"]
                if webrtc_state == "streaming":
                    webrtc_icon = "🟢"
                elif webrtc_state == "connecting":
                    webrtc_icon = "🟡"
                else:
                    webrtc_icon = "🔴"

            lines.append(
                f"Script:{script_icon} MQTT:{mqtt_icon} Camera:{camera_icon} WebRTC:{webrtc_icon}".ljust(
                    70
                )
            )
            lines.append("-" * 70)

            # Statistics
            stats = f"FPS:{status['fps']} Cam:{status['camera_fps']} Prod:{status['messages_produced']} Filt:{status['messages_filtered']} Err:{status['errors']}"
            lines.append(stats.ljust(70))
            lines.append("-" * 70)

            # Joint states - show leader and follower temperatures
            if status["joint_states"]:
                index_to_name = status_tracker.joint_index_to_name
                lines.append("Motors:".ljust(70))
                for joint_index in sorted(status["joint_states"].keys()):
                    position = status["joint_states"][joint_index]
                    joint_name = index_to_name.get(joint_index, joint_index)

                    # Get temperatures for this motor from both leader and follower
                    leader_temp = status["joint_temperatures"].get(f"leader_{joint_index}", None)
                    follower_temp = status["joint_temperatures"].get(
                        f"follower_{joint_index}", None
                    )

                    # Build temperature display with indicators
                    temp_parts = []
                    if leader_temp is not None:
                        leader_indicator = "🔥" if leader_temp > 40 else ""
                        temp_parts.append(f"L:{leader_temp:3.0f}°C{leader_indicator}")
                    if follower_temp is not None:
                        follower_indicator = "🔥" if follower_temp > 40 else ""
                        temp_parts.append(f"F:{follower_temp:3.0f}°C{follower_indicator}")

                    temp_str = " ".join(temp_parts) if temp_parts else "N/A"

                    # Format: "  shoulder_pan:  pos=  0.79rad  L:32°C F:34°C🔥"
                    line = f"  {joint_name:16s}  pos={position:6.3f}rad  {temp_str}"
                    lines.append(line[:70].ljust(70))
            else:
                lines.append("Motors: (waiting)".ljust(70))

            lines.append("=" * 70)
            lines.append("Press 'q' to stop".ljust(70))

            # Clear screen and move to top, then write all lines
            # Use \r\n for proper line breaks in raw terminal mode
            output = "\033[2J\033[H"  # Clear screen and move to home
            output += "\r\n".join(lines)

            try:
                sys.stdout.write(output)
                sys.stdout.flush()
            except (IOError, OSError):
                pass

            # Wait for next update
            time.sleep(status_interval)
    finally:
        # Show cursor again when done
        sys.stdout.write("\033[?25h")  # Show cursor
        sys.stdout.flush()


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
                            stop_event.set()
                            break
            finally:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old_settings)
        except (ImportError, OSError):
            # If termios is not available (e.g., Windows), keyboard input disabled
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
    publish_interval: float,
    time_reference: TimeReference,
    status_tracker: Optional[StatusTracker] = None,
    heartbeat_interval: float = 1.0,
    control_rate_hz: int = 100,
) -> tuple[int, int]:
    """
    Main teleoperation loop: read from leader, send to follower, send data to Cyberwave.

    The loop always runs at control_rate_hz (default 100Hz) to ensure responsive robot
    control and frequent timestamp updates for camera synchronization. MQTT publishing
    is rate-limited to publish_interval (1/fps) to avoid flooding the network.

    Args:
        leader: SO101Leader instance
        follower: Optional SO101Follower instance (required when sending to Cyberwave)
        action_queue: Queue for Cyberwave updates
        stop_event: Event to signal loop to stop
        last_observation: Dictionary tracking last sent observation state (normalized positions)
        position_threshold: Minimum change in normalized position to trigger update
        velocity_threshold: Unused (kept for compatibility)
        effort_threshold: Unused (kept for compatibility)
        publish_interval: Minimum interval between MQTT publishes (1/fps)
        time_reference: TimeReference instance
        heartbeat_interval: Interval in seconds to send heartbeat if no changes (default 1.0)
        control_rate_hz: Control loop frequency in Hz (default 100Hz for SO101)
    Returns:
        Tuple of (update_count, skip_count)
    """
    total_update_count = 0
    total_skip_count = 0

    # Track last send time per joint for heartbeat
    last_send_times: Dict[str, float] = {}

    # Control loop timing - always run at control_rate_hz for responsive control
    control_frame_time = 1.0 / control_rate_hz

    # Track last publish time for rate limiting MQTT updates
    last_publish_time = 0.0

    try:
        while not stop_event.is_set():
            loop_start = time.time()

            # Generate timestamp for this iteration (before reading action)
            # This runs at 100Hz to provide fresh timestamps for camera sync
            timestamp, timestamp_monotonic = time_reference.update()

            # Read action from leader (for sending to follower)
            leader_action = leader.get_action() if leader is not None else {}

            # Read follower observation (for sending to Cyberwave - this is the actual robot state)
            follower_action = None
            if follower is not None:
                follower_action = follower.get_observation()
            else:
                # Fallback to leader if no follower (shouldn't happen when robot twin is provided)
                follower_action = leader_action

            # Send action to follower if provided - always do this at control_rate_hz
            if follower is not None and leader is not None:
                try:
                    # Send leader action to follower (follower handles safety limits and normalization)
                    follower.send_action(leader_action)
                except Exception:
                    if status_tracker:
                        status_tracker.increment_errors()

            # Rate-limit MQTT publishing to publish_interval
            # Control runs at 100Hz but we only publish at fps (e.g., 30Hz)
            current_time = time.time()
            if current_time - last_publish_time >= publish_interval:
                # Process Cyberwave updates (queue changed joints from follower)
                # Worker thread handles all conversion
                update_count, skip_count = _process_cyberwave_updates(
                    action=follower_action,
                    last_observation=last_observation,
                    action_queue=action_queue,
                    position_threshold=position_threshold,
                    velocity_threshold=velocity_threshold,
                    effort_threshold=effort_threshold,
                    timestamp=timestamp,  # Pass timestamp to processing function
                    status_tracker=status_tracker,
                    last_send_times=last_send_times,
                    heartbeat_interval=heartbeat_interval,
                )
                total_update_count += update_count
                total_skip_count += skip_count
                last_publish_time = current_time

            # Rate limiting - maintain control_rate_hz
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
    leader: Optional[SO101Leader],
    cyberwave_client: Optional[Cyberwave] = None,
    follower: Optional[SO101Follower] = None,
    fps: int = 30,
    camera_fps: int = 30,
    position_threshold: float = 0.1,
    velocity_threshold: float = 100.0,
    effort_threshold: float = 0.1,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
    camera_type: str = "cv2",
    camera_id: Union[int, str] = 0,
    camera_resolution: Resolution = Resolution.VGA,
    enable_depth: bool = False,
    depth_fps: int = 30,
    depth_resolution: Optional[Resolution] = None,
    depth_publish_interval: int = 30,
) -> None:
    """
    Run teleoperation loop: read from leader, send to follower, and send follower data to Cyberwave.

    Uses a separate thread with a FIFO queue to send updates to Cyberwave,
    keeping the main loop responsive. Only sends updates when values change.
    Follower data (actual robot state) is sent to Cyberwave, not leader data.
    Always converts positions to radians.

    The control loop always runs at 100Hz (SO101's max frequency) for responsive robot control
    and accurate timestamp synchronization with the camera. MQTT publishing is rate-limited
    to the 'fps' parameter to avoid flooding the network.

    Supports both CV2 (USB/webcam/IP) cameras and Intel RealSense cameras.

    Args:
        leader: SO101Leader instance (optional if camera_only=True)
        cyberwave_client: Cyberwave client instance (required)
        follower: SO101Follower instance (required when robot twin is provided)
        fps: MQTT publishing rate in frames per second (control loop runs at 100Hz regardless)
        camera_fps: Frames per second for camera streaming
        position_threshold: Minimum change in position to trigger an update (in normalized units)
        velocity_threshold: Minimum change in velocity to trigger an update
        effort_threshold: Minimum change in effort to trigger an update
        robot: Robot twin instance
        camera: Camera twin instance
        camera_type: Camera type - "cv2" for USB/webcam/IP, "realsense" for Intel RealSense
        camera_id: Camera device ID (int) or stream URL (str) for CV2 cameras (default: 0)
        camera_resolution: Video resolution (default: VGA 640x480)
        enable_depth: Enable depth streaming for RealSense (default: False)
        depth_fps: Depth stream FPS for RealSense (default: 30)
        depth_resolution: Depth resolution for RealSense (default: same as color)
        depth_publish_interval: Publish depth every N frames for RealSense (default: 30)
    """
    time_reference = TimeReference()

    # Disable all logging to avoid interfering with status display
    logging.disable(logging.CRITICAL)

    # Create status tracker
    status_tracker = StatusTracker()
    status_tracker.script_started = True
    status_tracker.camera_enabled = camera is not None

    # Set twin info for status display
    robot_uuid = robot.uuid if robot else ""
    robot_name = robot.name if robot and hasattr(robot, "name") else "so101-teleop"
    camera_uuid_val = camera.uuid if camera else ""
    camera_name = camera.name if camera and hasattr(camera, "name") else ""
    status_tracker.set_twin_info(robot_uuid, robot_name, camera_uuid_val, camera_name)

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
            raise RuntimeError(
                "Leader is not calibrated. Please calibrate the leader first using the calibration script."
            )

    # Use follower motors for mappings when sending to Cyberwave (follower data is what we send)
    # Fall back to leader motors if follower not available (for camera-only mode)
    motors_for_mapping = (
        follower.motors if follower is not None else (leader.motors if leader is not None else {})
    )

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
        target=_keyboard_input_thread,
        args=(stop_event,),
        daemon=True,
    )
    keyboard_thread.start()

    # Start status logging thread
    status_thread = threading.Thread(
        target=_status_logging_thread,
        args=(status_tracker, stop_event, fps, camera_fps, leader, follower),
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
                time_reference,
                status_tracker,
                follower_calibration,
            ),
            daemon=True,
        )
        worker_thread.start()

    # Start camera streaming thread if follower has cameras configured or camera twin is provided
    camera_thread = None
    if camera is not None and cyberwave_client is not None:
        # Determine camera_id from follower config or use provided camera_id
        actual_camera_id = camera_id
        if follower is not None and follower.config.cameras and len(follower.config.cameras) > 0:
            actual_camera_id = follower.config.cameras[0]

        camera_thread = threading.Thread(
            target=_camera_worker_thread,
            args=(
                cyberwave_client,
                actual_camera_id,
                camera_fps,
                camera.uuid,
                stop_event,
                time_reference,
                status_tracker,
                camera_type,
                camera_resolution,
                enable_depth,
                depth_fps,
                depth_resolution,
                depth_publish_interval,
            ),
            daemon=True,
        )
        camera_thread.start()
    else:
        status_tracker.update_camera_status(detected=False, started=False)

    # TimeReference synchronization: The teleop loop always runs at 100Hz (control_rate_hz),
    # updating TimeReference frequently. The camera reads from time_reference to get timestamps.
    # MQTT publishing is rate-limited to the 'fps' parameter, but control and timestamp updates
    # run at full speed for accurate synchronization.
    status_tracker.fps = fps

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
                        # Fallback if no calibration
                        norm_mode = joint_name_to_norm_mode[name]
                        if norm_mode == MotorNormMode.RANGE_M100_100:
                            degrees = (normalized_pos / 100.0) * 180.0
                            radians = degrees * math.pi / 180.0
                        elif norm_mode == MotorNormMode.RANGE_0_100:
                            degrees = (normalized_pos / 100.0) * 360.0
                            radians = degrees * math.pi / 180.0
                        else:
                            radians = normalized_pos * math.pi / 180.0
                    observations[joint_index] = radians
            # Send follower observations to Cyberwave as single update
            # together with the desired actual frequency
            mqtt_client.publish_initial_observation(
                twin_uuid=robot.uuid,
                observations=observations,
                fps=fps
            )

    except Exception:
        if status_tracker:
            status_tracker.increment_errors()

    # Publish interval for MQTT rate limiting (matches fps parameter)
    publish_interval = 1.0 / fps

    try:
        if leader is not None:
            _teleop_loop(
                leader=leader,
                follower=follower,
                action_queue=action_queue,
                stop_event=stop_event,
                last_observation=last_observation,
                position_threshold=position_threshold,
                velocity_threshold=velocity_threshold,
                effort_threshold=effort_threshold,
                publish_interval=publish_interval,
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

        if camera_thread is not None:
            camera_thread.join(timeout=5.0)

        if status_thread is not None:
            status_thread.join(timeout=1.0)


def _parse_resolution(resolution_str: str) -> Resolution:
    """Parse resolution string to Resolution enum.

    Args:
        resolution_str: Resolution string like "VGA", "HD", "FULL_HD", or "WIDTHxHEIGHT"

    Returns:
        Resolution enum value
    """
    resolution_str = resolution_str.upper().strip()

    # Try to match enum name
    resolution_map = {
        "QVGA": Resolution.QVGA,
        "VGA": Resolution.VGA,
        "SVGA": Resolution.SVGA,
        "HD": Resolution.HD,
        "720P": Resolution.HD,
        "FULL_HD": Resolution.FULL_HD,
        "1080P": Resolution.FULL_HD,
    }

    if resolution_str in resolution_map:
        return resolution_map[resolution_str]

    # Try to parse WIDTHxHEIGHT format
    if "x" in resolution_str.lower():
        try:
            width, height = resolution_str.lower().split("x")
            width, height = int(width), int(height)
            # Try to find matching resolution enum
            match = Resolution.from_size(width, height)
            if match:
                return match
            # Return closest resolution
            return Resolution.closest(width, height)
        except ValueError:
            pass

    raise ValueError(
        f"Invalid resolution: {resolution_str}. "
        "Use QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT format."
    )


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
        default=os.getenv("CYBERWAVE_TWIN_UUID"),
        help="UUID of the twin to update",
    )
    parser.add_argument(
        "--leader-port",
        type=str,
        required=False,
        default=os.getenv("CYBERWAVE_METADATA_LEADER_PORT"),
        help="Serial port for leader device (optional, will try to find if not provided). "
        "Not required when using --camera-only.",
    )
    parser.add_argument(
        "--follower-port",
        type=str,
        default=os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT"),
        help="Serial port for follower device (optional)",
    )
    parser.add_argument(
        "--fps",
        type=int,
        default=30,
        help="Target frames per second (default: 30)",
    )
    parser.add_argument(
        "--max-relative-target",
        type=float,
        default=None,
        help="Maximum change per update for follower (raw encoder units, default: None = no limit). "
        "Set to a value to enable safety limit (e.g., 50.0 for slower/safer movement).",
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
        default=os.getenv("CYBERWAVE_METADATA_CAMERA_TWIN_UUID"),
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
        "--camera-type",
        type=str,
        choices=["cv2", "realsense"],
        default=os.getenv("CYBERWAVE_METADATA_CAMERA_TYPE", "cv2"),
        help="Camera type: 'cv2' for USB/webcam/IP cameras, 'realsense' for Intel RealSense (default: 'cv2')",
    )
    parser.add_argument(
        "--camera-id",
        type=str,
        default="0",
        help="Camera device ID (integer) or stream URL (string) for CV2 cameras. "
        "Examples: '0' for first USB camera, 'rtsp://192.168.1.100:554/stream' for RTSP (default: '0')",
    )
    parser.add_argument(
        "--camera-resolution",
        type=str,
        default=os.getenv("CYBERWAVE_METADATA_CAMERA_RESOLUTION", "VGA"),
        help="Camera resolution: QVGA (320x240), VGA (640x480), SVGA (800x600), HD (1280x720), "
        "FULL_HD (1920x1080), or WIDTHxHEIGHT format (default: VGA)",
    )
    parser.add_argument(
        "--enable-depth",
        action="store_true",
        help="Enable depth streaming for RealSense cameras",
    )
    parser.add_argument(
        "--depth-fps",
        type=int,
        default=30,
        help="Depth stream FPS for RealSense cameras (default: 30)",
    )
    parser.add_argument(
        "--depth-resolution",
        type=str,
        default=None,
        help="Depth stream resolution for RealSense cameras (default: same as --camera-resolution)",
    )
    parser.add_argument(
        "--depth-publish-interval",
        type=int,
        default=30,
        help="Publish depth frame every N frames for RealSense cameras (default: 30)",
    )
    parser.add_argument(
        "--list-realsense",
        action="store_true",
        help="List available RealSense devices and exit",
    )
    parser.add_argument(
        "--camera-config",
        type=str,
        default=None,
        help=f"Path to camera configuration JSON file. If provided, camera settings from the file "
        f"will be used instead of CLI arguments. Generate with --generate-camera-config.",
    )
    parser.add_argument(
        "--generate-camera-config",
        type=str,
        nargs="?",
        const=DEFAULT_CAMERA_CONFIG_PATH,
        metavar="PATH",
        help=f"Generate a camera configuration file and exit. "
        f"Optionally specify output path (default: {DEFAULT_CAMERA_CONFIG_PATH}). "
        f"Use with --camera-type to specify camera type, --auto-detect for RealSense auto-detection.",
    )
    parser.add_argument(
        "--auto-detect",
        action="store_true",
        help="Auto-detect RealSense device capabilities when generating config (use with --generate-camera-config)",
    )

    args = parser.parse_args()

    # Handle --generate-camera-config
    if args.generate_camera_config:
        output_path = args.generate_camera_config
        print(f"Generating camera configuration file: {output_path}")

        try:
            config = generate_camera_config(
                output_path=output_path,
                camera_type=args.camera_type,
                auto_detect=args.auto_detect,
            )
            print(f"Configuration saved to: {output_path}")
            print(f"Config: {config}")
            print("\nYou can now use this config with:")
            print(f"  python teleoperate.py --camera-config {output_path} ...")
        except Exception as e:
            print(f"Error generating config: {e}")
            sys.exit(1)
        sys.exit(0)

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

    # Validate: --camera-only requires --camera-uuid
    if args.camera_only and not args.camera_uuid:
        print("Error: --camera-uuid is required when using --camera-only")
        sys.exit(1)

    # Initialize Cyberwave client
    cyberwave_client = Cyberwave()

    # Camera setup is optional - only configure when explicitly requested
    # via --camera-uuid or --camera-config. Camera streaming is being moved
    # to a separate codebase; this support is kept for backwards compatibility.
    camera_config: Optional[TeleoperateCameraConfig] = None
    camera_twin = None

    # Default camera values (only used when a camera twin is configured)
    camera_type = args.camera_type
    camera_fps = args.camera_fps
    enable_depth = args.enable_depth
    depth_fps = args.depth_fps
    depth_publish_interval = args.depth_publish_interval
    camera_id: Union[int, str] = args.camera_id
    try:
        camera_id = int(args.camera_id)
    except ValueError:
        pass  # Keep as string (URL)
    camera_resolution = _parse_resolution(args.camera_resolution)
    depth_resolution = _parse_resolution(args.depth_resolution) if args.depth_resolution else None

    # Check for camera config file in default location first (like calibration)
    default_camera_config_path = get_default_camera_config_path()
    camera_config_file_to_load = None

    if args.camera_config:
        # Explicit config file provided (highest priority)
        camera_config_file_to_load = args.camera_config
    elif os.path.exists(default_camera_config_path):
        # Check default location (~/.cyberwave/so101_lib/camera/camera_config.json)
        camera_config_file_to_load = default_camera_config_path

    if camera_config_file_to_load:
        # Load from JSON file
        if not os.path.exists(camera_config_file_to_load):
            print(f"Error: Camera config file not found: {camera_config_file_to_load}")
            print(
                f"Generate one with: python teleoperate.py --generate-camera-config {camera_config_file_to_load}"
            )
            sys.exit(1)

        try:
            camera_config = TeleoperateCameraConfig.load(camera_config_file_to_load)
            print(f"Loaded camera config from: {camera_config_file_to_load}")
            print(f"Config: {camera_config}")
        except Exception as e:
            print(f"Error loading camera config: {e}")
            sys.exit(1)

        # Use values from config file
        camera_type = camera_config.camera_type
        camera_id = camera_config.camera_id
        camera_fps = camera_config.fps
        camera_resolution = camera_config.get_resolution()
        enable_depth = camera_config.enable_depth
        depth_fps = camera_config.depth_fps
        depth_resolution = camera_config.get_depth_resolution()
        depth_publish_interval = camera_config.depth_publish_interval

        # Create camera twin if camera-uuid is also provided
        if args.camera_uuid:
            camera_twin = cyberwave_client.twin(twin_id=args.camera_uuid)
    elif args.camera_uuid:
        # Fetch twin by UUID and infer camera type from twin type
        try:
            camera_twin = cyberwave_client.twin(twin_id=args.camera_uuid)

            # Infer camera type from twin type
            if isinstance(camera_twin, DepthCameraTwin):
                # DepthCameraTwin -> RealSense
                camera_type = "realsense"
                camera_config = _get_default_camera_config("realsense")
            elif isinstance(camera_twin, CameraTwin):
                # CameraTwin -> CV2
                camera_type = "cv2"
                camera_config = _get_default_camera_config("cv2")
            else:
                # Regular Twin - error or default to CV2
                print(
                    f"Warning: Twin {args.camera_uuid} is not a camera twin (type: {type(camera_twin).__name__})",
                    file=sys.stderr,
                )
                print("Defaulting to CV2 camera type", file=sys.stderr)
                camera_type = "cv2"
                camera_config = _get_default_camera_config("cv2")

            # Use values from detected config
            camera_id = camera_config.camera_id
            camera_fps = camera_config.fps
            camera_resolution = camera_config.get_resolution()
            enable_depth = camera_config.enable_depth
            depth_fps = camera_config.depth_fps
            depth_resolution = camera_config.get_depth_resolution()
            depth_publish_interval = camera_config.depth_publish_interval

            logger.info(
                f"Auto-detected camera type '{camera_type}' from twin type '{type(camera_twin).__name__}', using default settings"
            )
        except Exception as e:
            print(f"Error fetching camera twin {args.camera_uuid}: {e}", file=sys.stderr)
            sys.exit(1)

    robot = cyberwave_client.twin(
        asset_key="the-robot-studio/so101", twin_id=args.twin_uuid, name="robot"
    )
    camera = camera_twin
    mqtt_client = cyberwave_client.mqtt

    # Initialize leader (optional if camera-only mode)
    leader = None
    if not args.camera_only:
        from config import LeaderConfig
        from utils import find_port

        leader_port = args.leader_port
        if not leader_port:
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

        # Only configure cameras on the follower if a camera twin is being used
        follower_cameras = None
        if camera_twin is not None:
            # Support both int (device index) and str (URL) for camera_id
            follower_cameras = [camera_id]

        follower_config = FollowerConfig(
            port=args.follower_port,
            max_relative_target=args.max_relative_target,
            cameras=follower_cameras,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()

    try:
        teleoperate(
            leader=leader,
            cyberwave_client=cyberwave_client,
            follower=follower,
            fps=args.fps,
            camera_fps=camera_fps,
            robot=robot,
            camera=camera,
            camera_type=camera_type,
            camera_id=camera_id,
            camera_resolution=camera_resolution,
            enable_depth=enable_depth,
            depth_fps=depth_fps,
            depth_resolution=depth_resolution,
            depth_publish_interval=depth_publish_interval,
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
