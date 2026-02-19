"""Remote operation loop for SO101 follower via Cyberwave MQTT."""

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
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from cyberwave import Cyberwave, Twin

# Import camera configuration and stream manager from cyberwave SDK
from cyberwave.sensor import (
    CameraStreamManager,
    CV2CameraStreamer,
    RealSenseStreamer,
    Resolution,
)
from cyberwave.utils import TimeReference
from dotenv import load_dotenv

# RealSense is optional - only import if available
try:
    from cyberwave.sensor import (
        RealSenseConfig,
        RealSenseDiscovery,
    )

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None
    RealSenseDiscovery = None

from config import get_setup_config_path
from cw_setup import load_setup_config
from follower import SO101Follower
from motors import MotorNormMode
from utils import load_calibration
from write_position import validate_position

logger = logging.getLogger(__name__)

DEFAULT_CAMERA_CONFIG_PATH = str(get_setup_config_path())


def _get_sensor_camera_name(twin: Twin, sensor_index: int = 0) -> Optional[str]:
    """Get camera/sensor name from twin capabilities for multi-stream routing.

    Uses the sensor id from capabilities.sensors when available (e.g. "head_camera",
    "integrated_camera"). Returns None for single-stream backward compatibility.

    Args:
        twin: Twin with potential sensor capabilities
        sensor_index: Index of sensor to use (default: 0)

    Returns:
        Sensor id string or None
    """
    sensors = twin.capabilities.get("sensors", [])
    if sensors and sensor_index < len(sensors):
        sensor = sensors[sensor_index]
        if isinstance(sensor, dict):
            return sensor.get("id")
    return None


@dataclass
class RemoteoperateCameraConfig:
    """Camera configuration for remote operation.

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
    def load(cls, path: str = DEFAULT_CAMERA_CONFIG_PATH) -> "RemoteoperateCameraConfig":
        """Load configuration from JSON file.

        Args:
            path: Path to the configuration file

        Returns:
            RemoteoperateCameraConfig instance

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
    ) -> "RemoteoperateCameraConfig":
        """Create configuration from connected RealSense device.

        Auto-detects device capabilities and creates optimal configuration.

        Args:
            prefer_resolution: Preferred resolution (will find closest match)
            prefer_fps: Preferred FPS (will find closest match)
            enable_depth: Whether to enable depth streaming
            serial_number: Target device serial number (None = first device)

        Returns:
            RemoteoperateCameraConfig configured for the device
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
    ) -> "RemoteoperateCameraConfig":
        """Create default CV2 camera configuration.

        Args:
            camera_id: Camera device ID or stream URL
            fps: Frames per second
            resolution: Video resolution

        Returns:
            RemoteoperateCameraConfig for CV2 camera
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
                f"RemoteoperateCameraConfig(type={self.camera_type}, "
                f"color={res_str}@{self.fps}fps, "
                f"depth={depth_res[0]}x{depth_res[1]}@{self.depth_fps}fps)"
            )
        return f"RemoteoperateCameraConfig(type={self.camera_type}, id={self.camera_id}, {res_str}@{self.fps}fps)"


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


def _get_default_camera_config(camera_type: str) -> RemoteoperateCameraConfig:
    """
    Get default camera configuration based on camera type.

    Args:
        camera_type: Camera type ("cv2" or "realsense")

    Returns:
        RemoteoperateCameraConfig with default settings
    """
    if camera_type == "realsense":
        return RemoteoperateCameraConfig(
            camera_type="realsense",
            fps=30,
            resolution=[640, 480],
            enable_depth=True,
            depth_fps=15,
            depth_resolution=[640, 480],
            depth_publish_interval=30,
        )
    else:
        return RemoteoperateCameraConfig.create_default_cv2()


class StatusTracker:
    """Thread-safe status tracker for remote operation system."""

    def __init__(self):
        self.lock = threading.Lock()
        self.script_started = False
        self.mqtt_connected = False
        self.camera_enabled = False  # Whether camera streaming was configured at all
        # Per-camera: camera_name -> {detected, started, webrtc_state}
        self.camera_states: Dict[str, Dict[str, Any]] = {}
        self.fps = 0
        self.camera_fps = 0
        self.messages_received = 0
        self.messages_processed = 0
        self.messages_filtered = 0
        self.errors = 0
        self.joint_states: Dict[str, float] = {}
        self.joint_temperatures: Dict[
            str, float
        ] = {}  # Format: "follower_1" -> temperature
        self.joint_index_to_name: Dict[str, str] = {}
        self.robot_uuid: str = ""
        self.robot_name: str = ""
        self.camera_infos: List[Dict[str, str]] = []  # [{uuid, name}, ...] per camera

    def update_mqtt_status(self, connected: bool):
        with self.lock:
            self.mqtt_connected = connected

    def set_camera_infos(self, infos: List[Dict[str, str]]) -> None:
        """Set camera info list and initialize per-camera states."""
        with self.lock:
            self.camera_infos = list(infos)
            for info in infos:
                name = info.get("name", "default")
                if name not in self.camera_states:
                    self.camera_states[name] = {
                        "detected": False,
                        "started": False,
                        "webrtc_state": "idle",
                    }

    def update_camera_status(self, camera_name: str, detected: bool, started: bool = False):
        with self.lock:
            if camera_name not in self.camera_states:
                self.camera_states[camera_name] = {
                    "detected": False,
                    "started": False,
                    "webrtc_state": "idle",
                }
            self.camera_states[camera_name]["detected"] = detected
            self.camera_states[camera_name]["started"] = started

    def update_webrtc_state(self, camera_name: str, state: str):
        """Update WebRTC state for a camera: 'idle', 'connecting', or 'streaming'."""
        with self.lock:
            if camera_name not in self.camera_states:
                self.camera_states[camera_name] = {
                    "detected": False,
                    "started": False,
                    "webrtc_state": "idle",
                }
            self.camera_states[camera_name]["webrtc_state"] = state

    def increment_received(self):
        with self.lock:
            self.messages_received += 1

    def increment_processed(self):
        with self.lock:
            self.messages_processed += 1

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
        """Set twin information for display (legacy single-camera)."""
        with self.lock:
            self.robot_uuid = robot_uuid
            self.robot_name = robot_name
            if not self.camera_infos:
                self.camera_infos = [{"uuid": camera_uuid, "name": camera_name}]

    def get_status(self) -> Dict:
        """Get a snapshot of current status."""
        with self.lock:
            return {
                "script_started": self.script_started,
                "mqtt_connected": self.mqtt_connected,
                "camera_enabled": self.camera_enabled,
                "camera_states": {k: dict(v) for k, v in self.camera_states.items()},
                "camera_infos": list(self.camera_infos),
                "fps": self.fps,
                "camera_fps": self.camera_fps,
                "messages_received": self.messages_received,
                "messages_processed": self.messages_processed,
                "messages_filtered": self.messages_filtered,
                "errors": self.errors,
                "joint_states": self.joint_states.copy(),
                "joint_temperatures": self.joint_temperatures.copy(),
                "robot_uuid": self.robot_uuid,
                "robot_name": self.robot_name,
            }


def _read_temperatures(
    follower: Optional[SO101Follower],
    joint_index_to_name: Dict[str, str],
) -> Dict[str, float]:
    """
    Read temperatures from follower motors.

    Args:
        follower: SO101Follower instance (optional)
        joint_index_to_name: Mapping from joint index to joint name

    Returns:
        Dictionary mapping "follower_{motor_id}" to temperature in Celsius
    """
    temperatures = {}

    try:
        from motors.tables import ADDR_PRESENT_TEMPERATURE

        addr = ADDR_PRESENT_TEMPERATURE[0]  # Temperature is 1 byte

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
    follower: Optional[SO101Follower] = None,
) -> None:
    """
    Thread that logs status information at 1 fps.

    Args:
        status_tracker: StatusTracker instance
        stop_event: Event to signal thread to stop
        fps: Target frames per second for remote operation loop
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
            # Read temperatures from follower if available
            joint_index_to_name = status_tracker.joint_index_to_name
            temperatures = _read_temperatures(follower, joint_index_to_name)
            if temperatures:
                status_tracker.update_joint_temperatures(temperatures)

            status = status_tracker.get_status()

            # Build status display with fixed width lines
            lines = []
            lines.append("=" * 70)
            lines.append("SO101 Remote Operation Status".center(70))
            lines.append("=" * 70)

            # Twin info
            robot_name = status["robot_name"] or "N/A"
            lines.append(f"Robot:  {robot_name} ({status['robot_uuid']})"[:70].ljust(70))
            if status["camera_enabled"]:
                camera_infos = status.get("camera_infos", [])
                camera_states = status.get("camera_states", {})
                for info in camera_infos:
                    cam_name = info.get("name", "default")
                    cam_uuid = info.get("uuid", "")[:8]
                    state = camera_states.get(cam_name, {})
                    detected = state.get("detected", False)
                    started = state.get("started", False)
                    webrtc = state.get("webrtc_state", "idle")
                    cam_icon = "🟢" if (detected and started) else ("🟡" if detected else "🔴")
                    webrtc_icon = (
                        "🟢" if webrtc == "streaming" else ("🟡" if webrtc == "connecting" else "🔴")
                    )
                    lines.append(
                        f"  {cam_name}: Cam{cam_icon} WebRTC{webrtc_icon} ({cam_uuid}...)"[:70].ljust(70)
                    )
            else:
                lines.append("Camera: not configured".ljust(70))
            lines.append("-" * 70)

            # Status indicators
            script_icon = "🟢" if status["script_started"] else "🟡"
            mqtt_icon = "🟢" if status["mqtt_connected"] else "🔴"
            lines.append(f"Script:{script_icon} MQTT:{mqtt_icon}".ljust(70))
            lines.append("-" * 70)

            # Statistics
            stats = f"FPS:{status['fps']} Cam:{status['camera_fps']} Recv:{status['messages_received']} Proc:{status['messages_processed']} Filt:{status['messages_filtered']} Err:{status['errors']}"
            lines.append(stats.ljust(70))
            lines.append("-" * 70)

            # Joint states - always show (populated at startup from motor readings)
            index_to_name = status_tracker.joint_index_to_name
            lines.append("Motors:".ljust(70))
            for joint_index in sorted(status["joint_states"].keys()):
                position = status["joint_states"][joint_index]
                joint_name = index_to_name.get(joint_index, joint_index)

                # Get temperature for this motor from follower
                follower_temp = status["joint_temperatures"].get(
                    f"follower_{joint_index}", None
                )

                # Build temperature display with indicator
                temp_str = "N/A"
                if follower_temp is not None:
                    follower_indicator = "🔥" if follower_temp > 40 else ""
                    temp_str = f"F:{follower_temp:3.0f}°C{follower_indicator}"

                # Format: "  shoulder_pan:  pos=  0.79rad  F:34°C🔥"
                line = f"  {joint_name:16s}  pos={position:6.3f}rad  {temp_str}"
                lines.append(line[:70].ljust(70))

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
    camera_name: Optional[str] = None,
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
        camera_name: Optional sensor identifier for multi-stream twins (from capabilities.sensors)
    Returns:
        None
    """
    if status_tracker:
        status_tracker.update_camera_status(camera_name or "default", detected=True, started=False)

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
                camera_name=camera_name,
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
                camera_name=camera_name,
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
                    status_tracker.update_webrtc_state(camera_name or "default", "streaming")
                elif "stopped" in msg.lower():
                    status_tracker.update_webrtc_state(camera_name or "default", "idle")

        try:
            # Update status when camera starts
            if status_tracker:
                status_tracker.update_camera_status(camera_name or "default", detected=True, started=True)
                # WebRTC starts in idle state, waiting for start_video command
                status_tracker.update_webrtc_state(camera_name or "default", "idle")

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


def _radians_to_normalized(
    radians: float,
    norm_mode: MotorNormMode,
    calib: Optional[Any] = None,
) -> float:
    """
    Convert radians to normalized position based on motor normalization mode and calibration.

    This is the inverse of teleoperate's normalized->radians conversion.
    Uses calibration data when available for accurate conversion.

    Args:
        radians: Position in radians
        norm_mode: Motor normalization mode
        calib: Calibration data with range_min and range_max (optional)

    Returns:
        Normalized position value
    """
    if calib is not None:
        # Use calibration-based conversion (inverse of teleoperate's conversion)
        r_min = calib.range_min
        r_max = calib.range_max
        delta_r = (r_max - r_min) / 2.0

        if norm_mode == MotorNormMode.RANGE_M100_100:
            # Inverse of: raw_offset = (normalized / 100.0) * delta_r
            #             radians = raw_offset * (2.0 * math.pi / 4095.0)
            # So: raw_offset = radians / (2.0 * math.pi / 4095.0)
            #     normalized = (raw_offset / delta_r) * 100.0
            raw_offset = radians / (2.0 * math.pi / 4095.0)
            normalized = (raw_offset / delta_r) * 100.0
            return normalized
        elif norm_mode == MotorNormMode.RANGE_0_100:
            # Inverse of teleoperate's conversion for RANGE_0_100
            delta_r_full = r_max - r_min
            # radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
            # raw_value = radians / (2.0 * math.pi / 4095.0) + r_min
            # normalized = ((raw_value - r_min) / delta_r_full) * 100.0
            raw_value = radians / (2.0 * math.pi / 4095.0) + r_min
            normalized = ((raw_value - r_min) / delta_r_full) * 100.0
            return normalized
        else:  # DEGREES
            return radians * 180.0 / math.pi
    else:
        # Fallback without calibration
        if norm_mode == MotorNormMode.DEGREES:
            degrees = radians * 180.0 / math.pi
            return degrees
        elif norm_mode == MotorNormMode.RANGE_M100_100:
            degrees = radians * 180.0 / math.pi
            normalized = (degrees / 180.0) * 100.0
            return normalized
        elif norm_mode == MotorNormMode.RANGE_0_100:
            degrees = radians * 180.0 / math.pi
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
    status_tracker: Optional[StatusTracker] = None,
) -> None:
    """
    Worker thread that reads actions from queue and writes to follower motors.

    Splits large position changes into multiple smaller steps based on max_relative_target.

    Args:
        action_queue: Queue containing action dictionaries (normalized positions)
        follower: SO101Follower instance
        stop_event: Event to signal thread to stop
        status_tracker: Optional status tracker for statistics
    """
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
            except queue.Empty:
                continue

            try:
                # If max_relative_target is set, split large movements into smaller steps
                if follower.config.max_relative_target is not None:
                    # Get initial current positions (only once, at the start)
                    # get_observation() handles SerialException internally and returns last known positions
                    present_pos = follower.get_observation()
                    # Handle empty dict gracefully (shouldn't happen, but be safe)
                    if not present_pos:
                        # Skip this action if we can't get current positions
                        continue
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
                    for _ in range(max_steps):
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
                            if status_tracker:
                                status_tracker.increment_processed()
                        finally:
                            # Restore original max_relative_target
                            follower.config.max_relative_target = original_max_relative

                        # No sleep - send steps as fast as possible
                        # The motors will catch up naturally
                else:
                    # No max_relative_target limit, send action directly
                    follower.send_action(action)
                    processed_count += 1
                    if status_tracker:
                        status_tracker.increment_processed()

            except Exception:
                error_count += 1
                if status_tracker:
                    status_tracker.increment_errors()
            finally:
                action_queue.task_done()

        except Exception:
            error_count += 1
            if status_tracker:
                status_tracker.increment_errors()


def _create_joint_state_callback(
    current_state: Dict[str, float],
    action_queue: queue.Queue,
    joint_index_to_name: Dict[int, str],
    joint_name_to_norm_mode: Dict[str, MotorNormMode],
    follower: SO101Follower,
    follower_calibration: Optional[Dict[str, Any]] = None,
    status_tracker: Optional[StatusTracker] = None,
) -> callable:
    """
    Create a callback function for MQTT joint state updates.

    Args:
        current_state: Dictionary to maintain current state (joint_name -> normalized position)
        action_queue: Queue to put validated actions
        joint_index_to_name: Mapping from joint index (1-6) to joint name
        joint_name_to_norm_mode: Mapping from joint name to normalization mode
        follower: Follower instance for validation
        follower_calibration: Calibration data for converting radians to normalized positions
        status_tracker: Optional status tracker for statistics

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
        if status_tracker:
            status_tracker.increment_received()

        try:
            # Check if topic ends with "update" - only process update messages
            if not topic.endswith("/update"):
                if status_tracker:
                    status_tracker.increment_filtered()
                return

            # Extract joint index/name and position from data
            # Message format:
            # {"joint_name": "5", "joint_state": {"position": -1.22, "velocity": 0, "effort": 0}, "timestamp": ...}

            # Extract joint_name and joint_state
            if "joint_name" not in data or "joint_state" not in data:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            joint_name_str = data.get("joint_name")
            joint_state = data.get("joint_state", {})
            position_radians = joint_state.get("position")

            if position_radians is None:
                if status_tracker:
                    status_tracker.increment_errors()
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
                    if status_tracker:
                        status_tracker.increment_errors()
                    return

            # Get joint name from index
            joint_name = joint_index_to_name.get(joint_index)
            if joint_name is None:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Convert position_radians to float
            try:
                position_radians = float(position_radians)
            except (ValueError, TypeError):
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Get normalization mode for this joint
            norm_mode = joint_name_to_norm_mode.get(joint_name)
            if norm_mode is None:
                if status_tracker:
                    status_tracker.increment_errors()
                return

            # Get calibration for this joint (if available)
            calib = follower_calibration.get(joint_name) if follower_calibration else None

            # Convert radians to normalized position using calibration
            normalized_position = _radians_to_normalized(position_radians, norm_mode, calib)

            # Validate position
            motor_id = follower.motors[joint_name].id
            is_valid, error_msg = validate_position(motor_id, normalized_position)
            if not is_valid:
                if status_tracker:
                    status_tracker.increment_filtered()
                return

            # Update current state (merge with previous state)
            # Create full action state by taking current state and updating the changed joint
            action = current_state.copy()
            action[f"{joint_name}.pos"] = normalized_position
            # Also update the current_state dictionary
            current_state[f"{joint_name}.pos"] = normalized_position

            # Update joint states in status tracker (store radians for display)
            if status_tracker:
                joint_states = {str(joint_index): position_radians}
                status_tracker.update_joint_states(joint_states)

            # Put action in queue (non-blocking)
            try:
                action_queue.put_nowait(action)
            except queue.Full:
                if status_tracker:
                    status_tracker.increment_errors()

        except Exception:
            if status_tracker:
                status_tracker.increment_errors()

    return callback


CONTROL_RATE_HZ = 100


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
    cameras: Optional[List[Dict[str, Any]]] = None,
    fps: int = 30,
    camera_fps: int = 30,
    camera_type: str = "cv2",
    camera_id: Union[int, str] = 0,
    camera_resolution: Resolution = Resolution.VGA,
    enable_depth: bool = False,
    depth_fps: int = 30,
    depth_resolution: Optional[Resolution] = None,
    depth_publish_interval: int = 30,
) -> None:
    """
    Run remote operation loop: receive joint states via MQTT and write to follower motors.

    Supports both CV2 (USB/webcam/IP) cameras and Intel RealSense cameras.

    Args:
        client: Cyberwave client instance
        follower: SO101Follower instance
        robot: Robot twin instance
        camera: Camera twin instance (single camera, backward compat). Ignored if cameras is set.
        cameras: List of camera configs for multi-stream. Each dict: twin, camera_id, camera_name
            (optional, from sensor id), camera_type, camera_resolution, enable_depth, etc.
            Each entry gets its own stream manager to avoid cascading failures.
        fps: Target frames per second (not used directly, but kept for compatibility)
        camera_fps: Frames per second for camera streaming
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

    # Build camera twins for CameraStreamManager (twin + optional overrides)
    camera_twins: List[Any] = []
    if cameras is not None:
        # cameras: list of dicts with "twin" and overrides -> (twin, overrides)
        for cfg in cameras:
            twin = cfg["twin"]
            overrides = {k: v for k, v in cfg.items() if k != "twin"}
            camera_twins.append((twin, overrides) if overrides else twin)
    elif camera is not None:
        actual_camera_id = camera_id
        if follower is not None and follower.config.cameras and len(follower.config.cameras) > 0:
            actual_camera_id = follower.config.cameras[0]
        overrides = {
            "camera_id": actual_camera_id,
            "camera_type": camera_type,
            "camera_resolution": camera_resolution,
            "enable_depth": enable_depth,
            "depth_fps": depth_fps,
            "depth_resolution": depth_resolution,
            "depth_publish_interval": depth_publish_interval,
        }
        camera_twins.append((camera, overrides))

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

    # Upload follower calibration to twin if available
    if follower.calibration is not None:
        _upload_calibration_to_twin(follower, robot, "follower")

    # Get follower calibration for proper conversion to/from radians
    follower_calibration = None
    if follower is not None and follower.calibration is not None:
        follower_calibration = follower.calibration
        assert follower_calibration.keys() == follower.motors.keys()
        for joint_name, calibration in follower_calibration.items():
            assert joint_name in follower.motors
            assert calibration.range_min is not None
            assert calibration.range_max is not None
            assert calibration.range_min < calibration.range_max

    # Create mapping from joint index (motor ID) to joint name
    joint_index_to_name = {motor.id: name for name, motor in follower.motors.items()}

    # Create mapping from joint indexes to joint names (for status display, string keys)
    joint_index_to_name_str = {str(motor.id): name for name, motor in follower.motors.items()}
    status_tracker.set_joint_index_to_name(joint_index_to_name_str)

    # Create mapping from joint names to normalization modes
    joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in follower.motors.items()}

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

                # Convert normalized position to radians using calibration
                if follower_calibration and name in follower_calibration:
                    calib = follower_calibration[name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        raw_offset = (normalized_pos / 100.0) * delta_r
                        radians = raw_offset * (2.0 * math.pi / 4095.0)
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        delta_r_full = r_max - r_min
                        raw_value = r_min + (normalized_pos / 100.0) * delta_r_full
                        radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                    else:
                        radians = normalized_pos * math.pi / 180.0
                else:
                    # Fallback without calibration
                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        degrees = (normalized_pos / 100.0) * 180.0
                        radians = degrees * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        degrees = (normalized_pos / 100.0) * 360.0
                        radians = degrees * math.pi / 180.0
                    else:
                        radians = normalized_pos * math.pi / 180.0

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

                # Convert normalized position to radians using calibration (like teleoperate)
                if follower_calibration and name in follower_calibration:
                    calib = follower_calibration[name]
                    r_min = calib.range_min
                    r_max = calib.range_max
                    delta_r = (r_max - r_min) / 2.0

                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        raw_offset = (normalized_pos / 100.0) * delta_r
                        radians = raw_offset * (2.0 * math.pi / 4095.0)
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        delta_r_full = r_max - r_min
                        raw_value = r_min + (normalized_pos / 100.0) * delta_r_full
                        radians = (raw_value - r_min) * (2.0 * math.pi / 4095.0)
                    else:
                        radians = normalized_pos * math.pi / 180.0
                else:
                    # Fallback without calibration
                    if norm_mode == MotorNormMode.RANGE_M100_100:
                        degrees = (normalized_pos / 100.0) * 180.0
                        radians = degrees * math.pi / 180.0
                    elif norm_mode == MotorNormMode.RANGE_0_100:
                        degrees = (normalized_pos / 100.0) * 360.0
                        radians = degrees * math.pi / 180.0
                    else:
                        radians = normalized_pos * math.pi / 180.0

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
        target=_keyboard_input_thread,
        args=(stop_event,),
        daemon=True,
    )
    keyboard_thread.start()

    # Start status logging thread
    status_thread = threading.Thread(
        target=_status_logging_thread,
        args=(status_tracker, stop_event, CONTROL_RATE_HZ, camera_fps, follower),
        daemon=True,
    )
    status_thread.start()

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
    joint_state_callback = _create_joint_state_callback(
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
        follower_calibration=follower_calibration,
        status_tracker=status_tracker,
    )

    # Subscribe to joint states
    mqtt_client.subscribe_joint_states(twin_uuid, joint_state_callback)

    # Start motor writer worker thread
    writer_thread = threading.Thread(
        target=_motor_writer_worker,
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

        # Stop camera streaming
        if camera_manager is not None:
            camera_manager.join(timeout=5.0)

        # Stop status thread
        if status_thread is not None:
            status_thread.join(timeout=1.0)


def _upload_calibration_to_twin(
    follower: SO101Follower,
    twin: Twin,
    robot_type: str = "follower",
) -> None:
    """
    Upload calibration data from follower device to Cyberwave twin.

    Args:
        follower: SO101Follower instance with calibration
        twin: Twin instance to upload calibration to
        robot_type: Robot type (default: "follower")
    """
    try:
        # Get calibration path from follower config
        calibration_path = follower.config.calibration_dir / f"{follower.config.id}.json"

        if not calibration_path.exists():
            logger.debug(f"No calibration file found at {calibration_path}, skipping upload")
            return

        # Load calibration file
        calib_data = load_calibration(calibration_path)

        # Convert calibration format to backend format
        # Backend expects: joint_calibration dict with JointCalibration objects
        # Keys should be motor IDs as strings (e.g., "1", "2", "3") not joint names
        # Note: drive_mode and id must be strings per the schema
        joint_calibration = {}
        for joint_name, calib in calib_data.items():
            # Get motor ID from follower motors mapping
            if joint_name not in follower.motors:
                logger.warning(f"Joint '{joint_name}' not found in follower motors, skipping")
                continue

            motor_id = follower.motors[joint_name].id
            motor_id_str = str(motor_id)

            joint_calibration[motor_id_str] = {
                "range_min": calib["range_min"],
                "range_max": calib["range_max"],
                "homing_offset": calib["homing_offset"],
                "drive_mode": str(calib["drive_mode"]),
                "id": str(calib["id"]),
            }

        # Upload calibration to twin
        logger.info(f"Uploading {robot_type} calibration to twin {twin.uuid}...")
        twin.update_calibration(joint_calibration, robot_type=robot_type)
        logger.info(f"Calibration uploaded successfully to twin {twin.uuid}")
    except ImportError:
        logger.warning(
            "Cyberwave SDK not installed. Skipping calibration upload. "
            "Install with: pip install cyberwave"
        )
    except Exception as e:
        logger.warning(f"Failed to upload calibration: {e}")
        logger.debug("Calibration upload failed, continuing without upload", exc_info=True)


def _parse_resolution(resolution_str: str) -> Resolution:
    """Parse resolution string to Resolution enum."""
    resolution_str = resolution_str.upper().strip()

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

    if "x" in resolution_str.lower():
        try:
            width, height = resolution_str.lower().split("x")
            width, height = int(width), int(height)
            match = Resolution.from_size(width, height)
            if match:
                return match
            return Resolution.closest(width, height)
        except ValueError:
            pass

    raise ValueError(
        f"Invalid resolution: {resolution_str}. "
        "Use QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT format."
    )


def main():
    """Main entry point for remote operation script."""
    # Load environment variables from .env file
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Remote operate SO101 follower via Cyberwave MQTT"
    )
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

    camera_twin = None
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

    # Default camera values (used when cameras_list is empty - no camera streaming)
    camera_type = "cv2"
    camera_fps = setup_config.get("camera_fps", 30)
    enable_depth = False
    depth_fps = 30
    depth_publish_interval = 30
    camera_id: Union[int, str] = 0
    camera_resolution = Resolution.VGA
    depth_resolution = None

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
            wrist_res_enum = _parse_resolution(wrist_res)
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

        if cameras_list:
            camera_twin = cameras_list[0]["twin"]
            camera_fps = cameras_list[0].get("fps", 30)

    # Resolve twin UUID and ports: CLI/env > setup.json (so remoteoperate works with no args when setup exists)
    effective_twin_uuid = args.twin_uuid or setup_config.get("twin_uuid") or setup_config.get(
        "wrist_camera_twin_uuid"
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
    # Pass cameras list when we have configs (single or multi), else single camera for backward compat
    camera = camera_twin if not cameras_list else None
    cameras = cameras_list if cameras_list else None
    mqtt_client = cyberwave_client.mqtt

    # Initialize follower
    from config import FollowerConfig

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
            camera=camera,
            cameras=cameras,
            camera_fps=camera_fps,
            camera_type=camera_type,
            camera_id=camera_id,
            camera_resolution=camera_resolution,
            enable_depth=enable_depth,
            depth_fps=depth_fps,
            depth_resolution=depth_resolution,
            depth_publish_interval=depth_publish_interval,
        )
    finally:
        if follower is not None:
            follower.disconnect()
        # Disconnect MQTT client
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
