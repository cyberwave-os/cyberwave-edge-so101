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
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, Optional, Union

from cyberwave import Cyberwave, Twin
from cyberwave.utils import TimeReference

# Import camera configuration and streamers from cyberwave SDK
from cyberwave.sensor import (
    CV2CameraStreamer,
    CameraConfig,
    Resolution,
)

# RealSense is optional - only import if available
try:
    from cyberwave.sensor import (
        RealSenseStreamer,
        RealSenseConfig,
        RealSenseDiscovery,
    )

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseStreamer = None
    RealSenseConfig = None
    RealSenseDiscovery = None

from dotenv import load_dotenv
from follower import SO101Follower
from motors import MotorNormMode
from write_position import validate_position

logger = logging.getLogger(__name__)

# Default camera config file path
DEFAULT_CAMERA_CONFIG_PATH = "camera_config.json"


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
        """Save configuration to JSON file."""
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Camera configuration saved to {path}")

    @classmethod
    def load(cls, path: str = DEFAULT_CAMERA_CONFIG_PATH) -> "RemoteoperateCameraConfig":
        """Load configuration from JSON file."""
        with open(path, "r") as f:
            data = json.load(f)

        # Handle resolution conversion
        if "resolution" in data and isinstance(data["resolution"], str):
            width, height = map(int, data["resolution"].lower().split("x"))
            data["resolution"] = [width, height]

        if "depth_resolution" in data and isinstance(data["depth_resolution"], str):
            width, height = map(int, data["depth_resolution"].lower().split("x"))
            data["depth_resolution"] = [width, height]

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
        """Create configuration from connected RealSense device."""
        if not _has_realsense:
            raise ImportError(
                "RealSense support requires pyrealsense2. Install with: pip install pyrealsense2"
            )

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
        """Create default CV2 camera configuration."""
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
        self.camera_detected = False
        self.camera_started = False
        # WebRTC states: "idle" (red), "connecting" (yellow), "streaming" (green)
        self.webrtc_state = "idle"
        self.fps = 0
        self.camera_fps = 0
        self.messages_received = 0
        self.messages_processed = 0
        self.messages_filtered = 0
        self.errors = 0
        self.joint_states: Dict[str, float] = {}
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
                "camera_detected": self.camera_detected,
                "camera_started": self.camera_started,
                "webrtc_state": self.webrtc_state,
                "fps": self.fps,
                "camera_fps": self.camera_fps,
                "messages_received": self.messages_received,
                "messages_processed": self.messages_processed,
                "messages_filtered": self.messages_filtered,
                "errors": self.errors,
                "joint_states": self.joint_states.copy(),
                "robot_uuid": self.robot_uuid,
                "robot_name": self.robot_name,
                "camera_uuid": self.camera_uuid,
                "camera_name": self.camera_name,
            }


def _status_logging_thread(
    status_tracker: StatusTracker,
    stop_event: threading.Event,
    camera_fps: int,
) -> None:
    """
    Thread that logs status information at 1 fps.

    Args:
        status_tracker: StatusTracker instance
        stop_event: Event to signal thread to stop
        camera_fps: Frames per second for camera streaming
    """
    status_tracker.camera_fps = camera_fps
    status_interval = 1.0  # Update status at 1 fps

    # Hide cursor and save position
    sys.stdout.write("\033[?25l")  # Hide cursor
    sys.stdout.flush()

    try:
        while not stop_event.is_set():
            status = status_tracker.get_status()

            # Build status display with fixed width lines
            lines = []
            lines.append("=" * 70)
            lines.append("SO101 Remote Operation Status".center(70))
            lines.append("=" * 70)

            # Twin info
            robot_name = status["robot_name"] or "N/A"
            camera_name = status["camera_name"] or "N/A"
            lines.append(f"Robot:  {robot_name} ({status['robot_uuid']})"[:70].ljust(70))
            lines.append(f"Camera: {camera_name} ({status['camera_uuid']})"[:70].ljust(70))
            lines.append("-" * 70)

            # Status indicators
            script_icon = "🟢" if status["script_started"] else "🟡"
            mqtt_icon = "🟢" if status["mqtt_connected"] else "🔴"
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
            stats = f"Cam:{status['camera_fps']} Recv:{status['messages_received']} Proc:{status['messages_processed']} Filt:{status['messages_filtered']} Err:{status['errors']}"
            lines.append(stats.ljust(70))
            lines.append("-" * 70)

            # Joint states
            if status["joint_states"]:
                index_to_name = status_tracker.joint_index_to_name
                joint_parts = []
                for joint_index in sorted(status["joint_states"].keys()):
                    position = status["joint_states"][joint_index]
                    joint_name = index_to_name.get(joint_index, joint_index)
                    short_name = joint_name[:3]
                    joint_parts.append(f"{short_name}:{position:5.1f}")
                lines.append(" ".join(joint_parts).ljust(70))
            else:
                lines.append("Joints: (waiting)".ljust(70))

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

        # Ensure MQTT is connected
        if not client.mqtt.connected:
            client.mqtt.connect()

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
                # WebRTC starts in connecting state when camera worker begins
                status_tracker.update_webrtc_state("connecting")

            # Run with auto-reconnect - this handles all command subscriptions internally
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
    except Exception:
        if status_tracker:
            status_tracker.increment_errors()
    finally:
        if loop is not None:
            try:
                loop.close()
            except Exception:
                pass


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

            # Convert radians to normalized position
            normalized_position = _radians_to_normalized(position_radians, norm_mode)

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

            # Update joint states in status tracker
            if status_tracker:
                joint_states = {str(joint_index): normalized_position}
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


def remoteoperate(
    client: Cyberwave,
    follower: SO101Follower,
    robot: Optional[Twin] = None,
    camera: Optional[Twin] = None,
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
        camera: Camera twin instance
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

    # Create status tracker
    status_tracker = StatusTracker()
    status_tracker.script_started = True

    # Set twin info for status display
    robot_uuid = robot.uuid if robot else ""
    robot_name = robot.name if robot and hasattr(robot, "name") else "so101-remote"
    camera_uuid_val = camera.uuid if camera else ""
    camera_name = camera.name if camera and hasattr(camera, "name") else "camera-remote"
    status_tracker.set_twin_info(robot_uuid, robot_name, camera_uuid_val, camera_name)

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

    # Create mapping from joint index (motor ID) to joint name
    joint_index_to_name = {motor.id: name for name, motor in follower.motors.items()}

    # Create mapping from joint indexes to joint names (for status display, string keys)
    joint_index_to_name_str = {str(motor.id): name for name, motor in follower.motors.items()}
    status_tracker.set_joint_index_to_name(joint_index_to_name_str)

    # Create mapping from joint names to normalization modes
    joint_name_to_norm_mode = {name: motor.norm_mode for name, motor in follower.motors.items()}

    # Initialize current state with follower's current observation
    current_state: Dict[str, float] = {}
    try:
        initial_obs = follower.get_observation()
        current_state.update(initial_obs)
    except Exception:
        # Initialize with empty state
        for name in follower.motors.keys():
            current_state[f"{name}.pos"] = 0.0

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
        args=(status_tracker, stop_event, camera_fps),
        daemon=True,
    )
    status_thread.start()

    # Start camera streaming thread if camera twin is provided
    camera_thread = None
    if camera is not None:
        # Determine camera_id from follower config or use provided camera_id
        actual_camera_id = camera_id
        if follower.config.cameras and len(follower.config.cameras) > 0:
            actual_camera_id = follower.config.cameras[0]

        camera_thread = threading.Thread(
            target=_camera_worker_thread,
            args=(
                client,
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

    # Create callback for joint state updates
    joint_state_callback = _create_joint_state_callback(
        current_state=current_state,
        action_queue=action_queue,
        joint_index_to_name=joint_index_to_name,
        joint_name_to_norm_mode=joint_name_to_norm_mode,
        follower=follower,
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

        # Stop camera streaming thread
        if camera_thread is not None:
            camera_thread.join(timeout=5.0)

        # Stop status thread
        if status_thread is not None:
            status_thread.join(timeout=1.0)


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
        "--camera-type",
        type=str,
        choices=["cv2", "realsense"],
        default="cv2",
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
        default="VGA",
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
        "--camera-config",
        type=str,
        default=None,
        help=f"Path to camera configuration JSON file. If provided, camera settings from the file "
        f"will be used instead of CLI arguments.",
    )
    parser.add_argument(
        "--list-realsense",
        action="store_true",
        help="List available RealSense devices and exit",
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

                detailed = RealSenseDiscovery.get_device_info(dev.serial_number)
                if detailed:
                    color_res = detailed.get_color_resolutions()
                    depth_res = detailed.get_depth_resolutions()
                    print(f"  Color Resolutions: {color_res}")
                    print(f"  Depth Resolutions: {depth_res}")
                print()
        sys.exit(0)

    # Default camera-uuid to twin-uuid if not provided
    if args.camera_uuid is None:
        args.camera_uuid = args.twin_uuid

    # Initialize Cyberwave client early to check for camera settings
    cyberwave_client = Cyberwave()

    # Load camera configuration: priority order is config file > auto-detect from asset > CLI args
    camera_config: Optional[RemoteoperateCameraConfig] = None
    camera_twin = None

    if args.camera_config:
        # Load from JSON file (highest priority)
        if not os.path.exists(args.camera_config):
            print(f"Error: Camera config file not found: {args.camera_config}")
            sys.exit(1)

        try:
            camera_config = RemoteoperateCameraConfig.load(args.camera_config)
            print(f"Loaded camera config from: {args.camera_config}")
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

        # Determine camera asset from config
        if camera_type == "realsense":
            camera_asset = "intel/realsensed455"
        else:
            camera_asset = "cyberwave/standard-cam"
    elif args.camera_uuid:
        # If camera-uuid is provided, auto-detect camera type from asset
        # Try to detect camera type by fetching the twin with different asset keys
        camera_asset = None

        # Try RealSense asset first
        try:
            camera_twin = cyberwave_client.twin(
                asset_key="intel/realsensed455", twin_id=args.camera_uuid, name="camera"
            )
            camera_asset = "intel/realsensed455"
        except Exception:
            # Try standard-cam asset
            try:
                camera_twin = cyberwave_client.twin(
                    asset_key="cyberwave/standard-cam", twin_id=args.camera_uuid, name="camera"
                )
                camera_asset = "cyberwave/standard-cam"
            except Exception as e:
                logger.debug(f"Could not fetch camera twin: {e}")
                # Fall back to detecting from args.camera_type if available
                if hasattr(args, "camera_type") and args.camera_type:
                    if args.camera_type == "realsense":
                        camera_asset = "intel/realsensed455"
                    else:
                        camera_asset = "cyberwave/standard-cam"
                else:
                    # Default to standard-cam if we can't determine
                    camera_asset = "cyberwave/standard-cam"
                # Create twin with detected/default asset
                camera_twin = cyberwave_client.twin(
                    asset_key=camera_asset, twin_id=args.camera_uuid, name="camera"
                )

        # Auto-detect camera type from asset and use defaults
        if camera_asset is None:
            camera_asset = "cyberwave/standard-cam"

        detected_type = _detect_camera_type_from_asset(camera_asset)
        camera_config = _get_default_camera_config(detected_type)

        camera_type = camera_config.camera_type
        camera_id = camera_config.camera_id
        camera_fps = camera_config.fps
        camera_resolution = camera_config.get_resolution()
        enable_depth = camera_config.enable_depth
        depth_fps = camera_config.depth_fps
        depth_resolution = camera_config.get_depth_resolution()
        depth_publish_interval = camera_config.depth_publish_interval

        logger.info(
            f"Auto-detected camera type '{detected_type}' from asset '{camera_asset}', using default settings"
        )
    else:
        # Use CLI arguments or defaults
        camera_type = args.camera_type
        camera_fps = args.camera_fps
        enable_depth = args.enable_depth
        depth_fps = args.depth_fps
        depth_publish_interval = args.depth_publish_interval

        # Parse camera_id (convert to int if it's a number)
        camera_id: Union[int, str] = args.camera_id
        try:
            camera_id = int(args.camera_id)
        except ValueError:
            camera_id = args.camera_id  # Keep as string (URL)

        # Parse resolutions
        camera_resolution = _parse_resolution(args.camera_resolution)
        depth_resolution = None
        if args.depth_resolution:
            depth_resolution = _parse_resolution(args.depth_resolution)

        # Determine camera asset based on camera_type
        if camera_type == "realsense":
            camera_asset = "intel/realsensed455"
        else:
            camera_asset = "cyberwave/standard-cam"

    robot = cyberwave_client.twin(
        asset_key="the-robot-studio/so101", twin_id=args.twin_uuid, name="robot"
    )
    camera = camera_twin

    # Initialize follower
    from config import FollowerConfig

    follower_config = FollowerConfig(
        port=args.follower_port,
        max_relative_target=args.max_relative_target,
        id=args.follower_id,
        cameras=[camera_id] if isinstance(camera_id, int) else [0],
    )
    follower = SO101Follower(config=follower_config)
    follower.connect()

    try:
        remoteoperate(
            client=cyberwave_client,
            follower=follower,
            robot=robot,
            camera=camera,
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
        mqtt_client = cyberwave_client.mqtt
        if mqtt_client is not None and mqtt_client.connected:
            mqtt_client.disconnect()


if __name__ == "__main__":
    main()
