"""Status tracking components for SO101 teleoperation and remote operation."""

import sys
import threading
import time
from typing import Any, Dict, List, Literal, Optional

from utils.temperature import read_temperatures


class StatusTracker:
    """Thread-safe status tracker for teleoperation and remote operation systems.

    Supports both teleoperate (leader+follower, messages_produced) and remoteoperate
    (follower only, messages_received/messages_processed) modes. Unused fields remain 0.
    """

    def __init__(self):
        self.lock = threading.Lock()
        self.script_started = False
        self.mqtt_connected = False
        self.camera_enabled = False  # Whether camera streaming was configured at all
        # Per-camera: camera_name -> {detected, started, webrtc_state}
        self.camera_states: Dict[str, Dict[str, Any]] = {}
        self.fps = 0
        self.camera_fps = 0
        # Teleoperate: produced; Remoteoperate: received, processed
        self.messages_produced = 0
        self.messages_received = 0
        self.messages_processed = 0
        self.messages_filtered = 0
        self.errors = 0
        self.joint_states: Dict[str, float] = {}
        self.joint_temperatures: Dict[str, float] = {}  # "leader_1", "follower_1" -> temperature
        self.joint_index_to_name: Dict[str, str] = {}
        self.robot_uuid: str = ""
        self.robot_name: str = ""
        self.camera_infos: List[Dict[str, str]] = []  # [{uuid, name}, ...] per camera

    def update_mqtt_status(self, connected: bool) -> None:
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

    def update_camera_status(
        self, camera_name: str, detected: bool, started: bool = False
    ) -> None:
        with self.lock:
            if camera_name not in self.camera_states:
                self.camera_states[camera_name] = {
                    "detected": False,
                    "started": False,
                    "webrtc_state": "idle",
                }
            self.camera_states[camera_name]["detected"] = detected
            self.camera_states[camera_name]["started"] = started

    def update_webrtc_state(self, camera_name: str, state: str) -> None:
        """Update WebRTC state for a camera: 'idle', 'connecting', or 'streaming'."""
        with self.lock:
            if camera_name not in self.camera_states:
                self.camera_states[camera_name] = {
                    "detected": False,
                    "started": False,
                    "webrtc_state": "idle",
                }
            self.camera_states[camera_name]["webrtc_state"] = state

    def increment_produced(self) -> None:
        with self.lock:
            self.messages_produced += 1

    def increment_received(self) -> None:
        with self.lock:
            self.messages_received += 1

    def increment_processed(self) -> None:
        with self.lock:
            self.messages_processed += 1

    def increment_filtered(self) -> None:
        with self.lock:
            self.messages_filtered += 1

    def increment_errors(self) -> None:
        with self.lock:
            self.errors += 1

    def update_joint_states(self, states: Dict[str, float]) -> None:
        """Merge new joint states with existing ones (doesn't replace)."""
        with self.lock:
            self.joint_states.update(states)

    def update_joint_temperatures(self, temperatures: Dict[str, float]) -> None:
        """Merge new joint temperatures with existing ones (doesn't replace)."""
        with self.lock:
            self.joint_temperatures.update(temperatures)

    def set_joint_index_to_name(self, mapping: Dict[str, str]) -> None:
        """Set mapping from joint index to joint name."""
        with self.lock:
            self.joint_index_to_name = mapping.copy()

    def set_twin_info(
        self, robot_uuid: str, robot_name: str, camera_uuid: str, camera_name: str
    ) -> None:
        """Set twin information for display (legacy single-camera)."""
        with self.lock:
            self.robot_uuid = robot_uuid
            self.robot_name = robot_name
            if not self.camera_infos:
                self.camera_infos = [{"uuid": camera_uuid, "name": camera_name}]

    def get_status(self) -> Dict[str, Any]:
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
                "messages_produced": self.messages_produced,
                "messages_received": self.messages_received,
                "messages_processed": self.messages_processed,
                "messages_filtered": self.messages_filtered,
                "errors": self.errors,
                "joint_states": self.joint_states.copy(),
                "joint_temperatures": self.joint_temperatures.copy(),
                "robot_uuid": self.robot_uuid,
                "robot_name": self.robot_name,
            }


def run_status_logging_thread(
    status_tracker: StatusTracker,
    stop_event: threading.Event,
    fps: int,
    camera_fps: int,
    *,
    leader: Optional[Any] = None,
    follower: Optional[Any] = None,
    mode: Literal["teleoperate", "remoteoperate"] = "teleoperate",
) -> None:
    """
    Thread that logs status information at 1 fps.

    Args:
        status_tracker: StatusTracker instance
        stop_event: Event to signal thread to stop
        fps: Target frames per second for the operation loop
        camera_fps: Frames per second for camera streaming
        leader: Optional SO101Leader instance for reading temperatures (teleoperate only)
        follower: Optional SO101Follower instance for reading temperatures
        mode: "teleoperate" (Prod/Filt stats, L+F temps) or "remoteoperate" (Recv/Proc/Filt, F temps)
    """
    status_tracker.fps = fps
    status_tracker.camera_fps = camera_fps
    status_interval = 1.0  # Update status at 1 fps

    # Hide cursor
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()

    try:
        while not stop_event.is_set():
            joint_index_to_name = status_tracker.joint_index_to_name
            temperatures = read_temperatures(
                leader=leader, follower=follower, joint_index_to_name=joint_index_to_name
            )
            if temperatures:
                status_tracker.update_joint_temperatures(temperatures)

            status = status_tracker.get_status()

            lines = []
            lines.append("=" * 70)
            title = (
                "SO101 Teleoperation Status"
                if mode == "teleoperate"
                else "SO101 Remote Operation Status"
            )
            lines.append(title.center(70))
            lines.append("=" * 70)

            # Twin info
            robot_name = status["robot_name"] or "N/A"
            lines.append(f"Robot:  {robot_name} ({status['robot_uuid']})"[:70].ljust(70))
            if status["camera_enabled"]:
                camera_infos = status.get("camera_infos", [])
                camera_states = status.get("camera_states", {})
                for info in camera_infos:
                    cam_name = info.get("name", "default")
                    cam_uuid = info.get("uuid", "")
                    state = camera_states.get(cam_name, {})
                    detected = state.get("detected", False)
                    started = state.get("started", False)
                    webrtc = state.get("webrtc_state", "idle")
                    cam_icon = "🟢" if (detected and started) else ("🟡" if detected else "🔴")
                    webrtc_icon = (
                        "🟢" if webrtc == "streaming" else ("🟡" if webrtc == "connecting" else "🔴")
                    )
                    lines.append(
                        f"  {cam_name}: Cam{cam_icon} WebRTC{webrtc_icon} ({cam_uuid}...)"[:70].ljust(
                            70
                        )
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
            if mode == "teleoperate":
                stats = (
                    f"FPS:{status['fps']} Cam:{status['camera_fps']} "
                    f"Prod:{status['messages_produced']} Filt:{status['messages_filtered']} "
                    f"Err:{status['errors']}"
                )
            else:
                stats = (
                    f"FPS:{status['fps']} Cam:{status['camera_fps']} "
                    f"Recv:{status['messages_received']} Proc:{status['messages_processed']} "
                    f"Filt:{status['messages_filtered']} Err:{status['errors']}"
                )
            lines.append(stats.ljust(70))
            lines.append("-" * 70)

            # Joint states
            index_to_name = status_tracker.joint_index_to_name
            joint_states = status["joint_states"]
            joint_temps = status["joint_temperatures"]

            def _format_motor_line(
                joint_index: str,
                joint_name: str,
                position: Optional[float],
                mode: str,
                temps: Dict[str, float],
            ) -> str:
                pos_str = f"{position:6.3f}rad" if position is not None else "   waiting"
                if mode == "teleoperate":
                    leader_temp = temps.get(f"leader_{joint_index}")
                    follower_temp = temps.get(f"follower_{joint_index}")
                    temp_parts = []
                    if leader_temp is not None:
                        leader_indicator = "🔥" if leader_temp > 40 else ""
                        temp_parts.append(f"L:{leader_temp:3.0f}°C{leader_indicator}")
                    if follower_temp is not None:
                        follower_indicator = "🔥" if follower_temp > 40 else ""
                        temp_parts.append(f"F:{follower_temp:3.0f}°C{follower_indicator}")
                    temp_str = " ".join(temp_parts) if temp_parts else "N/A"
                else:
                    follower_temp = temps.get(f"follower_{joint_index}")
                    temp_str = "N/A"
                    if follower_temp is not None:
                        follower_indicator = "🔥" if follower_temp > 40 else ""
                        temp_str = f"F:{follower_temp:3.0f}°C{follower_indicator}"
                return f"  {joint_name:16s}  pos={pos_str:>10s}  {temp_str}"

            if joint_states:
                lines.append("Motors:".ljust(70))
                for joint_index in sorted(joint_states.keys()):
                    position = joint_states[joint_index]
                    joint_name = index_to_name.get(joint_index, joint_index)
                    lines.append(
                        _format_motor_line(
                            joint_index, joint_name, position, mode, joint_temps
                        )[:70].ljust(70)
                    )
            elif index_to_name:
                # Show motor names with "waiting" for position when no joint_states yet
                lines.append("Motors:".ljust(70))
                for joint_index in sorted(index_to_name.keys()):
                    joint_name = index_to_name[joint_index]
                    lines.append(
                        _format_motor_line(
                            joint_index, joint_name, None, mode, joint_temps
                        )[:70].ljust(70)
                    )
            else:
                lines.append(
                    ("Motors: (waiting)" if mode == "teleoperate" else "Motors:").ljust(70)
                )

            lines.append("=" * 70)
            lines.append("Press 'q' to stop".ljust(70))

            output = "\033[2J\033[H" + "\r\n".join(lines)
            try:
                sys.stdout.write(output)
                sys.stdout.flush()
            except (IOError, OSError):
                pass

            time.sleep(status_interval)
    finally:
        sys.stdout.write("\033[?25h")
        sys.stdout.flush()
