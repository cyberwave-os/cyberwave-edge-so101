"""Camera configuration for SO101 teleoperation and remote operation."""

import json
import logging
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Optional, Union

from cyberwave.sensor import Resolution

from utils.config import get_setup_config_path

logger = logging.getLogger(__name__)

# RealSense optional
try:
    from cyberwave.sensor import RealSenseConfig

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None


def get_default_camera_config_path() -> str:
    """Get default path for camera config file."""
    return str(get_setup_config_path())


def detect_camera_type_from_asset(asset_key: str) -> str:
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


@dataclass
class CameraConfig:
    """Camera configuration for SO101 teleoperation and remote operation.

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

    def save(self, path: Optional[str] = None) -> None:
        """Save configuration to JSON file.

        Args:
            path: Path to save the configuration file (default: config from setup)
        """
        if path is None:
            path = get_default_camera_config_path()
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Camera configuration saved to %s", path)

    @classmethod
    def load(cls, path: Optional[str] = None) -> "CameraConfig":
        """Load configuration from JSON file.

        Args:
            path: Path to the configuration file (default: config from setup)

        Returns:
            CameraConfig instance

        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            json.JSONDecodeError: If the file is not valid JSON
        """
        if path is None:
            path = get_default_camera_config_path()
        with open(path, "r") as f:
            data = json.load(f)

        # Handle resolution conversion
        if "resolution" in data and isinstance(data["resolution"], str):
            width, height = map(int, data["resolution"].lower().split("x"))
            data["resolution"] = [width, height]

        if "depth_resolution" in data and isinstance(data["depth_resolution"], str):
            width, height = map(int, data["depth_resolution"].lower().split("x"))
            data["depth_resolution"] = [width, height]

        # Normalize enable_depth: handle string "true"/"false" from JSON
        if "enable_depth" in data and isinstance(data["enable_depth"], str):
            data["enable_depth"] = data["enable_depth"].lower() in ("true", "1", "yes")

        logger.info("Camera configuration loaded from %s", path)
        return cls(**data)

    @classmethod
    def from_realsense_device(
        cls,
        prefer_resolution: Resolution = Resolution.VGA,
        prefer_fps: int = 30,
        enable_depth: bool = True,
        serial_number: Optional[str] = None,
    ) -> "CameraConfig":
        """Create configuration from connected RealSense device.

        Auto-detects device capabilities and creates optimal configuration.

        Args:
            prefer_resolution: Preferred resolution (will find closest match)
            prefer_fps: Preferred FPS (will find closest match)
            enable_depth: Whether to enable depth streaming
            serial_number: Target device serial number (None = first device)

        Returns:
            CameraConfig configured for the device
        """
        if not _has_realsense or RealSenseConfig is None:
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
    ) -> "CameraConfig":
        """Create default CV2 camera configuration.

        Args:
            camera_id: Camera device ID or stream URL
            fps: Frames per second
            resolution: Video resolution

        Returns:
            CameraConfig for CV2 camera
        """
        return cls(
            camera_type="cv2",
            camera_id=camera_id,
            fps=fps,
            resolution=[resolution.width, resolution.height],
        )

    @classmethod
    def get_default(cls, camera_type: str) -> "CameraConfig":
        """Get default camera configuration based on camera type.

        Args:
            camera_type: Camera type ("cv2" or "realsense")

        Returns:
            CameraConfig with default settings
        """
        if camera_type == "realsense":
            return cls(
                camera_type="realsense",
                fps=30,
                resolution=[640, 480],
                enable_depth=True,
                depth_fps=15,
                depth_resolution=[640, 480],
                depth_publish_interval=30,
            )
        return cls.create_default_cv2()

    def __str__(self) -> str:
        res_str = f"{self.resolution[0]}x{self.resolution[1]}"
        if self.camera_type == "realsense" and self.enable_depth:
            depth_res = self.depth_resolution or self.resolution
            return (
                f"CameraConfig(type={self.camera_type}, "
                f"color={res_str}@{self.fps}fps, "
                f"depth={depth_res[0]}x{depth_res[1]}@{self.depth_fps}fps)"
            )
        return f"CameraConfig(type={self.camera_type}, id={self.camera_id}, {res_str}@{self.fps}fps)"
