"""Camera configuration for SO101 teleoperation and remote operation.

Each ``setup.json`` ``cameras[]`` row is a :class:`CameraConfig` instance (with
``twin_uuid`` set). Standalone ``camera_config.json`` uses the same class with
``twin_uuid`` omitted; see :func:`utils.config.get_camera_config_path`.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, replace
from typing import Any, Dict, List, Optional, Union

from cyberwave.sensor import Resolution

from utils.config import get_camera_config_path

logger = logging.getLogger(__name__)

# RealSense optional
try:
    from cyberwave.sensor import RealSenseConfig

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None


def get_default_camera_config_path() -> str:
    """Default path for standalone camera JSON (not ``setup.json``)."""
    return str(get_camera_config_path())


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


def _resolution_value_to_str(
    resolution: Union[list, str, None],
    *,
    default_when_none: str = "VGA",
) -> str:
    if resolution is None:
        return default_when_none
    if isinstance(resolution, (list, tuple)) and len(resolution) >= 2:
        return f"{int(resolution[0])}x{int(resolution[1])}"
    return str(resolution)


def resolution_value_to_str(
    resolution: Union[list, str, None],
    *,
    default_when_none: str = "VGA",
) -> str:
    """Public helper for CLI/setup materializers (WIDTHxHEIGHT or preset name)."""
    return _resolution_value_to_str(resolution, default_when_none=default_when_none)


@dataclass
class CameraConfig:
    """Per-camera settings for SO101 (standalone JSON or one ``setup.json`` cameras[] row).

    Attributes:
        twin_uuid: Camera twin UUID when this row comes from ``setup.json``; ``None`` for standalone file only.
        camera_type, camera_id, fps, resolution: Core streaming (``fps`` / ``resolution`` may be omitted in JSON = SDK defaults at runtime).
        fourcc, keyframe_interval: Optional CV2 / stream-manager overrides (common in ``setup.json``).
        enable_depth, depth_*: RealSense depth; ``serial_number`` for device selection.
    """

    twin_uuid: Optional[str] = None
    camera_type: str = "cv2"
    camera_id: Union[int, str] = 0
    fps: Optional[int] = None
    resolution: Optional[Union[list, str]] = None
    enable_depth: bool = False
    depth_fps: Optional[int] = None
    depth_resolution: Optional[list] = None
    depth_publish_interval: Optional[int] = None
    serial_number: Optional[str] = None
    fourcc: Optional[str] = None
    keyframe_interval: Optional[int] = None

    def _default_resolution_list(self) -> List[int]:
        if self.resolution is None:
            return [640, 480]
        if isinstance(self.resolution, str):
            from utils.utils import parse_resolution_to_enum

            r = parse_resolution_to_enum(_resolution_value_to_str(self.resolution))
            return [r.width, r.height]
        return [int(self.resolution[0]), int(self.resolution[1])]

    def get_resolution(self) -> Resolution:
        """Resolve color resolution as :class:`Resolution`."""
        w, h = self._default_resolution_list()
        match = Resolution.from_size(w, h)
        if match:
            return match
        return Resolution.closest(w, h)

    def get_depth_resolution(self) -> Optional[Resolution]:
        """Depth resolution as :class:`Resolution`, if set."""
        if self.depth_resolution is None:
            return None
        width, height = int(self.depth_resolution[0]), int(self.depth_resolution[1])
        match = Resolution.from_size(width, height)
        if match:
            return match
        return Resolution.closest(width, height)

    @classmethod
    def from_setup_camera_dict(cls, row: dict[str, Any]) -> Optional[CameraConfig]:
        """Parse one ``setup.json`` ``cameras[]`` dict."""
        uuid = row.get("twin_uuid")
        if not uuid:
            return None
        cam_type = row.get("camera_type", "cv2")
        depth_res = row.get("depth_resolution")
        if depth_res is not None and not isinstance(depth_res, list):
            depth_res = None
        return cls(
            twin_uuid=str(uuid),
            camera_type=str(cam_type),
            camera_id=row.get("camera_id", 0),
            resolution=row.get("resolution"),
            fps=row.get("fps"),
            fourcc=row.get("fourcc"),
            keyframe_interval=row.get("keyframe_interval"),
            enable_depth=bool(row.get("enable_depth", False)) if cam_type == "realsense" else False,
            depth_fps=row.get("depth_fps"),
            depth_resolution=depth_res,
            depth_publish_interval=row.get("depth_publish_interval"),
            serial_number=row.get("serial_number"),
        )

    def to_setup_camera_dict(self) -> Dict[str, Any]:
        """Serialize for ``setup.json`` ``cameras[]`` (omit unset optionals)."""
        if not self.twin_uuid:
            raise ValueError("to_setup_camera_dict requires twin_uuid")
        d: Dict[str, Any] = {
            "twin_uuid": self.twin_uuid,
            "camera_type": self.camera_type,
            "camera_id": self.camera_id,
        }
        if self.resolution is not None:
            d["resolution"] = self.resolution
        if self.fps is not None:
            d["fps"] = self.fps
        if self.fourcc is not None:
            d["fourcc"] = self.fourcc
        if self.keyframe_interval is not None:
            d["keyframe_interval"] = self.keyframe_interval
        if self.camera_type == "realsense":
            if self.enable_depth:
                d["enable_depth"] = True
            if self.depth_fps is not None:
                d["depth_fps"] = self.depth_fps
            if self.depth_resolution is not None:
                d["depth_resolution"] = self.depth_resolution
            if self.depth_publish_interval is not None:
                d["depth_publish_interval"] = self.depth_publish_interval
            if self.serial_number is not None:
                d["serial_number"] = self.serial_number
        return d

    def to_hardware_dict(self) -> Dict[str, Any]:
        """Dict for ``main`` hardware / idle streaming."""
        if not self.twin_uuid:
            raise ValueError("to_hardware_dict requires twin_uuid")
        cam: Dict[str, Any] = {
            "twin_uuid": self.twin_uuid,
            "camera_type": self.camera_type,
            "camera_id": self.camera_id,
        }
        if self.resolution is not None:
            cam["resolution"] = self.resolution
        if self.fps is not None:
            cam["fps"] = self.fps
        if self.fourcc is not None:
            cam["fourcc"] = self.fourcc
        if self.keyframe_interval is not None:
            cam["keyframe_interval"] = self.keyframe_interval
        if self.camera_type == "realsense":
            cam["enable_depth"] = self.enable_depth
            if self.depth_fps is not None:
                cam["depth_fps"] = self.depth_fps
            if self.depth_resolution is not None:
                cam["depth_resolution"] = self.depth_resolution
            if self.depth_publish_interval is not None:
                cam["depth_publish_interval"] = self.depth_publish_interval
        return cam

    def to_runtime_camera_config(self) -> CameraConfig:
        """Densify streaming defaults and clear ``twin_uuid`` (SDK / standalone shape)."""
        from utils.utils import parse_resolution_to_enum

        res = self.resolution
        if res is None:
            res_list: List[int] = [640, 480]
        elif isinstance(res, (list, tuple)) and len(res) >= 2:
            res_list = [int(res[0]), int(res[1])]
        else:
            r_enum = parse_resolution_to_enum(_resolution_value_to_str(res, default_when_none="VGA"))
            res_list = [r_enum.width, r_enum.height]
        depth_res = self.depth_resolution
        depth_list: Optional[List[int]] = (
            [int(depth_res[0]), int(depth_res[1])]
            if depth_res is not None and len(depth_res) >= 2
            else None
        )
        return replace(
            self,
            twin_uuid=None,
            fps=self.fps if self.fps is not None else 30,
            resolution=res_list,
            enable_depth=self.enable_depth,
            depth_fps=self.depth_fps if self.depth_fps is not None else 30,
            depth_resolution=depth_list,
            depth_publish_interval=self.depth_publish_interval
            if self.depth_publish_interval is not None
            else 30,
            serial_number=None,
        )

    def to_setup_camera_row(
        self,
        twin_uuid: str,
        *,
        fourcc: Optional[str] = None,
        keyframe_interval: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Build one ``setup.json`` cameras[] dict (optional overrides for CLI helpers)."""
        merged = replace(
            self,
            twin_uuid=str(twin_uuid),
            fourcc=fourcc if fourcc is not None else self.fourcc,
            keyframe_interval=keyframe_interval
            if keyframe_interval is not None
            else self.keyframe_interval,
        )
        return merged.to_setup_camera_dict()

    @classmethod
    def for_setup_row_from_runtime_config(
        cls,
        twin_uuid: str,
        cfg: CameraConfig,
        *,
        fourcc: Optional[str] = None,
        keyframe_interval: Optional[int] = None,
    ) -> CameraConfig:
        """Build a ``setup.json`` row from a runtime (often twin-less) :class:`CameraConfig`."""
        if not isinstance(cfg, CameraConfig):
            raise TypeError(f"expected CameraConfig, got {type(cfg).__name__}")
        is_rs = cfg.camera_type == "realsense"
        depth_res = cfg.depth_resolution if is_rs else None
        depth_list: Optional[List[int]] = (
            [int(depth_res[0]), int(depth_res[1])]
            if depth_res is not None and len(depth_res) >= 2
            else None
        )
        res = cfg.resolution
        res_out: Optional[Union[list, str]] = (
            list(res) if isinstance(res, (list, tuple)) and len(res) >= 2 else res
        )
        return cls(
            twin_uuid=str(twin_uuid),
            camera_type=cfg.camera_type,
            camera_id=cfg.camera_id,
            resolution=res_out,
            fps=cfg.fps,
            fourcc=fourcc if fourcc is not None else cfg.fourcc,
            keyframe_interval=keyframe_interval if keyframe_interval is not None else cfg.keyframe_interval,
            enable_depth=bool(cfg.enable_depth) if is_rs else False,
            depth_fps=cfg.depth_fps if is_rs else None,
            depth_resolution=depth_list,
            depth_publish_interval=cfg.depth_publish_interval if is_rs else None,
            serial_number=cfg.serial_number if is_rs else None,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Full dict (includes ``None`` fields). Prefer :meth:`to_setup_camera_dict` for ``setup.json`` rows."""
        return asdict(self)

    def save(self, path: Optional[str] = None) -> None:
        """Save configuration to JSON file."""
        if path is None:
            path = get_default_camera_config_path()
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info("Camera configuration saved to %s", path)

    @classmethod
    def load(cls, path: Optional[str] = None) -> CameraConfig:
        """Load standalone camera JSON; densify missing ``fps`` / ``resolution`` defaults."""
        if path is None:
            path = get_default_camera_config_path()
        with open(path, "r") as f:
            data = json.load(f)

        if "resolution" in data and isinstance(data["resolution"], str):
            if "x" in data["resolution"].lower():
                width, height = map(int, data["resolution"].lower().split("x"))
                data["resolution"] = [width, height]

        if "depth_resolution" in data and isinstance(data["depth_resolution"], str):
            if "x" in data["depth_resolution"].lower():
                width, height = map(int, data["depth_resolution"].lower().split("x"))
                data["depth_resolution"] = [width, height]

        if "enable_depth" in data and isinstance(data["enable_depth"], str):
            data["enable_depth"] = data["enable_depth"].lower() in ("true", "1", "yes")

        if data.get("resolution") is None:
            data["resolution"] = [640, 480]
        if data.get("fps") is None:
            data["fps"] = 30
        if data.get("depth_fps") is None and str(data.get("camera_type", "cv2")) == "realsense":
            data.setdefault("depth_fps", 30)
        if data.get("depth_publish_interval") is None and str(data.get("camera_type", "cv2")) == "realsense":
            data.setdefault("depth_publish_interval", 30)

        logger.info("Camera configuration loaded from %s", path)
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})

    @classmethod
    def from_realsense_device(
        cls,
        prefer_resolution: Resolution = Resolution.VGA,
        prefer_fps: int = 30,
        enable_depth: bool = True,
        serial_number: Optional[str] = None,
    ) -> CameraConfig:
        """Create configuration from connected RealSense device."""
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
            twin_uuid=None,
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
    ) -> CameraConfig:
        """Create default CV2 camera configuration."""
        return cls(
            twin_uuid=None,
            camera_type="cv2",
            camera_id=camera_id,
            fps=fps,
            resolution=[resolution.width, resolution.height],
        )

    @classmethod
    def get_default(cls, camera_type: str) -> CameraConfig:
        """Default camera config by type."""
        if camera_type == "realsense":
            return cls(
                twin_uuid=None,
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
        res = self.resolution
        if res is None:
            res_str = "default"
        elif isinstance(res, (list, tuple)):
            res_str = f"{res[0]}x{res[1]}"
        else:
            res_str = str(res)
        fps_str = "default" if self.fps is None else str(self.fps)
        if self.camera_type == "realsense" and self.enable_depth:
            depth_res = self.depth_resolution or self._default_resolution_list()
            depth_fps_str = "default" if self.depth_fps is None else str(self.depth_fps)
            return (
                f"CameraConfig(type={self.camera_type}, "
                f"color={res_str}@{fps_str}fps, "
                f"depth={depth_res[0]}x{depth_res[1]}@{depth_fps_str}fps)"
            )
        return f"CameraConfig(type={self.camera_type}, id={self.camera_id}, {res_str}@{fps_str}fps)"
