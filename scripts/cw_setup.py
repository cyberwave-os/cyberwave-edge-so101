"""Setup SO101 configuration (robot twin + optional camera twins).

Run: python -m scripts.cw_setup [options]
Or: so101-setup [options] (if installed via pip install -e .)

Generates setup.json in ~/.cyberwave/so101_lib/ (same location as calibrate saves files).

Per-camera rows use :class:`so101.camera.CameraConfig` (parse with
:meth:`~so101.camera.CameraConfig.from_setup_camera_dict`, serialize with
:meth:`~so101.camera.CameraConfig.to_setup_camera_dict`). Standalone camera JSON
uses the same class with ``twin_uuid`` omitted; see
:func:`utils.config.get_camera_config_path`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from cyberwave.sensor import Resolution
from dotenv import load_dotenv

from so101.camera import CameraConfig, resolution_value_to_str
from utils.config import get_setup_config_path
from utils.utils import parse_resolution_to_enum

# RealSense optional
try:
    from cyberwave.sensor import RealSenseConfig, RealSenseDiscovery

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None
    RealSenseDiscovery = None


def so101_camera_name_override(camera_type: str) -> str | None:
    """Stream sensor key override for RealSense on SO101 (matches backend pairing)."""
    if "realsense" in str(camera_type).lower():
        return "default"
    return None


def so101_camera_asset_key(camera_type: str) -> str:
    """SDK asset key for a camera row in setup.json."""
    if "realsense" in str(camera_type).lower():
        return "intel/realsensed455"
    return "cyberwave/standard-cam"


@dataclass
class So101Config:
    """Typed view of SO101 ``setup.json``."""

    twin_uuid: str | None = None
    leader_port: str | None = None
    follower_port: str | None = None
    max_relative_target: float | None = None
    camera_only: bool = False
    mute_temperature_notifications: bool = False
    cameras: list[CameraConfig] = field(default_factory=list)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> So101Config:
        cameras: list[CameraConfig] = []
        for row in raw.get("cameras") or []:
            if isinstance(row, dict):
                c = CameraConfig.from_setup_camera_dict(row)
                if c is not None:
                    cameras.append(c)
        return cls(
            twin_uuid=raw.get("twin_uuid"),
            leader_port=raw.get("leader_port"),
            follower_port=raw.get("follower_port"),
            max_relative_target=raw.get("max_relative_target"),
            camera_only=bool(raw.get("camera_only", False)),
            mute_temperature_notifications=bool(raw.get("mute_temperature_notifications", False)),
            cameras=cameras,
        )

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "twin_uuid": self.twin_uuid,
            "max_relative_target": self.max_relative_target,
            "camera_only": self.camera_only,
            "mute_temperature_notifications": self.mute_temperature_notifications,
            "cameras": [c.to_setup_camera_dict() for c in self.cameras],
        }
        if self.leader_port is not None:
            d["leader_port"] = self.leader_port
        if self.follower_port is not None:
            d["follower_port"] = self.follower_port
        return d


def load_so101_config(path: Path | None = None) -> So101Config:
    """Load ``setup.json`` as :class:`So101Config`."""
    return So101Config.from_dict(load_setup_config(path))


def load_so101_config_for_robot_twin(twin_uuid: str, path: Path | None = None) -> So101Config:
    """Load setup when ``twin_uuid`` matches the file; otherwise return an empty config."""
    raw = load_setup_config(path)
    if raw.get("twin_uuid") != twin_uuid:
        return So101Config()
    return So101Config.from_dict(raw)


def save_so101_config(config: So101Config, path: Path | None = None) -> Path:
    """Persist :class:`So101Config` to JSON."""
    return save_setup_config(config.to_dict(), path)


def build_edge_hardware_dict(so101: So101Config) -> dict[str, Any]:
    """Hardware view for ``main`` (ports + env overrides, flat camera dicts)."""
    cameras = [c.to_hardware_dict() for c in so101.cameras]
    return {
        "follower_port": so101.follower_port or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT"),
        "leader_port": so101.leader_port or os.getenv("CYBERWAVE_METADATA_LEADER_PORT"),
        "max_relative_target": so101.max_relative_target
        or os.getenv("CYBERWAVE_METADATA_MAX_RELATIVE_TARGET"),
        "follower_id": "follower1",
        "leader_id": "leader1",
        "cameras": cameras,
    }


def materialize_camera_entries_for_cli(client: Any, cameras: list[CameraConfig]) -> list[dict[str, Any]]:
    """Build camera entries for standalone ``so101-teleoperate`` / ``so101-remoteoperate`` CLIs."""
    entries: list[dict[str, Any]] = []
    for c in cameras:
        twin = client.twin(twin_id=c.twin_uuid)
        entry: dict[str, Any] = {
            "twin": twin,
            "camera_id": c.camera_id,
            "camera_type": c.camera_type,
        }
        if c.resolution is not None:
            entry["camera_resolution"] = parse_resolution_to_enum(
                resolution_value_to_str(c.resolution, default_when_none="VGA")
            )
        if c.fps is not None:
            entry["fps"] = c.fps
        if c.fourcc is not None:
            entry["fourcc"] = c.fourcc
        if c.keyframe_interval is not None:
            entry["keyframe_interval"] = c.keyframe_interval
        name = so101_camera_name_override(c.camera_type)
        if name is not None:
            entry["camera_name"] = name
        if c.camera_type == "realsense":
            entry["enable_depth"] = c.enable_depth
            if c.depth_fps is not None:
                entry["depth_fps"] = c.depth_fps
            dr = c.depth_resolution
            if dr and len(dr) >= 2:
                entry["depth_resolution"] = Resolution.from_size(
                    dr[0], dr[1]
                ) or Resolution.closest(dr[0], dr[1])
            if c.depth_publish_interval is not None:
                entry["depth_publish_interval"] = c.depth_publish_interval
        entries.append(entry)
    return entries


def materialize_camera_entries_for_edge_operation(
    client: Any, cameras: list[CameraConfig]
) -> list[dict[str, Any]]:
    """Build camera entries for edge ``main`` teleoperate/remoteoperate (explicit asset + defaults)."""
    from utils.utils import parse_resolution_to_enum

    entries: list[dict[str, Any]] = []
    for c in cameras:
        camera_twin = client.twin(
            asset_key=so101_camera_asset_key(c.camera_type),
            twin_id=c.twin_uuid,
        )
        res_str = resolution_value_to_str(c.resolution, default_when_none="VGA")
        res_enum = parse_resolution_to_enum(res_str)
        entry: dict[str, Any] = {
            "twin": camera_twin,
            "camera_id": c.camera_id,
            "camera_type": c.camera_type,
            "camera_resolution": res_enum,
            "fps": c.fps if c.fps is not None else 30,
            "fourcc": c.fourcc,
            "keyframe_interval": c.keyframe_interval,
        }
        name = so101_camera_name_override(c.camera_type)
        if name is not None:
            entry["camera_name"] = name
        if c.camera_type == "realsense":
            entry["enable_depth"] = c.enable_depth
            entry["depth_fps"] = c.depth_fps if c.depth_fps is not None else 30
            entry["depth_resolution"] = c.depth_resolution
            entry["depth_publish_interval"] = (
                c.depth_publish_interval if c.depth_publish_interval is not None else 30
            )
        entries.append(entry)
    return entries


def materialize_idle_camera_twins(client: Any, cameras: list[CameraConfig]) -> list[Any]:
    """``(twin, overrides)`` pairs for idle camera streaming from setup."""
    pairs: list[Any] = []
    for c in cameras:
        cam_type = c.camera_type
        camera_twin = client.twin(
            asset_key=so101_camera_asset_key(cam_type),
            twin_id=c.twin_uuid,
        )
        overrides: dict[str, Any] = {
            "camera_id": c.camera_id,
            "camera_type": cam_type,
        }
        if c.resolution is not None:
            overrides["camera_resolution"] = parse_resolution_to_enum(
                resolution_value_to_str(c.resolution, default_when_none="VGA")
            )
        if c.fps is not None:
            overrides["fps"] = c.fps
        if c.fourcc is not None:
            overrides["fourcc"] = c.fourcc
        if c.keyframe_interval is not None:
            overrides["keyframe_interval"] = c.keyframe_interval
        name = so101_camera_name_override(cam_type)
        if name is not None:
            overrides["camera_name"] = name
        if cam_type == "realsense":
            overrides["enable_depth"] = c.enable_depth
            if c.depth_fps is not None:
                overrides["depth_fps"] = c.depth_fps
            if c.depth_resolution is not None:
                overrides["depth_resolution"] = c.depth_resolution
            if c.depth_publish_interval is not None:
                overrides["depth_publish_interval"] = c.depth_publish_interval
        pairs.append((camera_twin, overrides))
    return pairs


def _parse_resolution(res_str: str) -> list:
    """Parse resolution string to [width, height]."""
    res_str = res_str.strip().upper()
    if "X" in res_str:
        parts = res_str.split("X")
        return [int(parts[0]), int(parts[1])]
    # Preset names
    presets = {
        "QVGA": [320, 240],
        "VGA": [640, 480],
        "SVGA": [800, 600],
        "HD": [1280, 720],
        "FULL_HD": [1920, 1080],
    }
    if res_str in presets:
        return presets[res_str]
    raise ValueError(f"Unknown resolution: {res_str}")


def create_setup_config(
    twin_uuid: str | None = None,
    cameras: list[dict] | None = None,
) -> dict:
    """Build setup config dict.

    Each camera dict: twin_uuid (required), camera_type, camera_id, and optional
    resolution, fps, fourcc, keyframe_interval, enable_depth, depth_*.

    Streaming policy (resolution, fps, fourcc) defaults to SDK behavior unless explicitly set.

    twin_uuid: SO101 robot twin UUID (main device).
    cameras: List of camera configs. Each dict needs at minimum: twin_uuid, camera_type, camera_id.
    """
    config: dict = {
        "twin_uuid": twin_uuid,
        "max_relative_target": None,
        "camera_only": False,
        "mute_temperature_notifications": False,
        "cameras": cameras or [],
    }
    return config


def save_setup_config(config: dict, path: Path | None = None) -> Path:
    """Save setup config to JSON file."""
    path = path or get_setup_config_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(config, f, indent=2)
    return path


def load_setup_config(path: Path | None = None) -> dict:
    """Load setup config from JSON file."""
    path = path or get_setup_config_path()
    if not path.exists():
        return {}
    with open(path, "r") as f:
        return json.load(f)


def update_setup_port(port_type: str, port: str, path: Path | None = None) -> Path:
    """Update a port in setup.json (called by calibrate). Preserves existing config."""
    config = load_setup_config(path)
    if port_type == "leader":
        config["leader_port"] = port
    elif port_type == "follower":
        config["follower_port"] = port
    else:
        raise ValueError(f"port_type must be 'leader' or 'follower', got {port_type!r}")
    return save_setup_config(config, path)


def main() -> int:
    """Main entry point."""
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Setup SO101 configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # SO101 only (no cameras) - store twin for teleoperate
  so101-setup --twin-uuid <uuid>

  # SO101 with single camera
  so101-setup --twin-uuid <so101-uuid> --camera-twin-uuid <cam-uuid> --camera-id 0

  # SO101 with multiple cameras
  so101-setup --twin-uuid <so101-uuid> \\
      --camera-twin-uuid <cam1-uuid> --camera-id 0 \\
      --camera-twin-uuid <cam2-uuid> --camera-id 1 --camera-type realsense
        """,
    )

    parser.add_argument(
        "--twin-uuid",
        type=str,
        required=True,
        help="SO101 robot twin UUID (main device)",
    )
    parser.add_argument(
        "--leader-port",
        type=str,
        default=None,
        help="Leader serial port (uses saved port from calibrate if not provided)",
    )
    parser.add_argument(
        "--follower-port",
        type=str,
        default=None,
        help="Follower serial port (uses saved port from calibrate if not provided)",
    )
    parser.add_argument(
        "--camera-twin-uuid",
        type=str,
        action="append",
        dest="camera_twin_uuids",
        help="Camera twin UUID (can be specified multiple times for multiple cameras)",
    )
    parser.add_argument(
        "--camera-id",
        type=str,
        action="append",
        dest="camera_ids",
        help="Camera device ID (pairs with --camera-twin-uuid, default: 0, 1, 2...)",
    )
    parser.add_argument(
        "--camera-type",
        type=str,
        action="append",
        choices=["cv2", "realsense"],
        dest="camera_types",
        help="Camera type: cv2 or realsense (pairs with --camera-twin-uuid, default: cv2)",
    )
    parser.add_argument(
        "--camera-resolution",
        type=str,
        action="append",
        dest="camera_resolutions",
        help="Camera resolution (pairs with --camera-twin-uuid, omit for SDK default)",
    )
    parser.add_argument(
        "--camera-fps",
        type=int,
        action="append",
        dest="camera_fps_list",
        help="Camera FPS (pairs with --camera-twin-uuid, omit for SDK default)",
    )
    parser.add_argument(
        "--camera-fourcc",
        type=str,
        action="append",
        dest="camera_fourccs",
        help="Camera FOURCC for CV2 (pairs with --camera-twin-uuid, omit for SDK default)",
    )
    parser.add_argument(
        "--camera-keyframe-interval",
        type=int,
        action="append",
        dest="camera_keyframe_intervals",
        help="Keyframe interval (pairs with --camera-twin-uuid, omit for SDK default)",
    )
    parser.add_argument(
        "--enable-depth",
        action="store_true",
        help="Enable depth for all RealSense cameras (per-camera via JSON if needed)",
    )
    parser.add_argument(
        "--depth-fps",
        type=int,
        default=30,
        help="Depth FPS when --enable-depth is set (default: 30)",
    )
    parser.add_argument(
        "--depth-resolution",
        type=str,
        default=None,
        help="Depth resolution when --enable-depth is set (preset or WIDTHxHEIGHT)",
    )
    parser.add_argument(
        "--depth-publish-interval",
        type=int,
        default=30,
        help="Publish depth every N frames when --enable-depth (default: 30)",
    )
    parser.add_argument(
        "--max-relative-target",
        type=float,
        default=None,
        help="Max change per update for follower safety (default: None)",
    )
    parser.add_argument(
        "--camera-only",
        action="store_true",
        help="Camera streaming only, no teleop loop",
    )
    parser.add_argument(
        "--mute-temperature-notifications",
        action="store_true",
        help="Do not create motor overheating alerts (temperature still shown in status)",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default=None,
        help=f"Output path (default: {get_setup_config_path()})",
    )

    args = parser.parse_args()

    # Validate
    if not args.camera_twin_uuids:
        print("No cameras specified. Run 'so101-setup --twin-uuid <uuid>' for SO101-only config.")

    # Parse camera_id
    def parse_camera_id(s: str) -> int | str:
        try:
            return int(s)
        except ValueError:
            return s

    # Build cameras list
    cameras_list: list[dict] = []
    camera_twin_uuids = args.camera_twin_uuids or []
    camera_ids = args.camera_ids or []
    camera_types = args.camera_types or []
    camera_resolutions = args.camera_resolutions or []
    camera_fps_list = args.camera_fps_list or []
    camera_fourccs = args.camera_fourccs or []
    camera_keyframe_intervals = args.camera_keyframe_intervals or []

    for i, twin_uuid_val in enumerate(camera_twin_uuids):
        cam_id = parse_camera_id(camera_ids[i]) if i < len(camera_ids) else i
        cam_type = camera_types[i] if i < len(camera_types) else "cv2"
        cam_entry: dict = {
            "twin_uuid": twin_uuid_val,
            "camera_type": cam_type,
            "camera_id": cam_id,
        }
        if i < len(camera_resolutions) and camera_resolutions[i]:
            cam_entry["resolution"] = _parse_resolution(camera_resolutions[i])
        if i < len(camera_fps_list) and camera_fps_list[i]:
            cam_entry["fps"] = camera_fps_list[i]
        if i < len(camera_fourccs) and camera_fourccs[i]:
            cam_entry["fourcc"] = camera_fourccs[i]
        if i < len(camera_keyframe_intervals) and camera_keyframe_intervals[i]:
            cam_entry["keyframe_interval"] = camera_keyframe_intervals[i]
        if cam_type == "realsense" and args.enable_depth:
            cam_entry["enable_depth"] = True
            cam_entry["depth_fps"] = args.depth_fps
            if args.depth_resolution:
                cam_entry["depth_resolution"] = _parse_resolution(args.depth_resolution)
            cam_entry["depth_publish_interval"] = args.depth_publish_interval
        cameras_list.append(cam_entry)

    config = create_setup_config(
        twin_uuid=args.twin_uuid,
        cameras=cameras_list,
    )

    output_path = Path(args.output).expanduser() if args.output else get_setup_config_path()
    existing = load_setup_config(output_path)
    leader_port = args.leader_port or existing.get("leader_port")
    follower_port = args.follower_port or existing.get("follower_port")
    if leader_port:
        config["leader_port"] = leader_port
    if follower_port:
        config["follower_port"] = follower_port
    config["max_relative_target"] = args.max_relative_target
    config["camera_only"] = args.camera_only
    config["mute_temperature_notifications"] = (
        getattr(args, "mute_temperature_notifications", False)
        or existing.get("mute_temperature_notifications", False)
    )
    save_setup_config(config, output_path)

    print(f"Setup configuration saved to: {output_path}")
    print(json.dumps(config, indent=2))
    print("\nUse with teleoperate (loads setup by default):")
    print("  so101-teleoperate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
