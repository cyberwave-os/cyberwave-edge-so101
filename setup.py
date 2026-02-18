"""Setup SO101 configuration: wrist camera and additional cameras.

Run: python so101_setup.py [options]
Or: so101-setup [options] (if installed via pip install -e .)

Generates setup.json in ~/.cyberwave/so101_lib/ (same location as calibrate saves files).
Replaces --generate-camera-config from teleoperate.
"""

import argparse
import json
import sys
from pathlib import Path

from config import get_setup_config_path
from dotenv import load_dotenv

# RealSense optional
try:
    from cyberwave.sensor import RealSenseConfig, RealSenseDiscovery

    _has_realsense = True
except ImportError:
    _has_realsense = False
    RealSenseConfig = None
    RealSenseDiscovery = None


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
    wrist_camera: bool = False,
    wrist_camera_twin_uuid: str | None = None,
    wrist_camera_id: int | str = 0,
    additional_camera_type: str | None = None,
    additional_camera_id: int | str = 1,
    additional_camera_twin_uuid: str | None = None,
    camera_fps: int = 30,
    resolution: str = "VGA",
    enable_depth: bool = False,
    depth_fps: int = 30,
    depth_resolution: str | None = None,
    depth_publish_interval: int = 30,
    auto_detect: bool = False,
) -> dict:
    """Build setup config dict.

    twin_uuid: SO101 robot twin UUID (used by teleoperate when --setup, no other args).
    """
    config: dict = {
        "twin_uuid": twin_uuid or wrist_camera_twin_uuid,
        "camera_fps": camera_fps,
        "max_relative_target": None,
        "camera_only": False,
        "wrist_camera": wrist_camera,
        "wrist_camera_id": wrist_camera_id,
        "wrist_camera_twin_uuid": wrist_camera_twin_uuid,
        "wrist_camera_name": "wrist_camera",
        "additional_cameras": [],
    }

    if additional_camera_type:
        add_cam: dict = {
            "camera_type": additional_camera_type,
            "camera_id": additional_camera_id,
            "camera_name": "external",
            "twin_uuid": additional_camera_twin_uuid,
            "fps": camera_fps,
            "resolution": _parse_resolution(resolution),
            "enable_depth": enable_depth,
            "depth_fps": depth_fps,
            "depth_resolution": (
                _parse_resolution(depth_resolution) if depth_resolution else None
            ),
            "depth_publish_interval": depth_publish_interval,
        }
        config["additional_cameras"].append(add_cam)

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
        description="Setup SO101 configuration: wrist camera and additional cameras",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # SO101 only (no cameras) - store twin for teleoperate
  so101-setup --twin-uuid <uuid>

  # SO101 with wrist camera (streams to SO101 twin)
  so101-setup --wrist-camera --twin-uuid <uuid>

  # SO101 with wrist camera + external RealSense
  so101-setup --wrist-camera --twin-uuid <uuid> \\
      --additional-camera realsense --additional-camera-twin-uuid <uuid>

  # External CV2 camera only
  so101-setup --additional-camera cv2 --additional-camera-id 0 --additional-camera-twin-uuid <uuid>

  # Auto-detect RealSense for additional camera
  so101-setup --additional-camera realsense --additional-camera-twin-uuid <uuid> --auto-detect
        """,
    )

    parser.add_argument(
        "--twin-uuid",
        type=str,
        default=None,
        help="SO101 twin UUID (required if --wrist-camera; wrist camera streams to this twin)",
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
        "--wrist-camera",
        action="store_true",
        help="Follower has wrist camera (streams to SO101 twin via --twin-uuid)",
    )
    parser.add_argument(
        "--wrist-camera-id",
        type=str,
        default="0",
        help="Camera device ID for wrist camera (default: 0)",
    )
    parser.add_argument(
        "--additional-camera",
        type=str,
        choices=["cv2", "realsense"],
        default=None,
        help="Add one additional camera (cv2 or realsense)",
    )
    parser.add_argument(
        "--additional-camera-id",
        type=str,
        default="1",
        help="Camera device ID for additional camera (default: 1)",
    )
    parser.add_argument(
        "--additional-camera-twin-uuid",
        type=str,
        default=None,
        help="Twin UUID for additional camera stream",
    )
    parser.add_argument(
        "--camera-fps",
        type=int,
        default=30,
        help="Camera streaming FPS (default: 30)",
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
        "--resolution",
        type=str,
        default="VGA",
        help="Resolution: QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT (default: VGA)",
    )
    parser.add_argument(
        "--enable-depth",
        action="store_true",
        help="Enable depth streaming for RealSense",
    )
    parser.add_argument(
        "--depth-fps",
        type=int,
        default=30,
        help="Depth FPS for RealSense (default: 30)",
    )
    parser.add_argument(
        "--depth-resolution",
        type=str,
        default=None,
        help="Depth resolution for RealSense (default: same as color)",
    )
    parser.add_argument(
        "--depth-publish-interval",
        type=int,
        default=30,
        help="Publish depth every N frames (default: 30)",
    )
    parser.add_argument(
        "--auto-detect",
        action="store_true",
        help="Auto-detect RealSense device (for --additional-camera realsense)",
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
    if args.wrist_camera and not args.twin_uuid:
        print("Error: --twin-uuid is required when using --wrist-camera (wrist camera streams to SO101 twin)")
        return 1

    if args.additional_camera and not args.additional_camera_twin_uuid:
        print(
            "Error: --additional-camera-twin-uuid is required when using --additional-camera"
        )
        return 1

    if args.additional_camera == "realsense" and not _has_realsense:
        print("Error: RealSense support requires pyrealsense2. pip install pyrealsense2")
        return 1

    # Parse camera_id
    def parse_camera_id(s: str) -> int | str:
        try:
            return int(s)
        except ValueError:
            return s

    wrist_camera_id = parse_camera_id(args.wrist_camera_id)
    additional_camera_id = parse_camera_id(args.additional_camera_id)

    # Auto-detect RealSense if requested
    resolution_list = _parse_resolution(args.resolution)
    depth_resolution_str = args.depth_resolution

    if args.additional_camera == "realsense" and args.auto_detect and _has_realsense:
        from cyberwave.sensor import Resolution

        res_enum = Resolution.from_size(resolution_list[0], resolution_list[1])
        if res_enum is None:
            res_enum = Resolution.closest(resolution_list[0], resolution_list[1])
        rs_config = RealSenseConfig.from_device(
            serial_number=None,
            prefer_resolution=res_enum,
            prefer_fps=args.camera_fps,
            enable_depth=args.enable_depth,
        )
        resolution_list = [rs_config.color_width, rs_config.color_height]
        args.camera_fps = rs_config.color_fps
        args.enable_depth = rs_config.enable_depth
        args.depth_fps = rs_config.depth_fps
        depth_resolution_str = (
            f"{rs_config.depth_width}x{rs_config.depth_height}"
            if rs_config.enable_depth
            else None
        )

    config = create_setup_config(
        twin_uuid=args.twin_uuid,
        wrist_camera=args.wrist_camera,
        wrist_camera_twin_uuid=args.twin_uuid,
        wrist_camera_id=wrist_camera_id,
        additional_camera_type=args.additional_camera,
        additional_camera_id=additional_camera_id,
        additional_camera_twin_uuid=args.additional_camera_twin_uuid,
        camera_fps=args.camera_fps,
        resolution=f"{resolution_list[0]}x{resolution_list[1]}",
        enable_depth=args.enable_depth,
        depth_fps=args.depth_fps,
        depth_resolution=depth_resolution_str,
        depth_publish_interval=args.depth_publish_interval,
    )

    output_path = Path(args.output).expanduser() if args.output else get_setup_config_path()
    # Merge with existing setup to preserve ports saved by calibrate
    existing = load_setup_config(output_path)
    leader_port = args.leader_port or existing.get("leader_port")
    follower_port = args.follower_port or existing.get("follower_port")
    if leader_port:
        config["leader_port"] = leader_port
    if follower_port:
        config["follower_port"] = follower_port
    config["camera_fps"] = args.camera_fps
    config["max_relative_target"] = args.max_relative_target
    config["camera_only"] = args.camera_only
    save_setup_config(config, output_path)

    print(f"Setup configuration saved to: {output_path}")
    print(json.dumps(config, indent=2))
    print("\nUse with teleoperate (loads setup by default):")
    print("  so101-teleoperate")
    return 0


if __name__ == "__main__":
    sys.exit(main())
