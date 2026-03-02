"""Main entry point for the SO101 edge node.

Connects to the Cyberwave MQTT broker and listens for command events.
Runs as a long-running process inside the Docker container.
"""

import asyncio
import logging
import os
import re
import signal
import sys
import threading
from typing import Optional

from cyberwave import Cyberwave

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("so101-edge")

# ---------------------------------------------------------------------------
# Module-level state for tracking the currently running operation.
# Only one operation (teleoperate / localop) runs at a time.
# ---------------------------------------------------------------------------
_current_thread: Optional[threading.Thread] = None
_current_follower: Optional[object] = (
    None  # SO101Follower – typed loosely to avoid top-level import
)


def _trigger_alert_and_switch_to_calibration(
    client: Cyberwave,
    twin_uuid: str,
    follower_port: str,
    follower_id: str = "follower1",
    leader_port: Optional[str] = None,
    leader_id: str = "leader1",
) -> None:
    """
    We ended up here because:
    - The user tried to start local teleop or remote teleop AND
    - No calibration file was found
    So what happens is:
    - We should trigger an alert (check the alert example in the SDK)
    - We should switch to calibration mode

    Then once the calibration is done: We should resolve the alert and switch back to the original mode.
    """

    # Create a robot twin to access the alerts API
    robot = client.twin(
        twin_id=twin_uuid,
    )

    # Trigger an alert to notify the user that calibration is needed
    alert = robot.alerts.create(
        name="Calibration Needed",
        description="No calibration file found for the follower arm. Switching to calibration mode.",
        severity="warning",
        alert_type="calibration_needed",
    )
    logger.info("Created calibration alert %s for twin %s", alert.uuid, twin_uuid)

    if not sys.stdin.isatty() or not sys.stdout.isatty():
        logger.warning(
            "Non-interactive mode: calibration requires TTY. "
            "Run: docker exec -it <container> python -m scripts.cw_calibrate "
            "--type follower --port %s --id %s",
            follower_port,
            follower_id,
        )
        alert.update(
            severity="warning",
            description=(
                "Calibration requires interactive mode. "
                "SSH into the device and run: python -m scripts.cw_calibrate "
                f"--type follower --port {follower_port} --id {follower_id}"
            ),
        )
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    try:
        import subprocess

        calibrate_cmd = [
            sys.executable,
            "-m",
            "scripts.cw_calibrate",
            "--type",
            "follower",
            "--port",
            follower_port,
            "--id",
            follower_id,
        ]
        logger.info("Starting calibration subprocess: %s", " ".join(calibrate_cmd))

        result = subprocess.run(calibrate_cmd)

        if result.returncode != 0:
            logger.error("Calibration subprocess exited with code %s", result.returncode)
            alert.update(
                severity="error",
                description="Automatic calibration failed. Please calibrate manually.",
            )
            client.mqtt.publish_command_message(twin_uuid, "error")
            return

        logger.info("Calibration completed for follower %s", follower_id)

        # If there is also a leader, calibrate it too
        if leader_port is not None:
            leader_cmd = [
                sys.executable,
                "-m",
                "scripts.cw_calibrate",
                "--type",
                "leader",
                "--port",
                leader_port,
                "--id",
                leader_id,
            ]
            logger.info("Starting leader calibration subprocess: %s", " ".join(leader_cmd))

            result = subprocess.run(leader_cmd)

            if result.returncode != 0:
                logger.error("Leader calibration subprocess exited with code %s", result.returncode)
                alert.update(
                    severity="error",
                    description="Leader calibration failed. Please calibrate manually.",
                )
                client.mqtt.publish_command_message(twin_uuid, "error")
                return

            logger.info("Calibration completed for leader %s", leader_id)

        # Resolve the alert now that calibration is done
        alert.resolve()
        logger.info("Calibration alert %s resolved", alert.uuid)

    except Exception:
        logger.exception("Calibration failed for follower %s", follower_id)
        alert.update(
            severity="error",
            description="Automatic calibration failed. Please calibrate manually.",
        )
        client.mqtt.publish_command_message(twin_uuid, "error")


def _is_follower_calibrated(follower_id: str = "follower1") -> bool:
    """Check if the follower is calibrated."""
    from utils.config import get_so101_lib_dir

    return (get_so101_lib_dir() / "calibrations" / f"{follower_id}.json").exists()


def _is_leader_calibrated(leader_id: str = "leader1") -> bool:
    """Check if the leader is calibrated."""
    from utils.config import get_so101_lib_dir

    return (get_so101_lib_dir() / "calibrations" / f"{leader_id}.json").exists()


def _get_config_dir():
    """Edge config dir (twin JSONs from edge-core). Mounted at /app/.cyberwave in container."""
    from utils.config import get_so101_lib_dir

    return get_so101_lib_dir().parent


def _get_primary_robot_json_path(primary_uuid: str):
    """Path to primary robot twin JSON.

    Design (from cyberwave edge install flow):
    - CYBERWAVE_TWIN_UUID + CYBERWAVE_TWIN_JSON_FILE point to the primary robot (one env var).
    - When multiple twins are selected, all twin JSONs live in config_dir, but only
      the primary is passed via CYBERWAVE_TWIN_JSON_FILE.
    """
    from pathlib import Path

    env_path = os.getenv("CYBERWAVE_TWIN_JSON_FILE", "").strip()
    if env_path and env_path.endswith(f"{primary_uuid}.json"):
        p = Path(env_path)
        if p.is_file():
            return p
    config_dir = _get_config_dir()
    return config_dir / f"{primary_uuid}.json"


def _load_all_twin_jsons() -> list[dict]:
    """Load all twin JSON files from edge config dir (from edge-core)."""
    import json
    import uuid as uuid_module

    config_dir = _get_config_dir()
    if not config_dir.is_dir():
        return []
    result: list[dict] = []
    for p in config_dir.glob("*.json"):
        if p.name in ("credentials.json", "environment.json", "fingerprint.json", "cameras.json"):
            continue
        try:
            uuid_module.UUID(p.stem)
        except ValueError:
            continue
        try:
            with open(p) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        if isinstance(data, dict) and "uuid" in data:
            result.append(data)
    return result


RGB_SENSOR_TYPES = frozenset({"rgb", "camera", "rgb_camera"})
DEPTH_SENSOR_TYPES = frozenset({"depth", "rgbd"})


def _load_cameras_json() -> list[dict]:
    """Load cameras from cameras.json (written by cyberwave edge sync-devices / camera-discovery)."""
    import json

    config_dir = _get_config_dir()
    cameras_file = config_dir / "cameras.json"
    if not cameras_file.exists():
        return []
    try:
        with open(cameras_file) as f:
            data = json.load(f)
        return data.get("devices", [])
    except (json.JSONDecodeError, OSError):
        return []


def _twin_has_depth_sensor(twin: dict) -> bool:
    """True if twin has depth sensor (capabilities.sensors type depth/rgbd)."""
    caps = (twin.get("metadata") or {}).get("capabilities", {})
    if isinstance(caps, dict):
        sensors = caps.get("sensors", [])
        if isinstance(sensors, list):
            for s in sensors:
                if isinstance(s, dict) and (s.get("type") or "").lower() in DEPTH_SENSOR_TYPES:
                    return True
    asset = twin.get("asset") or {}
    caps = asset.get("metadata", {}).get("capabilities", caps)
    if isinstance(caps, dict):
        sensors = caps.get("sensors", [])
        if isinstance(sensors, list):
            for s in sensors:
                if isinstance(s, dict) and (s.get("type") or "").lower() in DEPTH_SENSOR_TYPES:
                    return True
    return False


def _twin_is_realsense(twin: dict) -> bool:
    """True if twin asset is Intel/RealSense (registry_id or name)."""
    asset = twin.get("asset") or {}
    reg = (asset.get("registry_id") or asset.get("metadata", {}).get("registry_id") or "").lower()
    name = (asset.get("name") or "").lower()
    return "realsense" in reg or "realsense" in name or "intel" in name


def _get_realsense_devices_from_cameras_json(devices: list[dict]) -> list[dict]:
    """Filter cameras.json devices to RealSense only (card contains Intel/RealSense)."""
    return [
        d
        for d in devices
        if "realsense" in (d.get("card") or "").lower() or "intel" in (d.get("card") or "").lower()
    ]


def _get_cv2_devices_from_cameras_json(devices: list[dict]) -> list[dict]:
    """Filter cameras.json devices to non-RealSense (CV2/USB webcams)."""
    return [
        d
        for d in devices
        if "realsense" not in (d.get("card") or "").lower() and "intel" not in (d.get("card") or "").lower()
    ]


def _get_robot_twin_sensor_cameras(primary_uuid: str) -> list[dict]:
    """Get camera entries from the primary robot twin's RGB sensors (sensors_devices).

    The primary robot is identified by CYBERWAVE_TWIN_UUID. Its JSON (from
    CYBERWAVE_TWIN_JSON_FILE or config_dir/{uuid}.json) contains sensors_devices
    mapping sensor IDs to /dev/video*. These are the wrist/primary camera(s).
    """
    robot_json = _get_primary_robot_json_path(primary_uuid)
    if not robot_json.is_file():
        return []

    try:
        import json

        with open(robot_json) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    if not isinstance(data, dict):
        return []

    asset = data.get("asset") or {}
    metadata = data.get("metadata") or {}
    if isinstance(asset, dict):
        metadata = asset.get("metadata") or metadata

    # sensors_devices can be in metadata or at root (edge sync / manual edit)
    sensors_devices = metadata.get("sensors_devices") or data.get("sensors_devices") or {}
    if isinstance(asset, dict) and not sensors_devices:
        sensors_devices = asset.get("metadata", {}).get("sensors_devices") or {}

    sensor_ids: list[str] = []
    schema = metadata.get("universal_schema") or asset.get("universal_schema")
    if schema and isinstance(schema, dict):
        sensors = schema.get("sensors", [])
        if isinstance(sensors, list):
            for i, s in enumerate(sensors):
                if isinstance(s, dict):
                    stype = (s.get("type") or "").lower()
                    if stype in RGB_SENSOR_TYPES:
                        sid = (
                            s.get("id")
                            or (s.get("parameters") or {}).get("id")
                            or s.get("name")
                            or f"sensor_{i}"
                        )
                        sensor_ids.append(str(sid))

    caps = metadata.get("capabilities", {}) if isinstance(metadata, dict) else {}
    if not sensor_ids and isinstance(caps, dict):
        sensors = caps.get("sensors", [])
        if isinstance(sensors, list):
            for i, s in enumerate(sensors):
                if isinstance(s, dict) and (s.get("type") or "").lower() in RGB_SENSOR_TYPES:
                    sid = s.get("id") or s.get("name") or f"sensor_{i}"
                    sensor_ids.append(str(sid))

    if not isinstance(sensors_devices, dict):
        sensors_devices = {}

    # Use sensor_ids from schema if found; else use sensors_devices keys (e.g. "Logitech C920 camera")
    ids_to_check = sensor_ids if sensor_ids else list(sensors_devices.keys())

    result: list[dict] = []
    for sid in ids_to_check:
        port = sensors_devices.get(sid)
        if port and str(port).strip():
            result.append({
                "twin_uuid": primary_uuid,
                "attach_to_link": "robot_sensor",
                "camera_type": "cv2",
                "camera_id": 0,
                "video_device": str(port).strip(),
                "sensor_id": sid,
            })
    return result


def _device_identifiers(dev: str | int) -> set[str | int]:
    """Return all identifiers for a device (path and index) for consistent matching."""
    ids: set[str | int] = set()
    if isinstance(dev, str) and dev.strip():
        ids.add(dev.strip())
        if "/dev/video" in dev:
            m = re.search(r"/dev/video(\d+)", dev.strip())
            if m:
                ids.add(int(m.group(1)))
    elif isinstance(dev, int):
        ids.add(dev)
        ids.add(f"/dev/video{dev}")
    return ids


def _resolve_camera_device_for_twin(
    twin: dict,
    cameras_json: list[dict],
    realsense_devices: list[dict],
) -> str | int | None:
    """Resolve video_device or camera_id for a twin.

    Priority (checks both twin.metadata and asset.metadata):
    1. video_device: direct device path (e.g. "/dev/video6")
    2. sensors_devices: map of sensor name -> device path (e.g. {"wrist_camera": "/dev/video6"})
       Uses the first non-empty value; for single-sensor camera twins this is correct.
    3. RealSense only: auto-bind if exactly one RealSense device (no sensors_devices needed).

    Returns the device path or index for cv2/RealSense to use.
    """
    meta = twin.get("metadata") or {}
    asset = twin.get("asset") or {}
    asset_meta = asset.get("metadata") or {} if isinstance(asset, dict) else {}

    def _get_meta(key: str):
        return meta.get(key) or asset_meta.get(key)

    # 1. Prefer explicit video_device (metadata, asset.metadata, or root)
    video_device = (
        _get_meta("video_device") or twin.get("video_device") or ""
    ).strip()
    if video_device:
        return str(video_device)

    # 2. Fall back to sensors_devices (metadata, asset.metadata, or root)
    sensors_devices = _get_meta("sensors_devices") or twin.get("sensors_devices")
    if isinstance(sensors_devices, dict):
        for _sensor_name, dev in sensors_devices.items():
            if dev and str(dev).strip():
                return str(dev).strip()

    # 3. RealSense only: auto-bind if single device (no sensors_devices needed)
    if not _twin_is_realsense(twin) and not _twin_has_depth_sensor(twin):
        return None  # Non-RealSense without sensors_devices: caller will use default

    if len(realsense_devices) == 1:
        return realsense_devices[0].get("primary_path") or realsense_devices[0].get("index", 0)
    if len(realsense_devices) > 1:
        return None  # Ambiguous: need sensors_devices
    return None


MAX_CAMERAS = 3
ADDITIONAL_SETUP_NAMES = ("primary", "secondary")


def _twin_is_camera_like(twin: dict) -> bool:
    """True if twin has sensors_devices, video_device, or is a camera/RealSense asset."""
    meta = twin.get("metadata") or {}
    asset = twin.get("asset") or {}
    asset_meta = asset.get("metadata") or {} if isinstance(asset, dict) else {}
    if meta.get("video_device") or meta.get("sensors_devices"):
        return True
    if asset_meta.get("video_device") or asset_meta.get("sensors_devices"):
        return True
    if twin.get("video_device") or twin.get("sensors_devices"):
        return True
    if _twin_is_realsense(twin) or _twin_has_depth_sensor(twin):
        return True
    reg = (asset.get("registry_id") or asset_meta.get("registry_id") or "").lower()
    return "camera" in reg or "realsense" in reg or "standard-cam" in reg


def _discover_cameras_for_so101(primary_uuid: str) -> list[dict]:
    """Find cameras for SO101: primary robot sensors + attached/workspace camera twins.

    Assigns devices from cameras.json to twins by sensor type:
    - Depth sensor twin → RealSense device
    - RGB sensor twin → CV2 device

    Each twin gets exactly one device; each device is used at most once.
    Supports max 3 cameras with semantic roles: wrist, primary, secondary.
    Includes both attached twins and workspace twins that are camera-like (have
    sensors_devices, video_device, or are camera assets).
    """
    cameras_json = _load_cameras_json()
    realsense_devices = _get_realsense_devices_from_cameras_json(cameras_json)
    cv2_devices = _get_cv2_devices_from_cameras_json(cameras_json)

    # 1. Collect all camera twins (robot sensors + attached + workspace camera twins)
    robot_cams = _get_robot_twin_sensor_cameras(primary_uuid)
    for rc in robot_cams:
        rc["setup_name"] = "wrist"
        rc["enable_depth"] = False
        rc["has_depth"] = False

    twins = _load_all_twin_jsons()
    attached: list[dict] = []
    additional_idx = 0
    for t in twins:
        t_uuid = t.get("uuid")
        if str(t_uuid) == str(primary_uuid):
            continue
        attach = t.get("attach_to_twin_uuid") or t.get("attach_to_twin")
        is_attached = str(attach) == str(primary_uuid)
        if not is_attached and not _twin_is_camera_like(t):
            continue
        has_depth = _twin_has_depth_sensor(t)
        is_realsense = _twin_is_realsense(t)
        is_depth_camera = has_depth or is_realsense
        meta = t.get("metadata") or {}
        setup_name = (meta.get("setup_name") or "").strip().lower()
        attach_link = (t.get("attach_to_link") or "").lower()
        if not setup_name:
            if "wrist" in attach_link or "robot_sensor" in attach_link:
                setup_name = "wrist"
            else:
                setup_name = ADDITIONAL_SETUP_NAMES[min(additional_idx, len(ADDITIONAL_SETUP_NAMES) - 1)]
                additional_idx += 1

        video_device = _resolve_camera_device_for_twin(t, cameras_json, realsense_devices)
        attached.append({
            "twin_uuid": t_uuid,
            "attach_to_link": t.get("attach_to_link", ""),
            "setup_name": setup_name,
            "has_depth": is_depth_camera,
            "video_device": video_device,
        })

    # 2. Assign devices: depth twins → RealSense, RGB twins → CV2 (one-to-one)
    used_devices: set[str | int] = set()
    result: list[dict] = []

    def _device_key(d: dict) -> str | int:
        return d.get("primary_path") or d.get("index", "?")

    def _is_device_used(dev: str | int) -> bool:
        """True if device (path or index) is already assigned."""
        return bool(_device_identifiers(dev) & used_devices)

    def _mark_device_used(dev: str | int) -> None:
        used_devices.update(_device_identifiers(dev))

    def _assign_from_pool(devices: list[dict]) -> str | int | None:
        """Return device from pool, or None if exhausted."""
        for d in devices:
            dev = d.get("primary_path") or d.get("index", 0)
            if dev is None or dev == "?":
                continue
            if not _is_device_used(dev):
                _mark_device_used(dev)
                return dev
        return None

    def _is_realsense_device(dev: str | int) -> bool:
        """True if device matches a RealSense in cameras.json."""
        for d in realsense_devices:
            if d.get("primary_path") == dev or d.get("index") == dev:
                return True
        return False

    # Depth twins first → RealSense
    depth_twins = [c for c in attached if c.get("has_depth")]
    for c in depth_twins:
        dev = c.get("video_device")
        if dev is not None:
            if _is_device_used(dev):
                continue
            _mark_device_used(dev)
        else:
            dev = _assign_from_pool(realsense_devices)
        if dev is not None:
            cam_type = "realsense" if _is_realsense_device(dev) else "cv2"
            result.append({
                "twin_uuid": c.get("twin_uuid"),
                "attach_to_link": c.get("attach_to_link", ""),
                "setup_name": c.get("setup_name", "primary"),
                "camera_type": cam_type,
                "camera_id": dev,
                "video_device": dev,
                "enable_depth": True,
            })

    # RGB twins (robot sensors + attached without depth) → CV2
    rgb_twins = robot_cams + [c for c in attached if not c.get("has_depth")]
    for c in rgb_twins:
        dev = c.get("video_device")
        if dev is not None:
            if _is_device_used(dev):
                continue
            _mark_device_used(dev)
        else:
            dev = _assign_from_pool(cv2_devices)
        if dev is not None:
            # Infer type from device: RealSense must use realsense, not cv2
            cam_type = "realsense" if _is_realsense_device(dev) else "cv2"
            result.append({
                "twin_uuid": c.get("twin_uuid"),
                "attach_to_link": c.get("attach_to_link", ""),
                "setup_name": c.get("setup_name", "primary"),
                "camera_type": cam_type,
                "camera_id": dev,
                "video_device": dev,
                "enable_depth": False,
            })

    # 3. Unassigned devices: assign to robot twin as wrist/primary (stream to robot)
    unassigned = [
        d for d in realsense_devices + cv2_devices
        if not _is_device_used(d.get("primary_path") or d.get("index", "?"))
    ]
    for i, d in enumerate(unassigned[: 3 - len(result)]):
        dev = d.get("primary_path") or d.get("index", 0)
        if _is_device_used(dev):
            continue
        _mark_device_used(dev)
        cam_type = "realsense" if d in realsense_devices else "cv2"
        setup_name = "wrist" if i == 0 else ADDITIONAL_SETUP_NAMES[min(i - 1, len(ADDITIONAL_SETUP_NAMES) - 1)]
        result.append({
            "twin_uuid": primary_uuid,
            "attach_to_link": "robot_sensor",
            "setup_name": setup_name,
            "camera_type": cam_type,
            "camera_id": dev,
            "video_device": dev,
            "enable_depth": cam_type == "realsense",
        })

    # Sort: wrist first, then primary, secondary, tertiary, etc.
    def _order(c: dict) -> int:
        sn = (c.get("setup_name") or "").lower()
        if sn == "wrist":
            return 0
        try:
            return 1 + ADDITIONAL_SETUP_NAMES.index(sn)
        except ValueError:
            return 1

    result.sort(key=lambda c: (_order(c), c.get("twin_uuid", "")))
    return result[:MAX_CAMERAS]


def _ensure_setup(twin_uuid: str) -> None:
    """Bootstrap setup.json from twin JSONs (attach_to_twin_uuid). Uses edge-core files.

    On startup, discovers SO101 devices on serial ports (/dev/ttyACM*, /dev/ttyUSB* on Linux,
    /dev/tty.usbmodem* on macOS), runs voltage detection, and assigns leader (lower voltage)
    and follower (higher voltage). Updates setup.json with discovered ports.
    """
    from utils.utils import ensure_video_device_permissions

    ensure_video_device_permissions()

    from scripts.cw_setup import (
        create_setup_config,
        load_setup_config,
        save_setup_config,
    )
    from utils.config import get_setup_config_path
    from utils.utils import discover_so101_ports_by_voltage

    path = get_setup_config_path()
    existing = load_setup_config(path) if path.exists() else {}

    # Discover SO101 ports by voltage (lower=leader, higher=follower)
    discovered = discover_so101_ports_by_voltage()
    if discovered.get("leader_port"):
        existing["leader_port"] = discovered["leader_port"]
    if discovered.get("follower_port"):
        existing["follower_port"] = discovered["follower_port"]

    cameras = _discover_cameras_for_so101(twin_uuid)
    # Partition: wrist vs additional (primary, secondary, tertiary, etc.)
    def _by_setup(c, name):
        return (c.get("setup_name") or "").lower() == name

    wrist_cam = next(
        (c for c in cameras if _by_setup(c, "wrist") or "wrist" in (c.get("attach_to_link") or "").lower()),
        None,
    )
    # Collect all additional cameras (not wrist), preserving order
    add_cams = [
        c
        for c in cameras
        if not _by_setup(c, "wrist") and "wrist" not in (c.get("attach_to_link") or "").lower()
        and (wrist_cam is None or c.get("twin_uuid") != wrist_cam.get("twin_uuid"))
    ]

    def _camera_id(cam: dict | None) -> int | str:
        if not cam:
            return 0
        return cam.get("video_device") or cam.get("camera_id", 0)

    config = create_setup_config(
        twin_uuid=twin_uuid,
        wrist_camera=bool(wrist_cam),
        wrist_camera_twin_uuid=wrist_cam.get("twin_uuid") if wrist_cam else None,
        wrist_camera_id=_camera_id(wrist_cam),
        wrist_camera_type=wrist_cam.get("camera_type", "cv2") if wrist_cam else "cv2",
        wrist_camera_enable_depth=wrist_cam.get("enable_depth", False) if wrist_cam else False,
        additional_cameras=[
            {
                "setup_name": c.get("setup_name") or ADDITIONAL_SETUP_NAMES[min(i, len(ADDITIONAL_SETUP_NAMES) - 1)],
                "camera_type": c.get("camera_type", "cv2"),
                "camera_id": _camera_id(c),
                "twin_uuid": str(c.get("twin_uuid", "")),
                "enable_depth": c.get("enable_depth", False),
            }
            for i, c in enumerate(add_cams)
        ],
    )
    # Merge with existing (ports from discovery or calibrate, max_relative_target, camera_fps)
    for k in ("leader_port", "follower_port", "max_relative_target", "camera_fps"):
        if existing.get(k) is not None:
            config[k] = existing[k]
    save_setup_config(config, path)
    logger.info("Setup updated at %s (wrist=%s, additional=%d)", path, bool(wrist_cam), len(add_cams))


def _get_hardware_config(twin_uuid: str) -> dict:
    """Load hardware config from setup.json (from edge-core mount), fall back to env vars.

    Returns cameras list: each {twin_uuid, camera_type, camera_id, resolution, fps,
    enable_depth, depth_fps, depth_resolution, depth_publish_interval}.
    """
    from scripts.cw_setup import load_setup_config

    setup = load_setup_config()
    if setup.get("twin_uuid") != twin_uuid:
        setup = {}
    fps = setup.get("camera_fps") or int(os.getenv("CYBERWAVE_METADATA_CAMERA_FPS", "30"))
    res = (
        setup.get("wrist_camera_resolution")
        or setup.get("camera_resolution")
        or setup.get("resolution")
        or os.getenv("CYBERWAVE_METADATA_CAMERA_RESOLUTION", "VGA")
    )
    cameras: list[dict] = []
    if setup.get("wrist_camera") and setup.get("wrist_camera_twin_uuid"):
        wrist_cam = {
            "twin_uuid": setup["wrist_camera_twin_uuid"],
            "camera_type": setup.get("wrist_camera_type", "cv2"),
            "camera_id": setup.get("wrist_camera_id", 0),
            "resolution": res,
            "fps": fps,
            "fourcc": setup.get("wrist_camera_fourcc"),
            "keyframe_interval": setup.get("wrist_camera_keyframe_interval"),
        }
        if wrist_cam["camera_type"] == "realsense":
            wrist_cam["enable_depth"] = setup.get("wrist_camera_enable_depth", False)
            wrist_cam["depth_fps"] = setup.get("depth_fps", 30)
            wrist_cam["depth_resolution"] = setup.get("depth_resolution")
            wrist_cam["depth_publish_interval"] = setup.get("depth_publish_interval", 30)
        cameras.append(wrist_cam)
    for i, ac in enumerate(setup.get("additional_cameras") or []):
        if ac.get("twin_uuid"):
            cam = {
                "twin_uuid": ac["twin_uuid"],
                "camera_type": ac.get("camera_type", "cv2"),
                "camera_id": ac.get("camera_id", i + 1),
                "resolution": ac.get("resolution", "VGA"),
                "fps": ac.get("fps", fps),
                "fourcc": ac.get("fourcc"),
                "keyframe_interval": ac.get("keyframe_interval"),
            }
            if cam["camera_type"] == "realsense":
                cam["enable_depth"] = ac.get("enable_depth", False)
                cam["depth_fps"] = ac.get("depth_fps", 30)
                cam["depth_resolution"] = ac.get("depth_resolution")
                cam["depth_publish_interval"] = ac.get("depth_publish_interval", 30)
            cameras.append(cam)
    if not cameras:
        legacy = (
            setup.get("wrist_camera_twin_uuid")
            or setup.get("camera_twin_uuid")
            or os.getenv("CYBERWAVE_METADATA_CAMERA_TWIN_UUID")
        )
        if legacy:
            cameras.append({
                "twin_uuid": legacy,
                "camera_type": os.getenv("CYBERWAVE_METADATA_CAMERA_TYPE", "cv2"),
                "camera_id": 0,
                "resolution": res,
                "fps": fps,
            })
    return {
        "follower_port": setup.get("follower_port") or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT"),
        "leader_port": setup.get("leader_port") or os.getenv("CYBERWAVE_METADATA_LEADER_PORT"),
        "max_relative_target": setup.get("max_relative_target")
        or os.getenv("CYBERWAVE_METADATA_MAX_RELATIVE_TARGET"),
        "follower_id": "follower1",
        "leader_id": "leader1",
        "cameras": cameras,
    }


def _stop_current_operation() -> None:
    """Stop any currently running operation and disconnect the follower."""
    global _current_thread, _current_follower

    if _current_thread is not None and _current_thread.is_alive():
        logger.info("Stopping current operation …")
        # The remoteoperate loop checks a threading.Event internally; disconnecting
        # the follower and interrupting the thread is the pragmatic way to stop it
        # from outside without modifying the remoteoperate module.
        if _current_follower is not None:
            try:
                _current_follower.disconnect()  # type: ignore[union-attr]
            except Exception:
                logger.exception("Error disconnecting follower")
        _current_thread.join(timeout=10.0)
        if _current_thread.is_alive():
            logger.warning("Operation thread did not stop in time")

    _current_thread = None
    _current_follower = None


# Commands that map to so101-* scripts (pyproject.toml [project.scripts])
# Long-running: remoteoperate, teleoperate (run in background thread)
# One-off: calibrate, find_port, read_device, setup, write_position (run as subprocess)
# stop: stop current operation (not a script, but supported)
SUPPORTED_COMMANDS = frozenset({
    "remoteoperate",
    "teleoperate",
    "calibrate",
    "find_port",
    "read_device",
    "setup",
    "write_position",
    "stop",
})


def _run_script_command(
    client: Cyberwave,
    twin_uuid: str,
    script_name: str,
    data: dict,
    extra_cli: list[str] | None = None,
) -> None:
    """Run a one-off script (calibrate, find_port, read_device, setup, write_position) as subprocess.

    extra_cli: raw CLI args from payload.args when it's a list (e.g. ["--find-port"]).
    """
    import subprocess

    module_map = {
        "find_port": "scripts.cw_find_port",
        "read_device": "scripts.cw_read_device",
        "calibrate": "scripts.cw_calibrate",
        "setup": "scripts.cw_setup",
        "write_position": "scripts.cw_write_position",
    }
    module = module_map.get(script_name)
    if not module:
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    cmd = [sys.executable, "-m", module]
    # Build args from data; fall back to CYBERWAVE_METADATA_* env vars
    match script_name:
        case "calibrate":
            calib_type = data.get("type") or "follower"
            cmd.extend(["--type", str(calib_type)])
            port = data.get("port") or (
                os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
                if calib_type == "leader"
                else os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
            )
            if port:
                cmd.extend(["--port", str(port)])
            if data.get("id"):
                cmd.extend(["--id", str(data["id"])])
        case "read_device":
            port = data.get("port") or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT") or os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
            if port:
                cmd.extend(["--port", str(port)])
        case "setup":
            if data.get("setup_path"):
                cmd.extend(["--setup-path", str(data["setup_path"])])
        case "write_position":
            if data.get("port"):
                cmd.extend(["--port", str(data["port"])])
            if data.get("position"):
                cmd.extend(["--position", str(data["position"])])
        case _:
            pass

    # Append extra CLI args from payload.args (when args is a list)
    if extra_cli:
        cmd.extend(str(a) for a in extra_cli)

    logger.info("Running script: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd)
        status = "ok" if result.returncode == 0 else "error"
        client.mqtt.publish_command_message(twin_uuid, status)
    except Exception:
        logger.exception("Script %s failed", script_name)
        client.mqtt.publish_command_message(twin_uuid, "error")


def _handle_controller_changed(
    client: Cyberwave, twin_uuid: str, payload: dict
) -> None:
    """React to controller-changed: start teleoperate for localop, remoteoperate otherwise."""
    controller = payload.get("controller")
    if not controller or not isinstance(controller, dict):
        logger.info("Controller cleared; stopping current operation")
        _stop_current_operation()
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return

    controller_type = (controller.get("controller_type") or "").strip().lower()
    logger.info("Controller changed to type=%s, starting corresponding operation", controller_type)

    _stop_current_operation()
    try:
        if controller_type == "localop":
            start_teleoperate(client, twin_uuid)
        else:
            start_remoteoperate(client, twin_uuid)
    except Exception:
        logger.exception("Failed to start operation for controller type %s", controller_type)
        client.mqtt.publish_command_message(twin_uuid, "error")


def handle_command(client: Cyberwave, twin_uuid: str, command: str, payload: dict) -> None:
    """Dispatch command to the corresponding so101-* script or operation.

    Payload may include:
    - data: dict of named args for the script
    - args: dict of additional args (merged with data, args override data)

    Special command "controller-changed": sent by backend when twin's controller
    policy changes. Starts teleoperate for "localop", remoteoperate otherwise.
    """
    # Handle controller-changed from backend (PUT /twins/{uuid} with new controller)
    if command == "controller-changed":
        _handle_controller_changed(client, twin_uuid, payload)
        return

    # Normalize: accept "so101-remoteoperate" or "remoteoperate"
    cmd = command.removeprefix("so101-") if isinstance(command, str) else ""
    if not cmd or cmd not in SUPPORTED_COMMANDS:
        logger.debug("Unsupported command: %s", command)
        return

    # Merge data + args for script commands. args can be:
    # - dict: merged with data (args overrides data)
    # - list: raw CLI args (e.g. ["--port", "/dev/ttyACM0"]) appended after built-in args
    data = dict(payload.get("data") or {}) if isinstance(payload.get("data"), dict) else {}
    raw_args = payload.get("args")
    script_args = {**data, **raw_args} if isinstance(raw_args, dict) else data
    extra_cli = raw_args if isinstance(raw_args, list) else []

    match cmd:
        case "stop":
            _stop_current_operation()
            client.mqtt.publish_command_message(twin_uuid, "ok")
        case "remoteoperate":
            _stop_current_operation()
            try:
                start_remoteoperate(client, twin_uuid)
            except Exception:
                logger.exception("Failed to start remoteoperate")
                client.mqtt.publish_command_message(twin_uuid, "error")
        case "teleoperate":
            _stop_current_operation()
            try:
                start_teleoperate(client, twin_uuid)
            except Exception:
                logger.exception("Failed to start teleoperate")
                client.mqtt.publish_command_message(twin_uuid, "error")
        case "calibrate" | "find_port" | "read_device" | "setup" | "write_position":
            _run_script_command(client, twin_uuid, cmd, script_args, extra_cli or [])
        case _:
            logger.debug("Unsupported command: %s", cmd)


def start_remoteoperate(client: Cyberwave, twin_uuid: str) -> None:
    """Start remote operation (so101-remoteoperate) for the SO101 follower.

    Receives joint states from the frontend via MQTT and writes to the follower.
    Reads hardware configuration from setup.json (from edge-core mount) or
    ``CYBERWAVE_METADATA_*`` env vars, creates the follower + twins, and
    launches the ``remoteoperate`` loop from :pymod:`scripts.cw_remoteoperate`.
    """
    global _current_thread, _current_follower

    logger.info("Starting remoteoperate")

    cfg = _get_hardware_config(twin_uuid)
    follower_port = cfg["follower_port"]
    if not follower_port:
        logger.error("follower_port not set in setup.json or env – cannot start remoteoperate")
        return

    follower_id = cfg["follower_id"]
    max_relative_target = (
        float(cfg["max_relative_target"]) if cfg["max_relative_target"] else None
    )

    if not _is_follower_calibrated(follower_id):
        _trigger_alert_and_switch_to_calibration(client, twin_uuid, follower_port, follower_id)
        return

    def _run() -> None:
        global _current_follower

        from scripts.cw_remoteoperate import remoteoperate
        from so101.follower import SO101Follower
        from utils.config import FollowerConfig
        from utils.utils import parse_resolution_to_enum

        # Robot twin
        robot = client.twin(
            asset_key="the-robot-studio/so101",
            twin_id=twin_uuid,
        )

        # Camera config from setup (wrist + additional, up to 3)
        cameras_list = []
        for cam in cfg.get("cameras") or []:
            cam_uuid = cam.get("twin_uuid")
            if not cam_uuid:
                continue
            cam_type = cam.get("camera_type", "cv2")
            camera_asset = (
                "intel/realsensed455"
                if "realsense" in str(cam_type).lower()
                else "cyberwave/standard-cam"
            )
            camera_twin = client.twin(
                asset_key=camera_asset,
                twin_id=cam_uuid,
            )
            res = cam.get("resolution", "VGA")
            res_str = f"{res[0]}x{res[1]}" if isinstance(res, (list, tuple)) and len(res) >= 2 else str(res)
            res_enum = parse_resolution_to_enum(res_str)
            entry = {
                "twin": camera_twin,
                "camera_id": cam.get("camera_id", 0),
                "camera_type": cam_type,
                "camera_resolution": res_enum,
                "fps": cam.get("fps", 30),
                "fourcc": cam.get("fourcc"),
                "keyframe_interval": cam.get("keyframe_interval"),
            }
            if cam_type == "realsense":
                entry["enable_depth"] = cam.get("enable_depth", False)
                entry["depth_fps"] = cam.get("depth_fps", 30)
                entry["depth_resolution"] = cam.get("depth_resolution")
                entry["depth_publish_interval"] = cam.get("depth_publish_interval", 30)
            cameras_list.append(entry)

        # Follower
        cam_ids = [c["camera_id"] for c in cameras_list] if cameras_list else None
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=cam_ids,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()
        _current_follower = follower

        try:
            remoteoperate(
                client=client,
                follower=follower,
                robot=robot,
                cameras=cameras_list if cameras_list else None,
            )
        except Exception:
            logger.exception("Remoteoperate loop failed")
        finally:
            try:
                follower.disconnect()
            except Exception:
                logger.exception("Error disconnecting follower after remoteoperate")
            _current_follower = None

    _current_thread = threading.Thread(target=_run, daemon=True, name="remoteoperate")
    _current_thread.start()
    logger.info("Remoteoperate thread started")


def start_teleoperate(client: Cyberwave, twin_uuid: str) -> None:
    """Start local teleoperation (so101-teleoperate) for the SO101 leader+follower.

    Reads hardware configuration from setup.json (from edge-core mount) or
    ``CYBERWAVE_METADATA_*`` env vars, creates the leader + follower + twins,
    and launches the ``teleoperate`` loop from :pymod:`scripts.cw_teleoperate`.
    """
    global _current_thread, _current_follower

    logger.info("Starting teleoperate")

    cfg = _get_hardware_config(twin_uuid)
    follower_port = cfg["follower_port"]
    leader_port = cfg["leader_port"]
    if not follower_port:
        logger.error("follower_port not set in setup.json or env – cannot start teleoperate")
        return
    if not leader_port:
        logger.error("leader_port not set in setup.json or env – cannot start teleoperate")
        return

    follower_id = cfg["follower_id"]
    leader_id = cfg["leader_id"]
    max_relative_target = (
        float(cfg["max_relative_target"]) if cfg["max_relative_target"] else None
    )

    if not _is_follower_calibrated(follower_id) or not _is_leader_calibrated(leader_id):
        _trigger_alert_and_switch_to_calibration(
            client, twin_uuid, follower_port, follower_id, leader_port, leader_id
        )
        return

    def _run() -> None:
        global _current_follower

        from scripts.cw_teleoperate import teleoperate
        from so101.follower import SO101Follower
        from so101.leader import SO101Leader
        from utils.config import FollowerConfig, LeaderConfig
        from utils.utils import parse_resolution_to_enum

        # Robot twin
        robot = client.twin(
            asset_key="the-robot-studio/so101",
            twin_id=twin_uuid,
        )

        # Camera config from setup (wrist + additional, up to 3)
        cameras_list = []
        for cam in cfg.get("cameras") or []:
            cam_uuid = cam.get("twin_uuid")
            if not cam_uuid:
                continue
            cam_type = cam.get("camera_type", "cv2")
            camera_asset = (
                "intel/realsensed455"
                if "realsense" in str(cam_type).lower()
                else "cyberwave/standard-cam"
            )
            camera_twin = client.twin(
                asset_key=camera_asset,
                twin_id=cam_uuid,
            )
            res = cam.get("resolution", "VGA")
            res_str = f"{res[0]}x{res[1]}" if isinstance(res, (list, tuple)) and len(res) >= 2 else str(res)
            res_enum = parse_resolution_to_enum(res_str)
            entry = {
                "twin": camera_twin,
                "camera_id": cam.get("camera_id", 0),
                "camera_type": cam_type,
                "camera_resolution": res_enum,
                "fps": cam.get("fps", 30),
                "fourcc": cam.get("fourcc"),
                "keyframe_interval": cam.get("keyframe_interval"),
            }
            if cam_type == "realsense":
                entry["enable_depth"] = cam.get("enable_depth", False)
                entry["depth_fps"] = cam.get("depth_fps", 30)
                entry["depth_resolution"] = cam.get("depth_resolution")
                entry["depth_publish_interval"] = cam.get("depth_publish_interval", 30)
            cameras_list.append(entry)

        # Leader
        leader_config = LeaderConfig(port=leader_port)
        leader = SO101Leader(config=leader_config)
        leader.connect()

        # Follower
        cam_ids = [c["camera_id"] for c in cameras_list] if cameras_list else None
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=cam_ids,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()
        _current_follower = follower

        try:
            teleoperate(
                leader=leader,
                cyberwave_client=client,
                follower=follower,
                robot=robot,
                cameras=cameras_list if cameras_list else None,
            )
        except Exception:
            logger.exception("Teleoperate loop failed")
        finally:
            try:
                follower.disconnect()
            except Exception:
                logger.exception("Error disconnecting follower after teleoperate")
            try:
                leader.disconnect()
            except Exception:
                logger.exception("Error disconnecting leader after teleoperate")
            _current_follower = None

    _current_thread = threading.Thread(target=_run, daemon=True, name="teleoperate")
    _current_thread.start()
    logger.info("Teleoperate thread started")


async def main() -> None:
    token = os.getenv("CYBERWAVE_API_KEY")
    twin_uuid = os.getenv("CYBERWAVE_TWIN_UUID")

    if not token:
        logger.error("CYBERWAVE_API_KEY environment variable is required")
        sys.exit(1)
    if not twin_uuid:
        logger.error("CYBERWAVE_TWIN_UUID environment variable is required")
        sys.exit(1)

    logger.info("Initializing SO101 edge node for twin %s", twin_uuid)

    try:
        _ensure_setup(twin_uuid)
    except Exception:
        logger.exception("Setup bootstrap failed; continuing with existing config")

    # Initialize the Cyberwave SDK client (reads remaining config from env vars)
    client = Cyberwave(token=token, source_type="edge")

    # Connect to the MQTT broker
    client.mqtt.connect()
    logger.info("Connected to MQTT broker")

    # --- command handler ------------------------------------------------
    def on_command(data: dict) -> None:
        """Handle incoming command messages for this twin.

        Status messages (responses) are published on the same topic, so we
        skip any payload that already contains a ``status`` key.
        """
        # Ignore our own status responses echoed back
        if "status" in data:
            return

        command = data.get("command")
        logger.info("Received command: %s (payload: %s)", command, data)

        if not command:
            logger.warning("Received message with no 'command' field: %s", data)
            client.mqtt.publish_command_message(twin_uuid, "error")
            return

        try:
            handle_command(client, twin_uuid, command, data)
        except Exception:
            logger.exception("Error handling command %s", command)
            try:
                client.mqtt.publish_command_message(twin_uuid, "error")
            except Exception:
                logger.exception("Failed to publish error status")

    # Subscribe to the command topic for this twin
    client.mqtt.subscribe_command_message(twin_uuid, on_command)
    logger.info("Subscribed to command topic for twin %s", twin_uuid)

    # --- graceful shutdown ---------------------------------------------
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Shutdown signal received, stopping...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Keep the process alive until a shutdown signal is received
    logger.info("SO101 edge node is running. Waiting for commands...")
    await stop_event.wait()

    # Cleanup
    logger.info("Stopping any running operation...")
    _stop_current_operation()
    logger.info("Disconnecting from MQTT broker...")
    client.mqtt.disconnect()
    logger.info("SO101 edge node stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutdown by user")
    except Exception:
        logger.exception("Fatal error in main loop")
        sys.exit(0)  # Exit 0 to avoid Docker restart loop
