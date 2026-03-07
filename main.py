"""Main entry point for the SO101 edge node.

Connects to the Cyberwave MQTT broker and listens for command events.
Runs as a long-running process inside the Docker container.
"""

import asyncio
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from typing import Any, Optional

from cyberwave import Cyberwave
from cyberwave.sensor import CameraStreamManager
from cyberwave.utils import TimeReference
from utils.cw_alerts import CALIBRATION_NEEDED_MEDIA_URL

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
# Stop event passed to remoteoperate/teleoperate so _stop_current_operation can
# signal them to exit (used when run by edge-core in Docker, no terminal).
_operation_stop_event: Optional[threading.Event] = None
# Client and twin for connect/disconnect messages (main sends these; scripts send telemetry)
_current_client: Optional[Cyberwave] = None
_current_twin_uuid: Optional[str] = None
_idle_camera_manager: Optional[CameraStreamManager] = None
_idle_camera_stop_event: Optional[threading.Event] = None
_idle_camera_time_reference: Optional[TimeReference] = None

# Calibration subprocess (Popen with stdin=PIPE) for "advance" command injection
_calibration_proc: Optional[subprocess.Popen] = None
_calibration_client: Optional[Cyberwave] = None
_calibration_twin_uuid: Optional[str] = None
_calibration_alert_uuid: Optional[str] = None

# When calibration completes, run this command. Only teleoperate/remoteoperate are
# stored; calibrate is never stored to avoid infinite loops.
RECOVERY_COMMANDS = frozenset({"teleoperate", "remoteoperate"})
_pending_recovery_command: Optional[str] = None

# Graceful shutdown: give scripts time to stop MQTT, send end messages, cleanup.
GRACEFUL_JOIN_TIMEOUT = 30.0  # seconds to wait for thread to exit and cleanup
FORCE_DISCONNECT_JOIN_TIMEOUT = 5.0  # seconds after force disconnect if graceful fails


def _is_control_operation_running() -> bool:
    """True when teleoperate/remoteoperate thread is active."""
    return _current_thread is not None and _current_thread.is_alive()


def _publish_operation_running_error(client: Cyberwave, twin_uuid: str) -> None:
    """Publish a consistent error payload for commands blocked by active operation."""
    client.mqtt.publish_command_message(
        twin_uuid,
        {"status": "error", "reason": "operation_running"},
    )


def _trigger_alert_and_switch_to_calibration(
    client: Cyberwave,
    twin_uuid: str,
    follower_port: str,
    follower_id: str = "follower1",
    leader_port: Optional[str] = None,
    leader_id: str = "leader1",
    recovery_command: Optional[str] = None,
    device_type: str = "follower",
) -> None:
    """
    User tried to start teleop/remoteoperate but calibration is missing.
    Create an alert, store recovery_command for when calibration completes,
    and start calibration directly. When calibration ends successfully,
    we run the recovery command (teleoperate or remoteoperate).
    """
    global _pending_recovery_command

    robot = client.twin(twin_id=twin_uuid)

    metadata = {
        "calibration": {
            "device_type": device_type,
            "follower_port": follower_port,
            "follower_id": follower_id,
        }
    }
    if leader_port is not None:
        metadata["calibration"]["leader_port"] = leader_port
        metadata["calibration"]["leader_id"] = leader_id

    port = follower_port if device_type == "follower" else (leader_port or follower_port)
    device_id = follower_id if device_type == "follower" else leader_id

    description = f"No calibration file! Bring the {device_type} to the position zero and then click on `Go Ahead`"
    # if not sys.stdin.isatty() or not sys.stdout.isatty():
    #     description = (
    #         "Calibration requires interactive mode. "
    #         "SSH into the device and run: python -m scripts.cw_calibrate "
    #         f"--type {device_type} --port {port} --id {device_id}"
    #     )

    alert = robot.alerts.create(
        name="Calibration Needed",
        description=description,
        media=CALIBRATION_NEEDED_MEDIA_URL,
        severity="warning",
        alert_type="calibration_needed",
        metadata=metadata,
    )
    logger.info("Created calibration alert %s for twin %s", alert.uuid, twin_uuid)

    if recovery_command and recovery_command in RECOVERY_COMMANDS:
        _pending_recovery_command = recovery_command
        logger.info("Stored recovery command: %s (will run after calibration)", recovery_command)

    # Start calibration directly
    if port:
        script_data = {
            "type": device_type,
            "follower_id": follower_id,
            "leader_id": leader_id,
            "alert_uuid": alert.uuid,
        }
        if device_type == "follower":
            script_data["follower_port"] = follower_port
        else:
            script_data["leader_port"] = leader_port
        _handle_calibration_start(client, twin_uuid, script_data)
    else:
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


def _load_primary_robot_twin(primary_uuid: str) -> dict | None:
    """Load primary robot twin JSON. Returns None if not found or invalid."""
    import json

    path = _get_primary_robot_json_path(primary_uuid)
    if not path.is_file():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) and data.get("uuid") else None
    except (json.JSONDecodeError, OSError):
        return None


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


def _load_edge_fingerprint() -> str | None:
    """Load edge fingerprint from fingerprint.json (written by edge-core)."""
    import json

    from utils.config import get_so101_lib_dir

    fingerprint_file = get_so101_lib_dir().parent / "fingerprint.json"
    if not fingerprint_file.is_file():
        return None
    try:
        with open(fingerprint_file) as f:
            data = json.load(f)
        fp = data.get("fingerprint")
        return str(fp).strip() if fp else None
    except (json.JSONDecodeError, OSError):
        return None


def _get_edge_camera_config_from_metadata(
    metadata: dict,
    fingerprint: str | None = None,
) -> dict:
    """Return canonical camera_config from twin metadata edge binding."""
    if not isinstance(metadata, dict):
        return {}
    edge_configs = metadata.get("edge_configs")
    if not isinstance(edge_configs, dict):
        return {}
    metadata_fp = metadata.get("edge_fingerprint")
    if not isinstance(metadata_fp, str) or not metadata_fp.strip():
        return {}
    if fingerprint and metadata_fp.strip() != str(fingerprint).strip():
        return {}
    camera_config = edge_configs.get("camera_config")
    return camera_config if isinstance(camera_config, dict) else {}


def _get_discovered_devices_from_edge_config(primary_uuid: str) -> list[dict]:
    """Fallback: collect discovered_devices from edge_config in twin metadata.

    Used when v4l2 returns empty (e.g. container without /dev/video* access).
    Merges from primary robot and all workspace twin JSONs.
    """
    import json

    seen: set[str | int] = set()
    result: list[dict] = []
    fp = _load_edge_fingerprint()
    if not fp:
        return []

    def _add_devices(devices: list) -> None:
        if not isinstance(devices, list):
            return
        for d in devices:
            if not isinstance(d, dict):
                continue
            path = d.get("primary_path") or (d.get("paths") or [None])[0]
            idx = d.get("index")
            key = path if path else (f"/dev/video{idx}" if idx is not None else None)
            if key is None:
                return
            if key in seen:
                return
            seen.add(key)
            result.append(d)

    def _from_twin(data: dict) -> None:
        meta = data.get("metadata") or {}
        camera_config = _get_edge_camera_config_from_metadata(meta, fp)
        if isinstance(camera_config, dict):
            _add_devices(camera_config.get("discovered_devices") or [])

    robot_path = _get_primary_robot_json_path(primary_uuid)
    if robot_path.is_file():
        try:
            with open(robot_path) as f:
                _from_twin(json.load(f))
        except (json.JSONDecodeError, OSError):
            pass
    for twin in _load_all_twin_jsons():
        _from_twin(twin)

    return result


def _discover_devices_via_v4l2() -> list[dict]:
    """Discover USB cameras via v4l2-ctl (Linux). Returns list of device dicts."""
    from utils.device_utils import discover_usb_cameras

    cameras = discover_usb_cameras()
    return [c.to_dict() for c in cameras]


def _merge_discovered_with_edge_config(
    discovered: list[dict], primary_uuid: str
) -> tuple[list[dict], int]:
    """Merge v4l2-discovered devices with edge_config fallback. Returns (merged_list, added_count)."""
    from_edge = _get_discovered_devices_from_edge_config(primary_uuid)
    if not from_edge:
        return discovered, 0
    seen_paths: set[str | int] = set()
    for d in discovered:
        path = d.get("primary_path") or (d.get("paths") or [None])[0]
        idx = d.get("index")
        key = path if path else (f"/dev/video{idx}" if idx is not None else None)
        if key:
            seen_paths.add(key)
    merged: list[dict] = list(discovered)
    for d in from_edge:
        path = d.get("primary_path") or (d.get("paths") or [None])[0]
        idx = d.get("index")
        key = path if path else (f"/dev/video{idx}" if idx is not None else None)
        if key and key not in seen_paths:
            seen_paths.add(key)
            merged.append(d)
    return merged, len(merged) - len(discovered)


def _push_discovered_devices_to_edge_config(
    twin_uuid: str,
    fingerprint: str,
    devices_list: list[dict],
) -> None:
    """Push discovered_devices to the twin's edge_config via pair_device (merge with existing)."""
    if not devices_list:
        return
    token = os.getenv("CYBERWAVE_API_KEY")
    if not token:
        return
    try:
        import json

        # Get existing camera_config from twin JSON (edge-core sync)
        existing_camera_config: dict = {}
        robot_path = _get_primary_robot_json_path(twin_uuid)
        if robot_path.is_file():
            with open(robot_path) as f:
                data = json.load(f)
            metadata = data.get("metadata") or {}
            existing_camera_config = _get_edge_camera_config_from_metadata(metadata, fingerprint)
            if not isinstance(existing_camera_config, dict):
                existing_camera_config = {}

        merged = {**existing_camera_config, "discovered_devices": devices_list}
        client = Cyberwave(api_key=token, source_type="edge")
        client.twins.pair_device(twin_uuid, fingerprint, edge_config=merged)
        logger.info(
            "Pushed discovered_devices (%d) to edge_config for twin %s",
            len(devices_list),
            twin_uuid,
        )
    except Exception as e:
        logger.warning("Could not push discovered_devices to edge_config: %s", e)


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


def _filter_realsense_devices(devices: list[dict]) -> list[dict]:
    """Filter to RealSense only (card contains Intel/RealSense)."""
    return [
        d
        for d in devices
        if "realsense" in (d.get("card") or "").lower() or "intel" in (d.get("card") or "").lower()
    ]


def _filter_cv2_devices(devices: list[dict]) -> list[dict]:
    """Filter to non-RealSense (CV2/USB webcams)."""
    return [
        d
        for d in devices
        if "realsense" not in (d.get("card") or "").lower()
        and "intel" not in (d.get("card") or "").lower()
    ]


def _extract_rgb_sensor_ids_from_twin_data(data: dict) -> list[str]:
    """Extract RGB-like sensor IDs from twin universal schema/capabilities."""
    if not isinstance(data, dict):
        return []

    asset = data.get("asset") or {}
    metadata = data.get("metadata") or {}
    if isinstance(asset, dict):
        metadata = asset.get("metadata") or metadata

    sensor_ids: list[str] = []

    schema = metadata.get("universal_schema") or (
        asset.get("universal_schema") if isinstance(asset, dict) else None
    )
    if isinstance(schema, dict):
        sensors = schema.get("sensors", [])
        if isinstance(sensors, list):
            for i, s in enumerate(sensors):
                if isinstance(s, dict) and (s.get("type") or "").lower() in RGB_SENSOR_TYPES:
                    sid = (
                        s.get("id")
                        or (s.get("parameters") or {}).get("id")
                        or s.get("name")
                        or f"sensor_{i}"
                    )
                    sensor_ids.append(str(sid))

    if not sensor_ids:
        caps = metadata.get("capabilities", {}) if isinstance(metadata, dict) else {}
        if isinstance(caps, dict):
            sensors = caps.get("sensors", [])
            if isinstance(sensors, list):
                for i, s in enumerate(sensors):
                    if isinstance(s, dict) and (s.get("type") or "").lower() in RGB_SENSOR_TYPES:
                        sid = s.get("id") or s.get("name") or f"sensor_{i}"
                        sensor_ids.append(str(sid))

    # De-duplicate while preserving order
    return list(dict.fromkeys(sensor_ids))


def _get_primary_robot_default_rgb_sensor_id(primary_uuid: str) -> str | None:
    """Best-effort first RGB sensor ID for the primary robot twin."""
    robot_json = _get_primary_robot_json_path(primary_uuid)
    if not robot_json.is_file():
        return None
    try:
        import json

        with open(robot_json) as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return None

    sensor_ids = _extract_rgb_sensor_ids_from_twin_data(data)
    return sensor_ids[0] if sensor_ids else None


def _get_robot_twin_sensor_cameras(primary_uuid: str) -> list[dict]:
    """Get primary robot RGB camera entries from configured sensor-device mapping.

    The primary robot is identified by CYBERWAVE_TWIN_UUID. Its JSON (from
    CYBERWAVE_TWIN_JSON_FILE or config_dir/{uuid}.json) contains canonical
    mapping at metadata.edge_configs.camera_config.sensors_devices.
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

    root_metadata = data.get("metadata") if isinstance(data.get("metadata"), dict) else {}

    fingerprint = _load_edge_fingerprint()
    camera_config = _get_edge_camera_config_from_metadata(root_metadata, fingerprint)
    sensors_devices = (
        camera_config.get("sensors_devices") if isinstance(camera_config, dict) else {}
    )

    sensor_ids = _extract_rgb_sensor_ids_from_twin_data(data)

    if not isinstance(sensors_devices, dict):
        sensors_devices = {}

    # Use sensor_ids from schema if found; else use sensors_devices keys (e.g. "Logitech C920 camera")
    ids_to_check = sensor_ids if sensor_ids else list(sensors_devices.keys())

    result: list[dict] = []
    for sid in ids_to_check:
        port = sensors_devices.get(sid)
        if port and str(port).strip():
            result.append(
                {
                    "twin_uuid": primary_uuid,
                    "attach_to_link": "robot_sensor",
                    "camera_type": "cv2",
                    "camera_id": 0,
                    "video_device": str(port).strip(),
                    "sensor_id": sid,
                }
            )
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
    realsense_devices: list[dict],
    fingerprint: str | None = None,
) -> str | int | None:
    """Resolve video_device or camera_id for a twin.

    Priority:
    1. edge_configs.camera_config.sensors_devices: canonical mapping of sensor->device path
    2. edge_config.camera_config.source: when it is a /dev/video* path (local USB camera)
    3. RealSense only: auto-bind if exactly one RealSense device.

    Returns the device path or index for cv2/RealSense to use.
    """
    meta = twin.get("metadata") or {}
    fp = fingerprint or _load_edge_fingerprint()

    # 1. Canonical mapping in edge_configs.camera_config.sensors_devices.
    camera_config = _get_edge_camera_config_from_metadata(meta, fp)
    edge_sensors_devices = (
        camera_config.get("sensors_devices") if isinstance(camera_config, dict) else {}
    )
    if isinstance(edge_sensors_devices, dict):
        for _sensor_name, dev in edge_sensors_devices.items():
            if dev and str(dev).strip():
                return str(dev).strip()

    # 2. Check edge_config.camera_config.source (when it is a /dev/video* path).
    if isinstance(camera_config, dict):
        source = camera_config.get("source")
        if isinstance(source, str) and source.strip().startswith("/dev/video"):
            return source.strip()

    # 3. RealSense only: auto-bind if single device.
    if not _twin_is_realsense(twin) and not _twin_has_depth_sensor(twin):
        return None  # Non-RealSense without canonical edge camera config: use pool default

    if len(realsense_devices) == 1:
        return realsense_devices[0].get("primary_path") or realsense_devices[0].get("index", 0)
    if len(realsense_devices) > 1:
        return None  # Ambiguous: need explicit edge camera mapping
    return None


MAX_CAMERAS = 3
ADDITIONAL_SETUP_NAMES = ("primary", "secondary")


def _twin_is_camera_like(twin: dict) -> bool:
    """True if twin has camera config/mappings or is a camera/RealSense asset."""
    meta = twin.get("metadata") or {}
    asset = twin.get("asset") or {}
    asset_meta = asset.get("metadata") or {} if isinstance(asset, dict) else {}
    edge_camera_config = _get_edge_camera_config_from_metadata(meta)
    if edge_camera_config.get("source") or edge_camera_config.get("sensors_devices"):
        return True
    if _twin_is_realsense(twin) or _twin_has_depth_sensor(twin):
        return True
    reg = (asset.get("registry_id") or asset_meta.get("registry_id") or "").lower()
    return "camera" in reg or "realsense" in reg or "standard-cam" in reg


def _discover_cameras_for_so101(primary_uuid: str) -> list[dict]:
    """Find cameras for SO101: primary robot sensors + attached/workspace camera twins.

    Assigns devices from twin metadata discovered_devices to twins by sensor type:
    - Depth sensor twin → RealSense device
    - RGB sensor twin → CV2 device

    Each twin gets exactly one device; each device is used at most once.
    Supports max 3 cameras with semantic roles: wrist, primary, secondary.
    Includes both attached twins and workspace twins that are camera-like by
    canonical edge camera config or camera asset type.
    """
    discovered = _discover_devices_via_v4l2()
    logger.info(
        "Camera discovery: v4l2 found %d device(s) %s",
        len(discovered),
        [(d.get("card"), d.get("primary_path") or d.get("index")) for d in discovered],
    )
    discovered, added = _merge_discovered_with_edge_config(discovered, primary_uuid)
    if added:
        logger.info(
            "Merged %d device(s) from edge_config (v4l2 found %d)",
            added,
            len(discovered) - added,
        )
    realsense_devices = _filter_realsense_devices(discovered)
    cv2_devices = _filter_cv2_devices(discovered)
    logger.info(
        "Device pools: realsense=%d %s, cv2=%d %s",
        len(realsense_devices),
        [(d.get("card"), d.get("primary_path") or d.get("index")) for d in realsense_devices],
        len(cv2_devices),
        [(d.get("card"), d.get("primary_path") or d.get("index")) for d in cv2_devices],
    )
    fingerprint = _load_edge_fingerprint()

    # 1. Collect all camera twins (robot sensors + attached + workspace camera twins)
    robot_cams = _get_robot_twin_sensor_cameras(primary_uuid)
    primary_rgb_sensor_id = _get_primary_robot_default_rgb_sensor_id(primary_uuid)
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
                setup_name = ADDITIONAL_SETUP_NAMES[
                    min(additional_idx, len(ADDITIONAL_SETUP_NAMES) - 1)
                ]
                additional_idx += 1

        video_device = _resolve_camera_device_for_twin(t, realsense_devices, fingerprint)
        attached.append(
            {
                "twin_uuid": t_uuid,
                "attach_to_link": t.get("attach_to_link", ""),
                "setup_name": setup_name,
                "has_depth": is_depth_camera,
                "video_device": video_device,
            }
        )

    logger.info(
        "Camera twins: robot_cams=%d, attached=%d (depth=%d, rgb=%d)",
        len(robot_cams),
        len(attached),
        sum(1 for c in attached if c.get("has_depth")),
        sum(1 for c in attached if not c.get("has_depth")),
    )
    for c in attached:
        logger.info(
            "  twin %s setup=%s has_depth=%s video_device=%s",
            c.get("twin_uuid"),
            c.get("setup_name"),
            c.get("has_depth"),
            c.get("video_device"),
        )

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
        """True if device matches a RealSense in discovered devices."""
        for d in realsense_devices:
            if d.get("primary_path") == dev or d.get("index") == dev:
                return True
        return False

    # Depth twins first → RealSense
    depth_twins = [c for c in attached if c.get("has_depth")]
    for c in depth_twins:
        dev = c.get("video_device")
        used_default = False
        if dev is not None:
            if _is_device_used(dev):
                continue
            _mark_device_used(dev)
        else:
            dev = _assign_from_pool(realsense_devices)
            used_default = dev is not None
        if dev is not None:
            cam_type = "realsense" if _is_realsense_device(dev) else "cv2"
            result.append(
                {
                    "twin_uuid": c.get("twin_uuid"),
                    "attach_to_link": c.get("attach_to_link", ""),
                    "setup_name": c.get("setup_name", "primary"),
                    "camera_type": cam_type,
                    "camera_id": dev,
                    "video_device": dev,
                    "enable_depth": True,
                    "used_default": used_default,
                }
            )

    logger.info("After depth assignment: %d camera(s) in result", len(result))

    # RGB twins (robot sensors + attached without depth) → CV2 only (never RealSense)
    rgb_twins = robot_cams + [c for c in attached if not c.get("has_depth")]
    for c in rgb_twins:
        dev = c.get("video_device")
        used_default = False
        if dev is not None:
            if _is_device_used(dev):
                continue
            if _is_realsense_device(dev):
                continue  # Do not assign RealSense to non-RealSense (RGB) twin
            _mark_device_used(dev)
        else:
            dev = _assign_from_pool(cv2_devices)
            used_default = dev is not None
        if dev is not None:
            # Infer type from device: RealSense must use realsense, not cv2
            cam_type = "realsense" if _is_realsense_device(dev) else "cv2"
            result_entry = {
                "twin_uuid": c.get("twin_uuid"),
                "attach_to_link": c.get("attach_to_link", ""),
                "setup_name": c.get("setup_name", "primary"),
                "camera_type": cam_type,
                "camera_id": dev,
                "video_device": dev,
                "enable_depth": False,
                "used_default": used_default,
            }
            if c.get("sensor_id"):
                result_entry["sensor_id"] = c.get("sensor_id")
            result.append(result_entry)

    logger.info("After RGB assignment: %d camera(s) in result", len(result))

    # 3. Unassigned devices: assign to robot twin as wrist/primary (stream to robot)
    # Do NOT assign RealSense to a non-RealSense twin (e.g. SO101 robot arm)
    primary_twin = _load_primary_robot_twin(primary_uuid)
    robot_accepts_realsense = primary_twin and (
        _twin_is_realsense(primary_twin) or _twin_has_depth_sensor(primary_twin)
    )
    all_unassigned = [
        d
        for d in realsense_devices + cv2_devices
        if not _is_device_used(d.get("primary_path") or d.get("index", "?"))
    ]
    unassigned = (
        all_unassigned
        if robot_accepts_realsense
        else [d for d in all_unassigned if d in cv2_devices]
    )
    if not robot_accepts_realsense and any(d in realsense_devices for d in all_unassigned):
        logger.info(
            "Primary robot is not RealSense/depth twin; skipping RealSense device(s) for unassigned assignment"
        )
    logger.info(
        "Unassigned devices: %d available %s, adding up to %d (slots left=%d)",
        len(unassigned),
        [(x.get("card"), x.get("primary_path") or x.get("index")) for x in unassigned],
        min(len(unassigned), 3 - len(result)),
        3 - len(result),
    )
    for i, d in enumerate(unassigned[: 3 - len(result)]):
        dev = d.get("primary_path") or d.get("index", 0)
        if _is_device_used(dev):
            continue
        _mark_device_used(dev)
        cam_type = "realsense" if d in realsense_devices else "cv2"
        setup_name = (
            "wrist"
            if i == 0
            else ADDITIONAL_SETUP_NAMES[min(i - 1, len(ADDITIONAL_SETUP_NAMES) - 1)]
        )
        result_entry = {
            "twin_uuid": primary_uuid,
            "attach_to_link": "robot_sensor",
            "setup_name": setup_name,
            "camera_type": cam_type,
            "camera_id": dev,
            "video_device": dev,
            "enable_depth": cam_type == "realsense",
            "used_default": True,  # No edge camera mapping configured; assigned from pool
        }
        if setup_name == "wrist" and primary_rgb_sensor_id:
            result_entry["sensor_id"] = primary_rgb_sensor_id
        result.append(result_entry)

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
    result = result[:MAX_CAMERAS]
    logger.info(
        "Camera setup result: %d camera(s) %s",
        len(result),
        [
            (c.get("setup_name"), c.get("camera_type"), c.get("video_device"), c.get("twin_uuid"))
            for c in result
        ],
    )
    return result


def _ensure_setup(twin_uuid: str) -> None:
    """Bootstrap setup.json from twin JSONs (attach_to_twin_uuid). Uses edge-core files.

    On startup, discovers SO101 devices on serial ports (/dev/ttyACM*, /dev/ttyUSB* on Linux,
    /dev/tty.usbmodem* on macOS), runs voltage detection, and assigns leader (lower voltage)
    and follower (higher voltage). Updates setup.json with discovered ports.
    """
    logger.info("Running setup for twin %s", twin_uuid)
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
    default_cameras = [c for c in cameras if c.get("used_default")]
    primary_rgb_sensor_id = _get_primary_robot_default_rgb_sensor_id(twin_uuid)

    # Ensure default wrist cameras include a stable sensor_id when we can infer one.
    if primary_rgb_sensor_id:
        for c in default_cameras:
            if c.get("sensor_id"):
                continue
            is_primary = str(c.get("twin_uuid")) == str(twin_uuid)
            is_robot_sensor = (c.get("attach_to_link") or "").lower() == "robot_sensor"
            if is_primary and is_robot_sensor:
                c["sensor_id"] = primary_rgb_sensor_id

    # Push discovered devices to edge_config (for frontend device selector)
    # Use same merged discovery as _discover_cameras_for_so101 (v4l2 + edge_config)
    discovered = _discover_devices_via_v4l2()
    discovered, _ = _merge_discovered_with_edge_config(discovered, twin_uuid)
    fingerprint = _load_edge_fingerprint()
    if discovered and fingerprint:
        _push_discovered_devices_to_edge_config(twin_uuid, fingerprint, discovered)

    if default_cameras:
        token = os.getenv("CYBERWAVE_API_KEY")
        if token:
            try:
                client = Cyberwave(api_key=token, source_type="edge")
                robot = client.twin(twin_id=twin_uuid)
                from utils.cw_alerts import create_camera_default_device_alert

                create_camera_default_device_alert(robot, default_cameras)
            except Exception as e:
                logger.warning("Could not create camera default device alert: %s", e)

    # Partition: wrist vs additional (primary, secondary, tertiary, etc.)
    def _by_setup(c, name):
        return (c.get("setup_name") or "").lower() == name

    wrist_cam = next(
        (
            c
            for c in cameras
            if _by_setup(c, "wrist") or "wrist" in (c.get("attach_to_link") or "").lower()
        ),
        None,
    )
    # Collect all additional cameras (not wrist), preserving order
    add_cams = [
        c
        for c in cameras
        if not _by_setup(c, "wrist")
        and "wrist" not in (c.get("attach_to_link") or "").lower()
        and (wrist_cam is None or c.get("twin_uuid") != wrist_cam.get("twin_uuid"))
    ]
    logger.info(
        "Setup partition: wrist=%s, additional=%d cameras %s",
        bool(wrist_cam),
        len(add_cams),
        [(c.get("setup_name"), c.get("camera_type"), c.get("video_device")) for c in add_cams],
    )

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
                "setup_name": c.get("setup_name")
                or ADDITIONAL_SETUP_NAMES[min(i, len(ADDITIONAL_SETUP_NAMES) - 1)],
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
    logger.info(
        "Setup updated at %s (wrist=%s, additional=%d)", path, bool(wrist_cam), len(add_cams)
    )


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
            cameras.append(
                {
                    "twin_uuid": legacy,
                    "camera_type": os.getenv("CYBERWAVE_METADATA_CAMERA_TYPE", "cv2"),
                    "camera_id": 0,
                    "resolution": res,
                    "fps": fps,
                }
            )
    return {
        "follower_port": setup.get("follower_port")
        or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT"),
        "leader_port": setup.get("leader_port") or os.getenv("CYBERWAVE_METADATA_LEADER_PORT"),
        "max_relative_target": setup.get("max_relative_target")
        or os.getenv("CYBERWAVE_METADATA_MAX_RELATIVE_TARGET"),
        "follower_id": "follower1",
        "leader_id": "leader1",
        "cameras": cameras,
    }


def _build_idle_camera_twins(client: Cyberwave, twin_uuid: str) -> list[Any]:
    """Build camera twins+overrides for idle streaming from setup.json."""
    from utils.utils import parse_resolution_to_enum

    cfg = _get_hardware_config(twin_uuid)
    camera_twins: list[Any] = []
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
        res_str = (
            f"{res[0]}x{res[1]}" if isinstance(res, (list, tuple)) and len(res) >= 2 else str(res)
        )
        res_enum = parse_resolution_to_enum(res_str)
        overrides = {
            "camera_id": cam.get("camera_id", 0),
            "camera_type": cam_type,
            "camera_resolution": res_enum,
            "fps": cam.get("fps", 30),
            "fourcc": cam.get("fourcc"),
            "keyframe_interval": cam.get("keyframe_interval"),
        }
        if cam_type == "realsense":
            overrides["enable_depth"] = cam.get("enable_depth", False)
            overrides["depth_fps"] = cam.get("depth_fps", 30)
            overrides["depth_resolution"] = cam.get("depth_resolution")
            overrides["depth_publish_interval"] = cam.get("depth_publish_interval", 30)
        camera_twins.append((camera_twin, overrides))
    return camera_twins


def _stop_idle_camera_streaming() -> None:
    """Stop background idle camera streaming if running."""
    global _idle_camera_manager, _idle_camera_stop_event, _idle_camera_time_reference

    if _idle_camera_stop_event is not None:
        _idle_camera_stop_event.set()
    if _idle_camera_manager is not None:
        try:
            _idle_camera_manager.join(timeout=5.0)
        except Exception:
            logger.exception("Error stopping idle camera streaming")
    _idle_camera_manager = None
    _idle_camera_stop_event = None
    _idle_camera_time_reference = None


def _start_idle_camera_streaming(client: Cyberwave, twin_uuid: str) -> None:
    """Start camera streaming at boot/idle, independent from controller assignment."""
    global _idle_camera_manager, _idle_camera_stop_event, _idle_camera_time_reference

    if _idle_camera_manager is not None and _idle_camera_stop_event is not None:
        if not _idle_camera_stop_event.is_set():
            logger.debug("Idle camera streaming already running")
            return

    camera_twins = _build_idle_camera_twins(client, twin_uuid)
    if not camera_twins:
        logger.info("No cameras configured for idle camera streaming")
        return

    _stop_idle_camera_streaming()

    _idle_camera_stop_event = threading.Event()
    _idle_camera_time_reference = TimeReference()

    def _camera_command_callback(status: str, msg: str, camera_name: str = "default") -> None:
        logger.debug(
            "Idle camera stream callback: camera=%s status=%s msg=%s",
            camera_name,
            status,
            msg,
        )

    _idle_camera_manager = CameraStreamManager(
        client=client,
        twins=camera_twins,
        stop_event=_idle_camera_stop_event,
        time_reference=_idle_camera_time_reference,
        command_callback=_camera_command_callback,
    )
    _idle_camera_manager.start()
    logger.info("Idle camera streaming started for %d camera(s)", len(camera_twins))


def _stop_current_operation() -> None:
    """Stop any currently running operation (teleoperate, remoteoperate, or calibration).

    Stops gracefully: signals the script to exit first, giving it time to stop MQTT,
    send end messages, and cleanup. Only force-disconnects if the thread does not
    exit within the graceful timeout.
    """
    global _current_thread, _current_follower, _operation_stop_event, _calibration_proc
    global _current_client, _current_twin_uuid

    # Stop teleoperate/remoteoperate gracefully
    if _current_thread is not None and _current_thread.is_alive():
        logger.info("Stopping current operation (graceful shutdown) …")
        # 1. Signal scripts to exit (they will cleanup MQTT, send telemetry_end, etc.)
        if _operation_stop_event is not None:
            _operation_stop_event.set()
        # 2. Wait for thread to exit and cleanup
        _current_thread.join(timeout=GRACEFUL_JOIN_TIMEOUT)
        # 3. If still alive, force disconnect and wait again
        if _current_thread.is_alive():
            logger.warning("Operation did not stop in time; force disconnecting follower")
            if _current_follower is not None:
                try:
                    _current_follower.disconnect()  # type: ignore[union-attr]
                except Exception:
                    logger.exception("Error disconnecting follower")
            _current_thread.join(timeout=FORCE_DISCONNECT_JOIN_TIMEOUT)
            if _current_thread.is_alive():
                logger.warning("Operation thread did not stop after force disconnect")

        # 4. Main sends disconnect after script has exited (scripts send telemetry_end)
        if _current_client is not None and _current_twin_uuid is not None:
            try:
                mqtt = _current_client.mqtt
                if hasattr(mqtt, "publish_disconnected"):
                    mqtt.publish_disconnected(_current_twin_uuid)
                else:
                    # Fallback for older SDK versions
                    import time as _time

                    topic = f"{mqtt.topic_prefix}cyberwave/twin/{_current_twin_uuid}/telemetry"
                    message = {"type": "disconnected", "timestamp": _time.time()}
                    mqtt.publish(topic, message)
            except Exception:
                logger.exception("Error publishing disconnect")

    _current_thread = None
    _current_follower = None
    _operation_stop_event = None
    _current_client = None
    _current_twin_uuid = None

    # Stop calibration subprocess gracefully (SIGTERM first, then SIGKILL)
    if _calibration_proc is not None:
        logger.info("Stopping calibration subprocess (graceful shutdown) …")
        try:
            _calibration_proc.terminate()
            _calibration_proc.wait(timeout=5)
        except Exception:
            try:
                _calibration_proc.kill()
            except Exception:
                pass
        finally:
            _calibration_proc = None


# Commands that map to so101-* scripts (pyproject.toml [project.scripts])
# Long-running: remoteoperate, teleoperate (run in background thread)
# One-off: calibrate, find_port, read_device, setup, write_position (run as subprocess)
# stop: stop current operation (not a script, but supported)
SUPPORTED_COMMANDS = frozenset(
    {
        "remoteoperate",
        "teleoperate",
        "recalibrate",
        "calibrate",
        "find_port",
        "read_device",
        "setup",
        "write_position",
        "stop",
    }
)


def _remove_local_calibration_files() -> int:
    """Remove local SO101 calibration JSON files.

    Returns the number of removed files.
    """
    from utils.config import get_so101_lib_dir

    calibration_dir = get_so101_lib_dir() / "calibrations"
    if not calibration_dir.is_dir():
        return 0

    removed = 0
    for calibration_file in calibration_dir.glob("*.json"):
        try:
            calibration_file.unlink()
            removed += 1
        except OSError:
            logger.warning("Failed to remove calibration file: %s", calibration_file)
    return removed


def _restart_current_process(delay_seconds: float = 0.5) -> None:
    """Restart the current process in-place after a short delay."""
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    try:
        os.execv(sys.executable, [sys.executable, *sys.argv])
    except Exception:
        logger.exception("Failed to restart process with execv; exiting for supervisor restart")
        os._exit(0)


def _handle_recalibrate(client: Cyberwave, twin_uuid: str) -> None:
    """Handle recalibration request by clearing calibration files and restarting."""
    _stop_idle_camera_streaming()
    _stop_current_operation()
    removed_files = _remove_local_calibration_files()
    logger.info(
        "Recalibration requested for twin %s: removed %d calibration file(s)",
        twin_uuid,
        removed_files,
    )
    client.mqtt.publish_command_message(twin_uuid, "ok")
    threading.Thread(
        target=_restart_current_process,
        daemon=True,
        name="so101-restart",
    ).start()


def _infer_calibration_device_type(calibration: dict, description: str = "") -> str:
    """Infer leader/follower type from calibration metadata."""
    device_type = calibration.get("device_type")
    if device_type in {"leader", "follower"}:
        return device_type
    if description:
        d = description.lower()
        if "leader" in d:
            return "leader"
        if "follower" in d:
            return "follower"
    return "follower" if calibration.get("follower_port") else "leader"


def _wait_for_running_calibration_proc(
    timeout_seconds: float = 1.5, poll_interval_seconds: float = 0.05
) -> Optional[subprocess.Popen]:
    """Wait briefly for a running calibration subprocess to become available."""
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        proc = _calibration_proc
        if proc is not None and proc.stdin is not None and proc.poll() is None:
            return proc
        time.sleep(poll_interval_seconds)
    return None


def _update_calibration_alert_metadata(
    client: Cyberwave, twin_uuid: str, alert_uuid: str, calibration_updates: dict
) -> None:
    """Best-effort update to alert.metadata.calibration."""
    if not alert_uuid:
        return
    try:
        robot = client.twin(twin_id=twin_uuid)
        alert = robot.alerts.get(alert_uuid)
        meta = dict(alert.metadata or {})
        calibration_meta = dict(meta.get("calibration") or {})
        calibration_meta.update(calibration_updates)
        meta["calibration"] = calibration_meta
        alert.update(metadata=meta)
    except Exception:
        logger.debug(
            "Could not update calibration alert metadata for alert %s",
            alert_uuid,
            exc_info=True,
        )


def _build_calibration_start_data_from_alert(
    client: Cyberwave, twin_uuid: str, data: dict
) -> Optional[dict]:
    """Build a start_calibration payload from alert metadata for recovery."""
    alert_uuid = data.get("alert_uuid")
    if not alert_uuid:
        return None
    try:
        robot = client.twin(twin_id=twin_uuid)
        alert = robot.alerts.get(alert_uuid)
        meta = dict(alert.metadata or {})
        calibration_meta = dict(meta.get("calibration") or {})
        device_type = data.get("type") or _infer_calibration_device_type(
            calibration_meta,
            str(alert.description or ""),
        )
        start_data = {
            "step": "start_calibration",
            "type": device_type,
            "alert_uuid": alert_uuid,
        }
        for key in ("follower_port", "follower_id", "leader_port", "leader_id"):
            value = data.get(key) or calibration_meta.get(key)
            if value:
                start_data[key] = value
        return start_data
    except Exception:
        logger.exception(
            "Failed to build calibration recovery payload from alert %s",
            alert_uuid,
        )
        return None


def _handle_calibration_advance(
    client: Cyberwave, twin_uuid: str, data: Optional[dict] = None
) -> None:
    """Send Enter to the running calibration subprocess (simulates user pressing Enter)."""
    global _calibration_proc

    proc = _calibration_proc
    if proc is None or proc.stdin is None or proc.poll() is not None:
        logger.warning("No calibration subprocess running; cannot advance")
        recovery_data = _build_calibration_start_data_from_alert(client, twin_uuid, data or {})
        if recovery_data is not None:
            logger.info(
                "Attempting calibration recovery before advance (alert=%s)",
                recovery_data.get("alert_uuid"),
            )
            _handle_calibration_start(
                client,
                twin_uuid,
                recovery_data,
                publish_status=False,
            )
            proc = _wait_for_running_calibration_proc()
        else:
            proc = None

        if proc is None:
            client.mqtt.publish_command_message(
                twin_uuid,
                {"status": "error", "reason": "calibration_not_running"},
            )
            return

    try:
        proc.stdin.write(b"\n")
        proc.stdin.flush()
        logger.info("Sent advance (Enter) to calibration subprocess")
        client.mqtt.publish_command_message(twin_uuid, "ok")
    except Exception:
        logger.exception("Failed to send advance to calibration subprocess")
        client.mqtt.publish_command_message(twin_uuid, "error")


def _handle_calibration_cancel(
    client: Cyberwave, twin_uuid: str, data: Optional[dict] = None
) -> None:
    """Terminate the running calibration subprocess."""
    global _calibration_proc
    alert_uuid = (data or {}).get("alert_uuid")
    if _calibration_proc is None:
        logger.warning("No calibration subprocess running; cannot cancel")
        if alert_uuid:
            _update_calibration_alert_metadata(
                client, twin_uuid, alert_uuid, {"state": "cancelled", "joints": {}}
            )
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return
    try:
        _calibration_proc.terminate()
        _calibration_proc.wait(timeout=5)
    except Exception:
        try:
            _calibration_proc.kill()
        except Exception:
            pass
    finally:
        _calibration_proc = None
    logger.info("Calibration subprocess cancelled")
    if alert_uuid:
        _update_calibration_alert_metadata(
            client, twin_uuid, alert_uuid, {"state": "cancelled", "joints": {}}
        )
    client.mqtt.publish_command_message(twin_uuid, "ok")


def _run_calibration_with_advance(
    client: Cyberwave,
    twin_uuid: str,
    device_type: str,
    port: str,
    device_id: str,
    alert_uuid: Optional[str] = None,
) -> None:
    """Run calibration subprocess with stdin=PIPE so MQTT 'advance' commands can inject Enter."""
    global _calibration_proc, _calibration_client, _calibration_twin_uuid, _calibration_alert_uuid

    _calibration_client = client
    _calibration_twin_uuid = twin_uuid
    _calibration_alert_uuid = alert_uuid

    cmd = [
        sys.executable,
        "-m",
        "scripts.cw_calibrate",
        "--type",
        device_type,
        "--port",
        port,
        "--id",
        device_id,
    ]
    if alert_uuid:
        cmd.extend(["--alert-uuid", alert_uuid, "--twin-uuid", twin_uuid])
    logger.info("Starting calibration subprocess (stdin=PIPE): %s", " ".join(cmd))

    try:
        _calibration_proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        result = _calibration_proc.wait()
    except Exception:
        logger.exception("Calibration subprocess failed")
        result = -1
    finally:
        _calibration_proc = None

    if result == 0:
        client.mqtt.publish_command_message(twin_uuid, "ok")
        if alert_uuid:
            _update_calibration_alert_metadata(
                client,
                twin_uuid,
                alert_uuid,
                {"state": "completed", "joints": {}},
            )
        if alert_uuid:
            try:
                robot = client.twin(twin_id=twin_uuid)
                alert = robot.alerts.get(alert_uuid)
                alert.resolve()
                logger.info("Calibration alert %s resolved", alert_uuid)
            except Exception:
                logger.exception("Failed to resolve calibration alert")
        # Recover: run the pending command (only teleoperate or remoteoperate)
        global _pending_recovery_command
        if _pending_recovery_command and _pending_recovery_command in RECOVERY_COMMANDS:
            cmd = _pending_recovery_command
            _pending_recovery_command = None
            logger.info("Calibration complete; recovering with command: %s", cmd)
            try:
                if cmd == "teleoperate":
                    start_teleoperate(client, twin_uuid)
                elif cmd == "remoteoperate":
                    start_remoteoperate(client, twin_uuid)
                else:
                    logger.warning("Unknown recovery command: %s", cmd)
            except Exception:
                logger.exception("Failed to run recovery command %s", cmd)
                client.mqtt.publish_command_message(twin_uuid, "error")
    else:
        if alert_uuid:
            _update_calibration_alert_metadata(
                client,
                twin_uuid,
                alert_uuid,
                {"state": "error"},
            )
        _pending_recovery_command = None
        client.mqtt.publish_command_message(twin_uuid, "error")


def _handle_calibration_start(
    client: Cyberwave,
    twin_uuid: str,
    data: dict,
    force_restart: bool = False,
    publish_status: bool = True,
) -> None:
    """Start calibration subprocess in background with stdin=PIPE for advance commands."""
    global _calibration_proc, _pending_recovery_command

    if _is_control_operation_running():
        logger.warning("Cannot start calibration while teleoperate/remoteoperate is running")
        _publish_operation_running_error(client, twin_uuid)
        return

    # Reject concurrent calibration (check before _stop_current_operation, which clears _calibration_proc)
    if _calibration_proc is not None and _calibration_proc.poll() is None:
        if not force_restart:
            logger.warning("Calibration already running; rejecting start_calibration")
            client.mqtt.publish_command_message(
                twin_uuid,
                {"status": "error", "reason": "calibration_already_running"},
            )
            return
        logger.info("Calibration restart requested; stopping current calibration first")

    # When restarting calibration, preserve recovery command: the running calibration thread
    # will clear _pending_recovery_command when its subprocess is terminated. Restore it
    # after _stop_current_operation so the new calibration can run recovery on completion.
    saved_recovery_command: Optional[str] = None
    if force_restart and _pending_recovery_command in RECOVERY_COMMANDS:
        saved_recovery_command = _pending_recovery_command

    # Only one script at a time: stop teleoperate/remoteoperate before starting calibration
    # Guard above ensures we never stop an active teleop/remoteop here.
    _stop_current_operation()

    if saved_recovery_command is not None:
        _pending_recovery_command = saved_recovery_command
        logger.info("Restored recovery command for after calibration: %s", saved_recovery_command)

    device_type = data.get("type") or "follower"
    cfg = _get_hardware_config(twin_uuid)
    # Prefer port from payload when present (e.g. old frontend); else use config.
    port = (
        (data.get("leader_port") if device_type == "leader" else data.get("follower_port"))
        or data.get("port")
        or (cfg.get("follower_port") if device_type == "follower" else cfg.get("leader_port"))
        or (
            os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
            if device_type == "follower"
            else os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
        )
    )
    if device_type == "leader":
        device_id = data.get("leader_id") or data.get("id") or cfg.get("leader_id") or "leader1"
    else:
        device_id = data.get("follower_id") or data.get("id") or cfg.get("follower_id") or "follower1"

    if not port:
        logger.error("No port for calibration (type=%s)", device_type)
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    alert_uuid = data.get("alert_uuid")

    # When no alert_uuid is provided (e.g. frontend "Run again" button),
    # create a calibration alert so the subprocess gets --twin-uuid and
    # --alert-uuid, enabling the alert-driven "Go Ahead" flow instead of
    # blocking on a console input() that nobody can advance.
    if not alert_uuid:
        try:
            robot = client.twin(twin_id=twin_uuid)
            cal_meta: dict = {
                "device_type": device_type,
                "state": "started",
                "follower_port": cfg.get("follower_port"),
                "follower_id": cfg.get("follower_id"),
                "leader_port": cfg.get("leader_port"),
                "leader_id": cfg.get("leader_id"),
            }
            cal_meta = {k: v for k, v in cal_meta.items() if v is not None}
            alert = robot.alerts.create(
                name="Calibration Needed",
                description=(
                    f"Bring the {device_type} to the position zero "
                    "and then click on `Go Ahead`"
                ),
                media=CALIBRATION_NEEDED_MEDIA_URL,
                severity="warning",
                alert_type="calibration_needed",
                metadata={"calibration": cal_meta},
            )
            alert_uuid = alert.uuid
            logger.info(
                "Created calibration alert %s for restart (twin %s)",
                alert_uuid,
                twin_uuid,
            )
        except Exception:
            logger.warning(
                "Could not create calibration alert; subprocess will use console flow",
                exc_info=True,
            )

    # Update alert state immediately so frontend knows calibration started.
    # When restarting, flush calibration metadata (state, joints) so UI shows clean state.
    if alert_uuid:
        try:
            robot = client.twin(twin_id=twin_uuid)
            alert = robot.alerts.get(alert_uuid)
            meta = dict(alert.metadata or {})
            cal = dict(meta.get("calibration") or {})
            if force_restart:
                cal = {
                    "device_type": device_type,
                    "state": "started",
                    "follower_port": cfg.get("follower_port"),
                    "follower_id": cfg.get("follower_id"),
                    "leader_port": cfg.get("leader_port"),
                    "leader_id": cfg.get("leader_id"),
                }
                cal = {k: v for k, v in cal.items() if v is not None}
            else:
                cal["state"] = "started"
                cal["device_type"] = cal.get("device_type") or data.get("type") or device_type
            meta["calibration"] = cal
            alert.update(metadata=meta)
        except Exception:
            logger.debug("Could not update alert state to started", exc_info=True)

    thread = threading.Thread(
        target=_run_calibration_with_advance,
        args=(client, twin_uuid, device_type, port, device_id),
        kwargs={"alert_uuid": alert_uuid},
        daemon=True,
        name="calibration",
    )
    thread.start()
    logger.info("Calibration thread started for %s on %s", device_type, port)
    if publish_status:
        client.mqtt.publish_command_message(twin_uuid, "ok")


def _handle_calibration_restart(client: Cyberwave, twin_uuid: str, data: dict) -> None:
    """Restart calibration by terminating current calibration process and starting again."""
    _handle_calibration_start(client, twin_uuid, data, force_restart=True)


def _run_script_command(
    client: Cyberwave,
    twin_uuid: str,
    script_name: str,
    data: dict,
    extra_cli: list[str] | None = None,
) -> None:
    """Run a one-off script (calibrate, find_port, read_device, setup, write_position) as subprocess.

    Stops any running teleoperate/remoteoperate/calibration first (only one script at a time).
    extra_cli: raw CLI args from payload.args when it's a list (e.g. ["--find-port"]).
    """
    if script_name == "calibrate" and _is_control_operation_running():
        logger.warning("Rejecting calibrate command while teleoperate/remoteoperate is running")
        _publish_operation_running_error(client, twin_uuid)
        return

    _stop_current_operation()
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
            port = (
                data.get("port")
                or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
                or os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
            )
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
    finally:
        _start_idle_camera_streaming(client, twin_uuid)


def _handle_controller_changed(client: Cyberwave, twin_uuid: str, payload: dict) -> None:
    """React to controller-changed: start teleoperate for localop, remoteoperate otherwise."""
    controller = payload.get("controller")
    if not controller or not isinstance(controller, dict):
        logger.info("Controller cleared; stopping current operation")
        _stop_current_operation()
        _start_idle_camera_streaming(client, twin_uuid)
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return

    controller_type = (controller.get("controller_type") or "").strip().lower()
    logger.info("Controller changed to type=%s, starting corresponding operation", controller_type)

    _stop_idle_camera_streaming()
    _stop_current_operation()
    try:
        if controller_type == "localop":
            start_teleoperate(client, twin_uuid)
        else:
            start_remoteoperate(client, twin_uuid)
    except Exception:
        logger.exception("Failed to start operation for controller type %s", controller_type)
        _start_idle_camera_streaming(client, twin_uuid)
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

    # Calibrate with step: frontend-driven flow (advance = inject Enter to subprocess)
    if cmd == "calibrate":
        step = script_args.get("step")
        if step in {"start_calibration", "restart_calibration", "restart", "advance", "cancel"}:
            if _is_control_operation_running():
                logger.warning(
                    "Rejecting calibrate step=%s while teleoperate/remoteoperate is running",
                    step,
                )
                _publish_operation_running_error(client, twin_uuid)
                return
        if step == "advance":
            _handle_calibration_advance(client, twin_uuid, script_args)
            return
        if step == "cancel":
            _handle_calibration_cancel(client, twin_uuid, script_args)
            return
        if step == "start_calibration":
            _handle_calibration_start(client, twin_uuid, script_args)
            return
        if step in {"restart_calibration", "restart"}:
            _handle_calibration_restart(client, twin_uuid, script_args)
            return

    match cmd:
        case "stop":
            _stop_current_operation()
            _start_idle_camera_streaming(client, twin_uuid)
            client.mqtt.publish_command_message(twin_uuid, "ok")
        case "recalibrate":
            _handle_recalibrate(client, twin_uuid)
        case "remoteoperate":
            _stop_idle_camera_streaming()
            _stop_current_operation()
            try:
                start_remoteoperate(client, twin_uuid)
            except Exception:
                logger.exception("Failed to start remoteoperate")
                _start_idle_camera_streaming(client, twin_uuid)
                client.mqtt.publish_command_message(twin_uuid, "error")
        case "teleoperate":
            _stop_idle_camera_streaming()
            _stop_current_operation()
            try:
                start_teleoperate(client, twin_uuid)
            except Exception:
                logger.exception("Failed to start teleoperate")
                _start_idle_camera_streaming(client, twin_uuid)
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
    global _current_thread, _current_follower, _operation_stop_event
    global _current_client, _current_twin_uuid

    logger.info("Starting remoteoperate")
    _stop_idle_camera_streaming()

    _current_client = client
    _current_twin_uuid = twin_uuid

    cfg = _get_hardware_config(twin_uuid)
    follower_port = cfg["follower_port"]
    if not follower_port:
        logger.error("follower_port not set in setup.json or env – cannot start remoteoperate")
        return

    follower_id = cfg["follower_id"]
    max_relative_target = float(cfg["max_relative_target"]) if cfg["max_relative_target"] else None

    if not _is_follower_calibrated(follower_id):
        _trigger_alert_and_switch_to_calibration(
            client,
            twin_uuid,
            follower_port,
            follower_id,
            recovery_command="remoteoperate",
        )
        return

    operation_stop_event = threading.Event()
    _operation_stop_event = operation_stop_event

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
            res_str = (
                f"{res[0]}x{res[1]}"
                if isinstance(res, (list, tuple)) and len(res) >= 2
                else str(res)
            )
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
                stop_event=operation_stop_event,
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
    global _current_thread, _current_follower, _operation_stop_event
    global _current_client, _current_twin_uuid

    logger.info("Starting teleoperate")
    _stop_idle_camera_streaming()

    _current_client = client
    _current_twin_uuid = twin_uuid

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
    max_relative_target = float(cfg["max_relative_target"]) if cfg["max_relative_target"] else None

    if not _is_follower_calibrated(follower_id):
        _trigger_alert_and_switch_to_calibration(
            client,
            twin_uuid,
            follower_port,
            follower_id,
            leader_port=leader_port,
            leader_id=leader_id,
            recovery_command="teleoperate",
            device_type="follower",
        )
        return
    if not _is_leader_calibrated(leader_id):
        _trigger_alert_and_switch_to_calibration(
            client,
            twin_uuid,
            follower_port,
            follower_id,
            leader_port=leader_port,
            leader_id=leader_id,
            recovery_command="teleoperate",
            device_type="leader",
        )
        return

    operation_stop_event = threading.Event()
    _operation_stop_event = operation_stop_event

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
            res_str = (
                f"{res[0]}x{res[1]}"
                if isinstance(res, (list, tuple)) and len(res) >= 2
                else str(res)
            )
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
        leader_config = LeaderConfig(port=leader_port, id=leader_id)
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
                stop_event=operation_stop_event,
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
    client = Cyberwave(api_key=token, source_type="edge")

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
    _start_idle_camera_streaming(client, twin_uuid)

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
    _stop_idle_camera_streaming()
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
