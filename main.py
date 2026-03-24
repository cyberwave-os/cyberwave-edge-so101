"""Main entry point for the SO101 edge node.

Connects to the Cyberwave MQTT broker and listens for command events.
Runs as a long-running process inside the Docker container.
"""

import asyncio
import json
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from cyberwave import Cyberwave
from cyberwave.sensor import CameraStreamManager
from cyberwave.utils import TimeReference

from utils.config import get_default_mqtt_port
from utils.cw_alerts import get_calibration_media_url
from utils.errors import DeviceConnectionError

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("so101-edge")


def _load_workspace_id_from_environment_json() -> Optional[str]:
    """Load workspace_uuid from environment.json (written by edge-core).

    The edge-core writes environment.json to the config directory with:
        {"uuid": "...", "workspace_uuid": "...", ...}

    Returns the workspace_uuid if found, None otherwise.
    """
    edge_config_dir = os.getenv("CYBERWAVE_EDGE_CONFIG_DIR", "").strip()
    if not edge_config_dir:
        edge_config_dir = "/etc/cyberwave"

    env_file = Path(edge_config_dir) / "environment.json"
    if not env_file.is_file():
        logger.debug("environment.json not found at %s", env_file)
        return None

    try:
        with open(env_file) as f:
            data = json.load(f)
        workspace_uuid = data.get("workspace_uuid")
        if workspace_uuid and isinstance(workspace_uuid, str):
            logger.info("Loaded workspace_id from environment.json: %s", workspace_uuid)
            return workspace_uuid.strip()
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read environment.json: %s", exc)

    return None

# ---------------------------------------------------------------------------
# Module-level state for tracking the currently running operation.
# Only one operation runs at a time: teleoperate, remoteoperate, or calibration.
# No concurrent calibration + teleop, teleop + remoteop, or multiple calibration.
# All entry points must call _stop_current_operation() before starting a new one.
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
_calibration_flow_step: Optional[str] = None
_calibration_context: Dict[str, Any] = {}
_calibration_last_exit_code: Optional[int] = None
# Lifecycle: cleared when starting a new calibration (_run_calibration_with_advance);
# set when the subprocess exits (success/failure in _run_calibration_with_advance,
# or terminate in _handle_calibration_cancel / _stop_current_operation).
# Wait on this to detect process exit (e.g. _wait_for_calibration_exit).
_calibration_finished_event = threading.Event()
_calibration_lock = threading.Lock()
# 0 = idle, 1 = calibration running. Protects against concurrent calibration starts.
_calibration_active_count = 0
# Set to True when a button handler is processing the calibration result.
# Used to prevent duplicate alert creation between button handler and background thread.
_calibration_button_processing = False

# When calibration completes, run this command. teleoperate/remoteoperate start operations;
# check_startup_calibration re-runs the startup calibration check (e.g. to chain leader after follower).
RECOVERY_COMMANDS = frozenset({"teleoperate", "remoteoperate", "check_startup_calibration"})
_pending_recovery_command: Optional[str] = None

CALIBRATION_BUTTON_FLOW = "so101_calibration"
CALIBRATION_STEP_IDLE = "idle"
CALIBRATION_STEP_ZERO = "zero_pose_waiting"
CALIBRATION_STEP_RANGE = "joint_calibration_waiting"
CALIBRATION_STEP_ERROR = "error"
CALIBRATION_ADVANCE_SETTLE_SECONDS = 1.2
CALIBRATION_COMPLETE_TIMEOUT_SECONDS = 30.0

# Graceful shutdown: give scripts time to stop MQTT, send end messages, cleanup.
GRACEFUL_JOIN_TIMEOUT = 30.0  # seconds to wait for thread to exit and cleanup
FORCE_DISCONNECT_JOIN_TIMEOUT = 5.0  # seconds after force disconnect if graceful fails


def _is_control_operation_running() -> bool:
    """True when teleoperate/remoteoperate thread is active."""
    return _current_thread is not None and _current_thread.is_alive()


def _is_calibration_running() -> bool:
    """True when calibration subprocess is active."""
    with _calibration_lock:
        return _calibration_active_count > 0 or _calibration_proc is not None


def _is_any_operation_running() -> bool:
    """True when any control operation (teleop/remoteop) or calibration is running."""
    return _is_control_operation_running() or _is_calibration_running()


def _publish_operation_running_error(client: Cyberwave, twin_uuid: str) -> None:
    """Publish a consistent error payload for commands blocked by active operation."""
    client.mqtt.publish_command_message(
        twin_uuid,
        {"status": "error", "reason": "operation_running"},
    )


def _build_calibration_context(
    twin_uuid: str,
    data: Dict[str, Any],
    cfg: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build normalized leader/follower calibration context from payload/config."""
    device_type = data.get("type") or data.get("device_type") or "follower"
    normalized = "leader" if str(device_type).lower() == "leader" else "follower"
    if str(device_type).lower() not in ("leader", "follower"):
        logger.warning(
            "Unexpected calibration device_type=%r, defaulting to %s",
            device_type,
            normalized,
        )
    cfg = cfg or _get_hardware_config(twin_uuid)
    recovery_command = data.get("recovery_command")
    if recovery_command not in RECOVERY_COMMANDS:
        recovery_command = _pending_recovery_command if _pending_recovery_command in RECOVERY_COMMANDS else None

    context = {
        "device_type": normalized,
        "follower_port": data.get("follower_port") or cfg.get("follower_port"),
        "follower_id": data.get("follower_id") or cfg.get("follower_id") or "follower1",
        "leader_port": data.get("leader_port") or cfg.get("leader_port"),
        "leader_id": data.get("leader_id") or cfg.get("leader_id") or "leader1",
        "recovery_command": recovery_command,
    }
    return context


def _build_calibration_button_payload(
    context: Dict[str, Any],
    action: str,
    alert_uuid: Optional[str] = None,
) -> Dict[str, Any]:
    """Create metadata.buttons payload for calibration flow actions."""
    payload: Dict[str, Any] = {
        "flow": CALIBRATION_BUTTON_FLOW,
        "action": action,
        "type": context.get("device_type", "follower"),
    }
    for key in ("follower_port", "follower_id", "leader_port", "leader_id", "recovery_command"):
        value = context.get(key)
        if value is not None:
            payload[key] = value
    if alert_uuid:
        payload["alert_uuid"] = alert_uuid
    return payload


def _build_restart_calibration_button(context: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Build a 'Restart calibration' button for error alerts."""
    return [
        {
            "label": "Restart calibration",
            "payload": _build_calibration_button_payload(context, "start"),
        }
    ]


def _resolve_alert_by_uuid(client: Cyberwave, twin_uuid: str, alert_uuid: Optional[str]) -> None:
    """Best-effort alert resolution by UUID."""
    if not alert_uuid:
        return
    try:
        robot = client.twin(twin_id=twin_uuid)
        alert = robot.alerts.get(alert_uuid)
        if alert is not None:
            alert.resolve()
    except Exception:
        logger.debug("Could not resolve alert %s", alert_uuid, exc_info=True)


# Base URL for SO101 documentation
SO101_DOCS_BASE_URL = "https://docs.cyberwave.com/hardware/so101/get-started"

# Path for reading connection error details (written by cw_calibrate.py)
CONNECTION_ERROR_FILE = Path("/tmp/cyberwave_calibration_error.json")


def _slugify_error_type(error_type: str) -> str:
    """Convert error type to URL-friendly slug (lowercase, hyphens instead of underscores)."""
    return re.sub(r"[^a-z0-9]+", "-", error_type.lower()).strip("-")


# Maximum age of error file in seconds before it's considered stale
_ERROR_FILE_MAX_AGE_SECONDS = 60.0


def _read_connection_error_details() -> Dict[str, Any]:
    """Read connection error details from temp file written by cw_calibrate.

    Returns empty dict if file doesn't exist, is stale (>60s old), or invalid.
    """
    try:
        if CONNECTION_ERROR_FILE.exists():
            data = json.loads(CONNECTION_ERROR_FILE.read_text())
            CONNECTION_ERROR_FILE.unlink()  # Clean up after reading

            # Validate timestamp to avoid reading stale error files
            timestamp = data.get("timestamp")
            if timestamp is not None:
                age = time.time() - float(timestamp)
                if age > _ERROR_FILE_MAX_AGE_SECONDS:
                    logger.debug(
                        "Ignoring stale error file (%.1fs old, max %.1fs)",
                        age,
                        _ERROR_FILE_MAX_AGE_SECONDS,
                    )
                    return {}

            return data
    except Exception as e:
        logger.debug("Could not read connection error details: %s", e)
    return {}


def _create_error_alert(
    client: Cyberwave,
    twin_uuid: str,
    error_type: str,
    error_message: str,
    *,
    severity: str = "error",
    details: Optional[str] = None,
    buttons: Optional[List[Dict[str, Any]]] = None,
    calibration_context: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Create an error alert with a link to documentation.

    These alerts are NOT auto-resolved so users can see them.
    The URL fragment includes both the error type and a slugified version of the message.

    Args:
        buttons: Optional list of button metadata for the alert (e.g., restart calibration)
        calibration_context: Optional calibration context to include in metadata for restart
    """
    try:
        robot = client.twin(twin_id=twin_uuid)
        # Create slug from error type and message for more specific doc linking
        error_type_slug = _slugify_error_type(error_type)
        message_slug = _slugify_error_type(error_message)
        # Combine: error-type--message-slug (truncate message slug to keep URL reasonable)
        full_slug = f"{error_type_slug}--{message_slug[:80]}" if message_slug else error_type_slug
        doc_url = f"{SO101_DOCS_BASE_URL}#{full_slug}"
        description = error_message
        # Only include details if they provide additional information
        # if details and details not in error_message:
            # description += f"\n\nDetails: {details}"
        description += f"\n\nFor troubleshooting, see: {doc_url}"

        metadata: Dict[str, Any] = {
            "error_type": error_type,
            "doc_url": doc_url,
        }
        if buttons:
            metadata["buttons"] = buttons
        if calibration_context:
            metadata["calibration"] = calibration_context

        alert = robot.alerts.create(
            name=f"Edge Error: {error_type}",
            description=description,
            severity=severity,
            alert_type=f"edge_error_{error_type}",
            metadata=metadata,
        )
        logger.info("Created error alert %s for %s", alert.uuid, error_type)
        return alert.uuid
    except Exception:
        logger.exception("Failed to create error alert for %s", error_type)
        return None


def _create_calibration_warning_alert(
    client: Cyberwave,
    twin_uuid: str,
    warnings: List[str],
    device_type: str,
) -> Optional[str]:
    """Create a warning alert for calibration with limited ranges (5-20%).

    This alert is separate from the guided calibration flow and won't be resolved automatically.
    """
    try:
        robot = client.twin(twin_id=twin_uuid)
        description = (
            f"Calibration for {device_type} completed with limited joint ranges. "
            "The robot will work but may have reduced accuracy.\n\n"
            "Joints with limited ranges:\n" + "\n".join(f"• {w}" for w in warnings) +
            "\n\nConsider recalibrating and moving joints further through their range of motion."
        )
        alert = robot.alerts.create(
            name=f"Calibration Warning: {device_type}",
            description=description,
            severity="warning",
            alert_type="calibration_limited_range",
            metadata={
                "device_type": device_type,
                "warnings": warnings,
            },
        )
        logger.info("Created calibration warning alert %s", alert.uuid)
        return alert.uuid
    except Exception:
        logger.exception("Failed to create calibration warning alert")
        return None


def _create_guided_calibration_alert(
    client: Cyberwave,
    twin_uuid: str,
    context: Dict[str, Any],
    *,
    stage: str,
    error_message: str = "",
    resolve_previous_alert_uuid: Optional[str] = None,
    warnings: Optional[List[str]] = None,
    severity: str = "warning",
) -> Optional[str]:
    """Create one calibration alert step with a metadata button payload."""
    if resolve_previous_alert_uuid:
        _resolve_alert_by_uuid(client, twin_uuid, resolve_previous_alert_uuid)

    robot = client.twin(twin_id=twin_uuid)
    device_type = context.get("device_type", "follower")
    is_error_stage = stage == CALIBRATION_STEP_ERROR

    if stage == CALIBRATION_STEP_IDLE:
        name = "Calibration Needed"
        description = (
            f"No calibration file found for {device_type}. "
            "Click `Start calibration` to begin the guided flow."
        )
        button_label = "Start calibration"
        button_action = "start"
    elif stage == CALIBRATION_STEP_ZERO:
        name = "Calibration - Step 1/2"
        description = (
            f"Move the {device_type} to the zero position and click `Next`."
        )
        button_label = "Next"
        button_action = "next"
    elif is_error_stage:
        name = "Calibration failed"
        description = (
            "Calibration failed due to insufficient joint movement. "
            "Move all joints through their full ranges and restart calibration."
        )
        if warnings:
            description += "\n\nJoints that need more movement:\n" + "\n".join(
                f"• {w}" for w in warnings
            )
        elif error_message:
            description += f"\n\n{error_message}"
        button_label = "Restart calibration"
        button_action = "restart"
    else:
        name = "Calibration - Step 2/2"
        description = (
            "Move all joints through their full ranges of motion, then click `Complete`."
        )
        button_label = "Complete"
        button_action = "complete"

    if error_message and not is_error_stage:
        description = f"{description}\n\nLast error: {error_message}"

    calibration_meta: Dict[str, Any] = {
        "state": stage,
        "device_type": context.get("device_type"),
        "follower_port": context.get("follower_port"),
        "follower_id": context.get("follower_id"),
        "leader_port": context.get("leader_port"),
        "leader_id": context.get("leader_id"),
        "recovery_command": context.get("recovery_command"),
    }
    if is_error_stage:
        calibration_meta["error"] = "insufficient_range"
        calibration_meta["action_blocked"] = True
        if warnings:
            calibration_meta["warnings"] = list(warnings)

    metadata: Dict[str, Any] = {
        "calibration": calibration_meta,
        "buttons": [
            {
                "label": button_label,
                "payload": _build_calibration_button_payload(context, button_action),
            }
        ],
    }

    media_step = (
        "range"
        if stage in (CALIBRATION_STEP_RANGE, CALIBRATION_STEP_ERROR)
        else "zero"
    )
    try:
        alert = robot.alerts.create(
            name=name,
            description=description,
            media=get_calibration_media_url(device_type, media_step),
            severity=severity,
            alert_type="calibration_needed",
            metadata=metadata,
        )
        payload = _build_calibration_button_payload(context, button_action, alert_uuid=alert.uuid)
        metadata["buttons"][0]["payload"] = payload
        alert.update(metadata=metadata)
        return alert.uuid
    except Exception:
        logger.exception("Failed to create guided calibration alert (stage=%s)", stage)
        return None


def _set_calibration_flow_state(
    step: str,
    context: Dict[str, Any],
    alert_uuid: Optional[str],
) -> None:
    """Persist in-memory guided calibration flow status."""
    global _calibration_flow_step, _calibration_context, _calibration_alert_uuid
    _calibration_flow_step = step
    _calibration_context = dict(context)
    _calibration_alert_uuid = alert_uuid


def _clear_calibration_flow_state(clear_recovery: bool = False) -> None:
    """Clear in-memory calibration flow state."""
    global _calibration_flow_step, _calibration_context, _calibration_alert_uuid, _pending_recovery_command, _calibration_button_processing
    _calibration_flow_step = None
    _calibration_context = {}
    _calibration_alert_uuid = None
    _calibration_button_processing = False
    if clear_recovery:
        _pending_recovery_command = None


def _wait_for_calibration_exit(timeout_seconds: float) -> Optional[int]:
    """Wait for calibration process exit and return code, or None on timeout."""
    with _calibration_lock:
        if _calibration_proc is None and _calibration_last_exit_code is not None:
            return _calibration_last_exit_code
    if not _calibration_finished_event.wait(timeout_seconds):
        return None
    with _calibration_lock:
        return _calibration_last_exit_code


def _run_pending_recovery_command(client: Cyberwave, twin_uuid: str) -> None:
    """Execute pending command after calibration success."""
    global _pending_recovery_command
    if not _pending_recovery_command or _pending_recovery_command not in RECOVERY_COMMANDS:
        return
    cmd = _pending_recovery_command
    _pending_recovery_command = None
    logger.info("Calibration complete; recovering with command: %s", cmd)
    if cmd == "teleoperate":
        start_teleoperate(client, twin_uuid)
    elif cmd == "remoteoperate":
        start_remoteoperate(client, twin_uuid)
    elif cmd == "check_startup_calibration":
        _check_startup_calibration(client, twin_uuid)


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
    Start calibration immediately (zero position step), same as "Run again" in twin-editor.
    No "Start calibration" button – go straight to asking user to put device at zero position.
    """
    global _pending_recovery_command
    if recovery_command and recovery_command in RECOVERY_COMMANDS:
        _pending_recovery_command = recovery_command
        logger.info("Stored recovery command: %s (will run after calibration)", recovery_command)

    data = {
        "device_type": device_type,
        "follower_port": follower_port,
        "follower_id": follower_id,
        "leader_port": leader_port,
        "leader_id": leader_id,
        "recovery_command": recovery_command,
    }
    _handle_calibration_start(client, twin_uuid, data, force_restart=False, publish_status=True)


def _is_follower_calibrated(follower_id: str = "follower1") -> bool:
    """Check if the follower is calibrated."""
    from utils.config import get_so101_lib_dir

    return (get_so101_lib_dir() / "calibrations" / f"{follower_id}.json").exists()


def _is_leader_calibrated(leader_id: str = "leader1") -> bool:
    """Check if the leader is calibrated."""
    from utils.config import get_so101_lib_dir

    return (get_so101_lib_dir() / "calibrations" / f"{leader_id}.json").exists()


def _check_startup_calibration(client: Cyberwave, twin_uuid: str) -> None:
    """Check if calibration files exist at startup. If missing, trigger the same flow as
    when teleoperate/remoteoperate or calibration command is run without calibration.

    Follower is checked first (required for both remoteoperate and teleoperate).
    Leader is checked second (required only for teleoperate).
    """
    cfg = _get_hardware_config(twin_uuid)
    follower_port = cfg.get("follower_port")
    leader_port = cfg.get("leader_port")
    follower_id = cfg.get("follower_id", "follower1")
    leader_id = cfg.get("leader_id", "leader1")

    if follower_port and not _is_follower_calibrated(follower_id):
        logger.info(
            "Startup: follower calibration missing for %s – triggering calibration flow",
            follower_id,
        )
        # When leader is also missing, chain so leader calibration runs after follower completes
        recovery = (
            "check_startup_calibration"
            if (leader_port and not _is_leader_calibrated(leader_id))
            else None
        )
        _trigger_alert_and_switch_to_calibration(
            client,
            twin_uuid,
            follower_port=follower_port,
            follower_id=follower_id,
            leader_port=leader_port,
            leader_id=leader_id,
            recovery_command=recovery,
            device_type="follower",
        )
        return

    if leader_port and not _is_leader_calibrated(leader_id):
        logger.info(
            "Startup: leader calibration missing for %s – triggering calibration flow",
            leader_id,
        )
        _trigger_alert_and_switch_to_calibration(
            client,
            twin_uuid,
            follower_port=follower_port or "",
            follower_id=follower_id,
            leader_port=leader_port,
            leader_id=leader_id,
            recovery_command=None,
            device_type="leader",
        )


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


def _load_primary_robot_twin(primary_uuid: str) -> Optional[Dict[str, Any]]:
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


def _load_all_twin_jsons() -> List[Dict[str, Any]]:
    """Load all twin JSON files from edge config dir (from edge-core)."""
    import json
    import uuid as uuid_module

    config_dir = _get_config_dir()
    if not config_dir.is_dir():
        return []
    result: List[Dict[str, Any]] = []
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


def _load_edge_fingerprint() -> Optional[str]:
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
    metadata: Dict[str, Any],
    fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
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


def _get_discovered_devices_from_edge_config(primary_uuid: str) -> List[Dict[str, Any]]:
    """Fallback: collect discovered_devices from edge_config in twin metadata.

    Used when v4l2 returns empty (e.g. container without /dev/video* access).
    Merges from primary robot and all workspace twin JSONs.
    """
    import json

    seen: set = set()
    result: List[Dict[str, Any]] = []
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
                continue
            if key in seen:
                continue
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


def _discover_devices_via_v4l2() -> List[Dict[str, Any]]:
    """Discover USB cameras via v4l2-ctl (Linux). Returns list of device dicts."""
    from utils.device_utils import discover_usb_cameras

    cameras = discover_usb_cameras()
    return [c.to_dict() for c in cameras]


def _merge_discovered_with_edge_config(
    discovered: List[Dict[str, Any]], primary_uuid: str
) -> tuple:
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
    merged: List[Dict[str, Any]] = list(discovered)
    for d in from_edge:
        path = d.get("primary_path") or (d.get("paths") or [None])[0]
        idx = d.get("index")
        key = path if path else (f"/dev/video{idx}" if idx is not None else None)
        if key and key not in seen_paths:
            seen_paths.add(key)
            merged.append(d)
    return merged, len(merged) - len(discovered)


def _annotate_discovered_devices_with_compatibility(discovered: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Refresh compatibility metadata for discovered or cached camera devices."""
    from utils.device_utils import annotate_discovered_devices

    return annotate_discovered_devices(discovered)


def _load_discovered_devices(primary_uuid: str) -> tuple:
    """Load camera devices from V4L2 and cached metadata, then probe compatibility."""
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
    discovered = _annotate_discovered_devices_with_compatibility(discovered)
    incompatible_devices = [
        (
            d.get("card"),
            d.get("primary_path") or d.get("index"),
            d.get("compatibility_reason"),
        )
        for d in discovered
        if d.get("is_compatible") is False
    ]
    if incompatible_devices:
        logger.info("Skipping incompatible camera device(s): %s", incompatible_devices)
    return discovered, added


def _push_discovered_devices_to_edge_config(
    twin_uuid: str,
    fingerprint: str,
    devices_list: List[Dict[str, Any]],
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
        client = Cyberwave(
            api_key=token,
            mqtt_port=get_default_mqtt_port(),
            source_type="edge",
        )
        client.twins.pair_device(twin_uuid, fingerprint, edge_config=merged)
        logger.info(
            "Pushed discovered_devices (%d) to edge_config for twin %s",
            len(devices_list),
            twin_uuid,
        )
    except Exception as e:
        logger.warning("Could not push discovered_devices to edge_config: %s", e)


def _twin_has_depth_sensor(twin: Dict[str, Any]) -> bool:
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


def _filter_realsense_devices(devices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter to RealSense only (card contains Intel/RealSense)."""
    return [
        d
        for d in devices
        if (d.get("is_compatible") is not False)
        and (
            "realsense" in (d.get("card") or "").lower()
            or "intel" in (d.get("card") or "").lower()
        )
    ]


def _filter_cv2_devices(devices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Filter to non-RealSense (CV2/USB webcams)."""
    return [
        d
        for d in devices
        if (d.get("is_compatible") is not False)
        and "realsense" not in (d.get("card") or "").lower()
        and "intel" not in (d.get("card") or "").lower()
    ]


def _extract_rgb_sensor_ids_from_twin_data(data: Dict[str, Any]) -> List[str]:
    """Extract RGB-like sensor IDs from twin universal schema/capabilities."""
    if not isinstance(data, dict):
        return []

    asset = data.get("asset") or {}
    metadata = data.get("metadata") or {}
    if isinstance(asset, dict):
        metadata = asset.get("metadata") or metadata

    sensor_ids: List[str] = []

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


def _get_primary_robot_default_rgb_sensor_id(primary_uuid: str) -> Optional[str]:
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


def _get_robot_twin_sensor_cameras(primary_uuid: str) -> List[Dict[str, Any]]:
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


def _device_identifiers(dev: Union[str, int]) -> set:
    """Return all identifiers for a device (path and index) for consistent matching."""
    ids: set = set()
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
    twin: Dict[str, Any],
    realsense_devices: List[Dict[str, Any]],
    fingerprint: Optional[str] = None,
) -> Optional[Union[str, int]]:
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


def _twin_is_camera_like(twin: Dict[str, Any]) -> bool:
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


def _discover_cameras_for_so101(primary_uuid: str) -> List[Dict[str, Any]]:
    """Find cameras for SO101: primary robot sensors + attached/workspace camera twins.

    Assigns devices from twin metadata discovered_devices to twins by sensor type:
    - Depth sensor twin → RealSense device
    - RGB sensor twin → CV2 device

    Each twin gets exactly one device; each device is used at most once.
    Supports max 3 cameras with semantic roles: wrist, primary, secondary.
    Includes both attached twins and workspace twins that are camera-like by
    canonical edge camera config or camera asset type.
    """
    discovered, _ = _load_discovered_devices(primary_uuid)
    compatible_discovered = [d for d in discovered if d.get("is_compatible") is not False]
    if len(compatible_discovered) != len(discovered):
        logger.info(
            "Camera compatibility: %d/%d discovered device(s) are usable",
            len(compatible_discovered),
            len(discovered),
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
    attached: List[Dict[str, Any]] = []
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
    result: List[Dict[str, Any]] = []

    def _device_key(d: Dict[str, Any]) -> Union[str, int]:
        return d.get("primary_path") or d.get("index", "?")

    def _is_device_used(dev: Union[str, int]) -> bool:
        """True if device (path or index) is already assigned."""
        return bool(_device_identifiers(dev) & used_devices)

    def _mark_device_used(dev: Union[str, int]) -> None:
        used_devices.update(_device_identifiers(dev))

    def _assign_from_pool(devices: List[Dict[str, Any]]) -> Optional[Union[str, int]]:
        """Return device from pool, or None if exhausted."""
        for d in devices:
            dev = d.get("primary_path") or d.get("index", 0)
            if dev is None or dev == "?":
                continue
            if not _is_device_used(dev):
                _mark_device_used(dev)
                return dev
        return None

    def _is_realsense_device(dev: Union[str, int]) -> bool:
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
    discovered, _ = _load_discovered_devices(twin_uuid)
    fingerprint = _load_edge_fingerprint()
    if discovered and fingerprint:
        _push_discovered_devices_to_edge_config(twin_uuid, fingerprint, discovered)

    if default_cameras:
        token = os.getenv("CYBERWAVE_API_KEY")
        if token:
            try:
                client = Cyberwave(
                    api_key=token,
                    mqtt_port=get_default_mqtt_port(),
                    source_type="edge",
                )
                robot = client.twin(twin_id=twin_uuid)
                from utils.cw_alerts import create_camera_default_device_alert

                # create_camera_default_device_alert(robot, default_cameras)
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
    for k in ("leader_port", "follower_port", "max_relative_target", "camera_fps", "mute_temperature_notifications"):
        if existing.get(k) is not None:
            config[k] = existing[k]
    save_setup_config(config, path)
    logger.info(
        "Setup updated at %s (wrist=%s, additional=%d)", path, bool(wrist_cam), len(add_cams)
    )


def _get_hardware_config(twin_uuid: str) -> Dict[str, Any]:
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
    cameras: List[Dict[str, Any]] = []
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


def _camera_name_override(camera_type: str) -> Optional[str]:
    """Return stream sensor override for known camera types.

    RealSense in SO101 should use the default stream sensor so backend RGB/depth
    pairing for colored point clouds matches the standalone camera driver path.
    """
    if "realsense" in str(camera_type).lower():
        return "default"
    return None


def _build_idle_camera_twins(client: Cyberwave, twin_uuid: str) -> List[Any]:
    """Build camera twins+overrides for idle streaming from setup.json."""
    from utils.utils import parse_resolution_to_enum

    cfg = _get_hardware_config(twin_uuid)
    camera_twins: List[Any] = []
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
        camera_name = _camera_name_override(cam_type)
        if camera_name is not None:
            overrides["camera_name"] = camera_name
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


def _stop_current_operation(
    client: Optional[Cyberwave] = None,
    twin_uuid: Optional[str] = None,
) -> None:
    """Stop any currently running operation (teleoperate, remoteoperate, or calibration).

    Stops gracefully: signals the script to exit first, giving it time to stop MQTT,
    send end messages, and cleanup. Only force-disconnects if the thread does not
    exit within the graceful timeout.

    When client and twin_uuid are provided (e.g. from controller-changed), any
    calibration alert is resolved. Otherwise uses _calibration_client and
    _calibration_twin_uuid when available.
    """
    global _current_thread, _current_follower, _operation_stop_event, _calibration_proc
    global _calibration_last_exit_code, _calibration_active_count
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
    with _calibration_lock:
        proc = _calibration_proc
    if proc is not None:
        logger.info("Stopping calibration subprocess (graceful shutdown) …")
        try:
            proc.terminate()
            _calibration_last_exit_code = proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
                _calibration_last_exit_code = proc.wait(timeout=2)
            except Exception:
                pass
        finally:
            with _calibration_lock:
                _calibration_proc = None
                _calibration_active_count = 0
            _calibration_finished_event.set()

    # Resolve calibration alert and clear flow state when calibration was active
    # (either subprocess was running or step-0 alert was shown)
    alert_uuid = _calibration_alert_uuid
    if alert_uuid:
        resolve_client = client or _calibration_client
        resolve_twin = twin_uuid or _calibration_twin_uuid
        if resolve_client and resolve_twin:
            _resolve_alert_by_uuid(resolve_client, resolve_twin, alert_uuid)
        _clear_calibration_flow_state(clear_recovery=False)


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
        logger.exception("Failed to restart process with execv; exiting with error code for supervisor restart")
        # Use sys.exit instead of os._exit to allow cleanup handlers to run
        sys.exit(1)


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
        with _calibration_lock:
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


def _get_alert_calibration_metadata(
    client: Cyberwave, twin_uuid: str, alert_uuid: str
) -> Optional[dict]:
    """Fetch calibration metadata from an alert (for extracting warnings on error)."""
    if not alert_uuid:
        return None
    try:
        robot = client.twin(twin_id=twin_uuid)
        alert = robot.alerts.get(alert_uuid)
        meta = dict(alert.metadata or {})
        return dict(meta.get("calibration") or {})
    except Exception:
        logger.debug("Could not fetch alert calibration metadata for %s", alert_uuid)
        return None


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
    client: Cyberwave,
    twin_uuid: str,
    data: Optional[dict] = None,
    *,
    publish_status: bool = True,
) -> bool:
    """Send Enter to the running calibration subprocess (simulates user pressing Enter)."""
    global _calibration_proc

    def _try_advance() -> Optional[bool]:
        """Try to send Enter. Returns True/False on success/failure, None if proc invalid."""
        with _calibration_lock:
            proc = _calibration_proc
            if proc is None or proc.stdin is None or proc.poll() is not None:
                return None
            try:
                proc.stdin.write(b"\n")
                proc.stdin.flush()
            except (BrokenPipeError, OSError) as e:
                logger.warning("Failed to send advance to calibration subprocess: %s", e)
                if publish_status:
                    client.mqtt.publish_command_message(twin_uuid, "error")
                return False
            except Exception:
                logger.exception("Failed to send advance to calibration subprocess")
                if publish_status:
                    client.mqtt.publish_command_message(twin_uuid, "error")
                return False
        logger.info("Sent advance (Enter) to calibration subprocess")
        if publish_status:
            client.mqtt.publish_command_message(twin_uuid, "ok")
        return True

    result = _try_advance()
    if result is not None:
        return result

    logger.warning("No calibration subprocess running; cannot advance")
    recovery_data = _build_calibration_start_data_from_alert(client, twin_uuid, data or {})
    stale_alert_uuid = (data or {}).get("alert_uuid")
    if stale_alert_uuid:
        _resolve_alert_by_uuid(client, twin_uuid, stale_alert_uuid)
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
        if proc is not None:
            result = _try_advance()
            if result is not None:
                return result

    if publish_status:
        client.mqtt.publish_command_message(
            twin_uuid,
            {"status": "error", "reason": "calibration_not_running"},
        )
    return False


def _handle_calibration_cancel(
    client: Cyberwave, twin_uuid: str, data: Optional[dict] = None
) -> None:
    """Terminate the running calibration subprocess."""
    global _calibration_proc, _calibration_last_exit_code, _calibration_active_count
    if not twin_uuid or not twin_uuid.strip():
        logger.warning("Invalid twin_uuid for calibration cancel; skipping")
        return
    alert_uuid = (data or {}).get("alert_uuid")
    with _calibration_lock:
        proc = _calibration_proc
    if proc is None:
        # Calibration may not have started yet; mark as cancelled so thread aborts
        with _calibration_lock:
            _calibration_active_count = 0
        logger.warning("No calibration subprocess running; cannot cancel")
        if alert_uuid:
            _update_calibration_alert_metadata(
                client, twin_uuid, alert_uuid, {"state": "cancelled", "joints": {}}
            )
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return
    try:
        proc.terminate()
        _calibration_last_exit_code = proc.wait(timeout=5)
    except Exception:
        try:
            proc.kill()
            _calibration_last_exit_code = proc.wait(timeout=2)
        except Exception:
            pass
    finally:
        with _calibration_lock:
            _calibration_proc = None
            _calibration_active_count = 0
        _calibration_finished_event.set()
    logger.info("Calibration subprocess cancelled")
    if alert_uuid:
        _update_calibration_alert_metadata(
            client, twin_uuid, alert_uuid, {"state": "cancelled", "joints": {}}
        )
        _resolve_alert_by_uuid(client, twin_uuid, alert_uuid)
    elif _calibration_alert_uuid:
        _resolve_alert_by_uuid(client, twin_uuid, _calibration_alert_uuid)
    _clear_calibration_flow_state(clear_recovery=True)
    client.mqtt.publish_command_message(twin_uuid, "ok")


def _handle_calibration_next(
    client: Cyberwave,
    twin_uuid: str,
    data: Optional[dict] = None,
) -> None:
    """Handle step-1 Next button: advance process and transition to step-2 alert."""
    global _pending_recovery_command, _calibration_button_processing
    data = data or {}

    # Idempotency: only process if we're in step-1 (zero pose)
    with _calibration_lock:
        current_step = _calibration_flow_step
        current_alert = _calibration_alert_uuid
    if current_step != CALIBRATION_STEP_ZERO:
        logger.warning(
            "Ignoring 'next' action - not in zero pose step (current: %s)",
            current_step,
        )
        return

    # Reject stale clicks from old alerts
    incoming_alert_uuid = data.get("alert_uuid")
    if incoming_alert_uuid and incoming_alert_uuid != current_alert:
        logger.warning("Ignoring 'next' action - stale alert_uuid %s", incoming_alert_uuid)
        return

    # Mark that button is being processed (prevents duplicate alerts from background thread)
    with _calibration_lock:
        global _calibration_button_processing
        _calibration_button_processing = True

    current_alert_uuid = data.get("alert_uuid") or _calibration_alert_uuid
    context = _build_calibration_context(twin_uuid, {**_calibration_context, **data})
    if not _handle_calibration_advance(client, twin_uuid, data, publish_status=False):
        client.mqtt.publish_command_message(
            twin_uuid,
            {"status": "error", "reason": "calibration_not_running"},
        )
        return

    time.sleep(CALIBRATION_ADVANCE_SETTLE_SECONDS)
    with _calibration_lock:
        proc = _calibration_proc
        last_code = _calibration_last_exit_code
    return_code = proc.poll() if proc is not None else last_code

    if return_code is None:
        next_alert_uuid = _create_guided_calibration_alert(
            client,
            twin_uuid,
            context,
            stage=CALIBRATION_STEP_RANGE,
            resolve_previous_alert_uuid=current_alert_uuid,
        )
        _set_calibration_flow_state(CALIBRATION_STEP_RANGE, context, next_alert_uuid)
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return

    # Exit codes 2, 3, 4 or any other = calibration errors: resolve calibration alert and show separate error
    _resolve_alert_by_uuid(client, twin_uuid, current_alert_uuid)
    _clear_calibration_flow_state(clear_recovery=False)  # Keep recovery command for restart
    device_type = context.get("device_type", "follower")
    port = context.get(f"{device_type}_port", "unknown")
    error_info = _read_connection_error_details()
    default_error_types = {
        2: "insufficient_range",
        3: "device_disconnected",
        4: "calibration_failed",
    }
    default_messages = {
        2: "Calibration failed: insufficient joint movement. Move all joints through their full ranges.",
        3: f"Device not found on {port}. Check that the robot is connected and powered on.",
        4: "Calibration failed unexpectedly.",
    }
    _create_error_alert(
        client,
        twin_uuid,
        error_type=error_info.get("error_type", default_error_types.get(return_code, "calibration_failed")),
        error_message=error_info.get("description", default_messages.get(return_code, f"Calibration failed (exit code {return_code}).")),
        details=error_info.get("details"),
        buttons=_build_restart_calibration_button(context),
        calibration_context=context,
    )
    client.mqtt.publish_command_message(twin_uuid, "error")


def _handle_calibration_complete(
    client: Cyberwave,
    twin_uuid: str,
    data: Optional[dict] = None,
) -> None:
    """Handle step-2 Complete button: finalize calibration and resolve flow."""
    global _pending_recovery_command, _calibration_proc, _calibration_active_count, _calibration_button_processing
    data = data or {}

    # Idempotency: only process if we're in step-2 (range of motion)
    with _calibration_lock:
        current_step = _calibration_flow_step
        current_alert = _calibration_alert_uuid
    if current_step != CALIBRATION_STEP_RANGE:
        logger.warning(
            "Ignoring 'complete' action - not in range step (current: %s)",
            current_step,
        )
        return

    # Reject stale clicks from old alerts
    incoming_alert_uuid = data.get("alert_uuid")
    if incoming_alert_uuid and incoming_alert_uuid != current_alert:
        logger.warning("Ignoring 'complete' action - stale alert_uuid %s", incoming_alert_uuid)
        return

    # Mark that button is being processed (prevents duplicate alerts from background thread)
    with _calibration_lock:
        global _calibration_button_processing
        _calibration_button_processing = True

    current_alert_uuid = data.get("alert_uuid") or _calibration_alert_uuid
    context = _build_calibration_context(twin_uuid, {**_calibration_context, **data})
    if not _handle_calibration_advance(client, twin_uuid, data, publish_status=False):
        client.mqtt.publish_command_message(
            twin_uuid,
            {"status": "error", "reason": "calibration_not_running"},
        )
        return

    return_code = _wait_for_calibration_exit(CALIBRATION_COMPLETE_TIMEOUT_SECONDS)
    if return_code is None:
        with _calibration_lock:
            proc = _calibration_proc
        if proc is not None:
            try:
                proc.terminate()
                return_code = proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                    return_code = proc.wait(timeout=2)
                except Exception:
                    return_code = -1
            finally:
                with _calibration_lock:
                    _calibration_proc = None
                    _calibration_active_count = 0
                _calibration_finished_event.set()
        retry_alert_uuid = _create_guided_calibration_alert(
            client,
            twin_uuid,
            context,
            stage=CALIBRATION_STEP_RANGE,
            error_message="Calibration timed out while finalizing.",
            resolve_previous_alert_uuid=current_alert_uuid,
        )
        _set_calibration_flow_state(CALIBRATION_STEP_RANGE, context, retry_alert_uuid)
        # Keep _pending_recovery_command so retry calibration can still run teleoperate/remoteoperate on success
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    if return_code == 0:
        # Check for warnings (5-20% range) before resolving the alert
        time.sleep(0.5)  # Allow backend to persist cw_calibrate's alert update
        cal_meta = _get_alert_calibration_metadata(client, twin_uuid, current_alert_uuid)
        if cal_meta:
            warnings_list = list(cal_meta.get("warnings") or [])
            severity = cal_meta.get("severity")
            if warnings_list and severity == "warning":
                device_type = context.get("device_type", "follower")
                _create_calibration_warning_alert(client, twin_uuid, warnings_list, device_type)

        _resolve_alert_by_uuid(client, twin_uuid, current_alert_uuid)
        _clear_calibration_flow_state(clear_recovery=False)
        try:
            _run_pending_recovery_command(client, twin_uuid)
        except Exception:
            logger.exception("Failed to run pending recovery command")
            _stop_current_operation()
            _start_idle_camera_streaming(client, twin_uuid)
            client.mqtt.publish_command_message(twin_uuid, "error")
            return
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return

    # Exit codes 2, 3, 4 = calibration errors: resolve calibration alert and show separate error
    if return_code in (2, 3, 4):
        _resolve_alert_by_uuid(client, twin_uuid, current_alert_uuid)
        _clear_calibration_flow_state(clear_recovery=False)  # Keep recovery command for restart
        device_type = context.get("device_type", "follower")
        port = context.get(f"{device_type}_port", "unknown")
        error_info = _read_connection_error_details()
        default_error_types = {
            2: "insufficient_range",
            3: "device_disconnected",
            4: "calibration_failed",
        }
        default_messages = {
            2: "Calibration failed: insufficient joint movement. Move all joints through their full ranges.",
            3: f"Device not found on {port}. Check that the robot is connected and powered on.",
            4: "Calibration failed unexpectedly.",
        }
        _create_error_alert(
            client,
            twin_uuid,
            error_type=error_info.get("error_type", default_error_types.get(return_code, "calibration_failed")),
            error_message=error_info.get("description", default_messages.get(return_code, "Calibration failed.")),
            details=error_info.get("details"),
            buttons=_build_restart_calibration_button(context),
            calibration_context=context,
        )
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    # Other failures (exit code 1 or unknown)
    _resolve_alert_by_uuid(client, twin_uuid, current_alert_uuid)
    _clear_calibration_flow_state(clear_recovery=False)  # Keep recovery command for restart
    error_info = _read_connection_error_details()
    _create_error_alert(
        client,
        twin_uuid,
        error_type=error_info.get("error_type", "calibration_failed"),
        error_message=error_info.get("description", f"Calibration failed (exit code {return_code})."),
        details=error_info.get("details"),
        buttons=_build_restart_calibration_button(context),
        calibration_context=context,
    )
    client.mqtt.publish_command_message(twin_uuid, "error")


def _handle_calibration_button(client: Cyberwave, twin_uuid: str, data: dict) -> bool:
    """Handle calibration actions coming from alert metadata buttons."""
    if not isinstance(data, dict):
        return False
    flow = data.get("flow")
    action = data.get("action")
    step = data.get("step")
    if flow != CALIBRATION_BUTTON_FLOW and action is None and step is None:
        return False

    action_value = str(action or "").strip().lower()
    step_value = str(step or "").strip().lower()
    if not action_value:
        if step_value in {"start_calibration", "restart_calibration", "restart"}:
            action_value = "start"
        elif step_value == "cancel":
            action_value = "cancel"
        elif step_value == "advance":
            action_value = (
                "complete" if _calibration_flow_step == CALIBRATION_STEP_RANGE else "next"
            )

    if action_value in {"start", "restart"}:
        _handle_calibration_start(client, twin_uuid, data, force_restart=action_value == "restart")
        return True
    if action_value == "next":
        _handle_calibration_next(client, twin_uuid, data)
        return True
    if action_value == "complete":
        _handle_calibration_complete(client, twin_uuid, data)
        return True
    if action_value == "cancel":
        _handle_calibration_cancel(client, twin_uuid, data)
        return True
    return False


def _run_calibration_with_advance(
    client: Cyberwave,
    twin_uuid: str,
    device_type: str,
    port: str,
    device_id: str,
    alert_uuid: Optional[str] = None,
) -> None:
    """Run calibration subprocess with stdin=PIPE for guided button flow."""
    global _calibration_proc, _calibration_client, _calibration_twin_uuid, _calibration_alert_uuid
    global _calibration_last_exit_code, _calibration_active_count

    with _calibration_lock:
        if _calibration_active_count == 0:
            return  # Cancelled before starting
    _calibration_client = client
    _calibration_twin_uuid = twin_uuid
    _calibration_alert_uuid = alert_uuid
    _calibration_last_exit_code = None
    _calibration_finished_event.clear()

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

    result = -1  # Default for exception/early-exit paths
    proc = None
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        with _calibration_lock:
            if _calibration_active_count == 0:
                # Cancelled before starting
                proc.terminate()
                try:
                    result = proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    result = proc.wait(timeout=2)
                return
            _calibration_proc = proc
        result = proc.wait()

        # Handle calibration errors (exit codes 2, 3, 4)
        # Only create alert here if this is an early failure (no button handler is processing).
        # Otherwise, the button handler will create the alert.
        if result in (2, 3, 4) and not _calibration_button_processing:
            error_names = {2: "insufficient range", 3: "device connection error", 4: "calibration error"}
            logger.warning("Calibration subprocess exited early with %s (code %d)", error_names.get(result, "error"), result)
            _resolve_alert_by_uuid(client, twin_uuid, alert_uuid)
            _clear_calibration_flow_state(clear_recovery=False)  # Keep recovery command for restart
            # Read error details from temp file written by cw_calibrate
            error_info = _read_connection_error_details()
            default_error_types = {
                2: "insufficient_range",
                3: "device_disconnected",
                4: "calibration_failed",
            }
            default_messages = {
                2: "Calibration failed: insufficient joint movement. Move all joints through their full ranges.",
                3: f"Device not found on {port}. Check that the robot is connected and powered on.",
                4: "Calibration failed unexpectedly.",
            }
            # Build context for restart button
            context: Dict[str, Any] = {
                "device_type": device_type,
                f"{device_type}_port": port,
                f"{device_type}_id": device_id,
            }
            if _pending_recovery_command:
                context["recovery_command"] = _pending_recovery_command
            _create_error_alert(
                client,
                twin_uuid,
                error_type=error_info.get("error_type", default_error_types.get(result, "calibration_failed")),
                error_message=error_info.get("description", default_messages.get(result, "Calibration failed.")),
                details=error_info.get("details"),
                buttons=_build_restart_calibration_button(context),
                calibration_context=context,
            )
    except Exception as e:
        logger.exception("Calibration subprocess failed")
        result = -1
        # Build context for restart button
        context: Dict[str, Any] = {
            "device_type": device_type,
            f"{device_type}_port": port,
            f"{device_type}_id": device_id,
        }
        if _pending_recovery_command:
            context["recovery_command"] = _pending_recovery_command
        _create_error_alert(
            client,
            twin_uuid,
            error_type="calibration_subprocess_failed",
            error_message="Calibration process crashed unexpectedly.",
            details=str(e),
            buttons=_build_restart_calibration_button(context),
            calibration_context=context,
        )
        # Ensure subprocess is terminated if it was created
        if proc is not None:
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                    proc.wait(timeout=2)
                except Exception:
                    pass
    finally:
        # Close stdin to avoid resource leaks
        if proc is not None and proc.stdin is not None:
            try:
                proc.stdin.close()
            except Exception:
                pass
        with _calibration_lock:
            _calibration_proc = None
            _calibration_active_count = 0
            _calibration_last_exit_code = result
        _calibration_finished_event.set()


def _handle_calibration_start(
    client: Cyberwave,
    twin_uuid: str,
    data: dict,
    force_restart: bool = False,
    publish_status: bool = True,
) -> None:
    """Start calibration subprocess in background with stdin=PIPE for advance commands."""
    global _calibration_proc, _pending_recovery_command, _calibration_active_count

    # Idempotency: reject start if calibration is already in an active step (zero or range)
    # unless force_restart is True (which explicitly allows restarting)
    with _calibration_lock:
        current_step = _calibration_flow_step
    if not force_restart and current_step in (CALIBRATION_STEP_ZERO, CALIBRATION_STEP_RANGE):
        logger.warning(
            "Ignoring 'start' action - calibration already in progress (step: %s)",
            current_step,
        )
        return

    if _is_control_operation_running() and not force_restart:
        logger.warning("Cannot start calibration while teleoperate/remoteoperate is running")
        _publish_operation_running_error(client, twin_uuid)
        return

    # Reject concurrent calibration (lock + counter ensure only one runs)
    with _calibration_lock:
        if _calibration_active_count > 0:
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

    # Only one script at a time: stop teleoperate/remoteoperate and calibration before starting
    # Guard above ensures we never stop an active teleop/remoteop here.
    _stop_current_operation()

    with _calibration_lock:
        _calibration_active_count = 1

    if saved_recovery_command is not None:
        _pending_recovery_command = saved_recovery_command
        logger.info("Restored recovery command for after calibration: %s", saved_recovery_command)

    cfg = _get_hardware_config(twin_uuid)
    context = _build_calibration_context(twin_uuid, data, cfg)
    if context.get("recovery_command") in RECOVERY_COMMANDS:
        _pending_recovery_command = context["recovery_command"]

    device_type = context.get("device_type", "follower")
    # Prefer port from payload when present (e.g. old frontend); else use config.
    port = (
        (data.get("leader_port") if device_type == "leader" else data.get("follower_port"))
        or data.get("port")
        or (context.get("follower_port") if device_type == "follower" else context.get("leader_port"))
        or (
            os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
            if device_type == "follower"
            else os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
        )
    )
    if device_type == "leader":
        device_id = data.get("leader_id") or data.get("id") or context.get("leader_id") or "leader1"
    else:
        device_id = (
            data.get("follower_id") or data.get("id") or context.get("follower_id") or "follower1"
        )

    if not port:
        logger.error("No port for calibration (type=%s)", device_type)
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    previous_alert_uuid = data.get("alert_uuid")
    alert_uuid = _create_guided_calibration_alert(
        client,
        twin_uuid,
        context,
        stage=CALIBRATION_STEP_ZERO,
        resolve_previous_alert_uuid=previous_alert_uuid,
    )
    _set_calibration_flow_state(CALIBRATION_STEP_ZERO, context, alert_uuid)

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
    """Restart calibration. If control operation is running, stop it gracefully with a warning."""
    if _is_control_operation_running():
        try:
            robot = client.twin(twin_id=twin_uuid)
            robot.alerts.create(
                name="Operation stopped for calibration",
                description=(
                    "Stopping the current operation to run calibration. "
                    "Please assign a controller to the twin afterward."
                ),
                alert_type="operation_interrupted",
                severity="warning",
                source_type="edge",
            )
            logger.info("Created operation_interrupted alert before stopping for calibration")
        except Exception:
            logger.debug("Could not create operation interrupted alert", exc_info=True)
    _handle_calibration_start(client, twin_uuid, data, force_restart=True)


def _run_script_command(
    client: Cyberwave,
    twin_uuid: str,
    script_name: str,
    data: dict,
    extra_cli: Optional[List[str]] = None,
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
    if script_name == "calibrate":
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
    elif script_name == "read_device":
        port = (
            data.get("port")
            or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
            or os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
        )
        if port:
            cmd.extend(["--port", str(port)])
    elif script_name == "setup":
        if data.get("setup_path"):
            cmd.extend(["--setup-path", str(data["setup_path"])])
    elif script_name == "write_position":
        if data.get("port"):
            cmd.extend(["--port", str(data["port"])])
        if data.get("position"):
            cmd.extend(["--position", str(data["position"])])

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
        _stop_current_operation(client, twin_uuid)
        _start_idle_camera_streaming(client, twin_uuid)
        client.mqtt.publish_command_message(twin_uuid, "ok")
        return

    controller_type = (controller.get("controller_type") or "").strip().lower()
    logger.info("Controller changed to type=%s, starting corresponding operation", controller_type)

    _stop_idle_camera_streaming()
    _stop_current_operation(client, twin_uuid)
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

    if command == "button":
        button_data = payload.get("data")
        if isinstance(button_data, dict) and _handle_calibration_button(client, twin_uuid, button_data):
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
        if step in {
            "start_calibration",
            "restart_calibration",
            "restart",
            "advance",
            "next",
            "complete",
            "cancel",
        }:
            # restart_calibration is allowed when control op running (stops it gracefully)
            if _is_control_operation_running() and step not in {"restart_calibration", "restart"}:
                logger.warning(
                    "Rejecting calibrate step=%s while teleoperate/remoteoperate is running",
                    step,
                )
                _publish_operation_running_error(client, twin_uuid)
                return
        if step in {"next"}:
            _handle_calibration_next(client, twin_uuid, script_args)
            return
        if step in {"complete"}:
            _handle_calibration_complete(client, twin_uuid, script_args)
            return
        if step == "advance":
            if _calibration_flow_step == CALIBRATION_STEP_RANGE:
                _handle_calibration_complete(client, twin_uuid, script_args)
            else:
                _handle_calibration_next(client, twin_uuid, script_args)
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

    Ensures single operation: stops any running teleop/remoteop/calibration first.
    """
    global _current_thread, _current_follower, _operation_stop_event
    global _current_client, _current_twin_uuid

    logger.info("Starting remoteoperate")
    _stop_current_operation()
    _stop_idle_camera_streaming()

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

    # Set global state only after validation passes (avoid stale state on early return)
    _current_client = client
    _current_twin_uuid = twin_uuid

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
            camera_name = _camera_name_override(cam_type)
            if camera_name is not None:
                entry["camera_name"] = camera_name
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

        try:
            follower.connect()
        except DeviceConnectionError as e:
            logger.error("Failed to connect follower: %s", e)
            _create_error_alert(
                client,
                twin_uuid,
                error_type=e.error_type,
                error_message=e.description,
                details=str(e),
            )
            return
        except Exception as e:
            logger.exception("Failed to connect follower")
            _create_error_alert(
                client,
                twin_uuid,
                error_type="follower_connection_failed",
                error_message=f"Failed to connect follower on {follower_port}. Check that the device is connected.",
                details=str(e),
            )
            return

        _current_follower = follower

        try:
            remoteoperate(
                client=client,
                follower=follower,
                robot=robot,
                cameras=cameras_list if cameras_list else None,
                stop_event=operation_stop_event,
            )
        except Exception as e:
            logger.exception("Remoteoperate loop failed")
            _create_error_alert(
                client,
                twin_uuid,
                error_type="remoteoperate_failed",
                error_message="Remote operation failed unexpectedly.",
                details=str(e),
            )
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

    Ensures single operation: stops any running teleop/remoteop/calibration first.
    """
    global _current_thread, _current_follower, _operation_stop_event
    global _current_client, _current_twin_uuid

    logger.info("Starting teleoperate")
    _stop_current_operation()
    _stop_idle_camera_streaming()

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

    # Set global state only after validation passes (avoid stale state on early return)
    _current_client = client
    _current_twin_uuid = twin_uuid

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
            camera_name = _camera_name_override(cam_type)
            if camera_name is not None:
                entry["camera_name"] = camera_name
            if cam_type == "realsense":
                entry["enable_depth"] = cam.get("enable_depth", False)
                entry["depth_fps"] = cam.get("depth_fps", 30)
                entry["depth_resolution"] = cam.get("depth_resolution")
                entry["depth_publish_interval"] = cam.get("depth_publish_interval", 30)
            cameras_list.append(entry)

        # Leader
        leader_config = LeaderConfig(port=leader_port, id=leader_id)
        leader = SO101Leader(config=leader_config)
        follower: Optional[SO101Follower] = None

        try:
            leader.connect()
        except DeviceConnectionError as e:
            logger.error("Failed to connect leader: %s", e)
            _create_error_alert(
                client,
                twin_uuid,
                error_type=e.error_type,
                error_message=e.description,
                details=str(e),
            )
            return
        except Exception as e:
            logger.exception("Failed to connect leader")
            _create_error_alert(
                client,
                twin_uuid,
                error_type="leader_connection_failed",
                error_message=f"Failed to connect leader on {leader_port}. Check that the device is connected.",
                details=str(e),
            )
            return

        # Follower
        cam_ids = [c["camera_id"] for c in cameras_list] if cameras_list else None
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=cam_ids,
        )
        follower = SO101Follower(config=follower_config)

        try:
            follower.connect()
        except DeviceConnectionError as e:
            logger.error("Failed to connect follower: %s", e)
            _create_error_alert(
                client,
                twin_uuid,
                error_type=e.error_type,
                error_message=e.description,
                details=str(e),
            )
            try:
                leader.disconnect()
            except Exception:
                pass
            return
        except Exception as e:
            logger.exception("Failed to connect follower")
            _create_error_alert(
                client,
                twin_uuid,
                error_type="follower_connection_failed",
                error_message=f"Failed to connect follower on {follower_port}. Check that the device is connected.",
                details=str(e),
            )
            try:
                leader.disconnect()
            except Exception:
                pass
            return

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
        except Exception as e:
            logger.exception("Teleoperate loop failed")
            _create_error_alert(
                client,
                twin_uuid,
                error_type="teleoperate_failed",
                error_message="Local teleoperation failed unexpectedly.",
                details=str(e),
            )
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

    # Load workspace_id from environment.json if not set via env var
    if not os.getenv("CYBERWAVE_WORKSPACE_ID"):
        workspace_id = _load_workspace_id_from_environment_json()
        if workspace_id:
            os.environ["CYBERWAVE_WORKSPACE_ID"] = workspace_id

    # Initialize the Cyberwave SDK client (reads remaining config from env vars)
    client = Cyberwave(
        api_key=token,
        mqtt_port=get_default_mqtt_port(),
        source_type="edge",
    )

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
        except Exception as e:
            logger.exception("Error handling command %s", command)
            try:
                _create_error_alert(
                    client,
                    twin_uuid,
                    error_type=f"command_{command}",
                    error_message=f"Failed to handle command: {command}",
                    details=str(e),
                )
                client.mqtt.publish_command_message(twin_uuid, "error")
            except Exception:
                logger.exception("Failed to publish error status")

    # Subscribe to the command topic for this twin
    client.mqtt.subscribe_command_message(twin_uuid, on_command)
    logger.info("Subscribed to command topic for twin %s", twin_uuid)

    # Check calibration at startup; if missing, trigger same flow as teleop/remoteop/calibrate
    try:
        _check_startup_calibration(client, twin_uuid)
    except Exception:
        logger.exception("Startup calibration check failed; continuing")

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
