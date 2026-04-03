"""Alert helpers for SO101 edge scripts (teleoperate, remoteoperate)."""

import logging
import math
import numbers
import sys
import threading
import time
from typing import Dict, List, Optional

from cyberwave import Twin

logger = logging.getLogger(__name__)

CALIBRATION_NEEDED_LEADER_ZERO_MEDIA_URL = (
    "https://static.cyberwave.com/media/urdf_projects/so101_new_caliburdf/"
    "leader_calibration_zero.mp4"
)
CALIBRATION_NEEDED_FOLLOWER_ZERO_MEDIA_URL = (
    "https://static.cyberwave.com/media/urdf_projects/so101_new_caliburdf/"
    "so101_calibration_zero_follower.mp4"
)

CALIBRATION_NEEDED_LEADER_STEP_2 = (
    "https://static.cyberwave.com/media/urdf_projects/so101_new_caliburdf/"
    "leader_calibration_step2.mp4"
)

CALIBRATION_NEEDED_FOLLOWER_STEP_2 = (
    "https://static.cyberwave.com/media/urdf_projects/so101_new_caliburdf/"
    "follower_calibration_step2.mp4"
)

_CALIBRATION_MEDIA_MAP: Dict[str, Dict[str, str]] = {
    "leader": {
        "zero": CALIBRATION_NEEDED_LEADER_ZERO_MEDIA_URL,
        "range": CALIBRATION_NEEDED_LEADER_STEP_2,
    },
    "follower": {
        "zero": CALIBRATION_NEEDED_FOLLOWER_ZERO_MEDIA_URL,
        "range": CALIBRATION_NEEDED_FOLLOWER_STEP_2,
    },
}


def get_calibration_media_url(device: str, step: str = "zero") -> str:
    """Return the calibration video URL for a given device type and step.

    Args:
        device: "leader" or "follower".
        step: "zero" (step 1 – zero position) or "range" (step 2 – range of motion).
              Defaults to "zero".
    """
    device_lower = device.lower()
    if device_lower not in _CALIBRATION_MEDIA_MAP:
        logger.warning(
            "Unknown calibration device type %r, defaulting to 'follower'", device
        )
    device_map = _CALIBRATION_MEDIA_MAP.get(device_lower, _CALIBRATION_MEDIA_MAP["follower"])
    if step not in device_map:
        logger.warning(
            "Unknown calibration step %r for device %r, defaulting to 'zero'", step, device
        )
    return device_map.get(step, device_map["zero"])


# Throttle: minimum seconds between creating the same alert type
_TEMP_ALERT_THROTTLE = 10.0  # 10 seconds per joint
_MQTT_ALERT_THROTTLE = 60.0  # 1 minute
_ERROR_ALERT_THROTTLE = 60.0  # 1 minute (calibration, etc.)
_MOTOR_ALERT_THROTTLE = 10.0  # 10 seconds (fast detection for motor issues)
_MQTT_ERROR_ALERT_THROTTLE = 60.0  # 1 minute for MQTT error rate
_MAX_VALID_MOTOR_TEMPERATURE = 120.0  # Ignore invalid "maxed out" servo reads
_TEMP_CONSECUTIVE_SAMPLES_REQUIRED = 5  # Mitigate transient stale/spike reads

_last_alert_times: Dict[str, float] = {}
_alert_active_counts: Dict[str, int] = {}
_alert_lock = threading.Lock()
_PRUNE_AGE_SECONDS = 3600  # Prune throttle entries older than 1 hour
_PRUNE_WHEN_KEYS_EXCEED = 100  # Prune when dict exceeds this many keys


def _prune_stale_throttle_entries() -> None:
    """Remove stale entries from throttle dicts to avoid unbounded growth."""
    now = time.time()
    stale = [k for k, t in _last_alert_times.items() if now - t > _PRUNE_AGE_SECONDS]
    for k in stale:
        _last_alert_times.pop(k, None)
        _alert_active_counts.pop(k, None)
    # Also prune orphaned keys in _alert_active_counts not in _last_alert_times
    orphaned = [k for k in _alert_active_counts if k not in _last_alert_times]
    for k in orphaned:
        _alert_active_counts.pop(k, None)


def _should_create_alert(
    alert_key: str,
    throttle_seconds: float,
    *,
    consecutive_hits: int = 1,
    is_active: bool = True,
) -> bool:
    """Return True when an alert passes activity and throttle gates.

    Args:
        alert_key: Identifier for per-alert throttling and activity tracking.
        throttle_seconds: Minimum seconds between emitted alerts for this key.
        consecutive_hits: Number of consecutive active samples required before emit.
        is_active: Whether current sample is active (e.g. temp above threshold).
            When False, activity streak is reset and no alert is emitted.
    """
    with _alert_lock:
        # Prune before accessing to avoid issues with dictionary size
        if len(_last_alert_times) > _PRUNE_WHEN_KEYS_EXCEED:
            _prune_stale_throttle_entries()

        required_hits = max(1, int(consecutive_hits))
        if required_hits > 1 or not is_active:
            if not is_active:
                _alert_active_counts.pop(alert_key, None)
                return False
            _alert_active_counts[alert_key] = _alert_active_counts.get(alert_key, 0) + 1
            if _alert_active_counts[alert_key] < required_hits:
                return False

        now = time.time()
        last = _last_alert_times.get(alert_key, 0.0)
        if now - last < throttle_seconds:
            return False
        _last_alert_times[alert_key] = now
        _alert_active_counts.pop(alert_key, None)  # Reset streak after successful emit
        return True


def _log_alert_failure(alert_type: str, exc: Exception) -> None:
    """Log alert creation failure. Uses stderr so it shows even when logging is disabled."""
    msg = f"Alert creation failed ({alert_type}): {exc}"
    logger.warning(msg)
    try:
        print(msg, file=sys.stderr, flush=True)
    except OSError:
        pass


def create_temperature_alert(
    twin: Twin,
    joint_name: str,
    device: str,
    temperature: float,
    *,
    warning_threshold: float = 42.0,
    critical_threshold: float = 50.0,
) -> bool:
    """
    Create a motor overheating alert if temperature exceeds thresholds.

    Args:
        twin: Twin instance with alerts (twin.alerts)
        joint_name: Joint/motor name (e.g. shoulder_pan)
        device: "leader" or "follower"
        temperature: Current temperature in °C
        warning_threshold: Create warning alert above this (default 42)
        critical_threshold: Create critical alert above this (default 50)

    Returns:
        True if alert was created, False if throttled or below threshold
    """
    # Validate temperature is a number (supports numpy types via numbers.Number)
    if not isinstance(temperature, numbers.Number):
        logger.debug(
            "Ignoring non-numeric temperature reading for %s (%s): %r",
            joint_name,
            device,
            temperature,
        )
        return False

    alert_key = f"temp_{device}_{joint_name}"
    if not math.isfinite(temperature) or temperature >= _MAX_VALID_MOTOR_TEMPERATURE:
        _should_create_alert(
            alert_key,
            _TEMP_ALERT_THROTTLE,
            consecutive_hits=_TEMP_CONSECUTIVE_SAMPLES_REQUIRED,
            is_active=False,
        )
        logger.debug(
            "Ignoring invalid motor temperature reading for %s (%s): %.2f°C",
            joint_name,
            device,
            temperature,
        )
        return False

    if temperature < warning_threshold:
        _should_create_alert(
            alert_key,
            _TEMP_ALERT_THROTTLE,
            consecutive_hits=_TEMP_CONSECUTIVE_SAMPLES_REQUIRED,
            is_active=False,
        )
        return False

    severity = "critical" if temperature >= critical_threshold else "warning"
    if not _should_create_alert(
        alert_key,
        _TEMP_ALERT_THROTTLE,
        consecutive_hits=_TEMP_CONSECUTIVE_SAMPLES_REQUIRED,
    ):
        return False

    try:
        twin.alerts.create(
            name=f"Motor overheating: {joint_name} ({device})",
            description=f"{joint_name} on {device} is at {temperature:.0f}°C",
            alert_type="motor_overheating",
            severity=severity,
            source_type="edge",
        )
        logger.info(f"Created {severity} alert: {joint_name} ({device}) at {temperature:.0f}°C")
        return True
    except Exception as e:
        _log_alert_failure("temperature", e)
        return False


def create_mqtt_disconnected_alert(twin: Twin) -> bool:
    """
    Create an alert when MQTT is disconnected.

    Args:
        twin: Twin instance with alerts

    Returns:
        True if alert was created, False if throttled
    """
    alert_key = "mqtt_disconnected"
    if not _should_create_alert(alert_key, _MQTT_ALERT_THROTTLE):
        return False

    try:
        twin.alerts.create(
            name="MQTT disconnected",
            description="Lost connection to Cyberwave MQTT broker",
            alert_type="mqtt_disconnected",
            severity="error",
            source_type="edge",
        )
        logger.info("Created MQTT disconnected alert")
        return True
    except Exception as e:
        _log_alert_failure("mqtt_disconnected", e)
        return False


def create_calibration_upload_failed_alert(
    twin: Twin,
    device: str,
    error: Exception,
) -> bool:
    """
    Create an alert when calibration upload to the twin fails.

    Args:
        twin: Twin instance with alerts
        device: "leader" or "follower"
        error: The exception that caused the failure

    Returns:
        True if alert was created
    """
    alert_key = f"calibration_upload_failed_{device}"
    if not _should_create_alert(alert_key, _ERROR_ALERT_THROTTLE):
        return False

    try:
        twin.alerts.create(
            name=f"{device.capitalize()} calibration upload failed",
            description=str(error),
            alert_type="calibration_upload_failed",
            severity="error",
            source_type="edge",
        )
        logger.info(f"Created calibration upload failed alert for {device}: {error}")
        return True
    except Exception as e:
        _log_alert_failure("calibration_upload_failed", e)
        return False


def create_calibration_needed_alert(
    twin: Twin,
    device: str,
    *,
    step: str = "zero",
    description: str = "",
) -> bool:
    """
    Create a calibration needed alert for leader or follower.

    Args:
        twin: Twin instance with alerts
        device: "leader" or "follower"
        step: "zero" (step 1) or "range" (step 2). Defaults to "zero".
        description: Optional details

    Returns:
        True if alert was created
    """
    try:
        twin.alerts.create(
            name=f"{device.capitalize()} calibration needed",
            description=description or f"The {device} device requires calibration.",
            media=get_calibration_media_url(device, step),
            alert_type="calibration_needed",
            severity="warning",
            source_type="edge",
        )
        logger.info(f"Created calibration needed alert for {device}")
        return True
    except Exception as e:
        _log_alert_failure("calibration_needed", e)
        return False


def _normalize_video_device_path(value: object) -> str:
    """Normalize camera_id/video_device values to a /dev/video* path-like string."""
    if isinstance(value, str):
        v = value.strip()
        if not v:
            return "/dev/video?"
        if v.startswith("/dev/"):
            return v
        if v.startswith("video") and v[5:].isdigit():
            return f"/dev/{v}"
        if v.isdigit():
            return f"/dev/video{v}"
        return v
    if isinstance(value, int):
        return f"/dev/video{value}"
    return "/dev/video?"


def _extract_first_rgb_sensor_id_from_twin(twin: Twin) -> Optional[str]:
    """Best-effort extraction of the first RGB sensor id from twin metadata."""
    metadata = getattr(twin, "metadata", None)
    if not isinstance(metadata, dict):
        return None

    schema = metadata.get("universal_schema")
    if isinstance(schema, dict):
        sensors = schema.get("sensors", [])
        if isinstance(sensors, list):
            for i, sensor in enumerate(sensors):
                if isinstance(sensor, dict) and (sensor.get("type") or "").lower() in {
                    "rgb",
                    "camera",
                    "rgb_camera",
                }:
                    sid = (
                        sensor.get("id")
                        or (sensor.get("parameters") or {}).get("id")
                        or sensor.get("name")
                        or f"sensor_{i}"
                    )
                    return str(sid)

    caps = metadata.get("capabilities", {})
    if isinstance(caps, dict):
        sensors = caps.get("sensors", [])
        if isinstance(sensors, list):
            for i, sensor in enumerate(sensors):
                if isinstance(sensor, dict) and (sensor.get("type") or "").lower() in {
                    "rgb",
                    "camera",
                    "rgb_camera",
                }:
                    sid = sensor.get("id") or sensor.get("name") or f"sensor_{i}"
                    return str(sid)
    return None


def create_camera_default_device_alert(
    twin: Twin,
    default_cameras: List[dict],
) -> bool:
    """
    Create an info alert when cameras use default device assignment.

    Args:
        twin: Twin instance with alerts (typically the robot twin)
        default_cameras: List of camera dicts with used_default=True, each containing
            setup_name, twin_uuid, video_device (or camera_id)

    Returns:
        True if alert was created, False if empty list
    """
    if not default_cameras:
        return False

    primary_uuid = str(getattr(twin, "uuid", "") or "")
    primary_sensor_id = _extract_first_rgb_sensor_id_from_twin(twin)

    parts = []
    metadata_cameras: List[dict] = []
    for c in default_cameras:
        setup_name = str(c.get("setup_name") or "camera")
        twin_uuid = str(c.get("twin_uuid") or primary_uuid)
        video_device = _normalize_video_device_path(c.get("video_device") or c.get("camera_id"))
        parts.append(f"{setup_name}={video_device}")

        sensor_id = c.get("sensor_id")
        is_primary_robot_sensor = (
            twin_uuid == primary_uuid
            and str(c.get("attach_to_link") or "").lower() == "robot_sensor"
        )
        if not sensor_id and is_primary_robot_sensor:
            sensor_id = primary_sensor_id
        if not sensor_id and not is_primary_robot_sensor:
            sensor_id = "camera"

        metadata_entry = {
            "twin_uuid": twin_uuid,
            "setup_name": setup_name,
            "video_device": video_device,
        }
        if sensor_id:
            metadata_entry["sensor_id"] = str(sensor_id)
        metadata_cameras.append(metadata_entry)

    description = (
        "Cameras using default device assignment "
        "(no edge_configs.camera_config.sensors_devices or video_device in metadata): "
        + ", ".join(parts)
        + ". Configure edge camera mapping in Sensor Settings for stable binding."
    )

    try:
        twin.alerts.create(
            name="Cameras using default device assignment",
            description=description,
            alert_type="camera_default_device",
            severity="warning",
            source_type="edge",
            metadata={"default_cameras": metadata_cameras},
        )
        logger.info(
            "Created camera default device alert: %d camera(s) using pool assignment",
            len(default_cameras),
        )
        return True
    except Exception as e:
        _log_alert_failure("camera_default_device", e)
        return False


def create_motor_error_alert(
    twin: Twin,
    error_count: int,
    *,
    threshold: int = 10,
) -> bool:
    """
    Create an alert when motor/serial error count exceeds threshold.

    Motor failures need fast detection (low threshold, short throttle).

    Args:
        twin: Twin instance with alerts
        error_count: Current motor error count from status tracker
        threshold: Create alert when errors exceed this (default 10)

    Returns:
        True if alert was created, False if throttled or below threshold
    """
    if error_count < threshold:
        return False

    alert_key = "motor_error_rate"
    if not _should_create_alert(alert_key, _MOTOR_ALERT_THROTTLE):
        return False

    try:
        twin.alerts.create(
            name="Motor communication errors",
            description=f"Edge script reported {error_count} motor/serial errors",
            alert_type="motor_error_rate",
            severity="warning",
            source_type="edge",
        )
        logger.info(f"Created motor error alert: {error_count} motor errors")
        return True
    except Exception as e:
        _log_alert_failure("motor_error_rate", e)
        return False


def create_mqtt_error_alert(
    twin: Twin,
    error_count: int,
    *,
    threshold: int = 100,
) -> bool:
    """
    Create an alert when MQTT/queue error count exceeds threshold.

    MQTT errors can tolerate more (100 per 60 sec) before alerting.

    Args:
        twin: Twin instance with alerts
        error_count: Current MQTT error count from status tracker
        threshold: Create alert when errors exceed this (default 100)

    Returns:
        True if alert was created, False if throttled or below threshold
    """
    if error_count < threshold:
        return False

    alert_key = "mqtt_error_rate"
    if not _should_create_alert(alert_key, _MQTT_ERROR_ALERT_THROTTLE):
        return False

    try:
        twin.alerts.create(
            name="MQTT/queue error rate",
            description=f"Edge script reported {error_count} MQTT/queue errors",
            alert_type="mqtt_error_rate",
            severity="warning",
            source_type="edge",
        )
        logger.info(f"Created MQTT error alert: {error_count} MQTT errors")
        return True
    except Exception as e:
        _log_alert_failure("mqtt_error_rate", e)
        return False


def create_robot_setup_alert(twin: Twin) -> Optional[str]:
    """Create an info alert indicating robot setup is in progress.

    Resolve via twin.alerts.get(uuid).resolve() once teleop/remoteop is ready.

    Returns:
        Alert UUID string, or None if creation failed.
    """
    try:
        alert = twin.alerts.create(
            name="Setting up robot",
            description="Setting up robot, you will be able to control robot soon.",
            alert_type="robot_setup",
            severity="info",
            source_type="edge",
        )
        logger.info("Created robot setup alert: %s", alert.uuid)
        return str(alert.uuid)
    except Exception as e:
        _log_alert_failure("robot_setup", e)
        return None


ROBOT_SETUP_DONE_AUTO_RESOLVE_DELAY_S = 10.0


def resolve_alert_by_uuid(twin: Twin, alert_uuid: str) -> None:
    """Resolve an alert by UUID if it still exists (safe if already resolved or missing)."""
    try:
        alert = twin.alerts.get(alert_uuid)
        if alert is not None:
            alert.resolve()
    except Exception:
        logger.debug(
            "Could not resolve alert %s",
            alert_uuid,
            exc_info=True,
        )


def schedule_robot_setup_done_resolve(
    twin: Twin,
    alert_uuid: str,
    delay_seconds: float = ROBOT_SETUP_DONE_AUTO_RESOLVE_DELAY_S,
) -> threading.Timer:
    """Start a daemon timer that resolves ``robot_setup_done`` after ``delay_seconds``.

    Caller should ``cancel()`` the timer on script exit and call :func:`resolve_alert_by_uuid`
    so the alert is cleared immediately when teleop/remoteop ends (whether or not the timer
    already fired).
    """

    def _resolve() -> None:
        resolve_alert_by_uuid(twin, alert_uuid)

    timer = threading.Timer(delay_seconds, _resolve)
    timer.daemon = True
    timer.start()
    return timer


def create_robot_setup_done_alert(twin: Twin) -> Optional[str]:
    """Create an info alert indicating robot setup completed successfully.

    Edge resolves this after :data:`ROBOT_SETUP_DONE_AUTO_RESOLVE_DELAY_S` or when the
    teleop/remoteop script exits (whichever comes first).

    Returns:
        Alert UUID string, or None if creation failed.
    """
    try:
        alert = twin.alerts.create(
            name="Setup completed",
            description="Robot set up properly, start using the robot.",
            alert_type="robot_setup_done",
            severity="info",
            source_type="edge",
        )
        logger.info("Created robot setup done alert: %s", alert.uuid)
        return str(alert.uuid)
    except Exception as e:
        _log_alert_failure("robot_setup_done", e)
        return None
