"""Alert helpers for SO101 edge scripts (teleoperate, remoteoperate)."""

import logging
import sys
import threading
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Throttle: minimum seconds between creating the same alert type
_TEMP_ALERT_THROTTLE = 2.0  # 5 minutes per joint
_MQTT_ALERT_THROTTLE = 300.0  # 5 minutes
_ERROR_ALERT_THROTTLE = 300.0  # 5 minutes
_SESSION_ALERT_THROTTLE = 3600.0  # 1 hour (once per session)

_last_alert_times: Dict[str, float] = {}
_session_alert_fired = False
_alert_lock = threading.Lock()


def _should_create_alert(alert_key: str, throttle_seconds: float) -> bool:
    """Return True if we should create this alert (not throttled)."""
    with _alert_lock:
        now = time.time()
        last = _last_alert_times.get(alert_key, 0.0)
        if now - last < throttle_seconds:
            return False
        _last_alert_times[alert_key] = now
        return True


def _log_alert_failure(alert_type: str, exc: Exception) -> None:
    """Log alert creation failure. Uses stderr so it shows even when logging is disabled."""
    msg = f"Alert creation failed ({alert_type}): {exc}"
    logger.warning(msg)
    try:
        print(msg, file=sys.stderr, flush=True)
    except OSError:
        pass


def create_session_started_alert(
    robot: Any,
    mode: str,
) -> bool:
    """
    Create a session-started alert once when the edge script connects.

    Use this to verify the alert pipeline works. Fires once per process.

    Args:
        robot: Twin instance with alerts
        mode: "teleoperate" or "remoteoperate"

    Returns:
        True if alert was created
    """
    global _session_alert_fired
    with _alert_lock:
        if _session_alert_fired:
            return False
        _session_alert_fired = True

    try:
        robot.alerts.create(
            name=f"SO101 {mode} session started",
            description=f"Edge device connected and running {mode}",
            alert_type="edge_session_started",
            severity="info",
            source_type="edge",
        )
        logger.info(f"Created session started alert ({mode})")
        return True
    except Exception as e:
        _log_alert_failure("session_started", e)
        return False


def create_temperature_alert(
    robot: Any,
    joint_name: str,
    device: str,
    temperature: float,
    *,
    warning_threshold: float = 55.0,
    critical_threshold: float = 65.0,
) -> bool:
    """
    Create a motor overheating alert if temperature exceeds thresholds.

    Args:
        robot: Twin instance with alerts (robot.alerts)
        joint_name: Joint/motor name (e.g. shoulder_pan)
        device: "leader" or "follower"
        temperature: Current temperature in °C
        warning_threshold: Create warning alert above this (default 55)
        critical_threshold: Create critical alert above this (default 65)

    Returns:
        True if alert was created, False if throttled or below threshold
    """
    if temperature < warning_threshold:
        return False

    severity = "critical" if temperature >= critical_threshold else "warning"
    alert_key = f"temp_{device}_{joint_name}"

    if not _should_create_alert(alert_key, _TEMP_ALERT_THROTTLE):
        return False

    try:
        robot.alerts.create(
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


def create_mqtt_disconnected_alert(robot: Any) -> bool:
    """
    Create an alert when MQTT is disconnected.

    Args:
        robot: Twin instance with alerts

    Returns:
        True if alert was created, False if throttled
    """
    alert_key = "mqtt_disconnected"
    if not _should_create_alert(alert_key, _MQTT_ALERT_THROTTLE):
        return False

    try:
        robot.alerts.create(
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


def create_calibration_needed_alert(
    robot: Any,
    device: str,
    *,
    description: str = "",
) -> bool:
    """
    Create a calibration needed alert for leader or follower.

    Args:
        robot: Twin instance with alerts
        device: "leader" or "follower"
        description: Optional details

    Returns:
        True if alert was created
    """
    try:
        robot.alerts.create(
            name=f"{device.capitalize()} calibration needed",
            description=description or f"The {device} device requires calibration.",
            alert_type="calibration_needed",
            severity="warning",
            source_type="edge",
        )
        logger.info(f"Created calibration needed alert for {device}")
        return True
    except Exception as e:
        _log_alert_failure("calibration_needed", e)
        return False


def create_high_error_rate_alert(
    robot: Any,
    error_count: int,
    *,
    threshold: int = 100,
) -> bool:
    """
    Create an alert when error count exceeds threshold.

    Args:
        robot: Twin instance with alerts
        error_count: Current error count from status tracker
        threshold: Create alert when errors exceed this (default 100)

    Returns:
        True if alert was created, False if throttled or below threshold
    """
    if error_count < threshold:
        return False

    alert_key = "high_error_rate"
    if not _should_create_alert(alert_key, _ERROR_ALERT_THROTTLE):
        return False

    try:
        robot.alerts.create(
            name="High error rate",
            description=f"Edge script reported {error_count} errors",
            alert_type="high_error_rate",
            severity="warning",
            source_type="edge",
        )
        logger.info(f"Created high error rate alert: {error_count} errors")
        return True
    except Exception as e:
        _log_alert_failure("high_error_rate", e)
        return False
