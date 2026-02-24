"""Alert helpers for SO101 edge scripts (teleoperate, remoteoperate)."""

import logging
import threading
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

# Throttle: minimum seconds between creating the same alert type
_TEMP_ALERT_THROTTLE = 300.0  # 5 minutes per joint
_MQTT_ALERT_THROTTLE = 300.0  # 5 minutes
_ERROR_ALERT_THROTTLE = 300.0  # 5 minutes

_last_alert_times: Dict[str, float] = {}
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


def create_temperature_alert(
    robot: Any,
    joint_name: str,
    device: str,
    temperature: float,
    *,
    warning_threshold: float = 22.0,
    critical_threshold: float = 50.0,
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
        logger.warning(f"Created {severity} alert: {joint_name} ({device}) at {temperature:.0f}°C")
        return True
    except Exception as e:
        logger.warning(f"Failed to create temperature alert: {e}")
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
        logger.warning("Created MQTT disconnected alert")
        return True
    except Exception as e:
        logger.warning(f"Failed to create MQTT alert: {e}")
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
        logger.warning(f"Created calibration needed alert for {device}")
        return True
    except Exception as e:
        logger.warning(f"Failed to create calibration alert: {e}")
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
        logger.warning(f"Created high error rate alert: {error_count} errors")
        return True
    except Exception as e:
        logger.warning(f"Failed to create error rate alert: {e}")
        return False
