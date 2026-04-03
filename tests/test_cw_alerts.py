"""Unit tests for SO101 alert helpers."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils import cw_alerts
from utils.cw_alerts import (
    CALIBRATION_NEEDED_FOLLOWER_ZERO_MEDIA_URL,
    CALIBRATION_NEEDED_LEADER_STEP_2,
    create_calibration_needed_alert,
    create_camera_default_device_alert,
    get_calibration_media_url,
)


class _FakeAlerts:
    def __init__(self) -> None:
        self.calls = []

    def create(self, **kwargs):  # noqa: ANN003 - mirrors SDK surface
        self.calls.append(kwargs)


def _fake_twin() -> SimpleNamespace:
    return SimpleNamespace(alerts=_FakeAlerts())


def _reset_alert_state() -> None:
    cw_alerts._last_alert_times.clear()
    cw_alerts._alert_active_counts.clear()


def test_temperature_alert_requires_consecutive_hot_samples() -> None:
    _reset_alert_state()
    twin = _fake_twin()
    required = cw_alerts._TEMP_CONSECUTIVE_SAMPLES_REQUIRED

    for _ in range(required - 1):
        created = cw_alerts.create_temperature_alert(
            twin, "elbow", "follower", 70.0, warning_threshold=55.0, critical_threshold=65.0
        )
        assert created is False
        assert len(twin.alerts.calls) == 0

    created = cw_alerts.create_temperature_alert(
        twin, "elbow", "follower", 70.0, warning_threshold=55.0, critical_threshold=65.0
    )
    assert created is True
    assert len(twin.alerts.calls) == 1
    assert twin.alerts.calls[0]["severity"] == "critical"


def test_invalid_max_temperature_resets_streak() -> None:
    _reset_alert_state()
    twin = _fake_twin()
    required = cw_alerts._TEMP_CONSECUTIVE_SAMPLES_REQUIRED

    for _ in range(required - 1):
        assert (
            cw_alerts.create_temperature_alert(
                twin, "wrist", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
            )
            is False
        )
    assert (
        cw_alerts.create_temperature_alert(
            twin, "wrist", "leader", 255.0, warning_threshold=55.0, critical_threshold=65.0
        )
        is False
    )

    for _ in range(required - 1):
        assert (
            cw_alerts.create_temperature_alert(
                twin, "wrist", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
            )
            is False
        )

    assert (
        cw_alerts.create_temperature_alert(
            twin, "wrist", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
        )
        is True
    )
    assert len(twin.alerts.calls) == 1


def test_below_threshold_temperature_resets_streak() -> None:
    _reset_alert_state()
    twin = _fake_twin()
    required = cw_alerts._TEMP_CONSECUTIVE_SAMPLES_REQUIRED

    for _ in range(required - 1):
        assert (
            cw_alerts.create_temperature_alert(
                twin, "shoulder", "follower", 60.0, warning_threshold=55.0, critical_threshold=65.0
            )
            is False
        )
    assert (
        cw_alerts.create_temperature_alert(
            twin, "shoulder", "follower", 45.0, warning_threshold=55.0, critical_threshold=65.0
        )
        is False
    )
    for _ in range(required - 1):
        assert (
            cw_alerts.create_temperature_alert(
                twin, "shoulder", "follower", 60.0, warning_threshold=55.0, critical_threshold=65.0
            )
            is False
        )
    assert (
        cw_alerts.create_temperature_alert(
            twin, "shoulder", "follower", 60.0, warning_threshold=55.0, critical_threshold=65.0
        )
        is True
    )
    assert twin.alerts.calls[0]["severity"] == "warning"


def test_temperature_alert_throttle_is_preserved(monkeypatch) -> None:
    """After first alert, throttle blocks until 10s passes. Second alert allowed after throttle."""
    _reset_alert_state()
    twin = _fake_twin()
    required = cw_alerts._TEMP_CONSECUTIVE_SAMPLES_REQUIRED
    now = [1000.0]
    monkeypatch.setattr(cw_alerts.time, "time", lambda: now[0])

    for _ in range(required - 1):
        assert (
            cw_alerts.create_temperature_alert(
                twin, "base", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
            )
            is False
        )
        now[0] += 1.0

    assert (
        cw_alerts.create_temperature_alert(
            twin, "base", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
        )
        is True
    )
    now[0] += 1.0
    assert (
        cw_alerts.create_temperature_alert(
            twin, "base", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
        )
        is False
    )
    now[0] += cw_alerts._TEMP_ALERT_THROTTLE + 1.0
    created = cw_alerts.create_temperature_alert(
        twin, "base", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
    )
    if not created:
        for _ in range(required - 1):
            created = cw_alerts.create_temperature_alert(
                twin, "base", "leader", 70.0, warning_threshold=55.0, critical_threshold=65.0
            )
            now[0] += 1.0
            if created:
                break
    assert created is True
    assert len(twin.alerts.calls) == 2


def test_calibration_needed_alert_includes_media_url() -> None:
    twin = _fake_twin()

    created = create_calibration_needed_alert(twin, "follower")

    assert created is True
    assert len(twin.alerts.calls) == 1
    assert twin.alerts.calls[0]["alert_type"] == "calibration_needed"
    assert twin.alerts.calls[0]["media"] == CALIBRATION_NEEDED_FOLLOWER_ZERO_MEDIA_URL


def test_calibration_media_url_varies_by_device_and_step() -> None:
    twin_zero = _fake_twin()
    twin_range = _fake_twin()

    create_calibration_needed_alert(twin_zero, "follower", step="zero")
    create_calibration_needed_alert(twin_range, "leader", step="range")

    assert twin_zero.alerts.calls[0]["media"] == CALIBRATION_NEEDED_FOLLOWER_ZERO_MEDIA_URL
    assert twin_range.alerts.calls[0]["media"] == CALIBRATION_NEEDED_LEADER_STEP_2


def test_get_calibration_media_url_defaults_to_zero() -> None:
    assert get_calibration_media_url("follower") == CALIBRATION_NEEDED_FOLLOWER_ZERO_MEDIA_URL
    assert get_calibration_media_url("leader") == get_calibration_media_url("leader", "zero")


class TestCreateCameraDefaultDeviceAlert:
    """Tests for camera_default_device alert metadata generation."""

    def test_creates_alert_with_default_cameras_metadata(self):
        """Alert payload includes normalized devices and sensor ids."""
        twin = MagicMock()
        twin.uuid = "robot-primary-uuid"
        twin.metadata = {
            "universal_schema": {
                "sensors": [
                    {"id": "wrist_camera", "type": "rgb"},
                ]
            }
        }

        default_cameras = [
            {
                "twin_uuid": "robot-primary-uuid",
                "setup_name": "wrist",
                "camera_id": 6,
                "attach_to_link": "robot_sensor",
            },
            {
                "twin_uuid": "camera-twin-2",
                "setup_name": "primary",
                "video_device": "/dev/video8",
            },
            {
                "twin_uuid": "camera-twin-3",
                "setup_name": "secondary",
                "camera_id": "2",
                "sensor_id": "overhead",
            },
        ]

        created = create_camera_default_device_alert(twin, default_cameras)

        assert created is True
        twin.alerts.create.assert_called_once()
        kwargs = twin.alerts.create.call_args.kwargs
        assert kwargs["alert_type"] == "camera_default_device"
        assert kwargs["severity"] == "warning"
        assert kwargs["source_type"] == "edge"
        assert kwargs["metadata"] == {
            "default_cameras": [
                {
                    "twin_uuid": "robot-primary-uuid",
                    "setup_name": "wrist",
                    "video_device": "/dev/video6",
                    "sensor_id": "wrist_camera",
                },
                {
                    "twin_uuid": "camera-twin-2",
                    "setup_name": "primary",
                    "video_device": "/dev/video8",
                    "sensor_id": "camera",
                },
                {
                    "twin_uuid": "camera-twin-3",
                    "setup_name": "secondary",
                    "video_device": "/dev/video2",
                    "sensor_id": "overhead",
                },
            ]
        }
        assert "wrist=/dev/video6" in kwargs["description"]
        assert "primary=/dev/video8" in kwargs["description"]

    def test_returns_false_when_no_default_cameras(self):
        """No alert should be created when camera list is empty."""
        twin = MagicMock()

        created = create_camera_default_device_alert(twin, [])

        assert created is False
        twin.alerts.create.assert_not_called()


def test_create_robot_setup_alert_returns_uuid() -> None:
    twin = MagicMock()
    mock_alert = MagicMock()
    mock_alert.uuid = "setup-alert-uuid-1"
    twin.alerts.create.return_value = mock_alert

    uid = cw_alerts.create_robot_setup_alert(twin)

    assert uid == "setup-alert-uuid-1"
    twin.alerts.create.assert_called_once()
    kwargs = twin.alerts.create.call_args.kwargs
    assert kwargs["alert_type"] == "robot_setup"
    assert kwargs["severity"] == "info"
    assert kwargs["source_type"] == "edge"
    assert "Setting up robot" in kwargs["name"]


def test_create_robot_setup_alert_returns_none_on_failure() -> None:
    twin = MagicMock()
    twin.alerts.create.side_effect = RuntimeError("api down")

    uid = cw_alerts.create_robot_setup_alert(twin)

    assert uid is None


def test_create_robot_setup_done_alert_returns_uuid() -> None:
    twin = MagicMock()
    mock_alert = MagicMock()
    mock_alert.uuid = "setup-done-uuid-1"
    twin.alerts.create.return_value = mock_alert

    uid = cw_alerts.create_robot_setup_done_alert(twin)

    assert uid == "setup-done-uuid-1"
    twin.alerts.create.assert_called_once()
    kwargs = twin.alerts.create.call_args.kwargs
    assert kwargs["alert_type"] == "robot_setup_done"
    assert kwargs["severity"] == "info"
    assert kwargs["source_type"] == "edge"
    assert "Setup completed" in kwargs["name"]


def test_create_robot_setup_done_alert_returns_none_on_failure() -> None:
    twin = MagicMock()
    twin.alerts.create.side_effect = RuntimeError("api down")

    uid = cw_alerts.create_robot_setup_done_alert(twin)

    assert uid is None


def test_resolve_alert_by_uuid_resolves_when_present() -> None:
    twin = MagicMock()
    mock_alert = MagicMock()
    twin.alerts.get.return_value = mock_alert

    cw_alerts.resolve_alert_by_uuid(twin, "a1")

    twin.alerts.get.assert_called_once_with("a1")
    mock_alert.resolve.assert_called_once()

    twin.alerts.get.reset_mock()
    twin.alerts.get.return_value = None
    cw_alerts.resolve_alert_by_uuid(twin, "a2")
    twin.alerts.get.assert_called_once_with("a2")


def test_schedule_robot_setup_done_resolve_fires() -> None:
    import time

    twin = MagicMock()
    mock_alert = MagicMock()
    twin.alerts.get.return_value = mock_alert

    timer = cw_alerts.schedule_robot_setup_done_resolve(twin, "done-1", delay_seconds=0.05)
    time.sleep(0.12)
    mock_alert.resolve.assert_called()
    timer.cancel()
