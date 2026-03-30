"""Unit tests for main.py (handle_command, SUPPORTED_COMMANDS, etc.)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as main_module


class TestSupportedCommands:
    """Tests for SUPPORTED_COMMANDS constant."""

    def test_supported_commands_contains_expected(self):
        """SUPPORTED_COMMANDS includes all script commands and stop."""
        expected = {
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
        assert main_module.SUPPORTED_COMMANDS == expected

    def test_supported_commands_is_frozenset(self):
        """SUPPORTED_COMMANDS is immutable."""
        assert isinstance(main_module.SUPPORTED_COMMANDS, frozenset)


class TestHandleCommand:
    """Tests for handle_command dispatch logic."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Cyberwave client."""
        client = MagicMock()
        client.mqtt.publish_command_message = MagicMock()
        return client

    def test_unsupported_command_ignored(self, mock_client):
        """Unsupported command is ignored (no publish, no error)."""
        main_module.handle_command(
            mock_client,
            "twin-123",
            "unknown_command",
            {},
        )
        mock_client.mqtt.publish_command_message.assert_not_called()

    def test_empty_command_ignored(self, mock_client):
        """Empty or invalid command is ignored."""
        main_module.handle_command(mock_client, "twin-123", "", {})
        mock_client.mqtt.publish_command_message.assert_not_called()

    def test_so101_prefix_stripped(self, mock_client):
        """Command with so101- prefix is normalized (e.g. so101-remoteoperate -> remoteoperate)."""
        with patch.object(
            main_module, "start_remoteoperate", MagicMock()
        ) as mock_start:
            with patch.object(main_module, "_stop_current_operation"):
                main_module.handle_command(
                    mock_client,
                    "twin-123",
                    "so101-remoteoperate",
                    {},
                )
        mock_start.assert_called_once_with(mock_client, "twin-123")

    def test_controller_changed_dispatches(self, mock_client):
        """controller-changed command calls _handle_controller_changed."""
        with patch.object(
            main_module, "_handle_controller_changed", MagicMock()
        ) as mock_handler:
            main_module.handle_command(
                mock_client,
                "twin-123",
                "controller-changed",
                {"controller_type": "localop"},
            )
        mock_handler.assert_called_once_with(
            mock_client, "twin-123", {"controller_type": "localop"}
        )

    def test_recalibrate_dispatches(self, mock_client):
        """recalibrate command calls _handle_recalibrate."""
        with patch.object(
            main_module, "_handle_recalibrate", MagicMock()
        ) as mock_recalibrate:
            main_module.handle_command(
                mock_client,
                "twin-123",
                "recalibrate",
                {},
            )

        mock_recalibrate.assert_called_once_with(mock_client, "twin-123")

    def test_controller_cleared_restarts_idle_camera_streaming(self, mock_client):
        """Clearing controller stops operation and starts idle camera streaming."""
        with (
            patch.object(main_module, "_stop_current_operation") as mock_stop_operation,
            patch.object(main_module, "_start_idle_camera_streaming") as mock_start_idle,
        ):
            main_module._handle_controller_changed(
                mock_client,
                "twin-123",
                {"controller": None},
            )

        mock_stop_operation.assert_called_once_with(mock_client, "twin-123")
        mock_start_idle.assert_called_once_with(mock_client, "twin-123")
        mock_client.mqtt.publish_command_message.assert_called_once_with("twin-123", "ok")

    def test_controller_change_to_localop_stops_idle_streaming(self, mock_client):
        """Switching to localop stops idle stream before teleoperate starts."""
        with (
            patch.object(main_module, "_stop_idle_camera_streaming") as mock_stop_idle,
            patch.object(main_module, "_stop_current_operation") as mock_stop_operation,
            patch.object(main_module, "start_teleoperate") as mock_start_teleop,
        ):
            main_module._handle_controller_changed(
                mock_client,
                "twin-123",
                {"controller": {"controller_type": "localop"}},
            )

        mock_stop_idle.assert_called_once()
        mock_stop_operation.assert_called_once_with(mock_client, "twin-123")
        mock_start_teleop.assert_called_once_with(mock_client, "twin-123")

    def test_remoteoperate_command_stops_idle_streaming(self, mock_client):
        """remoteoperate command stops idle stream before starting operation."""
        with (
            patch.object(main_module, "_stop_idle_camera_streaming") as mock_stop_idle,
            patch.object(main_module, "_stop_current_operation") as mock_stop_operation,
            patch.object(main_module, "start_remoteoperate") as mock_start_remote,
        ):
            main_module.handle_command(
                mock_client,
                "twin-123",
                "remoteoperate",
                {},
            )

        mock_stop_idle.assert_called_once()
        mock_stop_operation.assert_called_once()
        mock_start_remote.assert_called_once_with(mock_client, "twin-123")

    def test_remoteoperate_failure_restarts_idle_streaming(self, mock_client):
        """remoteoperate startup error restores idle camera streaming."""
        with (
            patch.object(main_module, "_stop_idle_camera_streaming"),
            patch.object(main_module, "_stop_current_operation"),
            patch.object(
                main_module, "start_remoteoperate", side_effect=RuntimeError("boom")
            ),
            patch.object(main_module, "_start_idle_camera_streaming") as mock_start_idle,
        ):
            main_module.handle_command(
                mock_client,
                "twin-123",
                "remoteoperate",
                {},
            )

        mock_start_idle.assert_called_once_with(mock_client, "twin-123")
        mock_client.mqtt.publish_command_message.assert_called_once_with("twin-123", "error")

    def test_stop_command_restarts_idle_camera_streaming(self, mock_client):
        """stop command calls _stop_current_operation (no client/twin) and restarts idle streaming."""
        with (
            patch.object(main_module, "_stop_current_operation") as mock_stop_operation,
            patch.object(main_module, "_start_idle_camera_streaming") as mock_start_idle,
        ):
            main_module.handle_command(
                mock_client,
                "twin-123",
                "stop",
                {},
            )

        mock_stop_operation.assert_called_once_with()
        mock_start_idle.assert_called_once_with(mock_client, "twin-123")
        mock_client.mqtt.publish_command_message.assert_called_once_with("twin-123", "ok")


class TestControllerChangedCalibrationAlertResolution:
    """Tests that controller-changed resolves calibration alerts before starting teleop."""

    @pytest.fixture
    def mock_client(self):
        """Create a mock Cyberwave client."""
        client = MagicMock()
        client.mqtt.publish_command_message = MagicMock()
        return client

    def test_controller_changed_with_calibration_alert_resolves_alert(self, mock_client):
        """When calibration alert is active (step-0) and controller-changed arrives, alert is resolved."""
        with (
            patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve,
            patch.object(main_module, "_stop_idle_camera_streaming"),
            patch.object(main_module, "start_teleoperate"),
        ):
            main_module._calibration_alert_uuid = "alert-uuid-456"
            main_module._calibration_client = None
            main_module._calibration_twin_uuid = None
            try:
                main_module._handle_controller_changed(
                    mock_client,
                    "twin-123",
                    {"controller": {"controller_type": "localop"}},
                )
                mock_resolve.assert_called_once_with(
                    mock_client, "twin-123", "alert-uuid-456"
                )
            finally:
                main_module._calibration_alert_uuid = None

    def test_stop_current_operation_with_client_resolves_calibration_alert(self, mock_client):
        """_stop_current_operation(client, twin_uuid) resolves calibration alert when set."""
        with patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve:
            main_module._calibration_alert_uuid = "alert-uuid-789"
            main_module._calibration_client = None
            main_module._calibration_twin_uuid = None
            main_module._calibration_proc = None
            try:
                main_module._stop_current_operation(mock_client, "twin-123")
                mock_resolve.assert_called_once_with(
                    mock_client, "twin-123", "alert-uuid-789"
                )
            finally:
                main_module._calibration_alert_uuid = None

    def test_stop_current_operation_uses_calibration_client_when_no_params(self, mock_client):
        """_stop_current_operation() without params uses _calibration_client when alert exists."""
        with patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve:
            main_module._calibration_alert_uuid = "alert-uuid-abc"
            main_module._calibration_client = mock_client
            main_module._calibration_twin_uuid = "twin-123"
            main_module._calibration_proc = None
            try:
                main_module._stop_current_operation()
                mock_resolve.assert_called_once_with(
                    mock_client, "twin-123", "alert-uuid-abc"
                )
            finally:
                main_module._calibration_alert_uuid = None
                main_module._calibration_client = None
                main_module._calibration_twin_uuid = None

    def test_stop_current_operation_no_resolve_when_no_alert(self, mock_client):
        """_stop_current_operation does not call _resolve_alert_by_uuid when no calibration alert."""
        with patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve:
            main_module._calibration_alert_uuid = None
            main_module._calibration_proc = None
            main_module._stop_current_operation(mock_client, "twin-123")
            mock_resolve.assert_not_called()


class TestRecalibrationHelpers:
    """Tests for recalibration file cleanup and handler flow."""

    def test_remove_local_calibration_files_deletes_json_only(self, tmp_path, monkeypatch):
        """Cleanup removes *.json calibration files and keeps non-json files."""
        monkeypatch.setenv("CYBERWAVE_EDGE_CONFIG_DIR", str(tmp_path))
        calibration_dir = tmp_path / "so101_lib" / "calibrations"
        calibration_dir.mkdir(parents=True, exist_ok=True)
        (calibration_dir / "leader1.json").write_text("{}")
        (calibration_dir / "follower1.json").write_text("{}")
        (calibration_dir / "README.txt").write_text("keep")

        removed = main_module._remove_local_calibration_files()

        assert removed == 2
        assert not (calibration_dir / "leader1.json").exists()
        assert not (calibration_dir / "follower1.json").exists()
        assert (calibration_dir / "README.txt").exists()

    def test_handle_recalibrate_stops_publishes_and_restarts(self):
        """Handler stops operation, acknowledges, and starts restart thread."""
        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        with patch.object(main_module, "_stop_idle_camera_streaming") as mock_stop_idle:
            with patch.object(main_module, "_stop_current_operation") as mock_stop:
                with patch.object(
                    main_module, "_remove_local_calibration_files", return_value=2
                ) as mock_remove:
                    with patch.object(main_module.threading, "Thread") as mock_thread:
                        mock_thread.return_value = MagicMock()
                        main_module._handle_recalibrate(mock_client, "twin-123")

        mock_stop_idle.assert_called_once()
        mock_stop.assert_called_once()
        mock_remove.assert_called_once()
        mock_client.mqtt.publish_command_message.assert_called_once_with("twin-123", "ok")
        mock_thread.assert_called_once()
        thread_kwargs = mock_thread.call_args.kwargs
        assert thread_kwargs["target"] == main_module._restart_current_process
        assert thread_kwargs["daemon"] is True


class TestEdgeCameraConfigResolution:
    """Tests for camera device resolution from edge_configs camera mapping."""

    def test_resolve_camera_device_from_edge_config_sensors_devices(self):
        """_resolve_camera_device_for_twin prefers edge_configs.camera_config.sensors_devices."""
        twin = {
            "metadata": {
                "edge_fingerprint": "fp-1",
                "edge_configs": {
                    "camera_config": {
                        "sensors_devices": {
                            "wrist_camera": "/dev/video6",
                        }
                    }
                },
            }
        }

        dev = main_module._resolve_camera_device_for_twin(
            twin=twin,
            realsense_devices=[],
            fingerprint="fp-1",
        )

        assert dev == "/dev/video6"

    def test_get_robot_sensor_cameras_reads_edge_config_mapping(self, tmp_path):
        """_get_robot_twin_sensor_cameras reads canonical edge camera mapping."""
        import json

        robot_uuid = "robot-uuid-1"
        robot_json = tmp_path / f"{robot_uuid}.json"
        robot_json.write_text(
            json.dumps(
                {
                    "uuid": robot_uuid,
                    "metadata": {
                        "edge_fingerprint": "fp-1",
                        "edge_configs": {
                            "camera_config": {
                                "sensors_devices": {
                                    "wrist_camera": "/dev/video4",
                                }
                            }
                        },
                        "universal_schema": {
                            "sensors": [
                                {"id": "wrist_camera", "type": "rgb"},
                            ]
                        },
                    },
                }
            )
        )

        with patch.object(main_module, "_get_primary_robot_json_path", return_value=robot_json):
            with patch.object(main_module, "_load_edge_fingerprint", return_value="fp-1"):
                cameras = main_module._get_robot_twin_sensor_cameras(robot_uuid)

        assert cameras == [
            {
                "twin_uuid": robot_uuid,
                "attach_to_link": "robot_sensor",
                "camera_type": "cv2",
                "camera_id": 0,
                "video_device": "/dev/video4",
                "sensor_id": "wrist_camera",
            }
        ]


class TestCameraAutodiscoveryCompatibility:
    """Tests for compatibility-aware fallback selection."""

    def test_skips_incompatible_autodiscovered_devices(self):
        """Default camera assignment should use the first compatible device only."""
        discovered = [
            {
                "card": "bcm2835-codec-decode",
                "primary_path": "/dev/video10",
                "index": 10,
                "is_compatible": False,
                "compatibility_reason": "excluded_virtual_device",
            },
            {
                "card": "USB2.0_CAM1",
                "primary_path": "/dev/video0",
                "index": 0,
                "is_compatible": True,
                "compatibility_reason": "ok",
            },
        ]

        with (
            patch.object(main_module, "_load_discovered_devices", return_value=(discovered, 0)),
            patch.object(main_module, "_get_robot_twin_sensor_cameras", return_value=[]),
            patch.object(main_module, "_load_all_twin_jsons", return_value=[]),
            patch.object(main_module, "_load_edge_fingerprint", return_value=None),
            patch.object(main_module, "_load_primary_robot_twin", return_value={}),
            patch.object(main_module, "_get_primary_robot_default_rgb_sensor_id", return_value=None),
        ):
            cameras = main_module._discover_cameras_for_so101("robot-uuid-1")

        assert cameras == [
            {
                "twin_uuid": "robot-uuid-1",
                "attach_to_link": "robot_sensor",
                "setup_name": "wrist",
                "camera_type": "cv2",
                "camera_id": "/dev/video0",
                "video_device": "/dev/video0",
                "enable_depth": False,
                "used_default": True,
            }
        ]


class TestCameraStreamSensorOverride:
    """Tests for stream sensor naming overrides."""

    def test_camera_name_override_realsense_uses_default(self):
        """RealSense streams should always publish under default sensor key."""
        assert main_module._camera_name_override("realsense") == "default"
        assert main_module._camera_name_override("RealSense") == "default"

    def test_camera_name_override_cv2_returns_none(self):
        """Non-RealSense streams keep SDK sensor inference behavior."""
        assert main_module._camera_name_override("cv2") is None

    def test_build_idle_camera_twins_sets_default_sensor_for_realsense(self):
        """Idle camera stream config includes camera_name=default for RealSense."""
        client = MagicMock()
        client.twin.return_value = MagicMock()
        cfg = {
            "cameras": [
                {
                    "twin_uuid": "camera-twin-1",
                    "camera_type": "realsense",
                    "camera_id": "/dev/video6",
                    "resolution": "VGA",
                    "fps": 15,
                    "enable_depth": True,
                }
            ]
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            camera_twins = main_module._build_idle_camera_twins(client, "robot-twin-1")

        assert len(camera_twins) == 1
        _, overrides = camera_twins[0]
        assert overrides["camera_name"] == "default"
