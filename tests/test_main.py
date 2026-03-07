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

        mock_stop_operation.assert_called_once()
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
        mock_stop_operation.assert_called_once()
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
        """stop command should return to idle camera streaming mode."""
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

        mock_stop_operation.assert_called_once()
        mock_start_idle.assert_called_once_with(mock_client, "twin-123")
        mock_client.mqtt.publish_command_message.assert_called_once_with("twin-123", "ok")


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
