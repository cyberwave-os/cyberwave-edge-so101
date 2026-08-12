"""Unit tests for main.py (handle_command, SUPPORTED_COMMANDS, etc.)."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as main_module
from scripts.cw_setup import So101Config


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

    def test_controller_change_to_localop_pins_op_and_evaluates(self, mock_client):
        """Switching to localop stops idle stream, pins the teleoperate op, and defers to
        _evaluate_and_drive (which calibrates if needed, then resumes the op)."""
        main_module._calibration_flow_step = None
        main_module._pending_recovery_command = None
        with (
            patch.object(main_module, "_calibration_active_count", 0),
            patch.object(main_module, "_calibration_proc", None),
            patch.object(main_module, "_stop_idle_camera_streaming") as mock_stop_idle,
            patch.object(main_module, "_stop_current_operation") as mock_stop_operation,
            patch.object(main_module, "_evaluate_and_drive") as mock_eval,
        ):
            main_module._handle_controller_changed(
                mock_client,
                "twin-123",
                {"controller": {"controller_type": "localop"}},
            )

        mock_stop_idle.assert_called_once()
        mock_stop_operation.assert_called_once_with(mock_client, "twin-123")
        mock_eval.assert_called_once_with(mock_client, "twin-123")
        assert main_module._pending_recovery_command == "teleoperate"
        mock_client.mqtt.publish_command_message.assert_called_once_with("twin-123", "ok")

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

    def test_controller_changed_during_calibration_keeps_alert_and_calibration(self, mock_client):
        """A controller-changed echo during an active calibration flow must not resolve
        the alert, stop the calibration, or (re)start an operation.

        Regression for the detach/abort/re-attach loop: the frontend detaches the
        controller when the calibration alert appears, which echoes back here as a
        controller-changed command. Acting on it killed the in-progress calibration
        subprocess and resolved its alert, only for the next echo to restart the
        whole flow — making the alert flicker and never letting the leader stage run.
        """
        with (
            patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve,
            patch.object(main_module, "_stop_current_operation") as mock_stop,
            patch.object(main_module, "_stop_idle_camera_streaming") as mock_stop_idle,
            patch.object(main_module, "start_teleoperate") as mock_start_teleop,
            patch.object(main_module, "start_remoteoperate") as mock_start_remote,
        ):
            main_module._calibration_alert_uuid = "alert-uuid-456"
            main_module._calibration_flow_step = main_module.CALIBRATION_STEP_ZERO
            main_module._pending_recovery_command = None
            main_module._calibration_client = None
            main_module._calibration_twin_uuid = None
            try:
                # Detach echo (controller cleared) — expected during calibration.
                main_module._handle_controller_changed(
                    mock_client,
                    "twin-123",
                    {"controller": None},
                )
                # Re-attach echo (localop) — should only update recovery intent.
                main_module._handle_controller_changed(
                    mock_client,
                    "twin-123",
                    {"controller": {"controller_type": "localop"}},
                )

                mock_resolve.assert_not_called()
                mock_stop.assert_not_called()
                mock_stop_idle.assert_not_called()
                mock_start_teleop.assert_not_called()
                mock_start_remote.assert_not_called()
                assert main_module._pending_recovery_command == "teleoperate"
                assert mock_client.mqtt.publish_command_message.call_count == 2
            finally:
                main_module._calibration_alert_uuid = None
                main_module._calibration_flow_step = None
                main_module._pending_recovery_command = None

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


class TestCameraChildTwinsOnly:
    """Cameras are assigned only when CYBERWAVE_CHILD_TWIN_UUIDS is set (no Path B)."""

    def test_no_child_uuids_returns_empty_without_loading_devices(self):
        """Without child UUIDs, return [] and do not probe V4L2 / merge edge config."""
        mock_load = MagicMock()
        with (
            patch.object(main_module, "_parse_child_camera_twin_uuids_from_env", return_value=[]),
            patch.object(main_module, "_load_discovered_devices", mock_load),
        ):
            assert main_module._discover_cameras_for_so101("robot-uuid-1") == []
        mock_load.assert_not_called()

    def test_no_child_uuids_returns_empty_even_if_devices_patched(self):
        """Explicit: empty child list yields no cameras regardless of discovery mocks."""
        discovered = [
            {
                "card": "USB2.0_CAM1",
                "primary_path": "/dev/video0",
                "index": 0,
                "is_compatible": True,
            },
        ]
        mock_load = MagicMock(return_value=(discovered, 0))
        with (
            patch.object(main_module, "_parse_child_camera_twin_uuids_from_env", return_value=[]),
            patch.object(main_module, "_load_discovered_devices", mock_load),
        ):
            assert main_module._discover_cameras_for_so101("robot-uuid-1") == []
        mock_load.assert_not_called()

    def test_child_path_empty_when_no_devices_in_pools(self):
        """Child UUID set and twin JSON present but no V4L devices -> no assignment."""
        twin_a = {"uuid": "child-a", "metadata": {}, "asset": {"registry_id": "x/standard-cam"}}
        with (
            patch.object(main_module, "_load_discovered_devices", return_value=([], 0)),
            patch.object(
                main_module,
                "_parse_child_camera_twin_uuids_from_env",
                return_value=["child-a"],
            ),
            patch.object(main_module, "_load_all_twin_jsons", return_value=[twin_a]),
            patch.object(main_module, "_resolve_camera_device_for_twin", return_value=None),
            patch.object(main_module, "_load_edge_fingerprint", return_value=None),
        ):
            cameras = main_module._discover_cameras_for_so101("robot-parent")
        assert cameras == []

    def test_child_path_warns_when_twin_json_missing(self, caplog):
        """Missing twin JSON for a child UUID logs a warning."""
        import logging

        caplog.set_level(logging.WARNING)
        with (
            patch.object(main_module, "_load_discovered_devices", return_value=([], 0)),
            patch.object(
                main_module,
                "_parse_child_camera_twin_uuids_from_env",
                return_value=["missing-id"],
            ),
            patch.object(main_module, "_load_all_twin_jsons", return_value=[]),
            patch.object(main_module, "_load_edge_fingerprint", return_value=None),
        ):
            assert main_module._discover_cameras_for_so101("robot-parent") == []
        assert any(
            "no twin JSON for missing-id" in r.message for r in caplog.records
        )


_D455_GROUP = {
    "card": "Intel(R) RealSense(TM) Depth Camera 455",
    "bus_info": "usb-0000:00:14.0-1",
    "paths": [
        "/dev/video0",
        "/dev/video1",
        "/dev/video2",
        "/dev/video3",
        "/dev/video4",
        "/dev/video5",
    ],
    "primary_path": "/dev/video0",
    "index": 0,
    "is_compatible": True,
}

_USB_CAM = {
    "card": "USB2.0_CAM1",
    "bus_info": "usb-0000:00:14.0-4",
    "paths": ["/dev/video6", "/dev/video7"],
    "primary_path": "/dev/video6",
    "index": 6,
    "is_compatible": True,
}


def _twin(uuid: str, registry_id: str, sensors_devices: dict | None = None) -> dict:
    """Build a child camera twin JSON with an optional edge camera mapping."""
    metadata: dict = {"edge_fingerprint": "fp-1"}
    if sensors_devices is not None:
        metadata["edge_configs"] = {"camera_config": {"sensors_devices": sensors_devices}}
    return {"uuid": uuid, "metadata": metadata, "asset": {"registry_id": registry_id}}


def _discover(child_uuids: list[str], twins: list[dict], discovered: list[dict]) -> list[dict]:
    """Run ``_discover_cameras_for_so101`` against fixed discovery output."""
    with (
        patch.object(main_module, "_load_discovered_devices", return_value=(discovered, 0)),
        patch.object(
            main_module,
            "_parse_child_camera_twin_uuids_from_env",
            return_value=child_uuids,
        ),
        patch.object(main_module, "_load_all_twin_jsons", return_value=twins),
        patch.object(main_module, "_load_edge_fingerprint", return_value="fp-1"),
    ):
        return main_module._discover_cameras_for_so101("robot-parent")


class TestMultiNodeCameraDevices:
    """A single physical camera exposes several ``/dev/video*`` nodes.

    ``v4l2-ctl --list-devices`` groups them under one card; a RealSense D4xx has
    one node per depth/IR/colour stream plus a metadata node each. Assignment
    must treat the whole group as one device.
    """

    def test_realsense_sibling_node_classified_as_realsense(self):
        """A mapping onto a non-primary RealSense node still binds librealsense."""
        twins = [_twin("child-rs", "intel/realsensed455", {"color_camera": "/dev/video4"})]

        cameras = _discover(["child-rs"], twins, [_D455_GROUP])

        assert len(cameras) == 1
        assert cameras[0]["camera_type"] == "realsense"

    def test_assigning_realsense_reserves_sibling_nodes(self):
        """A second twin cannot claim another node of an already-bound camera."""
        twins = [
            _twin("child-rs", "intel/realsensed455", {"color_camera": "/dev/video0"}),
            _twin("child-rgb", "x/standard-cam", {"wrist_camera": "/dev/video2"}),
        ]

        cameras = _discover(["child-rs", "child-rgb"], twins, [_D455_GROUP])

        assert [c["twin_uuid"] for c in cameras] == ["child-rs"]

    def test_stale_mapped_device_falls_back_to_discovery(self):
        """A mapped node that no longer exists must not be used blindly."""
        twins = [_twin("child-rgb", "x/standard-cam", {"wrist_camera": "/dev/video9"})]

        cameras = _discover(["child-rgb"], twins, [_USB_CAM])

        assert len(cameras) == 1
        assert cameras[0]["video_device"] == "/dev/video6"
        assert cameras[0]["camera_type"] == "cv2"

    def test_realsense_twin_not_downgraded_when_mapping_is_stale(self):
        """A RealSense twin binds the discovered RealSense, never cv2."""
        twins = [_twin("child-rs", "intel/realsensed455", {"color_camera": "/dev/video9"})]

        cameras = _discover(["child-rs"], twins, [_D455_GROUP, _USB_CAM])

        assert len(cameras) == 1
        assert cameras[0]["camera_type"] == "realsense"
        assert cameras[0]["video_device"] == "/dev/video0"

    def test_rgb_twin_mapped_to_realsense_node_falls_back_to_cv2_pool(self):
        """An RGB twin never opens a RealSense node; it takes a free cv2 device."""
        twins = [_twin("child-rgb", "x/standard-cam", {"wrist_camera": "/dev/video2"})]

        cameras = _discover(["child-rgb"], twins, [_D455_GROUP, _USB_CAM])

        assert len(cameras) == 1
        assert cameras[0]["camera_type"] == "cv2"
        assert cameras[0]["video_device"] == "/dev/video6"

    def test_realsense_enumerated_off_video0_does_not_leak_into_cv2(self):
        """USB re-enumeration moves the RealSense; a stale mapping must not win.

        Reproduces the field case: a webcam takes video0/video1 so the D455 lands
        on video2-video7, and a ``sensors_devices`` entry saved earlier still names
        /dev/video6 for the RGB twin. That node belongs to the RealSense, so cv2
        would fight librealsense for it.
        """
        d455_shifted = {
            "card": "Intel(R) RealSense(TM) Depth Camera 455",
            "bus_info": "usb-0000:00:14.0-1",
            "paths": [f"/dev/video{i}" for i in range(2, 8)],
            "primary_path": "/dev/video2",
            "index": 2,
            "is_compatible": True,
        }
        webcam = {
            "card": "USB2.0_CAM1",
            "bus_info": "usb-0000:00:14.0-4",
            "paths": ["/dev/video0", "/dev/video1"],
            "primary_path": "/dev/video0",
            "index": 0,
            "is_compatible": True,
        }
        twins = [
            _twin("child-rs", "intel/realsensed455"),
            _twin("child-rgb", "x/standard-cam", {"wrist_camera": "/dev/video6"}),
        ]

        cameras = _discover(["child-rs", "child-rgb"], twins, [d455_shifted, webcam])

        by_twin = {c["twin_uuid"]: c for c in cameras}
        assert by_twin["child-rs"]["camera_type"] == "realsense"
        assert by_twin["child-rs"]["video_device"] == "/dev/video2"
        assert by_twin["child-rgb"]["camera_type"] == "cv2"
        assert by_twin["child-rgb"]["video_device"] == "/dev/video0"

    def test_mapping_trusted_when_discovery_is_empty(self):
        """Containers without /dev/video* access keep using the edge mapping."""
        twins = [_twin("child-rgb", "x/standard-cam", {"wrist_camera": "/dev/video6"})]

        cameras = _discover(["child-rgb"], twins, [])

        assert len(cameras) == 1
        assert cameras[0]["video_device"] == "/dev/video6"


class TestSensorScopedDeviceMapping:
    """``sensors_devices`` can map several sensors; pick the twin's own."""

    def test_prefers_entry_matching_a_declared_rgb_sensor(self):
        twin = {
            "metadata": {
                "edge_fingerprint": "fp-1",
                "edge_configs": {
                    "camera_config": {
                        "sensors_devices": {
                            "depth_camera": "/dev/video2",
                            "wrist_camera": "/dev/video6",
                        }
                    }
                },
                "universal_schema": {"sensors": [{"id": "wrist_camera", "type": "rgb"}]},
            }
        }

        dev = main_module._resolve_camera_device_for_twin(
            twin=twin,
            realsense_devices=[],
            fingerprint="fp-1",
        )

        assert dev == "/dev/video6"


class TestChildTwinUuidsFromEdgeCore:
    """CYBERWAVE_CHILD_TWIN_UUIDS matches edge-core driver contract."""

    def test_parse_child_camera_twin_uuids_strips_and_dedupes(self):
        with patch.dict(os.environ, {"CYBERWAVE_CHILD_TWIN_UUIDS": " u1 , u2 , u1 "}):
            assert main_module._parse_child_camera_twin_uuids_from_env() == ["u1", "u2"]

    def test_parse_child_camera_twin_uuids_empty_when_unset(self, monkeypatch):
        monkeypatch.delenv("CYBERWAVE_CHILD_TWIN_UUIDS", raising=False)
        assert main_module._parse_child_camera_twin_uuids_from_env() == []

    def test_discover_assigns_cv2_pool_in_child_uuid_order(self):
        """Child path uses env UUID order and assigns distinct CV2 devices."""
        discovered = [
            {
                "card": "USB_CAM_A",
                "primary_path": "/dev/video0",
                "index": 0,
                "is_compatible": True,
            },
            {
                "card": "USB_CAM_B",
                "primary_path": "/dev/video2",
                "index": 2,
                "is_compatible": True,
            },
        ]
        twin_a = {"uuid": "child-a", "metadata": {}, "asset": {"registry_id": "x/standard-cam"}}
        twin_b = {"uuid": "child-b", "metadata": {}, "asset": {"registry_id": "x/standard-cam"}}
        with (
            patch.object(main_module, "_load_discovered_devices", return_value=(discovered, 0)),
            patch.object(
                main_module,
                "_parse_child_camera_twin_uuids_from_env",
                return_value=["child-a", "child-b"],
            ),
            patch.object(main_module, "_load_all_twin_jsons", return_value=[twin_a, twin_b]),
            patch.object(main_module, "_resolve_camera_device_for_twin", return_value=None),
            patch.object(main_module, "_load_edge_fingerprint", return_value=None),
        ):
            cameras = main_module._discover_cameras_for_so101("robot-parent")

        assert len(cameras) == 2
        assert [c["twin_uuid"] for c in cameras] == ["child-a", "child-b"]
        assert cameras[0]["camera_id"] == "/dev/video0"
        assert cameras[1]["camera_id"] == "/dev/video2"
        assert all(not c.get("setup_name") for c in cameras)


class TestGetHardwareConfigContract:
    """``_get_hardware_config`` uses only ``setup.json`` ``cameras`` (+ ports)."""

    def test_uses_flat_cameras_for_matching_twin(self):
        setup = {
            "twin_uuid": "t-1",
            "leader_port": "/dev/ttyACM0",
            "follower_port": "/dev/ttyACM1",
            "cameras": [
                {
                    "twin_uuid": "cam-1",
                    "camera_type": "cv2",
                    "camera_id": "/dev/video0",
                    "resolution": "640x480",
                    "fps": 30,
                }
            ],
        }
        with patch("scripts.cw_setup.load_setup_config", return_value=setup):
            cfg = main_module._get_hardware_config("t-1")

        assert cfg["leader_port"] == "/dev/ttyACM0"
        assert cfg["follower_port"] == "/dev/ttyACM1"
        assert len(cfg["cameras"]) == 1
        assert cfg["cameras"][0]["twin_uuid"] == "cam-1"
        assert cfg["cameras"][0]["camera_type"] == "cv2"
        assert cfg["cameras"][0]["camera_id"] == "/dev/video0"
        assert cfg["cameras"][0]["resolution"] == "640x480"
        assert cfg["cameras"][0]["fps"] == 30

    def test_empty_cameras_when_twin_uuid_mismatch(self):
        setup = {"twin_uuid": "other", "cameras": [{"twin_uuid": "x", "camera_id": 0}]}
        with patch("scripts.cw_setup.load_setup_config", return_value=setup):
            cfg = main_module._get_hardware_config("t-1")
        assert cfg["cameras"] == []

    def test_ignores_legacy_top_level_keys(self):
        """Old ``wrist_camera`` / ``additional_cameras`` keys are not merged into cameras."""
        setup = {
            "twin_uuid": "t-1",
            "wrist_camera": {"twin_uuid": "legacy-wrist", "camera_id": 9},
            "additional_cameras": [{"twin_uuid": "legacy-extra", "camera_id": 8}],
            "camera_twin_uuid": "legacy-single",
            "cameras": [
                {
                    "twin_uuid": "real-cam",
                    "camera_type": "cv2",
                    "camera_id": 0,
                }
            ],
        }
        with patch("scripts.cw_setup.load_setup_config", return_value=setup):
            cfg = main_module._get_hardware_config("t-1")
        assert [c["twin_uuid"] for c in cfg["cameras"]] == ["real-cam"]


class TestCameraStreamSensorKey:
    """The twin's declared sensors own the stream sensor key.

    The SDK infers it from ``capabilities.sensors[0].id`` — that is the key
    viewers subscribe to. Overriding it on the edge publishes under a key
    nobody listens on, so WebRTC connects and the frame counter advances while
    the viewer stays black.
    """

    def _idle_overrides(self, camera_type: str) -> dict:
        client = MagicMock()
        client.twin.return_value = MagicMock()
        so101 = So101Config.from_dict(
            {
                "twin_uuid": "robot-twin-1",
                "cameras": [
                    {
                        "twin_uuid": "camera-twin-1",
                        "camera_type": camera_type,
                        "camera_id": "/dev/video6",
                        "resolution": "VGA",
                        "fps": 15,
                        "enable_depth": camera_type == "realsense",
                    }
                ],
            }
        )
        with patch(
            "scripts.cw_setup.load_so101_config_for_robot_twin",
            return_value=so101,
        ):
            camera_twins = main_module._build_idle_camera_twins(client, "robot-twin-1")

        assert len(camera_twins) == 1
        _, overrides = camera_twins[0]
        return overrides

    def test_realsense_does_not_override_sensor_key(self):
        """A RealSense twin declares ``color_camera``; do not force ``camera``."""
        assert "camera_name" not in self._idle_overrides("realsense")

    def test_cv2_does_not_override_sensor_key(self):
        """cv2 already deferred to the twin, which is why those streams work."""
        assert "camera_name" not in self._idle_overrides("cv2")


class TestSo101PortRefresh:
    """Tests for voltage autodiscovery merge and setup.json refresh before teleop/remoteop.

    When ``setup.json`` paths **do not match** current hardware (e.g. USB reordering):

    - If discovery returns a path for a role (and env does not pin it), that value **replaces**
      the stored path — including when the old path was different or still exists on disk.
    - If discovery returns ``None`` for a role, the previous value from ``setup.json`` is
      **kept** (we never clear a port). The user may still hit connection errors until hardware
      or voltage read matches again.
    """

    def test_apply_ports_env_and_discovery_updates_from_scan(self, monkeypatch):
        """Discovery replaces stale paths when env does not pin a role."""
        monkeypatch.delenv("CYBERWAVE_METADATA_LEADER_PORT", raising=False)
        monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)
        existing = {"leader_port": "/dev/stale0", "follower_port": "/dev/stale1"}
        with patch(
            "utils.utils.discover_so101_ports_by_voltage",
            return_value={
                "leader_port": "/dev/ttyACM0",
                "follower_port": "/dev/ttyACM1",
            },
        ):
            main_module._apply_so101_ports_env_and_discovery(existing, phase="test")

        assert existing["leader_port"] == "/dev/ttyACM0"
        assert existing["follower_port"] == "/dev/ttyACM1"

    def test_apply_ports_env_and_discovery_env_overrides_discovery_per_role(self, monkeypatch):
        """CYBERWAVE_METADATA_* keeps that role; other role still from discovery."""
        monkeypatch.setenv("CYBERWAVE_METADATA_LEADER_PORT", "/dev/pinned_leader")
        monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)
        existing = {"leader_port": "/dev/old", "follower_port": "/dev/old_f"}
        with patch(
            "utils.utils.discover_so101_ports_by_voltage",
            return_value={
                "leader_port": "/dev/disc_l",
                "follower_port": "/dev/disc_f",
            },
        ):
            main_module._apply_so101_ports_env_and_discovery(existing, phase="test")

        assert existing["leader_port"] == "/dev/pinned_leader"
        assert existing["follower_port"] == "/dev/disc_f"

    def test_apply_ports_keeps_setup_when_discovery_returns_none_for_role(self, monkeypatch):
        """If voltage scan cannot classify a role, we do not overwrite with None — keep file value."""
        monkeypatch.delenv("CYBERWAVE_METADATA_LEADER_PORT", raising=False)
        monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)
        existing = {
            "leader_port": "/dev/ttyACM0",
            "follower_port": "/dev/ttyACM1",
        }
        with patch(
            "utils.utils.discover_so101_ports_by_voltage",
            return_value={
                "leader_port": None,
                "follower_port": "/dev/ttyACM9",
            },
        ):
            main_module._apply_so101_ports_env_and_discovery(existing, phase="test")

        assert existing["leader_port"] == "/dev/ttyACM0"
        assert existing["follower_port"] == "/dev/ttyACM9"

    def test_apply_ports_replaces_both_roles_when_usb_paths_swap(self, monkeypatch):
        """Discovery wins for both roles even when stored leader/follower paths are swapped."""
        monkeypatch.delenv("CYBERWAVE_METADATA_LEADER_PORT", raising=False)
        monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)
        existing = {
            "leader_port": "/dev/ttyACM1",
            "follower_port": "/dev/ttyACM0",
        }
        with patch(
            "utils.utils.discover_so101_ports_by_voltage",
            return_value={
                "leader_port": "/dev/ttyACM0",
                "follower_port": "/dev/ttyACM1",
            },
        ):
            main_module._apply_so101_ports_env_and_discovery(existing, phase="test")

        assert existing["leader_port"] == "/dev/ttyACM0"
        assert existing["follower_port"] == "/dev/ttyACM1"

    def test_start_remoteoperate_persists_discovery_over_stale_setup_then_load_matches(
        self, tmp_path, monkeypatch
    ):
        """Stale ports in setup.json are replaced on disk before config is read for the operation."""
        import json

        from scripts.cw_setup import build_edge_hardware_dict, load_so101_config_for_robot_twin

        monkeypatch.setenv("CYBERWAVE_EDGE_CONFIG_DIR", str(tmp_path))
        monkeypatch.delenv("CYBERWAVE_METADATA_LEADER_PORT", raising=False)
        monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)

        setup_path = tmp_path / "so101_lib" / "setup.json"
        setup_path.parent.mkdir(parents=True)
        setup_path.write_text(
            json.dumps(
                {
                    "twin_uuid": "t-1",
                    "leader_port": "/dev/stale_leader",
                    "follower_port": "/dev/stale_follower",
                    "cameras": [],
                }
            )
        )

        discovered = {
            "leader_port": "/dev/ttyACM7",
            "follower_port": "/dev/ttyACM8",
        }
        mock_client = MagicMock()
        with (
            patch("utils.utils.discover_so101_ports_by_voltage", return_value=discovered),
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch(
                "scripts.cw_setup.materialize_camera_entries_for_edge_operation",
                return_value=[],
            ),
            patch.object(main_module.threading, "Thread", new=MagicMock()),
        ):
            main_module.start_remoteoperate(mock_client, "t-1")

        on_disk = json.loads(setup_path.read_text())
        assert on_disk["leader_port"] == "/dev/ttyACM7"
        assert on_disk["follower_port"] == "/dev/ttyACM8"

        so101 = load_so101_config_for_robot_twin("t-1")
        cfg = build_edge_hardware_dict(so101)
        assert cfg["leader_port"] == "/dev/ttyACM7"
        assert cfg["follower_port"] == "/dev/ttyACM8"

    def test_refresh_skips_save_when_setup_json_missing(self, tmp_path, monkeypatch):
        """No setup file: log path and do not call save_setup_config."""
        monkeypatch.setenv("CYBERWAVE_EDGE_CONFIG_DIR", str(tmp_path))
        setup_path = tmp_path / "so101_lib" / "setup.json"
        assert not setup_path.exists()

        with patch("scripts.cw_setup.save_setup_config") as mock_save:
            main_module._refresh_so101_ports_in_setup()

        mock_save.assert_not_called()

    def test_refresh_ports_skips_when_setup_invalid(self, tmp_path, monkeypatch):
        """A corrupt config stays available for inspection and is not replaced by ports only."""
        monkeypatch.setenv("CYBERWAVE_EDGE_CONFIG_DIR", str(tmp_path))
        setup_path = tmp_path / "so101_lib" / "setup.json"
        setup_path.parent.mkdir(parents=True)
        setup_path.write_text("")

        with patch.object(main_module, "_apply_so101_ports_env_and_discovery") as apply_mock:
            main_module._refresh_so101_ports_in_setup()

        apply_mock.assert_not_called()
        assert setup_path.read_text() == ""

    def test_refresh_persists_discovered_ports_preserving_other_keys(self, tmp_path, monkeypatch):
        """Refresh loads setup.json, merges discovery, saves full dict."""
        import json

        monkeypatch.setenv("CYBERWAVE_EDGE_CONFIG_DIR", str(tmp_path))
        monkeypatch.delenv("CYBERWAVE_METADATA_LEADER_PORT", raising=False)
        monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)

        setup_path = tmp_path / "so101_lib" / "setup.json"
        setup_path.parent.mkdir(parents=True)
        setup_path.write_text(
            json.dumps(
                {
                    "twin_uuid": "robot-1",
                    "leader_port": "/dev/stale0",
                    "follower_port": "/dev/stale1",
                    "cameras": [{"twin_uuid": "c1", "camera_type": "cv2", "camera_id": 0}],
                }
            )
        )

        with patch(
            "utils.utils.discover_so101_ports_by_voltage",
            return_value={
                "leader_port": "/dev/ttyACM2",
                "follower_port": "/dev/ttyACM3",
            },
        ):
            main_module._refresh_so101_ports_in_setup()

        data = json.loads(setup_path.read_text())
        assert data["leader_port"] == "/dev/ttyACM2"
        assert data["follower_port"] == "/dev/ttyACM3"
        assert data["twin_uuid"] == "robot-1"
        assert len(data["cameras"]) == 1

    def test_start_remoteoperate_calls_port_refresh(self):
        """start_remoteoperate runs _refresh_so101_ports_in_setup before loading config."""
        mock_client = MagicMock()
        so101 = So101Config.from_dict(
            {
                "twin_uuid": "t-1",
                "leader_port": "/dev/a",
                "follower_port": "/dev/b",
            }
        )
        with (
            patch.object(main_module, "_refresh_so101_ports_in_setup") as mock_refresh,
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=True),
            patch(
                "scripts.cw_setup.load_so101_config_for_robot_twin",
                return_value=so101,
            ),
            patch(
                "scripts.cw_setup.materialize_camera_entries_for_edge_operation",
                return_value=[],
            ),
            patch.object(main_module.threading, "Thread", new=MagicMock()),
        ):
            main_module.start_remoteoperate(mock_client, "t-1")

        mock_refresh.assert_called_once()

    def test_start_remoteoperate_forces_present_leader_calibration(self):
        """A present-but-uncalibrated leader blocks a direct remoteoperate command:
        it triggers leader calibration instead of starting the operation."""
        mock_client = MagicMock()
        so101 = So101Config.from_dict(
            {
                "twin_uuid": "t-1",
                "leader_port": "/dev/a",
                "follower_port": "/dev/b",
            }
        )
        with (
            patch.object(main_module, "_refresh_so101_ports_in_setup"),
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=False),
            patch(
                "scripts.cw_setup.load_so101_config_for_robot_twin",
                return_value=so101,
            ),
            patch.object(
                main_module, "_trigger_alert_and_switch_to_calibration"
            ) as trigger,
            patch.object(main_module.threading, "Thread", new=MagicMock()) as thread,
        ):
            main_module.start_remoteoperate(mock_client, "t-1")

        trigger.assert_called_once()
        assert trigger.call_args.kwargs["device_type"] == "leader"
        thread.assert_not_called()

    def test_start_remoteoperate_runs_when_no_leader_present(self):
        """A follower-only setup (no leader port) is not blocked on leader calibration."""
        mock_client = MagicMock()
        so101 = So101Config.from_dict(
            {
                "twin_uuid": "t-1",
                "leader_port": "",
                "follower_port": "/dev/b",
            }
        )
        with (
            patch.object(main_module, "_refresh_so101_ports_in_setup"),
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=False),
            patch(
                "scripts.cw_setup.load_so101_config_for_robot_twin",
                return_value=so101,
            ),
            patch(
                "scripts.cw_setup.materialize_camera_entries_for_edge_operation",
                return_value=[],
            ),
            patch.object(
                main_module, "_trigger_alert_and_switch_to_calibration"
            ) as trigger,
            patch.object(main_module.threading, "Thread", new=MagicMock()) as thread,
        ):
            main_module.start_remoteoperate(mock_client, "t-1")

        trigger.assert_not_called()
        thread.assert_called_once()

    def test_start_teleoperate_calls_port_refresh(self):
        """start_teleoperate runs _refresh_so101_ports_in_setup before loading config."""
        mock_client = MagicMock()
        so101 = So101Config.from_dict(
            {
                "twin_uuid": "t-1",
                "leader_port": "/dev/a",
                "follower_port": "/dev/b",
            }
        )
        with (
            patch.object(main_module, "_refresh_so101_ports_in_setup") as mock_refresh,
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=True),
            patch(
                "scripts.cw_setup.load_so101_config_for_robot_twin",
                return_value=so101,
            ),
            patch(
                "scripts.cw_setup.materialize_camera_entries_for_edge_operation",
                return_value=[],
            ),
            patch.object(main_module.threading, "Thread", new=MagicMock()),
        ):
            main_module.start_teleoperate(mock_client, "t-1")

        mock_refresh.assert_called_once()


class TestEdgeHealth:
    """Tests for the twin edge_health publisher wiring (mirrors Piper)."""

    def teardown_method(self):
        """Ensure the module-level publisher is cleared between tests."""
        main_module._edge_health = None

    def test_extras_reports_idle_by_default(self):
        """No operation running -> operation_mode 'idle'."""
        with (
            patch.object(main_module, "_is_calibration_running", return_value=False),
            patch.object(main_module, "_is_control_operation_running", return_value=False),
        ):
            assert main_module._so101_edge_health_extras() == {"operation_mode": "idle"}

    def test_extras_reports_control_when_operation_running(self):
        """Teleop/remoteop thread active -> operation_mode 'control'."""
        with (
            patch.object(main_module, "_is_calibration_running", return_value=False),
            patch.object(main_module, "_is_control_operation_running", return_value=True),
        ):
            assert main_module._so101_edge_health_extras() == {"operation_mode": "control"}

    def test_extras_reports_calibration_when_calibrating(self):
        """Calibration takes precedence over control state."""
        with (
            patch.object(main_module, "_is_calibration_running", return_value=True),
            patch.object(main_module, "_is_control_operation_running", return_value=True),
        ):
            assert main_module._so101_edge_health_extras() == {
                "operation_mode": "calibration"
            }

    def test_start_edge_health_constructs_and_starts_publisher(self):
        """_start_edge_health builds EdgeHealthCheck for the robot twin and starts it."""
        client = MagicMock()
        with patch.object(main_module, "EdgeHealthCheck") as MockHealth:
            main_module._start_edge_health(client, "twin-123")

        MockHealth.assert_called_once_with(
            mqtt_client=client.mqtt,
            twin_uuids=["twin-123"],
            edge_id="twin-123",
            host_metrics_provider=main_module._so101_edge_health_extras,
            stream_config_provider=main_module._so101_joint_stream_config_provider,
        )
        MockHealth.return_value.start.assert_called_once()
        assert main_module._edge_health is MockHealth.return_value

    EXPECTED_JOINT_STREAM = {
        "joints": {
            "kind": "imu",
            "source": "cyberwave/joint/twin-123/update",
            "rate_hz": main_module.SO101_JOINT_STREAM_RATE_HZ,
        }
    }

    def test_joint_stream_provider_healthy_during_operation(self):
        """While any op runs, provider advertises the imu joint stream and
        bumps the liveness counter so the stream reports healthy."""
        health = MagicMock()
        health.edge_id = "twin-123"
        main_module._edge_health = health
        with (
            patch.object(main_module, "_is_any_operation_running", return_value=True),
            patch.object(main_module, "_follower_device_present", return_value=False),
        ):
            cfg = main_module._so101_joint_stream_config_provider()
        health.update_frame_count.assert_called_once()
        assert cfg == self.EXPECTED_JOINT_STREAM

    def test_joint_stream_provider_healthy_when_idle_with_device(self):
        """Idle but follower device node present -> stream advertised AND
        liveness bumped, so an idle robot never reads 'degraded' (CYB-2308)."""
        health = MagicMock()
        health.edge_id = "twin-123"
        main_module._edge_health = health
        with (
            patch.object(main_module, "_is_any_operation_running", return_value=False),
            patch.object(main_module, "_follower_device_present", return_value=True),
        ):
            cfg = main_module._so101_joint_stream_config_provider()
        health.update_frame_count.assert_called_once()
        assert cfg == self.EXPECTED_JOINT_STREAM

    def test_joint_stream_provider_stale_when_idle_without_device(self):
        """Idle and follower unplugged -> stream still advertised but no
        liveness bump, so it goes stale and the twin reads 'degraded'."""
        health = MagicMock()
        health.edge_id = "twin-123"
        main_module._edge_health = health
        with (
            patch.object(main_module, "_is_any_operation_running", return_value=False),
            patch.object(main_module, "_follower_device_present", return_value=False),
        ):
            cfg = main_module._so101_joint_stream_config_provider()
        health.update_frame_count.assert_not_called()
        assert cfg == self.EXPECTED_JOINT_STREAM

    def test_joint_stream_provider_empty_when_no_publisher(self):
        """No publisher yet -> provider is a safe no-op."""
        main_module._edge_health = None
        with patch.object(
            main_module, "_is_any_operation_running", return_value=True
        ):
            assert main_module._so101_joint_stream_config_provider() == {}

    def test_follower_device_present_checks_port_node(self, tmp_path):
        """_follower_device_present is True only when the configured follower
        port exists on disk, and never raises."""
        port = tmp_path / "ttyACM0"
        with patch.object(
            main_module,
            "_get_hardware_config",
            return_value={"follower_port": str(port)},
        ):
            assert main_module._follower_device_present("twin-123") is False
            port.touch()
            assert main_module._follower_device_present("twin-123") is True
        with patch.object(
            main_module, "_get_hardware_config", return_value={"follower_port": None}
        ):
            assert main_module._follower_device_present("twin-123") is False
        with patch.object(
            main_module, "_get_hardware_config", side_effect=RuntimeError("boom")
        ):
            assert main_module._follower_device_present("twin-123") is False

    def test_start_edge_health_is_idempotent(self):
        """A second _start_edge_health call does not create a second publisher."""
        client = MagicMock()
        with patch.object(main_module, "EdgeHealthCheck") as MockHealth:
            main_module._start_edge_health(client, "twin-123")
            main_module._start_edge_health(client, "twin-123")

        MockHealth.assert_called_once()

    def test_stop_edge_health_stops_and_clears(self):
        """_stop_edge_health stops the running publisher and clears the global."""
        client = MagicMock()
        with patch.object(main_module, "EdgeHealthCheck") as MockHealth:
            main_module._start_edge_health(client, "twin-123")
            main_module._stop_edge_health()

        MockHealth.return_value.stop.assert_called_once()
        assert main_module._edge_health is None

    def test_stop_edge_health_no_op_when_not_started(self):
        """_stop_edge_health is safe to call when nothing is running."""
        main_module._edge_health = None
        main_module._stop_edge_health()  # must not raise
        assert main_module._edge_health is None


class TestPendingReattachState:
    """Tests for the pending-controller-reattach state file helpers."""

    def test_save_and_load_roundtrip(self, tmp_path):
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("twin-1", "pol-1")
            assert main_module._load_pending_reattach("twin-1") == "pol-1"

    def test_load_returns_none_for_other_twin(self, tmp_path):
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("twin-1", "pol-1")
            assert main_module._load_pending_reattach("twin-2") is None

    def test_load_returns_none_when_absent(self, tmp_path):
        p = tmp_path / "missing.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            assert main_module._load_pending_reattach("twin-1") is None

    def test_clear_deletes_file(self, tmp_path):
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("twin-1", "pol-1")
            main_module._clear_pending_reattach()
            assert not p.exists()
            assert main_module._load_pending_reattach("twin-1") is None

    def test_save_is_noop_without_uuid(self, tmp_path):
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("twin-1", "")
            assert not p.exists()


class TestResolveAttachedControllerPolicyUuid:
    """Tests for reading the twin's currently-attached controller policy UUID."""

    def test_reads_uuid_from_raw_twin_schema(self):
        """Reads controller_policy_uuid off client.twins.get_raw()'s TwinSchema
        — NOT off client.twin() (a Twin handle whose __getattr__ raises for
        non-sensor attrs, silently yielding None). See
        _resolve_attached_controller_policy_uuid docstring."""
        client = MagicMock()
        twin_data = MagicMock()
        twin_data.controller_policy_uuid = "pol-9"
        client.twins.get_raw.return_value = twin_data
        assert (
            main_module._resolve_attached_controller_policy_uuid(client, "t-1")
            == "pol-9"
        )
        client.twins.get_raw.assert_called_once_with("t-1")

    def test_returns_none_when_backend_reports_no_controller(self):
        client = MagicMock()
        twin_data = MagicMock()
        twin_data.controller_policy_uuid = None
        client.twins.get_raw.return_value = twin_data
        assert (
            main_module._resolve_attached_controller_policy_uuid(client, "t-1")
            is None
        )

    def test_falls_back_to_twin_json_when_backend_unreachable(self):
        client = MagicMock()
        client.twins.get_raw.side_effect = RuntimeError("offline")
        with patch.object(
            main_module,
            "_load_primary_robot_twin",
            return_value={"controller_policy_uuid": "pol-json"},
        ):
            assert (
                main_module._resolve_attached_controller_policy_uuid(client, "t-1")
                == "pol-json"
            )


class TestResolveAssignedControllerType:
    """Startup controller-type resolution drives teleop vs remoteop at boot.

    Regression guard: this must read the backend TwinSchema (get_raw) — reading
    controller_policy off the client.twin() handle always yielded None, so the
    startup op was always None (calibrate both, never start the op)."""

    def _twin(self, *, uuid, metadata):
        twin_data = MagicMock()
        twin_data.controller_policy_uuid = uuid
        twin_data.metadata = metadata
        return twin_data

    def test_type_from_metadata_mirror(self):
        client = MagicMock()
        client.twins.get_raw.return_value = self._twin(
            uuid="pol-1", metadata={"controller_type": "LocalOp"}
        )
        assert (
            main_module._resolve_assigned_controller_type(client, "t-1") == "localop"
        )
        client.api.src_app_api_controller_policies_get_controller_policy.assert_not_called()

    def test_type_fetched_from_policy_when_mirror_absent(self):
        client = MagicMock()
        client.twins.get_raw.return_value = self._twin(uuid="pol-1", metadata={})
        policy = MagicMock()
        policy.controller_type = "teleop"
        client.api.src_app_api_controller_policies_get_controller_policy.return_value = (
            policy
        )
        assert (
            main_module._resolve_assigned_controller_type(client, "t-1") == "teleop"
        )
        client.api.src_app_api_controller_policies_get_controller_policy.assert_called_once_with(
            "pol-1"
        )

    def test_empty_string_when_attached_but_type_unresolvable(self):
        client = MagicMock()
        client.twins.get_raw.return_value = self._twin(uuid="pol-1", metadata={})
        client.api.src_app_api_controller_policies_get_controller_policy.side_effect = (
            RuntimeError("boom")
        )
        assert main_module._resolve_assigned_controller_type(client, "t-1") == ""

    def test_none_when_no_controller_attached(self):
        client = MagicMock()
        client.twins.get_raw.return_value = self._twin(uuid=None, metadata={})
        assert main_module._resolve_assigned_controller_type(client, "t-1") is None

    def test_offline_falls_back_to_twin_json(self):
        client = MagicMock()
        client.twins.get_raw.side_effect = RuntimeError("offline")
        with patch.object(
            main_module,
            "_resolve_assigned_controller_type_from_twin_json",
            return_value="localop",
        ):
            assert (
                main_module._resolve_assigned_controller_type(client, "t-1")
                == "localop"
            )

    def test_recovery_command_localop_is_teleoperate(self):
        client = MagicMock()
        with patch.object(
            main_module, "_resolve_assigned_controller_type", return_value="localop"
        ):
            assert (
                main_module._resolve_assigned_controller_recovery_command(client, "t-1")
                == "teleoperate"
            )

    def test_recovery_command_teleop_is_remoteoperate(self):
        client = MagicMock()
        with patch.object(
            main_module, "_resolve_assigned_controller_type", return_value="teleop"
        ):
            assert (
                main_module._resolve_assigned_controller_recovery_command(client, "t-1")
                == "remoteoperate"
            )

    def test_recovery_command_none_when_no_controller(self):
        client = MagicMock()
        with patch.object(
            main_module, "_resolve_assigned_controller_type", return_value=None
        ):
            assert (
                main_module._resolve_assigned_controller_recovery_command(client, "t-1")
                is None
            )


class TestTriggerAlertDoesNotPersistReattach:
    """_trigger_alert_and_switch_to_calibration must NOT persist a re-attach.

    Capture happens exclusively from the detach ``controller-changed`` echo
    (see TestControllerChangedCapturesReattach). A boot/trigger-time capture
    was removed because it clobbered the echo-captured value on the leader
    stage with a stale controller — this guards against reintroducing it.
    """

    def teardown_method(self):
        main_module._pending_recovery_command = None

    def test_no_file_written_on_calibration_trigger(self, tmp_path):
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with (
            patch.object(main_module, "_reattach_state_path", return_value=p),
            patch.object(main_module, "_handle_calibration_start"),
        ):
            main_module._trigger_alert_and_switch_to_calibration(
                client,
                "twin-1",
                follower_port="/dev/a",
                device_type="follower",
                recovery_command="remoteoperate",
            )
        assert not p.exists()


class TestMaybeReattach:
    """_maybe_reattach_controller_policy opens the re-attach window."""

    def test_opens_wait_window_when_none_attached(self, tmp_path):
        """Pending + nothing attached -> the 60s wait window opens (no direct PUT)."""
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("t-1", "pol-1")
            with (
                patch.object(
                    main_module,
                    "_resolve_attached_controller_policy_uuid",
                    return_value=None,
                ),
                patch.object(main_module, "_start_reattach_wait") as start_wait,
            ):
                main_module._maybe_reattach_controller_policy(client, "t-1")
        start_wait.assert_called_once_with(client, "t-1", "pol-1")
        client.twins.update.assert_not_called()

    def test_skips_window_but_clears_file_when_already_attached(self, tmp_path):
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("t-1", "pol-1")
            with (
                patch.object(
                    main_module,
                    "_resolve_attached_controller_policy_uuid",
                    return_value="pol-other",
                ),
                patch.object(main_module, "_start_reattach_wait") as start_wait,
            ):
                main_module._maybe_reattach_controller_policy(client, "t-1")
        start_wait.assert_not_called()
        client.twins.update.assert_not_called()
        assert not p.exists()

    def test_noop_when_no_pending_file(self, tmp_path):
        client = MagicMock()
        p = tmp_path / "missing.json"
        with (
            patch.object(main_module, "_reattach_state_path", return_value=p),
            patch.object(main_module, "_start_reattach_wait") as start_wait,
        ):
            main_module._maybe_reattach_controller_policy(client, "t-1")
        start_wait.assert_not_called()
        client.twins.update.assert_not_called()


class TestReattachWindow:
    """The post-calibration wait window and its completion step."""

    def teardown_method(self):
        with main_module._reattach_wait_lock:
            main_module._reattach_wait_active = False

    def test_start_wait_alerts_and_is_idempotent(self):
        """Opening the window fires the 'waiting for a controller' alert once;
        a second call while open is a no-op."""
        client = MagicMock()
        with (
            patch.object(main_module, "_create_status_alert") as status_alert,
            patch.object(main_module.threading, "Thread") as thread_cls,
        ):
            main_module._start_reattach_wait(client, "t-1", "pol-1")
            main_module._start_reattach_wait(client, "t-1", "pol-1")
        status_alert.assert_called_once()
        assert (
            status_alert.call_args.kwargs["alert_type"]
            == "controller_reattach_pending"
        )
        thread_cls.assert_called_once()
        thread_cls.return_value.start.assert_called_once()

    def test_complete_reattaches_alerts_and_clears(self, tmp_path):
        """Window expiry with nothing attached -> PUT + auto-attach alert + clear."""
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("t-1", "pol-1")
            with (
                patch.object(
                    main_module,
                    "_resolve_attached_controller_policy_uuid",
                    return_value=None,
                ),
                patch.object(main_module, "_resolve_alert_by_uuid") as resolve_alert,
                patch.object(main_module, "_create_status_alert") as status_alert,
            ):
                main_module._complete_pending_reattach(client, "t-1", "alert-1")
        client.twins.update.assert_called_once_with(
            "t-1", controller_policy_uuid="pol-1"
        )
        resolve_alert.assert_called_once_with(client, "t-1", "alert-1")
        assert (
            status_alert.call_args.kwargs["alert_type"] == "controller_auto_reattached"
        )
        assert not p.exists()

    def test_complete_defers_to_controller_attached_during_window(self, tmp_path):
        """A controller-changed that landed during the window wins."""
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("t-1", "pol-1")
            with (
                patch.object(
                    main_module,
                    "_resolve_attached_controller_policy_uuid",
                    return_value="pol-new",
                ),
                patch.object(main_module, "_resolve_alert_by_uuid"),
                patch.object(main_module, "_create_status_alert") as status_alert,
            ):
                main_module._complete_pending_reattach(client, "t-1", "alert-1")
        client.twins.update.assert_not_called()
        status_alert.assert_not_called()
        assert not p.exists()

    def test_complete_leaves_file_on_rest_failure(self, tmp_path):
        client = MagicMock()
        client.twins.update.side_effect = RuntimeError("offline")
        p = tmp_path / "reattach.json"
        with patch.object(main_module, "_reattach_state_path", return_value=p):
            main_module._save_pending_reattach("t-1", "pol-1")
            with (
                patch.object(
                    main_module,
                    "_resolve_attached_controller_policy_uuid",
                    return_value=None,
                ),
                patch.object(main_module, "_resolve_alert_by_uuid"),
                patch.object(main_module, "_create_status_alert"),
            ):
                main_module._complete_pending_reattach(client, "t-1", "alert-1")
            assert main_module._load_pending_reattach("t-1") == "pol-1"


class TestEvaluateAndDriveReattach:
    """_evaluate_and_drive re-attaches only when both follower + leader ready."""

    def teardown_method(self):
        main_module._pending_recovery_command = None

    def _cfg(self):
        return {
            "follower_port": "/dev/a",
            "leader_port": "/dev/b",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }

    def test_reattaches_when_both_calibrated(self):
        client = MagicMock()
        main_module._pending_recovery_command = "teleoperate"
        with (
            patch.object(main_module, "_is_calibration_running", return_value=False),
            patch.object(main_module, "_get_hardware_config", return_value=self._cfg()),
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=True),
            patch.object(
                main_module, "_is_control_operation_running", return_value=False
            ),
            patch.object(main_module, "start_teleoperate"),
            patch.object(
                main_module, "_maybe_reattach_controller_policy"
            ) as mock_reattach,
        ):
            main_module._evaluate_and_drive(client, "t-1")
        mock_reattach.assert_called_once_with(client, "t-1")

    def test_does_not_reattach_when_leader_missing(self):
        client = MagicMock()
        main_module._pending_recovery_command = "remoteoperate"
        with (
            patch.object(main_module, "_is_calibration_running", return_value=False),
            patch.object(main_module, "_get_hardware_config", return_value=self._cfg()),
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=False),
            patch.object(
                main_module, "_is_control_operation_running", return_value=False
            ),
            patch.object(main_module, "start_remoteoperate"),
            patch.object(
                main_module, "_maybe_reattach_controller_policy"
            ) as mock_reattach,
        ):
            main_module._evaluate_and_drive(client, "t-1")
        mock_reattach.assert_not_called()

    def test_reattaches_in_follower_only_setup(self):
        """No leader connected: the leader calibration requirement is waived,
        so a calibrated follower alone re-opens the re-attach window."""
        client = MagicMock()
        cfg = self._cfg() | {"leader_port": None}
        main_module._pending_recovery_command = "remoteoperate"
        main_module._device_notices_sent.add("follower_only")  # silence notice
        try:
            with (
                patch.object(main_module, "_is_calibration_running", return_value=False),
                patch.object(main_module, "_get_hardware_config", return_value=cfg),
                patch.object(main_module, "_is_follower_calibrated", return_value=True),
                patch.object(
                    main_module, "_is_leader_calibrated", return_value=False
                ),
                patch.object(
                    main_module, "_is_control_operation_running", return_value=False
                ),
                patch.object(main_module, "start_remoteoperate"),
                patch.object(
                    main_module, "_maybe_reattach_controller_policy"
                ) as mock_reattach,
            ):
                main_module._evaluate_and_drive(client, "t-1")
        finally:
            main_module._device_notices_sent.clear()
        mock_reattach.assert_called_once_with(client, "t-1")


class TestEvaluateAndDriveControllerGate:
    """The driver never starts an operation on its own initiative: after
    calibration the controller is still detached, and starting the pinned op
    would run the robot with no controller and then bounce it when the
    re-attach fires controller-changed. Only an attached controller (via the
    backend's controller-changed) may start teleoperate/remoteoperate."""

    def teardown_method(self):
        main_module._pending_recovery_command = None

    def _cfg(self):
        return {
            "follower_port": "/dev/a",
            "leader_port": "/dev/b",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }

    def _evaluate(self, client, attached_uuid):
        with (
            patch.object(main_module, "_is_calibration_running", return_value=False),
            patch.object(main_module, "_get_hardware_config", return_value=self._cfg()),
            patch.object(main_module, "_is_follower_calibrated", return_value=True),
            patch.object(main_module, "_is_leader_calibrated", return_value=True),
            patch.object(
                main_module, "_is_control_operation_running", return_value=False
            ),
            patch.object(main_module, "_maybe_reattach_controller_policy"),
            patch.object(
                main_module,
                "_resolve_attached_controller_policy_uuid",
                return_value=attached_uuid,
            ),
            patch.object(main_module, "start_teleoperate") as start_tele,
            patch.object(main_module, "start_remoteoperate") as start_remote,
        ):
            main_module._evaluate_and_drive(client, "t-1")
        return start_tele, start_remote

    def test_stays_idle_when_no_controller_attached(self):
        """Pinned op + all calibrated + nothing attached -> no op starts;
        the controller-changed from the (re-)attach will start it."""
        main_module._pending_recovery_command = "remoteoperate"
        start_tele, start_remote = self._evaluate(MagicMock(), attached_uuid=None)
        start_tele.assert_not_called()
        start_remote.assert_not_called()

    def test_starts_op_when_controller_attached(self):
        """With a controller attached, the pinned op resumes normally."""
        main_module._pending_recovery_command = "remoteoperate"
        start_tele, start_remote = self._evaluate(MagicMock(), attached_uuid="pol-1")
        start_remote.assert_called_once()
        start_tele.assert_not_called()

    def test_teleoperate_also_gated(self):
        main_module._pending_recovery_command = "teleoperate"
        start_tele, start_remote = self._evaluate(MagicMock(), attached_uuid=None)
        start_tele.assert_not_called()
        start_remote.assert_not_called()


class TestDevicePresence:
    """Follower-only is the supported minimum; alerts fire once per run."""

    def teardown_method(self):
        main_module._device_notices_sent.clear()
        main_module._pending_recovery_command = None

    def test_no_follower_alerts_once_and_reports_false(self):
        client = MagicMock()
        with patch.object(main_module, "_create_error_alert") as error_alert:
            assert (
                main_module._check_device_presence(client, "t-1", None, "/dev/b")
                is False
            )
            assert (
                main_module._check_device_presence(client, "t-1", None, "/dev/b")
                is False
            )
        error_alert.assert_called_once()
        assert (
            error_alert.call_args.kwargs["error_type"] == "no_follower_detected"
        )

    def test_follower_only_alerts_once_and_reports_true(self):
        client = MagicMock()
        with patch.object(main_module, "_create_status_alert") as status_alert:
            assert (
                main_module._check_device_presence(client, "t-1", "/dev/a", None)
                is True
            )
            assert (
                main_module._check_device_presence(client, "t-1", "/dev/a", None)
                is True
            )
        status_alert.assert_called_once()
        assert (
            status_alert.call_args.kwargs["alert_type"] == "follower_only_mode"
        )

    def test_both_arms_no_alerts(self):
        client = MagicMock()
        with (
            patch.object(main_module, "_create_error_alert") as error_alert,
            patch.object(main_module, "_create_status_alert") as status_alert,
        ):
            assert (
                main_module._check_device_presence(
                    client, "t-1", "/dev/a", "/dev/b"
                )
                is True
            )
        error_alert.assert_not_called()
        status_alert.assert_not_called()

    def test_evaluate_and_drive_idles_without_follower(self):
        """No follower port -> evaluate-and-drive alerts and stays idle
        (no calibration trigger, no op start)."""
        client = MagicMock()
        main_module._pending_recovery_command = "remoteoperate"
        cfg = {
            "follower_port": None,
            "leader_port": None,
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with (
            patch.object(main_module, "_is_calibration_running", return_value=False),
            patch.object(main_module, "_get_hardware_config", return_value=cfg),
            patch.object(main_module, "_create_error_alert") as error_alert,
            patch.object(
                main_module, "_trigger_alert_and_switch_to_calibration"
            ) as trigger_cal,
            patch.object(main_module, "start_remoteoperate") as start_op,
        ):
            main_module._evaluate_and_drive(client, "t-1")
        error_alert.assert_called_once()
        trigger_cal.assert_not_called()
        start_op.assert_not_called()


class TestControllerChangedCapturesReattach:
    """The detach echo's previous_controller is the race-free reattach source."""

    def teardown_method(self):
        main_module._pending_recovery_command = None
        main_module._calibration_flow_step = None

    def test_detach_echo_during_calibration_saves_reattach(self, tmp_path):
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with (
            patch.object(main_module, "_reattach_state_path", return_value=p),
            patch.object(main_module, "_is_calibration_running", return_value=True),
        ):
            main_module._handle_controller_changed(
                client,
                "twin-1",
                {
                    "command": "controller-changed",
                    "previous_controller": {
                        "uuid": "pol-kbd",
                        "controller_type": "teleop",
                    },
                    "controller": None,
                },
            )
            assert main_module._load_pending_reattach("twin-1") == "pol-kbd"

    def test_detach_echo_without_previous_uuid_no_save(self, tmp_path):
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with (
            patch.object(main_module, "_reattach_state_path", return_value=p),
            patch.object(main_module, "_is_calibration_running", return_value=True),
        ):
            main_module._handle_controller_changed(
                client,
                "twin-1",
                {"previous_controller": None, "controller": None},
            )
        assert not p.exists()


class TestRetryUntilDeadline:
    """Tests for the startup connectivity grace-window retry helper."""

    def test_returns_value_on_first_success_without_sleeping(self):
        """A call that succeeds immediately returns its value and never sleeps."""
        with (
            patch.object(main_module.time, "monotonic", return_value=0.0),
            patch.object(main_module.time, "sleep") as sleeper,
        ):
            result = main_module._retry_until_deadline(
                lambda: "ok", deadline=100.0, description="thing"
            )
        assert result == "ok"
        sleeper.assert_not_called()

    def test_retries_until_success_within_window(self):
        """Transient failures before the deadline are retried, then the value returns."""
        calls = {"n": 0}

        def flaky():
            calls["n"] += 1
            if calls["n"] < 3:
                raise RuntimeError("not yet")
            return "ok"

        with (
            patch.object(main_module.time, "monotonic", return_value=0.0),
            patch.object(main_module.time, "sleep") as sleeper,
        ):
            result = main_module._retry_until_deadline(
                flaky, deadline=100.0, description="thing"
            )
        assert result == "ok"
        assert calls["n"] == 3
        assert sleeper.call_count == 2

    def test_reraises_last_error_after_deadline(self):
        """Once the shared deadline passes, the last exception is re-raised."""
        clock = {"t": 0.0}

        def advancing_monotonic():
            return clock["t"]

        def advancing_sleep(secs):
            clock["t"] += secs

        def always_fails():
            raise RuntimeError("broker down")

        with (
            patch.object(main_module.time, "monotonic", advancing_monotonic),
            patch.object(main_module.time, "sleep", advancing_sleep),
        ):
            with pytest.raises(RuntimeError, match="broker down"):
                main_module._retry_until_deadline(
                    always_fails, deadline=10.0, description="thing"
                )


class TestEvaluateConnectionWatchdog:
    """Tests for the mid-session MQTT-loss watchdog decision state machine."""

    def test_connected_clears_timer_and_never_trips(self):
        """A live connection resets the outage timer and never trips."""
        since, trip = main_module._evaluate_connection_watchdog(
            connected=True,
            operation_running=True,
            disconnected_since=5.0,
            now=100.0,
            grace_seconds=30.0,
        )
        assert since is None
        assert trip is False

    def test_disconnected_while_idle_never_trips(self):
        """An outage with no active operation starts the timer but never trips."""
        since, trip = main_module._evaluate_connection_watchdog(
            connected=False,
            operation_running=False,
            disconnected_since=None,
            now=100.0,
            grace_seconds=30.0,
        )
        assert since == 100.0
        assert trip is False

        # Even long after the grace window, an idle outage does not trip.
        since2, trip2 = main_module._evaluate_connection_watchdog(
            connected=False,
            operation_running=False,
            disconnected_since=100.0,
            now=1000.0,
            grace_seconds=30.0,
        )
        assert since2 == 100.0
        assert trip2 is False

    def test_disconnected_during_operation_within_grace_does_not_trip(self):
        """During an operation, a brief outage within the grace window does not trip."""
        since, trip = main_module._evaluate_connection_watchdog(
            connected=False,
            operation_running=True,
            disconnected_since=100.0,
            now=120.0,
            grace_seconds=30.0,
        )
        assert since == 100.0
        assert trip is False

    def test_disconnected_during_operation_past_grace_trips(self):
        """During an operation, an outage past the grace window trips the watchdog."""
        since, trip = main_module._evaluate_connection_watchdog(
            connected=False,
            operation_running=True,
            disconnected_since=100.0,
            now=131.0,
            grace_seconds=30.0,
        )
        assert since == 100.0
        assert trip is True

    def test_first_disconnected_tick_seeds_timer(self):
        """The first disconnected observation seeds the outage start time."""
        since, trip = main_module._evaluate_connection_watchdog(
            connected=False,
            operation_running=True,
            disconnected_since=None,
            now=100.0,
            grace_seconds=30.0,
        )
        assert since == 100.0
        assert trip is False


class TestHandleMqttConnectionLost:
    """Safe-stop + controller detach + reconnect-allowance response to MQTT loss."""

    def test_no_reconnect_alerts_detaches_then_requests_shutdown(self):
        """Broker never returns: safe stop, controller detached, no-op alert,
        final shutdown alert, on_trip; returns False."""
        client = MagicMock()
        on_trip = MagicMock()
        with (
            patch.object(main_module, "_stop_current_operation") as stop_op,
            patch.object(
                main_module, "_detach_controller_for_mqtt_loss"
            ) as detach,
            patch.object(main_module, "_create_error_alert") as create_alert,
            patch.object(
                main_module, "_wait_for_mqtt_reconnect", return_value=False
            ),
        ):
            recovered = main_module._handle_mqtt_connection_lost(
                client, "twin-1", on_trip
            )

        assert recovered is False
        stop_op.assert_called_once()
        detach.assert_called_once_with(client, "twin-1")
        error_types = [
            c.kwargs["error_type"] for c in create_alert.call_args_list
        ]
        assert error_types == [
            "mqtt_connection_lost",
            "mqtt_connectivity_shutdown",
        ]
        on_trip.assert_called_once()

    def test_reconnect_within_allowance_alerts_and_resumes(self):
        """Broker returns within the allowance: 'device reconnected' alert +
        re-evaluate, no shutdown; returns True."""
        client = MagicMock()
        on_trip = MagicMock()
        with (
            patch.object(main_module, "_stop_current_operation"),
            patch.object(main_module, "_detach_controller_for_mqtt_loss"),
            patch.object(main_module, "_create_error_alert") as create_alert,
            patch.object(
                main_module, "_wait_for_mqtt_reconnect", return_value=True
            ),
            patch.object(main_module, "_create_status_alert") as status_alert,
            patch.object(main_module, "_evaluate_and_drive") as evaluate,
        ):
            recovered = main_module._handle_mqtt_connection_lost(
                client, "twin-1", on_trip
            )

        assert recovered is True
        assert (
            status_alert.call_args.kwargs["alert_type"] == "edge_mqtt_reconnected"
        )
        evaluate.assert_called_once_with(client, "twin-1")
        on_trip.assert_not_called()
        # Only the initial no-op alert, no shutdown alert.
        error_types = [
            c.kwargs["error_type"] for c in create_alert.call_args_list
        ]
        assert error_types == ["mqtt_connection_lost"]

    def test_still_requests_shutdown_when_alert_fails(self):
        """A failing alert must not prevent the safe stop or the shutdown request."""
        client = MagicMock()
        on_trip = MagicMock()
        with (
            patch.object(main_module, "_stop_current_operation") as stop_op,
            patch.object(main_module, "_detach_controller_for_mqtt_loss"),
            patch.object(
                main_module,
                "_create_error_alert",
                side_effect=RuntimeError("backend unreachable"),
            ),
            patch.object(
                main_module, "_wait_for_mqtt_reconnect", return_value=False
            ),
        ):
            recovered = main_module._handle_mqtt_connection_lost(
                client, "twin-1", on_trip
            )

        assert recovered is False
        stop_op.assert_called_once()
        on_trip.assert_called_once()

    def test_detach_saves_pending_then_puts_none(self, tmp_path):
        """_detach_controller_for_mqtt_loss persists the attached policy for
        later re-attach, then detaches it via REST."""
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with (
            patch.object(main_module, "_reattach_state_path", return_value=p),
            patch.object(
                main_module,
                "_resolve_attached_controller_policy_uuid",
                return_value="pol-1",
            ),
        ):
            main_module._detach_controller_for_mqtt_loss(client, "twin-1")
            assert main_module._load_pending_reattach("twin-1") == "pol-1"
        client.twins.update.assert_called_once_with(
            "twin-1", controller_policy_uuid=None
        )

    def test_detach_noop_without_attached_controller(self, tmp_path):
        """No controller attached (e.g. already detached for calibration) ->
        nothing saved, no REST call, existing pending file untouched."""
        client = MagicMock()
        p = tmp_path / "reattach.json"
        with (
            patch.object(main_module, "_reattach_state_path", return_value=p),
            patch.object(
                main_module,
                "_resolve_attached_controller_policy_uuid",
                return_value=None,
            ),
        ):
            main_module._save_pending_reattach("twin-1", "pol-old")
            main_module._detach_controller_for_mqtt_loss(client, "twin-1")
            assert main_module._load_pending_reattach("twin-1") == "pol-old"
        client.twins.update.assert_not_called()


class TestWaitForMqttReconnect:
    """The reconnect-allowance poll loop."""

    def test_returns_true_when_already_connected(self):
        client = MagicMock()
        client.mqtt.connected = True
        assert (
            main_module._wait_for_mqtt_reconnect(client, allowance_seconds=5.0)
            is True
        )

    def test_returns_false_when_allowance_expired(self):
        client = MagicMock()
        client.mqtt.connected = False
        assert (
            main_module._wait_for_mqtt_reconnect(
                client, allowance_seconds=0.0, poll_interval=0.0
            )
            is False
        )

    def test_returns_false_when_stop_event_set(self):
        client = MagicMock()
        client.mqtt.connected = False
        stop = main_module.threading.Event()
        stop.set()
        assert (
            main_module._wait_for_mqtt_reconnect(
                client,
                allowance_seconds=60.0,
                poll_interval=0.0,
                stop_event=stop,
            )
            is False
        )


class _FakeStop:
    """A stop-event stand-in whose ``wait`` returns False for the first
    ``false_ticks`` calls, then True — bounding the watchdog loop in tests."""

    def __init__(self, false_ticks: int):
        self._false_ticks = false_ticks
        self.calls = 0

    def wait(self, _timeout):
        self.calls += 1
        return self.calls > self._false_ticks


class TestConnectionWatchdogLoop:
    """Regression coverage for the watchdog thread loop (glue over the core)."""

    def test_trips_on_sustained_outage_during_operation(self):
        """A disconnect that outlasts the grace during an operation fires the handler."""
        client = MagicMock()
        client.mqtt.connected = False
        on_trip = MagicMock()
        stop = _FakeStop(false_ticks=5)

        with (
            patch.object(main_module, "_is_any_operation_running", return_value=True),
            patch.object(
                main_module, "_handle_mqtt_connection_lost", return_value=False
            ) as handler,
            patch.object(main_module.time, "monotonic", side_effect=[0.0, 100.0, 100.0]),
        ):
            main_module._connection_watchdog_loop(
                client,
                "twin-1",
                stop,
                on_trip,
                grace_seconds=30.0,
                poll_interval=0.0,
            )

        handler.assert_called_once_with(client, "twin-1", on_trip, stop_event=stop)

    def test_trips_during_calibration_too(self):
        """Calibration counts as an active operation for the watchdog: a hung
        calibration holds torque and can only be advanced/cancelled over MQTT."""
        client = MagicMock()
        client.mqtt.connected = False
        on_trip = MagicMock()

        with (
            patch.object(main_module, "_is_control_operation_running", return_value=False),
            patch.object(main_module, "_is_calibration_running", return_value=True),
            patch.object(
                main_module, "_handle_mqtt_connection_lost", return_value=False
            ) as handler,
            patch.object(main_module.time, "monotonic", side_effect=[0.0, 100.0, 100.0]),
        ):
            main_module._connection_watchdog_loop(
                client,
                "twin-1",
                _FakeStop(false_ticks=5),
                on_trip,
                grace_seconds=30.0,
                poll_interval=0.0,
            )

        handler.assert_called_once()

    def test_keeps_monitoring_after_recovered_trip(self):
        """A trip whose handler reports recovery resets the outage timer and
        the loop keeps running until stop."""
        client = MagicMock()
        client.mqtt.connected = False
        on_trip = MagicMock()

        with (
            patch.object(main_module, "_is_any_operation_running", return_value=True),
            patch.object(
                main_module, "_handle_mqtt_connection_lost", return_value=True
            ) as handler,
            patch.object(
                main_module.time,
                "monotonic",
                side_effect=[0.0, 100.0, 100.0, 100.0, 100.0, 100.0],
            ),
        ):
            main_module._connection_watchdog_loop(
                client,
                "twin-1",
                _FakeStop(false_ticks=3),
                on_trip,
                grace_seconds=30.0,
                poll_interval=0.0,
            )

        # Tick 1 seeds the timer, tick 2 trips (recovered), tick 3 re-seeds —
        # the loop did not exit after the recovered trip.
        handler.assert_called_once()
        on_trip.assert_not_called()

    def test_exits_on_stop_without_tripping_when_connected(self):
        """A healthy connection never fires the handler and the loop ends on stop."""
        client = MagicMock()
        client.mqtt.connected = True
        on_trip = MagicMock()

        with (
            patch.object(main_module, "_is_any_operation_running", return_value=True),
            patch.object(main_module, "_handle_mqtt_connection_lost") as handler,
            patch.object(main_module.time, "monotonic", return_value=0.0),
        ):
            main_module._connection_watchdog_loop(
                client,
                "twin-1",
                _FakeStop(false_ticks=3),
                on_trip,
                grace_seconds=30.0,
                poll_interval=0.0,
            )

        handler.assert_not_called()
        on_trip.assert_not_called()
