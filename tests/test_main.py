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


class TestCameraAutodiscoveryCv2Pool:
    """Tests for CV2 USB pool assignment when no explicit camera mapping exists."""

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
                "camera_type": "cv2",
                "camera_id": "/dev/video0",
                "video_device": "/dev/video0",
                "enable_depth": False,
                "used_default": True,
            }
        ]


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
        so101 = So101Config.from_dict(
            {
                "twin_uuid": "robot-twin-1",
                "cameras": [
                    {
                        "twin_uuid": "camera-twin-1",
                        "camera_type": "realsense",
                        "camera_id": "/dev/video6",
                        "resolution": "VGA",
                        "fps": 15,
                        "enable_depth": True,
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
        assert overrides["camera_name"] == "default"


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
