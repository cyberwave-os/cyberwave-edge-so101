"""Tests for typed SO101 setup.json config (scripts.cw_setup)."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as main_module  # noqa: E402
from scripts import cw_setup  # noqa: E402
from so101.camera import CameraConfig  # noqa: E402


class TestSo101ConfigModel:
    def test_from_dict_skips_rows_without_twin_uuid(self):
        raw = {
            "twin_uuid": "robot-1",
            "cameras": [
                {"camera_type": "cv2", "camera_id": 0},
                {"twin_uuid": "cam-1", "camera_type": "cv2", "camera_id": 1},
            ],
        }
        cfg = cw_setup.So101Config.from_dict(raw)
        assert len(cfg.cameras) == 1
        assert cfg.cameras[0].twin_uuid == "cam-1"

    def test_to_dict_from_dict_roundtrip_core_fields(self):
        raw = {
            "twin_uuid": "t-1",
            "leader_port": "/a",
            "follower_port": "/b",
            "max_relative_target": 0.5,
            "camera_only": True,
            "mute_temperature_notifications": True,
            "cameras": [
                {
                    "twin_uuid": "c1",
                    "camera_type": "realsense",
                    "camera_id": 0,
                    "enable_depth": True,
                    "depth_fps": 15,
                    "depth_resolution": [640, 480],
                    "depth_publish_interval": 10,
                }
            ],
        }
        cfg = cw_setup.So101Config.from_dict(raw)
        again = cw_setup.So101Config.from_dict(cfg.to_dict())
        assert again == cfg

    def test_to_hardware_dict_matches_edge_normalization(self):
        cam = CameraConfig(
            twin_uuid="c1",
            camera_type="cv2",
            camera_id="/dev/video0",
            resolution=[640, 480],
            fps=24,
        )
        d = cam.to_hardware_dict()
        assert d["twin_uuid"] == "c1"
        assert d["camera_id"] == "/dev/video0"
        assert d["resolution"] == [640, 480]
        assert d["fps"] == 24

    def test_load_so101_config_for_robot_twin_mismatch_returns_empty_cameras(self):
        raw = {"twin_uuid": "other", "cameras": [{"twin_uuid": "x", "camera_id": 0}]}
        fake_path = MagicMock()

        def fake_load(p=None):
            if p is fake_path:
                return raw
            return {}

        with patch.object(cw_setup, "load_setup_config", fake_load):
            cfg = cw_setup.load_so101_config_for_robot_twin("expected", path=fake_path)
        assert cfg.cameras == []


class TestCameraConfigBridge:
    """Setup row :class:`CameraConfig` <-> densified runtime config."""

    def test_setup_row_roundtrips_via_camera_config(self):
        row = CameraConfig(
            twin_uuid="cam-1",
            camera_type="cv2",
            camera_id="/dev/video0",
            resolution=[800, 600],
            fps=20,
            fourcc="MJPG",
            keyframe_interval=45,
        )
        cc = row.to_runtime_camera_config()
        assert isinstance(cc, CameraConfig)
        assert cc.twin_uuid is None
        assert cc.camera_id == "/dev/video0"
        assert cc.resolution == [800, 600]
        assert cc.fps == 20
        back = CameraConfig.for_setup_row_from_runtime_config(
            "cam-1",
            cc,
            fourcc="MJPG",
            keyframe_interval=45,
        )
        assert back.to_setup_camera_dict() == row.to_setup_camera_dict()

    def test_camera_config_to_setup_row_helper(self):
        cfg = CameraConfig(
            camera_type="cv2",
            camera_id=2,
            fps=15,
            resolution=[640, 480],
        )
        row = cfg.to_setup_camera_row("twin-a", fourcc="YUYV")
        assert row["twin_uuid"] == "twin-a"
        assert row["camera_id"] == 2
        assert row["fourcc"] == "YUYV"


class TestMaterializeCameraEntries:
    def test_cli_materialize_builds_twin_id_only(self):
        client = MagicMock()
        client.twin.return_value = MagicMock(name="Twin")

        cams = [
            CameraConfig(twin_uuid="u1", camera_type="cv2", camera_id=0),
            CameraConfig(
                twin_uuid="u2",
                camera_type="realsense",
                camera_id=1,
                enable_depth=True,
                depth_resolution=[640, 480],
            ),
        ]
        entries = cw_setup.materialize_camera_entries_for_cli(client, cams)
        assert len(entries) == 2
        for call in client.twin.call_args_list:
            assert "asset_key" not in call.kwargs
            assert "twin_id" in call.kwargs
        assert entries[0]["camera_id"] == 0
        assert entries[1]["enable_depth"] is True

    def test_edge_materialize_uses_asset_key_and_default_resolution(self):
        client = MagicMock()
        client.twin.return_value = MagicMock()

        cams = [CameraConfig(twin_uuid="u1", camera_type="cv2", camera_id=0)]
        entries = cw_setup.materialize_camera_entries_for_edge_operation(client, cams)
        assert len(entries) == 1
        client.twin.assert_called_once()
        assert client.twin.call_args.kwargs["asset_key"] == "cyberwave/standard-cam"
        assert entries[0]["fps"] == 30
        assert entries[0]["camera_resolution"] is not None


class TestMaxCamerasChildPath:
    def test_discover_child_path_stops_at_max_cameras(self):
        discovered = [
            {
                "card": f"USB{i}",
                "primary_path": f"/dev/video{i}",
                "index": i,
                "is_compatible": True,
            }
            for i in range(4)
        ]
        twins = [
            {"uuid": f"c{i}", "metadata": {}, "asset": {"registry_id": "x/standard-cam"}}
            for i in range(4)
        ]
        with patch.object(
            main_module, "_load_discovered_devices", return_value=(discovered, 0)
        ), patch.object(
            main_module,
            "_parse_child_camera_twin_uuids_from_env",
            return_value=["c0", "c1", "c2", "c3"],
        ), patch.object(main_module, "_load_all_twin_jsons", return_value=twins), patch.object(
            main_module, "_resolve_camera_device_for_twin", return_value=None
        ), patch.object(main_module, "_load_edge_fingerprint", return_value=None):
            cameras = main_module._discover_cameras_for_so101("robot-parent")

        assert len(cameras) == main_module.MAX_CAMERAS
        assert [c["twin_uuid"] for c in cameras] == ["c0", "c1", "c2"]
