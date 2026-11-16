"""Unit tests for camera discovery compatibility metadata."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils import device_utils


class TestAnnotateDiscoveredDevices:
    """Tests for compatibility metadata on discovered devices."""

    def test_marks_virtual_rpi_devices_incompatible(self):
        """Raspberry Pi codec and ISP nodes stay visible but are never auto-selected."""
        devices = [
            {
                "card": "bcm2835-isp",
                "driver": "bcm2835-isp",
                "primary_path": "/dev/video13",
                "index": 13,
                "paths": ["/dev/video13"],
            }
        ]

        annotated = device_utils.annotate_discovered_devices(devices)

        assert annotated[0]["is_compatible"] is False
        assert annotated[0]["compatibility_reason"] == "excluded_virtual_device"
        assert "last_checked_at" in annotated[0]

    def test_probes_cv2_devices_and_records_success(self):
        """Normal V4L2 cameras get probe results written into metadata."""
        devices = [
            {
                "card": "USB2.0_CAM1",
                "driver": "uvcvideo",
                "primary_path": "/dev/video0",
                "index": 0,
                "paths": ["/dev/video0"],
            }
        ]

        with patch.object(device_utils, "_probe_cv2_camera", return_value=(True, "ok")) as mock_probe:
            annotated = device_utils.annotate_discovered_devices(devices)

        mock_probe.assert_called_once_with("/dev/video0")
        assert annotated[0]["is_compatible"] is True
        assert annotated[0]["compatibility_reason"] == "ok"

    def test_marks_rpivid_decoder_incompatible(self):
        """``rpivid`` is the Pi stateless video decoder, not a camera."""
        devices = [
            {
                "card": "rpivid",
                "driver": "rpivid",
                "primary_path": "/dev/video19",
                "index": 19,
                "paths": ["/dev/video19"],
            }
        ]

        annotated = device_utils.annotate_discovered_devices(devices)

        assert annotated[0]["is_compatible"] is False
        assert annotated[0]["compatibility_reason"] == "excluded_virtual_device"


class TestProbeCv2Camera:
    """The probe must use the same backend that streaming will use."""

    def test_opens_local_device_with_explicit_v4l2_backend(self):
        """``/dev/video*`` is probed via CAP_V4L2, never OpenCV's FFmpeg fallback.

        ``cv2.VideoCapture(path)`` with no backend lets OpenCV pick, and for a
        string path it reaches for FFmpeg's libavdevice V4L2 demuxer first. That
        emits ``ioctl(VIDIOC_QBUF): Bad file descriptor`` on non-capture nodes and
        tests a backend the SDK never streams through.
        """
        fake_cv2 = MagicMock()
        fake_cv2.CAP_V4L2 = 200
        cap = fake_cv2.VideoCapture.return_value
        cap.isOpened.return_value = True
        cap.read.return_value = (True, object())

        with patch.object(device_utils.importlib, "import_module", return_value=fake_cv2):
            ok, reason = device_utils._probe_cv2_camera("/dev/video6")

        assert (ok, reason) == (True, "ok")
        fake_cv2.VideoCapture.assert_called_once_with("/dev/video6", 200)

    def test_non_device_source_keeps_default_backend(self):
        """A URL source has no V4L2 node, so let OpenCV choose."""
        fake_cv2 = MagicMock()
        fake_cv2.CAP_V4L2 = 200
        cap = fake_cv2.VideoCapture.return_value
        cap.isOpened.return_value = True
        cap.read.return_value = (True, object())

        with patch.object(device_utils.importlib, "import_module", return_value=fake_cv2):
            device_utils._probe_cv2_camera("http://10.0.0.5:8080/stream")

        fake_cv2.VideoCapture.assert_called_once_with("http://10.0.0.5:8080/stream")
