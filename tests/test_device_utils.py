"""Unit tests for camera discovery compatibility metadata."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

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
