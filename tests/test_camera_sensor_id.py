"""The twin's declared sensors are the single source of the stream sensor key."""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.utils import resolve_camera_sensor_id


def _twin(sensors: list | None) -> SimpleNamespace:
    capabilities = {} if sensors is None else {"sensors": sensors}
    return SimpleNamespace(uuid="twin-1", capabilities=capabilities)


class TestResolveCameraSensorId:
    """Never invent a sensor key — read the one the twin declares."""

    def test_uses_first_declared_sensor(self):
        """A RealSense declares ``color_camera`` first, then ``depth_camera``."""
        twin = _twin([{"id": "color_camera"}, {"id": "depth_camera"}])

        assert resolve_camera_sensor_id(twin) == "color_camera"

    def test_uses_declared_sensor_for_plain_webcam(self):
        twin = _twin([{"id": "color_camera", "type": "camera"}])

        assert resolve_camera_sensor_id(twin) == "color_camera"

    def test_falls_back_to_default_when_twin_declares_nothing(self):
        """Matches the SDK's own fallback so both sides agree."""
        assert resolve_camera_sensor_id(_twin([])) == "default"
        assert resolve_camera_sensor_id(_twin(None)) == "default"

    def test_tolerates_malformed_sensor_entries(self):
        assert resolve_camera_sensor_id(_twin(["not-a-dict"])) == "default"
        assert resolve_camera_sensor_id(_twin([{"name": "no-id-key"}])) == "default"

    def test_tolerates_twin_without_capabilities(self):
        assert resolve_camera_sensor_id(SimpleNamespace(uuid="t")) == "default"
