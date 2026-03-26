"""Pytest configuration and fixtures for SO101 tests."""

from __future__ import annotations

import sys
import types
from enum import Enum

# Install cyberwave mock BEFORE any project imports (main, so101, utils, etc.)
# so that tests can run without the real cyberwave SDK.
if "cyberwave" not in sys.modules:
    _fake = types.ModuleType("cyberwave")
    _fake.Cyberwave = type("Cyberwave", (), {})
    _fake.Twin = type("Twin", (), {})
    _fake.EdgeController = type("EdgeController", (), {})
    sys.modules["cyberwave"] = _fake

    _rest = types.ModuleType("cyberwave.rest")
    _rest.DefaultApi = type("DefaultApi", (), {})
    _rest.ApiClient = type("ApiClient", (), {})
    _rest.Configuration = type("Configuration", (), {})
    sys.modules["cyberwave.rest"] = _rest

    # Resolution mock: so101.camera uses Resolution.VGA, from_size(), closest()
    class _FakeResolution(Enum):
        QVGA = (320, 240)
        VGA = (640, 480)
        SVGA = (800, 600)
        HD = (1280, 720)
        FULL_HD = (1920, 1080)

        @classmethod
        def from_size(cls, width: int, height: int):
            for r in cls:
                if r.value == (width, height):
                    return r
            return None

        @classmethod
        def closest(cls, width: int, height: int):
            return cls.VGA

    _sensor = types.ModuleType("cyberwave.sensor")
    _sensor.Resolution = _FakeResolution
    _sensor.RealSenseConfig = type("RealSenseConfig", (), {})
    _sensor.RealSenseDiscovery = type("RealSenseDiscovery", (), {})
    _sensor.CameraStreamManager = type("CameraStreamManager", (), {})
    sys.modules["cyberwave.sensor"] = _sensor

    _constants = types.ModuleType("cyberwave.constants")
    _constants.SOURCE_TYPE_EDGE_FOLLOWER = "edge_follower"
    _constants.SOURCE_TYPE_EDGE_LEADER = "edge_leader"
    sys.modules["cyberwave.constants"] = _constants

    _utils = types.ModuleType("cyberwave.utils")
    _utils.TimeReference = type("TimeReference", (), {})
    sys.modules["cyberwave.utils"] = _utils
