"""
Test that SO101 edge node package and submodules can be imported correctly.

Requires conftest.py to mock cyberwave before imports. Run from project root:
    cd cyberwave-edge-so101
    pytest tests/test_imports.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure project root is on path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class TestPackageImports:
    """Test that SO101 package submodules can be imported."""

    def test_import_so101(self):
        """so101 package imports."""
        import so101

        assert hasattr(so101, "SO101Leader")
        assert hasattr(so101, "SO101Follower")
        assert hasattr(so101, "SO101Robot")

    def test_import_motors(self):
        """motors package imports (no cyberwave dependency)."""
        import motors

        assert hasattr(motors, "FeetechMotorsBus")
        assert hasattr(motors, "Motor")
        assert hasattr(motors, "MotorCalibration")

    def test_import_utils_config(self):
        """utils.config imports (no cyberwave dependency)."""
        from utils.config import FollowerConfig, LeaderConfig

        assert FollowerConfig is not None
        assert LeaderConfig is not None

    def test_import_utils_package(self):
        """utils package imports (requires cyberwave mock from conftest)."""
        import utils

        assert hasattr(utils, "StatusTracker")
        assert hasattr(utils, "run_status_logging_thread")
        assert hasattr(utils, "read_temperatures")

    def test_import_scripts_calibrate(self):
        """scripts.cw_calibrate imports."""
        import scripts.cw_calibrate

        assert hasattr(scripts.cw_calibrate, "main")

    def test_import_scripts_setup(self):
        """scripts.cw_setup imports."""
        import scripts.cw_setup

        assert hasattr(scripts.cw_setup, "load_setup_config")


class TestPackageExports:
    """Test that package __all__ exports are available (lazy-loaded)."""

    def test_root_exports_available(self):
        """Root package exports from __all__ are accessible (core exports)."""
        import importlib.util

        init_path = ROOT / "__init__.py"
        spec = importlib.util.spec_from_file_location("so101_lib", init_path)
        assert spec is not None and spec.loader is not None
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Core exports that don't require cyberwave/network
        core_exports = [
            "SO101Leader",
            "SO101Follower",
            "LeaderConfig",
            "FollowerConfig",
            "find_available_ports",
            "find_port",
            "is_port_available",
            "test_device_connection",
            "detect_voltage_rating",
        ]
        for name in core_exports:
            attr = getattr(mod, name, None)
            assert attr is not None, f"Export {name} should be available"

    def test_so101_leader_follower_import(self):
        """SO101Leader and SO101Follower can be imported from so101."""
        from so101.leader import SO101Leader
        from so101.follower import SO101Follower

        assert SO101Leader is not None
        assert SO101Follower is not None
