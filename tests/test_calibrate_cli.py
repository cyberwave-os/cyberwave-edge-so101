"""Unit tests for cw_calibrate.py CLI (standalone, no project imports)."""

from __future__ import annotations

import argparse


def _make_calibrate_parser():
    """Replicate cw_calibrate's argument parser for testing."""
    p = argparse.ArgumentParser()
    p.add_argument("--type", required=True, choices=["leader", "follower"])
    p.add_argument("--port")
    p.add_argument("--id", default=None)
    p.add_argument("--alert-uuid")
    p.add_argument("--twin-uuid")
    return p


class TestCalibrateArgparse:
    """Tests for cw_calibrate argument parser (--alert-uuid, --twin-uuid)."""

    def test_has_alert_uuid_and_twin_uuid_args(self):
        """Script accepts --alert-uuid and --twin-uuid (as used by main.py)."""
        parser = _make_calibrate_parser()
        argv = [
            "--type", "follower",
            "--port", "/dev/ttyACM0",
            "--id", "follower1",
            "--alert-uuid", "alert-123",
            "--twin-uuid", "twin-456",
        ]
        args = parser.parse_args(argv)
        assert args.alert_uuid == "alert-123"
        assert args.twin_uuid == "twin-456"

    def test_alert_uuid_optional(self):
        """--alert-uuid and --twin-uuid are optional (standalone CLI use)."""
        parser = _make_calibrate_parser()
        args = parser.parse_args([
            "--type", "follower",
            "--port", "/dev/ttyACM0",
        ])
        assert args.alert_uuid is None
        assert args.twin_uuid is None
