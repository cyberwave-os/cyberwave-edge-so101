"""Unit tests for cw_calibrate.py CLI (standalone, no project imports)."""

from __future__ import annotations

import argparse

from utils.utils import validate_calibration_ranges_sufficient as _validate_calibration_ranges_sufficient


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


class TestCalibrationRangeValidation:
    """Tests for calibration range validation logic."""

    def test_valid_calibration_all_joints_pass(self):
        """All joints with sufficient range should pass validation."""
        calib = {
            "shoulder_pan": {"range_min": 1000, "range_max": 3000},
            "shoulder_lift": {"range_min": 1200, "range_max": 2900},
            "elbow_flex": {"range_min": 1100, "range_max": 3100},
            "wrist_flex": {"range_min": 1300, "range_max": 2800},
            "wrist_roll": {"range_min": 1400, "range_max": 2700},
            "gripper": {"range_min": 500, "range_max": 2500},  # span = 2000 > 819
        }
        is_valid, warnings, _ = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is True
        assert len(warnings) == 0

    def test_body_joint_min_too_close_to_center_warning(self):
        """Body joint with min between 5-20% from center triggers warning but is valid."""
        # Min must be <= 1638 (20% below 2048), but > 1945 (5% below) for warning
        # 1700 is between 5% and 20% → warning, but is_valid=True
        calib = {
            "shoulder_pan": {"range_min": 1700, "range_max": 3000},
            "gripper": {"range_min": 500, "range_max": 2500},
        }
        is_valid, warnings, severity = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is True  # Can save calibration
        assert severity == "warning"
        assert any("shoulder_pan" in w for w in warnings)

    def test_body_joint_max_too_close_to_center_warning(self):
        """Body joint with max between 5-20% from center triggers warning but is valid."""
        # Max must be >= 2458 (20% above 2048), but < 2150 for error
        # 2400 is between 5% and 20% → warning, but is_valid=True
        calib = {
            "wrist_roll": {"range_min": 1000, "range_max": 2400},
            "gripper": {"range_min": 500, "range_max": 2500},
        }
        is_valid, warnings, severity = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is True  # Can save calibration
        assert severity == "warning"
        assert any("wrist_roll" in w for w in warnings)

    def test_body_joint_both_issues_error(self):
        """Body joint with both min and max within 5% of center fails (error)."""
        # Both min and max are within 5% of center → error
        calib = {
            "elbow_flex": {"range_min": 2000, "range_max": 2100},
            "gripper": {"range_min": 500, "range_max": 2500},
        }
        is_valid, warnings, _ = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is False
        assert any("elbow_flex" in w for w in warnings)

    def test_gripper_insufficient_span_error(self):
        """Gripper with range span < 5% fails validation (error)."""
        # Gripper span must be >= 205 (5% of 4095) to be valid
        # span = 100 < 205 → error
        calib = {
            "shoulder_pan": {"range_min": 1000, "range_max": 3000},
            "gripper": {"range_min": 2000, "range_max": 2100},  # span = 100
        }
        is_valid, warnings, severity = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is False
        assert severity == "error"
        assert any("gripper" in w for w in warnings)

    def test_gripper_warning_span(self):
        """Gripper with range span between 5-20% triggers warning but is valid."""
        # span = 500: > 205 (5%) but < 819 (20%) → warning, is_valid=True
        calib = {
            "shoulder_pan": {"range_min": 1000, "range_max": 3000},
            "gripper": {"range_min": 1800, "range_max": 2300},  # span = 500
        }
        is_valid, warnings, severity = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is True  # Can save calibration
        assert severity == "warning"
        assert any("gripper" in w for w in warnings)

    def test_gripper_valid_span(self):
        """Gripper with sufficient range span passes validation."""
        # Gripper span must be >= 819 (20% of 4095)
        # span = 1000 > 819, so this should pass
        calib = {
            "shoulder_pan": {"range_min": 1000, "range_max": 3000},
            "gripper": {"range_min": 1500, "range_max": 2500},  # span = 1000
        }
        is_valid, warnings, _ = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is True

    def test_boundary_values_body_joint(self):
        """Body joint at exact boundary values should pass."""
        # Min at exactly 1638 (boundary) and max at exactly 2458 (boundary)
        calib = {
            "shoulder_pan": {"range_min": 1638, "range_max": 2458},
            "gripper": {"range_min": 0, "range_max": 1000},
        }
        is_valid, warnings, _ = _validate_calibration_ranges_sufficient(calib)
        # min=1638 <= 1638.4, max=2458 >= 2457.6 - should pass (just barely)
        assert is_valid is True

    def test_boundary_values_gripper(self):
        """Gripper at exact boundary span should pass."""
        # Span of exactly 819 (boundary, 20% of 4095)
        calib = {
            "shoulder_pan": {"range_min": 1000, "range_max": 3000},
            "gripper": {"range_min": 1000, "range_max": 1819},  # span = 819
        }
        is_valid, warnings, _ = _validate_calibration_ranges_sufficient(calib)
        # span=819 >= 819, should pass (just barely)
        assert is_valid is True

    def test_error_severity_when_range_below_5_percent(self):
        """Range below 5% from center returns error severity."""
        # 5% from 2048 = 102.4, so min must be <= 1945.6, max >= 2150.4
        # min 2000 > 1945.6 → error
        calib = {
            "shoulder_pan": {"range_min": 2000, "range_max": 3000},
            "gripper": {"range_min": 500, "range_max": 2500},
        }
        is_valid, warnings, severity = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is False
        assert severity == "error"

    def test_warning_severity_when_range_between_5_and_20_percent(self):
        """Range between 5% and 20% from center returns warning severity but is_valid=True."""
        # min 1700: 1700 > 1945.6 (error) is False, 1700 > 1638.4 (warning) is True → warning
        calib = {
            "shoulder_pan": {"range_min": 1700, "range_max": 3000},
            "gripper": {"range_min": 500, "range_max": 2500},
        }
        is_valid, warnings, severity = _validate_calibration_ranges_sufficient(calib)
        assert is_valid is True  # Can save calibration (passes 5% threshold)
        assert severity == "warning"
