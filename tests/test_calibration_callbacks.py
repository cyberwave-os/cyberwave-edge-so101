"""Unit tests for calibration callback signatures (source-level checks, no imports)."""

from __future__ import annotations

import ast
from pathlib import Path


def _get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


class TestCalibrationCallbackSignatures:
    """Verify calibration callback signatures via source inspection."""

    def test_record_ranges_of_motion_has_on_progress_param(self):
        """FeetechMotorsBus.record_ranges_of_motion accepts on_progress callback."""
        bus_path = _get_project_root() / "motors" / "feetech_bus.py"
        source = bus_path.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "record_ranges_of_motion":
                params = [a.arg for a in node.args.args]
                assert "on_progress" in params, f"Expected on_progress in {params}"
                return
        raise AssertionError("record_ranges_of_motion not found")

    def test_robot_calibrate_has_callbacks(self):
        """SO101Robot.calibrate accepts on_state_change and on_joint_progress."""
        robot_path = _get_project_root() / "so101" / "robot.py"
        source = robot_path.read_text()
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == "calibrate":
                params = [a.arg for a in node.args.args]
                assert "on_state_change" in params, f"Expected on_state_change in {params}"
                assert "on_joint_progress" in params, f"Expected on_joint_progress in {params}"
                return
        raise AssertionError("calibrate not found")
