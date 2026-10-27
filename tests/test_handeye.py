"""SO-101 hand-eye integration: this arm's config and wiring against the SDK flow.

The flow, its states and its alert rendering moved into ``cyberwave.calibration``
and are tested there (``tests/test_calibration_flow.py`` in the SDK), including the
full alert-contract characterisation this file used to carry. What is left to test
*here* is the part that is still the driver's: that the SO-101's measured
thresholds, board, joint map and device adapters produce a working run.
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from utils.cw_handeye import (
    _DEFAULT_BOARD,
    HANDEYE_ALERT_TYPE,
    HANDEYE_BAD_STABILITY_M,
    HANDEYE_BUTTON_FLOW,
    HANDEYE_CAPTURE_SIZE,
    HANDEYE_GOOD_RESIDUAL_M,
    HANDEYE_GOOD_STABILITY_M,
    HANDEYE_JOINT_NAME_MAP,
    HANDEYE_MIN_SAMPLES,
    HANDEYE_SETTLE_TOLERANCE_RAD,
    HANDEYE_STATE_APPLIED,
    HANDEYE_STATE_CAPTURING,
    HANDEYE_URDF_JOINT_NAMES,
    HANDEYE_WORKING_DISTANCE_M,
    assert_constants_match_sdk,
    board_from_spec,
    build_config,
    intrinsics_from_env,
)


class FakeCameraTwin:
    def __init__(self) -> None:
        self.sensors = [{"type": "rgb", "parameters": {"fovy": 58.0}}]
        self.calibration = _FakeCalibrationHandle()
        self._data = {
            "attach_to_twin_uuid": "robot-uuid",
            "attach_to_link": "gripper",
        }

    def _data_get(self, key: str) -> Any:
        return self._data.get(key)


class _FakeCalibrationHandle:
    def __init__(self) -> None:
        self.set_calls: List[Dict[str, Any]] = []

    def set(self, result, **kwargs):
        self.set_calls.append({"result": result, **kwargs})
        return {}


class FakeClient:
    def twin(self, twin_id=None, **kwargs):
        return FakeCameraTwin()


def make_config(**overrides):
    kwargs = {
        "camera_twin_uuid": "camera-uuid",
        "arm_twin_uuid": "robot-uuid",
        "fk_frame": "gripper",
        "kinematics": object(),
    }
    kwargs.update(overrides)
    return build_config(**kwargs)


# --- the SDK contract this driver depends on -------------------------------


def test_the_drivers_restated_constants_match_the_sdk():
    """The driver restates a few SDK values so it can import without the SDK.

    That trade buys robustness at the cost of values that can silently
    desynchronise -- including the alert type and state strings, which are a
    frontend contract. This is the drift guard, called once the SDK is loaded.
    """
    assert_constants_match_sdk()


def test_the_alert_type_is_the_one_the_dashboard_matches():
    assert HANDEYE_ALERT_TYPE == "handeye_calibration"


def test_the_button_flow_is_this_drivers_namespace():
    """It must match what ``_handle_handeye_button`` compares against."""
    assert HANDEYE_BUTTON_FLOW == "so101_handeye"


# --- this arm's configuration ----------------------------------------------


class TestConfig:
    def test_the_config_carries_this_arms_measured_thresholds(self):
        """These were measured on this arm; the SDK deliberately has no defaults."""
        config = make_config()

        assert config.good_stability_m == HANDEYE_GOOD_STABILITY_M == 0.004
        assert config.bad_stability_m == HANDEYE_BAD_STABILITY_M == 0.008
        assert config.good_residual_m == HANDEYE_GOOD_RESIDUAL_M == 0.005
        assert config.settle_tolerance_rad == HANDEYE_SETTLE_TOLERANCE_RAD
        assert config.requested_capture_size == HANDEYE_CAPTURE_SIZE == (1280, 720)
        assert config.working_distance_m == HANDEYE_WORKING_DISTANCE_M

    def test_the_joint_map_renames_twin_schema_to_urdf(self):
        """Twin schema names the joints ``_1``..``_6``; the URDF names them ``1``.."""
        assert HANDEYE_JOINT_NAME_MAP == {"_1": "1", "_2": "2", "_3": "3", "_4": "4", "_5": "5"}

    def test_the_jaw_is_excluded_from_kinematics(self):
        """It does not move the wrist frame, so it contributes nothing."""
        assert "_6" not in HANDEYE_JOINT_NAME_MAP
        assert "6" not in HANDEYE_URDF_JOINT_NAMES
        assert make_config().urdf_joint_names == HANDEYE_URDF_JOINT_NAMES

    def test_the_flow_discriminator_is_carried_into_the_config(self):
        assert make_config().button_flow == HANDEYE_BUTTON_FLOW

    def test_a_board_override_round_trips_for_restart(self):
        """``restart`` re-enters with the button payload as its data, so anything
        dropped here is silently replaced by a default on restart."""
        spec = {
            "type": "charuco",
            "squares": [5, 7],
            "square_size_m": 0.02,
            "marker_size_m": 0.015,
            "dictionary": "DICT_4X4_50",
        }
        config = make_config(board_spec=spec)

        assert dict(config.board_spec) == spec

    def test_target_samples_cannot_go_below_the_solve_floor(self):
        assert make_config(target_samples=1).target_samples == HANDEYE_MIN_SAMPLES


# --- the bench board -------------------------------------------------------


class TestBoard:
    def test_the_default_is_this_benchs_printed_board(self):
        board = board_from_spec(None)

        assert board.squares == tuple(_DEFAULT_BOARD["squares"])
        assert board.square_size_m == _DEFAULT_BOARD["square_size_m"]

    def test_the_square_count_is_columns_then_rows(self):
        """Getting this backwards is not a graceful degradation: detectBoard
        interpolates zero corners even when every marker is found, so captures are
        refused with a message that blames the operator's framing."""
        assert _DEFAULT_BOARD["squares"] == [23, 12]

    def test_the_grid_fits_its_dictionary(self):
        """A ChArUco grid consumes cols*rows//2 marker ids."""
        cols, rows = _DEFAULT_BOARD["squares"]
        needed = cols * rows // 2
        capacity = int(_DEFAULT_BOARD["dictionary"].rsplit("_", 1)[1])

        assert needed <= capacity, f"{needed} ids needed, dictionary holds {capacity}"

    def test_a_spec_off_the_wire_may_arrive_as_strings(self):
        """It is JSON over MQTT, so a UI could send numbers as strings."""
        board = board_from_spec(
            {"squares": ["5", "7"], "square_size_m": "0.02", "marker_size_m": "0.015"}
        )

        assert board.squares == (5, 7)
        assert board.square_size_m == 0.02

    def test_a_checkerboard_spec_is_honoured(self):
        board = board_from_spec(
            {"type": "checkerboard", "inner_corners": [9, 6], "square_size_m": 0.025}
        )

        assert board.inner_corners == (9, 6)


# --- operator intrinsics override ------------------------------------------


class TestIntrinsicsFromEnv:
    def test_all_four_are_required(self, monkeypatch):
        for key in ("FX", "FY", "CX"):
            monkeypatch.setenv(f"CYBERWAVE_HANDEYE_{key}", "600")
        monkeypatch.delenv("CYBERWAVE_HANDEYE_CY", raising=False)

        assert intrinsics_from_env() is None

    def test_a_complete_set_is_used(self, monkeypatch):
        for key, value in (("FX", "1030"), ("FY", "1031"), ("CX", "640"), ("CY", "360")):
            monkeypatch.setenv(f"CYBERWAVE_HANDEYE_{key}", value)

        assert intrinsics_from_env() == {"fx": 1030.0, "fy": 1031.0, "cx": 640.0, "cy": 360.0}

    def test_non_numeric_values_are_ignored_not_fatal(self, monkeypatch):
        for key in ("FX", "FY", "CX", "CY"):
            monkeypatch.setenv(f"CYBERWAVE_HANDEYE_{key}", "not-a-number")

        assert intrinsics_from_env() is None


# --- driving the SDK flow with this arm's wiring ----------------------------


class TestDrivingTheFlow:
    @pytest.fixture
    def flow(self):
        from cyberwave.calibration import HandEyeFlow
        from cyberwave.calibration.testing import (
            FakeFrameSource,
            FakeHandEyeSession,
            FakeJointSource,
        )

        f = HandEyeFlow(
            client=FakeClient(),
            config=make_config(),
            joint_source=FakeJointSource({f"_{i}": 0.1 * i for i in range(1, 7)}),
            frame_source=FakeFrameSource(),
        )
        f._session = FakeHandEyeSession()
        f._intrinsics = {"fx": 1030.0, "fy": 1030.0, "cx": 640.0, "cy": 360.0}
        f._frame_shape = (720, 1280)
        return f

    def test_a_capture_renames_this_arms_joints_to_urdf_names(self, flow):
        flow.capture_sample()

        assert set(flow._session.last_joint_positions) == set(HANDEYE_URDF_JOINT_NAMES)

    def test_the_jaw_is_not_passed_to_kinematics(self, flow):
        """The bus reports it; the map drops it."""
        flow.capture_sample()

        assert "6" not in flow._session.last_joint_positions

    def test_the_snapshot_reports_this_arms_state(self, flow):
        snap = flow.capture_sample()

        assert snap["state"] == HANDEYE_STATE_CAPTURING
        assert snap["samples_captured"] == 1
        assert snap["capture_size"] == [1280, 720]

    def test_applying_writes_to_the_camera_twins_docking_offset(self, flow):
        from cyberwave.calibration.testing import FakeHandEyeResult

        flow._result = FakeHandEyeResult()
        flow._state = "solved"

        snap = flow.apply(solved_at="2026-01-01T00:00:00Z")

        assert snap["state"] == HANDEYE_STATE_APPLIED
