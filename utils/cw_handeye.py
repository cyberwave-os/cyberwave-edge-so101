"""SO-101 hand-eye calibration: this arm's numbers, board and device wiring.

The flow, the alert rendering and the maths all live in the SDK
(:mod:`cyberwave.calibration`). What stays here is everything that is genuinely
about *this* robot:

* the quality thresholds and capture settings measured on this arm and camera;
* the bench board this driver defaults to;
* the joint-name mapping between the twin schema and the URDF;
* the operator intrinsics override read from the environment;
* the two device adapters (see ``utils/cw_handeye_devices.py``).

A second arm or camera writes its own module like this one — a config object and
two adapters — and reuses the SDK flow unchanged.

Every ``cyberwave`` import here is function-local by design: this module has to
import, and the alert text has to render, on a driver whose SDK is missing or too
old. That is what turns "the button does nothing" into an actionable alert (see
the ImportError branch in ``main._handle_handeye_start``).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

#: ``metadata.buttons[].payload.flow`` discriminator, matching the
#: ``so101_calibration`` convention so ``_handle_*_button`` can tell flows apart.
HANDEYE_BUTTON_FLOW = "so101_handeye"

#: Below this the solve is refused outright -- fewer than three absolute poses is
#: fewer than two relative motions. Mirrors ``calibration.MIN_SAMPLES``, restated
#: as a plain int for the SDK-free import rule above;
#: :func:`assert_constants_match_sdk` checks the two agree once the SDK is loaded.
HANDEYE_MIN_SAMPLES = 3

#: Joint movement across the frame grab above which the operator is warned, in
#: radians. Not a refusal: the operator is hand-guiding the arm, the driver's own
#: reads bracket the grab tightly, and a threshold low enough to catch real drift
#: would mostly fire on servo noise. A capture taken while the arm was still
#: settling looks fine and quietly widens the solve, and no later statistic shows
#: it, so it is worth saying out loud at the moment it happens.
HANDEYE_SETTLE_TOLERANCE_RAD = 0.01

#: What the UI counts toward. Not a hard cap -- "Capture more" past it is allowed,
#: and more well-spread views only help.
HANDEYE_DEFAULT_TARGET_SAMPLES = 12

#: Leave-one-out stability bar for the "good calibration" verdict, in metres.
#: This is the primary quality gate: it re-solves with each sample dropped and
#: takes the standard deviation of where the camera lands, so it measures whether
#: the *answer* is stable rather than whether the observations merely agree with
#: each other. Measured on this arm, runs whose mount position was reproducible
#: across independent sessions land at 1.9-3.5 mm; ill-conditioned ones reach
#: 7.7-24 mm, almost entirely on the camera's optical axis.
HANDEYE_GOOD_STABILITY_M = 0.004

#: Above this the answer is unstable enough that applying it is likely worse than
#: leaving the nominal offset in place.
HANDEYE_BAD_STABILITY_M = 0.008

#: Residual bar, in metres. Retained as a secondary check: the residual cannot see
#: a systematically wrong input (a mislabelled frame, a wrong square size) because
#: such an error is self-consistent, and on this arm it tracked working distance
#: rather than answer quality. Used only to catch a grossly bad fit.
HANDEYE_GOOD_RESIDUAL_M = 0.005

#: Capture resolution requested from the wrist camera. The V4L2 default is VGA,
#: where a small printed marker spans too few pixels per module to decode. At the
#: ~1030 px focal length this camera solves at 720p, the 6 mm marker of
#: :data:`_DEFAULT_BOARD` spans about 31 px at 20 cm and 21 px at 30 cm -- and a
#: DICT_4X4 marker is 6 modules across including its border, so that is only
#: ~5 px and ~3.4 px per module. Detection needs roughly 3 px per module, so 720p
#: is not a luxury here: at VGA the same marker is under 3.5 px per module beyond
#: 20 cm and stops decoding. The camera is free to refuse the request --
#: intrinsics are derived from the frame we actually get, so a fallback stays
#: correct, just far less forgiving.
HANDEYE_CAPTURE_SIZE = (1280, 720)

#: Working distance the granted capture mode is judged at, in metres. The wrist
#: camera is hand-guided around a small printed board, so this is the far end of
#: the range an operator actually uses -- closer always decodes better.
HANDEYE_WORKING_DISTANCE_M = 0.30

#: URDF joints forward kinematics runs over. The jaw is excluded on purpose: it
#: does not move the wrist frame, so it contributes nothing to the camera mount.
HANDEYE_URDF_JOINT_NAMES = ("1", "2", "3", "4", "5")

#: Bus joint name -> URDF joint name. The twin schema names the follower's joints
#: ``_1``..``_6``; the URDF names them ``1``..``6``. The jaw (``_6``) is simply
#: absent, which states "the jaw does not move the wrist frame" as data rather
#: than as an interaction between a name rule and a separate tuple.
HANDEYE_JOINT_NAME_MAP = {f"_{name}": name for name in HANDEYE_URDF_JOINT_NAMES}

_DEFAULT_BOARD = {
    "type": "charuco",
    #: ``(columns, rows)`` of *total* squares, in that order. This is the axis
    #: order OpenCV's ``CharucoBoard`` expects, and getting it backwards is not a
    #: graceful degradation: ``detectBoard`` interpolates zero chessboard corners
    #: even when every marker is found with a valid id, so captures are refused
    #: with a message that blames the operator's framing.
    "squares": [23, 12],
    "square_size_m": 0.011,
    "marker_size_m": 0.008,
    #: A ChArUco grid consumes ``cols * rows // 2`` marker ids, so 23x12 needs 138.
    #: DICT_4X4_250 holds 250, so it fits -- but nothing validates that at
    #: construction time, only when an image is generated, so check the arithmetic
    #: when changing ``squares``.
    "dictionary": "DICT_4X4_250",
}


def assert_constants_match_sdk() -> None:
    """Fail loudly if :data:`HANDEYE_MIN_SAMPLES` has drifted from the SDK.

    The constant is restated here rather than imported because this module must
    import without the SDK present (see the module docstring). That buys
    robustness at the cost of a value that can silently desynchronise, so this
    closes the gap the only way that works: check at runtime, once the SDK is
    genuinely loaded.

    Called from the test suite rather than at import, because import is exactly
    the moment the SDK may be absent.
    """
    from cyberwave.calibration import presentation
    from cyberwave.calibration.handeye import MIN_SAMPLES

    if HANDEYE_MIN_SAMPLES != MIN_SAMPLES:
        raise AssertionError(
            f"HANDEYE_MIN_SAMPLES ({HANDEYE_MIN_SAMPLES}) has drifted from "
            f"cyberwave.calibration MIN_SAMPLES ({MIN_SAMPLES}). The driver "
            "restates it so this module imports without the SDK; update it here "
            "to match."
        )

    # The alert type and state values are a frontend contract, so a silent drift
    # between the driver's copy and the SDK's would break the dashboard rather
    # than fail anything here.
    for label, mine, theirs in (
        ("HANDEYE_ALERT_TYPE", HANDEYE_ALERT_TYPE, presentation.ALERT_TYPE),
        ("HANDEYE_STATE_CAPTURING", HANDEYE_STATE_CAPTURING, presentation.STATE_CAPTURING),
        ("HANDEYE_STATE_SOLVED", HANDEYE_STATE_SOLVED, presentation.STATE_SOLVED),
        ("HANDEYE_STATE_APPLIED", HANDEYE_STATE_APPLIED, presentation.STATE_APPLIED),
        ("HANDEYE_STATE_ERROR", HANDEYE_STATE_ERROR, presentation.STATE_ERROR),
    ):
        if mine != theirs:
            raise AssertionError(f"{label} ({mine!r}) has drifted from the SDK's {theirs!r}.")


def board_from_spec(spec: Optional[Dict[str, Any]]) -> Any:
    """Build a ``CharucoBoard`` / ``CheckerBoard`` from a plain dict.

    The spec comes off the start command so the UI can offer board settings later
    without an edge change; missing keys fall back to :data:`_DEFAULT_BOARD`.
    """
    from cyberwave.calibration import CharucoBoard, CheckerBoard

    merged = {**_DEFAULT_BOARD, **(spec or {})}
    board_type = str(merged.get("type") or "charuco").lower()
    square_size_m = float(merged["square_size_m"])

    if board_type in {"checker", "checkerboard"}:
        corners = merged.get("inner_corners") or [9, 6]
        return CheckerBoard(
            inner_corners=(int(corners[0]), int(corners[1])),
            square_size_m=square_size_m,
        )

    squares = merged["squares"]
    # Both sizes go through float(): an MQTT payload is JSON, so a UI could send
    # them as strings and CharucoBoard compares them numerically.
    return CharucoBoard(
        squares=(int(squares[0]), int(squares[1])),
        square_size_m=square_size_m,
        marker_size_m=float(merged["marker_size_m"]),
        dictionary=str(merged["dictionary"]),
    )


def intrinsics_from_env() -> Optional[Dict[str, float]]:
    """Operator-supplied intrinsics, when all four are set.

    The highest-precedence source: an operator who has measured this camera and
    exported the numbers means them, so they beat anything stored on the twin.
    All four or nothing -- three of four completed from a default is a measurement
    that was never taken, reported as one that was.
    """
    keys = ("FX", "FY", "CX", "CY")
    values = {k: os.environ.get(f"CYBERWAVE_HANDEYE_{k}") for k in keys}
    if not all(values.values()):
        return None
    try:
        return {k.lower(): float(v) for k, v in values.items()}  # type: ignore[arg-type]
    except ValueError:
        logger.warning("Ignoring non-numeric CYBERWAVE_HANDEYE_FX/FY/CX/CY")
        return None


def build_config(
    *,
    camera_twin_uuid: str,
    arm_twin_uuid: str,
    fk_frame: str,
    board_spec: Optional[Dict[str, Any]] = None,
    target_samples: int = HANDEYE_DEFAULT_TARGET_SAMPLES,
    kinematics: Any,
) -> Any:
    """This arm's :class:`~cyberwave.calibration.HandEyeFlowConfig`.

    Every threshold above is passed explicitly. The SDK deliberately has no
    defaults for them: a default would be a measurement nobody took, presented
    as one they did.
    """
    from cyberwave.calibration import HandEyeFlowConfig

    return HandEyeFlowConfig(
        camera_twin_uuid=camera_twin_uuid,
        arm_twin_uuid=arm_twin_uuid,
        fk_frame=fk_frame,
        joint_name_map=HANDEYE_JOINT_NAME_MAP,
        kinematics=kinematics,
        board=board_from_spec(board_spec),
        good_stability_m=HANDEYE_GOOD_STABILITY_M,
        bad_stability_m=HANDEYE_BAD_STABILITY_M,
        good_residual_m=HANDEYE_GOOD_RESIDUAL_M,
        settle_tolerance_rad=HANDEYE_SETTLE_TOLERANCE_RAD,
        requested_capture_size=HANDEYE_CAPTURE_SIZE,
        working_distance_m=HANDEYE_WORKING_DISTANCE_M,
        target_samples=max(int(target_samples), HANDEYE_MIN_SAMPLES),
        button_flow=HANDEYE_BUTTON_FLOW,
        board_spec=dict(board_spec) if board_spec else None,
    )


def build_kinematics(urdf_path: str, fk_frame: str) -> Any:
    """Pinocchio FK over this arm's URDF, for the flow to turn joints into a pose."""
    from cyberwave.driver.kinematics.arm import (
        ArmKinematicsConfig,
        BaseKinematicsManipulator,
    )

    return BaseKinematicsManipulator(
        ArmKinematicsConfig(
            urdf_path=str(urdf_path),
            ee_frame=str(fk_frame),
            arm_joints=HANDEYE_URDF_JOINT_NAMES,
        )
    )


def build_flow(
    *,
    client: Any,
    config: Any,
    video_device: Any,
    follower_port: str,
    follower_id: str,
    robot_twin_uuid: str,
) -> Any:
    """Open this arm's devices and hand them to the SDK flow.

    The bus is opened here rather than inside the flow because the failure
    message has to name the port, and only the node knows it.
    """
    from cyberwave.calibration import HandEyeError, HandEyeFlow

    from utils.cw_handeye_devices import FreedriveJointSource, V4L2FrameSource

    joint_source = FreedriveJointSource(
        client=client,
        robot_twin_uuid=robot_twin_uuid,
        port=follower_port,
        follower_id=follower_id,
        error_factory=HandEyeError,
    )
    joint_source.connect()

    frame_source = V4L2FrameSource(
        device=video_device,
        capture_size=HANDEYE_CAPTURE_SIZE,
        error_factory=HandEyeError,
    )
    try:
        frame_source.open()
    except Exception:
        # The bus is already open; do not leak it if the camera refuses.
        joint_source.disconnect()
        raise

    return HandEyeFlow(
        client=client,
        config=config,
        joint_source=joint_source,
        frame_source=frame_source,
        intrinsics=intrinsics_from_env(),
    )


# --- re-exports -------------------------------------------------------------
#
# The flow, its states and its alert rendering live in ``cyberwave.calibration``
# now. They are re-exported here on attribute access so existing call sites keep
# working and so this module stays the single place the driver imports hand-eye
# from -- without an eager import, which would break the SDK-free import rule
# above.
_SDK_NAMES = {
    "HandEyeError": "flow",
    "HandEyeFlow": "flow",
    "HandEyeRunner": "flow",
    "intrinsics_from_twin": "flow",
    "resolve_docked_fk_frame": "flow",
    "HandEyeFlowConfig": "config",
    "buttons_for_state": "presentation",
    "build_button": "presentation",
    "capture_mode_warning": "presentation",
    "describe_state": "presentation",
    "stability_from_result": "presentation",
}

#: Alert type and state values, restated so the alert contract can be asserted
#: without the SDK. :func:`assert_constants_match_sdk` is the drift guard.
HANDEYE_ALERT_TYPE = "handeye_calibration"
HANDEYE_STATE_CAPTURING = "capturing"
HANDEYE_STATE_SOLVED = "solved"
HANDEYE_STATE_APPLIED = "applied"
HANDEYE_STATE_ERROR = "error"


def __getattr__(name: str) -> Any:
    """Resolve a name that moved into the SDK, on first use."""
    module = _SDK_NAMES.get(name)
    if module is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import importlib

    return getattr(importlib.import_module(f"cyberwave.calibration.{module}"), name)
