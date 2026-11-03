"""Standalone eye-in-hand calibration for the SO101 wrist camera -- no platform.

Runs the same pipeline the driver's guided flow runs (``cyberwave.calibration`` for
the maths, pinocchio FK off the SO101 URDF, the same camera-grab discipline as
``utils.cw_handeye``) with the platform cut out: no ``Cyberwave`` client, no twins,
no MQTT, no alerts, no frontend. What is left is exactly the part that can be
wrong, and every input and intermediate it consumes is written to disk.

Nothing is *inferred* from the platform here, so the two values the twin normally
supplies have to be passed in: the camera intrinsics (``--fx/--fy/--cx/--cy``, or
``CYBERWAVE_HANDEYE_FX/FY/CX/CY``) and the link the camera is docked to
(``--fk-frame``). Everything else
comes off the local hardware and ``~/.cyberwave/so101_lib``.

Torque is never enabled -- the bus is opened read-only and the brakes released, so
the arm stays hand-guidable, same as the driver's flow.

Five subcommands, in the order you would reach for them:

``selftest``
    Solve a fully synthetic run end to end, with no hardware. Rendering the board
    and detecting it through the same camera model means the answer is known, so a
    failure here is a bug in this tool or the SDK -- never in your setup. Run it
    first when something looks wrong.

``board``
    Render the board you *think* you have to a PNG. Comparing it against the print
    falsifies a wrong ``--dictionary`` / ``--squares`` before any capture happens.

``capture``
    Live, interactive run against the arm and camera. Writes a run directory.

``intrinsics``
    Fit real intrinsics (``cv2.calibrateCamera``) from the frames a run already
    saved. Intrinsics are an input the solve cannot recover from and are usually
    the largest error term, so this replaces a guess with a measurement at no extra
    capture cost.

``solve``
    Re-solve an existing run offline, with a different board spec, different
    intrinsics, or a different solver. This is where a bad result gets pinned down:
    nothing is re-captured, so any change in the answer is attributable.

Everything a run touches lands under ``--out``::

    <run>/run.json                     config, device, granted capture mode, versions
    <run>/run.log                      full DEBUG log of the run
    <run>/samples/000_raw.png          the exact frame handed to the detector
    <run>/samples/000_overlay.png      detected corners + board axes drawn on it
    <run>/samples/000.json             joints (radians, normalized, raw counts), FK,
                                       board pose, correspondences, reprojection error
    <run>/solve.json                   the solved transform, per-sample residuals,
                                       and the attach_offset the platform would store

Captures where the board was *not* found are saved too, with their marker
diagnostics -- those are the frames worth looking at first.

Examples::

    # Check the board spec against the print.
    python -m scripts.cw_handeye_debug board --out /tmp/board.png

    # Live run. Hand-guide the wrist, press Enter per capture, 's' to solve.
    python -m scripts.cw_handeye_debug capture --fk-frame gripper \\
        --fx 1030 --fy 1030 --cx 640 --cy 360

    # The residual was bad: fit real intrinsics from what was captured, re-solve.
    python -m scripts.cw_handeye_debug intrinsics --run handeye_runs/<ts>
    python -m scripts.cw_handeye_debug solve --run handeye_runs/<ts> --intrinsics-from-fit
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np

logger = logging.getLogger("handeye_debug")

# The five arm joints in kinematic order as the URDF names them. Motor id N is URDF
# joint "N", so the driver's twin-schema hop ("_1" -> "1") does not exist here. The
# jaw (motor 6) is excluded for the same reason it is in main.py: it does not move
# the wrist frame, so it contributes nothing to FK of the camera mount.
URDF_JOINT_NAMES: Tuple[str, ...] = ("1", "2", "3", "4", "5")

DEFAULT_RUN_ROOT = Path("handeye_runs")


# --- pose formatting ------------------------------------------------------


def describe_pose(transform: np.ndarray) -> Dict[str, Any]:
    """Human-readable view of a 4x4: position, quaternion, and axis-angle.

    Delegates to the SDK so this tool's artifacts share key names with the
    driver's recorded runs, which is what makes the two diffable.
    """
    from cyberwave.calibration.frames import describe_pose as _describe

    return _describe(transform)


def _mean_transform(transforms: Sequence[np.ndarray]) -> np.ndarray:
    """Mean translation and sign-aligned mean rotation of a tight cluster.

    Mirrors ``handeye._mean_transform`` (private there). Reimplemented rather than
    reached into so this tool keeps working against an older installed SDK -- and
    so the per-sample residual breakdown below is explicit about its reference.
    """
    from cyberwave.calibration.frames import (
        make_transform,
        matrix_to_quat_wxyz,
        quat_wxyz_to_matrix,
    )

    translation = np.mean([t[:3, 3] for t in transforms], axis=0)
    reference = np.array(matrix_to_quat_wxyz(transforms[0][:3, :3]))
    accumulator = np.zeros(4)
    for transform in transforms:
        quaternion = np.array(matrix_to_quat_wxyz(transform[:3, :3]))
        if np.dot(quaternion, reference) < 0.0:
            quaternion = -quaternion
        accumulator += quaternion
    norm = float(np.linalg.norm(accumulator))
    quaternion = reference if norm < 1e-12 else accumulator / norm
    return make_transform(quat_wxyz_to_matrix(quaternion), translation)


# --- board + intrinsics ---------------------------------------------------


def board_spec_from_args(args: argparse.Namespace) -> Dict[str, Any]:
    """Board spec dict in the shape the driver's start command carries."""
    if args.board_type in {"checker", "checkerboard"}:
        return {
            "type": "checkerboard",
            "inner_corners": list(args.inner_corners),
            "square_size_m": args.square_size,
        }
    return {
        "type": "charuco",
        "squares": list(args.squares),
        "square_size_m": args.square_size,
        "marker_size_m": args.marker_size,
        "dictionary": args.dictionary,
    }


def build_board(spec: Dict[str, Any]) -> Any:
    """The production board builder, so a board-spec bug reproduces here."""
    from utils.cw_handeye import board_from_spec

    return board_from_spec(spec)


def resolve_intrinsics(
    args: argparse.Namespace, frame_shape: Sequence[int]
) -> Tuple[Dict[str, float], str]:
    """``({fx, fy, cx, cy}, source)`` for the frame size actually in hand.

    Explicit values beat the environment, and one of the two must be supplied.
    There is deliberately no field-of-view rung: deriving a focal length from a
    declared FOV hands back a plausible-looking solve built on a guess, and being
    an *input* to hand-eye its error shows up only as an inflated residual with
    nothing pointing at the cause. The driver dropped that fallback for the same
    reason; ``capture`` records the frames, so intrinsics can be *measured* from
    the run afterwards with the ``intrinsics`` subcommand.
    """
    from utils.cw_handeye import intrinsics_from_env

    explicit = (args.fx, args.fy, args.cx, args.cy)
    if all(v is not None for v in explicit):
        return {"fx": args.fx, "fy": args.fy, "cx": args.cx, "cy": args.cy}, "cli"
    if any(v is not None for v in explicit):
        raise SystemExit(
            "Pass all four of --fx --fy --cx --cy, or none of them. Three of four "
            "completed from a default is a measurement that was never taken."
        )

    from_env = intrinsics_from_env()
    if from_env is not None:
        return from_env, "env"

    raise SystemExit(
        "No intrinsics. Pass --fx/--fy/--cx/--cy or export "
        "CYBERWAVE_HANDEYE_FX/FY/CX/CY. Intrinsics are an input to hand-eye and are "
        "usually the largest error term, so capture a run and measure them from its "
        "own frames -- 'intrinsics --run <dir>' -- rather than guessing."
    )


def count_markers(frame: Any, dictionary_name: Optional[str]) -> Dict[str, Any]:
    """Raw ArUco marker count, for frames the board detector rejected.

    Mirrors ``HandEyeFlow._detection_diagnostics``: zero markers and "markers but
    no board" need opposite fixes, and "board not found" alone does not say which.
    """
    if not dictionary_name:
        return {}
    try:
        import cv2
        from cyberwave.calibration.board import resolve_dictionary

        dictionary = resolve_dictionary(cv2, str(dictionary_name))
        gray = frame if getattr(frame, "ndim", 0) == 2 else cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        detector = cv2.aruco.ArucoDetector(dictionary, cv2.aruco.DetectorParameters())
        _corners, ids, _rejected = detector.detectMarkers(gray)
        return {
            "markers_detected": 0 if ids is None else int(len(ids)),
            "dictionary": str(dictionary_name),
        }
    except Exception:
        logger.debug("Marker diagnostics failed", exc_info=True)
        return {}


def detect(
    frame: Any,
    board: Any,
    intrinsics: Dict[str, float],
    dist_coeffs: Optional[Sequence[float]],
) -> Dict[str, Any]:
    """Locate the board and report *why* it went the way it did.

    ``camera_to_target`` comes from the SDK's own ``detect_target_pose`` so it is
    bit-for-bit what production would use. The correspondence count, the
    reprojection error and the marker count are extra: they are the numbers that
    separate "the board spec is wrong" from "the intrinsics are wrong" from "the
    frame was blurred".
    """
    import cv2
    from cyberwave.calibration import detect_target_pose
    from cyberwave.calibration.board import camera_matrix, distortion_vector

    out: Dict[str, Any] = {"detected": False}
    correspondences = board.correspondences(cv2, np.asarray(frame))
    if correspondences is None:
        out["reject_reason"] = "no_correspondences"
        out.update(count_markers(frame, getattr(board, "dictionary", None)))
        return out

    object_points, image_points = correspondences
    out["correspondences"] = int(len(object_points))
    out.update(count_markers(frame, getattr(board, "dictionary", None)))

    camera_to_target = detect_target_pose(frame, board, intrinsics, dist_coeffs)
    if camera_to_target is None:
        out["reject_reason"] = "pnp_failed"
        return out

    rvec, _ = cv2.Rodrigues(camera_to_target[:3, :3])
    tvec = camera_to_target[:3, 3].reshape(3, 1)
    projected, _ = cv2.projectPoints(
        object_points.reshape(-1, 1, 3),
        rvec,
        tvec,
        camera_matrix(intrinsics),
        distortion_vector(dist_coeffs),
    )
    errors = np.linalg.norm(projected.reshape(-1, 2) - image_points.reshape(-1, 2), axis=1)

    out.update(
        {
            "detected": True,
            "camera_to_target": camera_to_target.tolist(),
            "camera_to_target_pose": describe_pose(camera_to_target),
            "board_distance_m": round(float(np.linalg.norm(camera_to_target[:3, 3])), 5),
            # A mean well above ~1 px on a sharp frame means the projection model
            # is wrong -- distortion, or a board that is not the shape claimed --
            # not that the detection was noisy. It does *not* catch a wrong focal
            # length: solvePnP absorbs that into the board distance and still
            # reprojects cleanly, which is why a guessed focal length goes
            # unpunished here and has to be measured instead.
            "reprojection_error_px": {
                "mean": round(float(np.mean(errors)), 4),
                "max": round(float(np.max(errors)), 4),
            },
            "_object_points": object_points,
            "_image_points": image_points,
        }
    )
    return out


def write_overlay(
    path: Path,
    frame: Any,
    detection: Dict[str, Any],
    intrinsics: Dict[str, float],
    dist_coeffs: Optional[Sequence[float]],
) -> None:
    """Draw the detected corners and board axes onto a copy of the frame."""
    import cv2
    from cyberwave.calibration.board import camera_matrix, distortion_vector

    image = np.asarray(frame).copy()
    if image.ndim == 2:
        image = cv2.cvtColor(image, cv2.COLOR_GRAY2BGR)
    for point in detection.get("_image_points", np.empty((0, 2))):
        cv2.circle(image, (int(point[0]), int(point[1])), 4, (0, 255, 0), 1)
    if detection.get("detected"):
        pose = np.asarray(detection["camera_to_target"], dtype=float)
        rvec, _ = cv2.Rodrigues(pose[:3, :3])
        cv2.drawFrameAxes(
            image,
            camera_matrix(intrinsics),
            distortion_vector(dist_coeffs),
            rvec,
            pose[:3, 3].reshape(3, 1),
            0.05,
        )
    cv2.imwrite(str(path), image)


# --- devices --------------------------------------------------------------


class Arm:
    """Read-only follower bus. Torque is released and never re-enabled."""

    def __init__(self, port: str, follower_id: str) -> None:
        from motors import FeetechMotorsBus, MotorCalibration
        from so101.robot import SO101_MOTORS
        from utils.config import get_so101_lib_dir
        from utils.utils import load_calibration

        self._motors = SO101_MOTORS
        self._calibration: Optional[Dict[str, Any]] = None
        path = get_so101_lib_dir() / "calibrations" / f"{follower_id}.json"
        if path.is_file():
            self._calibration = {
                name: MotorCalibration(**data) for name, data in load_calibration(path).items()
            }
            logger.info("Follower calibration: %s", path)
        else:
            # The driver refuses to run hand-eye at all in this state, because
            # normalized counts read as radians put FK on a pose the arm is not in.
            # Here it is allowed but shouted about: an uncalibrated run is a useful
            # thing to be able to reproduce deliberately.
            logger.warning(
                "No follower calibration at %s -- joint angles will be APPROXIMATE and "
                "every solve built on them is suspect. Run so101-calibrate first.",
                path,
            )

        self._bus = FeetechMotorsBus(port=port, motors=self._motors, calibration=self._calibration)
        self._bus.connect(preflight_check=False)
        self._bus.disable_torque()
        logger.info("Follower bus open on %s, torque released", port)

    @property
    def calibrated(self) -> bool:
        return self._calibration is not None

    def read(self) -> Dict[str, Any]:
        """One joint read: radians for FK, plus normalized and raw for diagnosis.

        The raw counts are the only view that survives a wrong calibration file, so
        they are recorded even though nothing downstream consumes them. They come
        from a second bus read a few milliseconds after the first -- fine while the
        arm is held still for a capture, which is the only time this is called.
        """
        from utils.utils import normalized_to_radians

        normalized = self._bus.sync_read("Present_Position", normalize=True, num_retry=2)
        raw = self._bus.sync_read("Present_Position", normalize=False, num_retry=2)
        if not normalized:
            raise RuntimeError("The arm reported no joint angles.")

        radians: Dict[str, float] = {}
        for name, value in normalized.items():
            motor = self._motors.get(name)
            if motor is None:
                continue
            calib = self._calibration.get(name) if self._calibration else None
            radians[str(motor.id)] = normalized_to_radians(value, motor.norm_mode, calib)

        missing = [j for j in URDF_JOINT_NAMES if j not in radians]
        if missing:
            raise RuntimeError(f"The arm did not report joint(s) {missing}, so FK cannot run.")
        return {
            "radians": {k: round(v, 6) for k, v in sorted(radians.items())},
            "normalized": {k: round(float(v), 4) for k, v in sorted(normalized.items())},
            "raw_counts": {k: float(v) for k, v in sorted(raw.items())},
            "fk_joints": {j: radians[j] for j in URDF_JOINT_NAMES},
        }

    def close(self) -> None:
        try:
            self._bus.disconnect()
        except Exception:
            logger.debug("Bus disconnect failed", exc_info=True)


def _camera_open_hint(device: Any) -> str:
    """Separate "no permission" from "someone else has it" -- opposite fixes.

    OpenCV reports both as ``isOpened() == False``, and a missing ``video`` group
    membership is easy to mistake for the driver still holding the device.
    """
    import grp
    import os

    path = Path(str(device)) if not isinstance(device, int) else None
    if path is None or not path.exists():
        return (
            "The device does not exist. Check `ls /dev/video*` (or pass an index, or an MJPEG URL)."
        )
    if not os.access(path, os.R_OK | os.W_OK):
        try:
            group = grp.getgrgid(path.stat().st_gid).gr_name
        except (KeyError, OSError):
            group = "video"
        return (
            f"Permission denied: {path} belongs to group {group!r} and this user is not "
            f"in it. Add yourself (`sudo usermod -aG {group} $USER`) and log back in. "
            "The driver does not hit this because its container is given the device "
            "directly."
        )
    return "Another process may hold it -- stop the driver first."


class Camera:
    """The wrist camera, opened and grabbed exactly as ``cw_handeye`` does it."""

    def __init__(self, device: Any) -> None:
        import cv2

        from utils.cw_handeye import HANDEYE_CAPTURE_SIZE, _fourcc_name

        v4l2 = getattr(cv2, "CAP_V4L2", None)
        if isinstance(device, int):
            capture = cv2.VideoCapture(device)
        elif str(device).startswith("/dev/video") and v4l2 is not None:
            capture = cv2.VideoCapture(str(device), v4l2)
        else:
            capture = cv2.VideoCapture(str(device))
        if not capture.isOpened():
            capture.release()
            raise RuntimeError(
                f"Could not open the camera at {device!r}. {_camera_open_hint(device)}"
            )
        width, height = HANDEYE_CAPTURE_SIZE
        # Ask for MJPG *before* the size, exactly as the driver does. Uncompressed
        # 720p YUYV is ~27 MB/s, right at the USB 2.0 ceiling: measured frame grabs
        # of 500-1250 ms, during which the arm drifts away from the joint angles the
        # sample is paired with. Order matters -- V4L2 renegotiates the format, and
        # setting the size first lets the driver pick a YUYV mode the later fourcc
        # cannot change. Without this the tool that exists to diagnose bad
        # calibrations was itself producing mis-paired samples.
        fourcc = getattr(cv2, "VideoWriter_fourcc", None)
        if fourcc is not None:
            capture.set(cv2.CAP_PROP_FOURCC, fourcc(*"MJPG"))
        capture.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        capture.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
        capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self._capture = capture
        self.granted = (
            int(capture.get(cv2.CAP_PROP_FRAME_WIDTH)),
            int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT)),
        )
        self.granted_fourcc = _fourcc_name(capture.get(cv2.CAP_PROP_FOURCC))
        # Log the granted codec next to the granted size: it is what decides the
        # frame-grab latency, and a camera that quietly refused MJPG is the
        # difference between a synchronous sample and a mis-paired one.
        logger.info(
            "Camera %r: requested %dx%d MJPG, granted %dx%d %s",
            device,
            width,
            height,
            *self.granted,
            self.granted_fourcc,
        )

    def grab(self) -> Any:
        """Flush the driver queue, then take one frame."""
        for _ in range(5):
            self._capture.grab()
        ok, frame = self._capture.read()
        if not ok or frame is None:
            raise RuntimeError("The camera returned no frame.")
        return frame

    def close(self) -> None:
        try:
            self._capture.release()
        except Exception:
            logger.debug("Camera release failed", exc_info=True)


def build_kinematics(urdf_path: str, fk_frame: str) -> Any:
    from cyberwave.driver.kinematics.arm import ArmKinematicsConfig, BaseKinematicsManipulator

    return BaseKinematicsManipulator(
        ArmKinematicsConfig(urdf_path=urdf_path, ee_frame=fk_frame, arm_joints=URDF_JOINT_NAMES)
    )


# --- run directory --------------------------------------------------------


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=False) + "\n")


def attach_log_file(path: Path) -> logging.Handler:
    """Tee everything at DEBUG into the run directory."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(path)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(name)s %(message)s"))
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(handler)
    return handler


def load_run(run_dir: Path) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    """A run's config plus its samples, in capture order.

    Normalises two shapes into one. Runs written by the driver's since-removed
    recorder marked a detected sample with ``outcome: "detected"`` and left
    ``intrinsics`` null whenever the camera model was solved at solve-time rather
    than supplied up front; runs written by this tool's own ``capture`` set
    ``detected`` and always carry intrinsics. Reading only the newer shape meant
    ``--no-redetect`` silently kept *zero* samples from an archived run, and
    ``dict(None)`` raised before the solve even started -- on exactly the runs the
    offline path exists to re-examine.
    """
    config = json.loads((run_dir / "run.json").read_text())
    samples = [json.loads(p.read_text()) for p in sorted((run_dir / "samples").glob("[0-9]*.json"))]
    for sample in samples:
        if "detected" not in sample and "outcome" in sample:
            sample["detected"] = sample.get("outcome") == "detected"
    config.setdefault("intrinsics_source", "unknown")
    return config, samples


def run_intrinsics(config: Dict[str, Any], run_dir: Path) -> Dict[str, float]:
    """The intrinsics a run recorded, or a refusal that says what to do instead.

    A recorded run legitimately carries none: the driver solved them from the
    captured views at solve time, so ``run.json`` never held them. That is a
    recoverable situation with a specific next step, not a ``TypeError`` from
    ``dict(None)`` three frames down.
    """
    intrinsics = config.get("intrinsics")
    if not intrinsics:
        raise SystemExit(
            f"{run_dir} recorded no intrinsics (its camera model was solved from "
            f"the captured views). Fit them first:\n"
            f"    python -m scripts.cw_handeye_debug intrinsics --run {run_dir}\n"
            f"then re-run this with --intrinsics-from-fit, or pass "
            f"--fx/--fy/--cx/--cy."
        )
    return dict(intrinsics)


# --- solving --------------------------------------------------------------


def solve_methods(
    gripper_poses: List[np.ndarray],
    target_poses: List[np.ndarray],
    indices: List[int],
    sensor_offset: np.ndarray,
    methods: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    """Solve and report per-sample residuals, keyed by method name.

    Defaults to :data:`DEFAULT_METHOD` alone -- the solver that actually produces
    the calibration. *methods* takes any subset of ``METHODS`` for the rare case
    where one solver is suspected of misbehaving on a particular set.
    """
    from cyberwave.calibration import (
        DEFAULT_METHOD,
        matrix_to_pose,
        optical_to_attach_offset,
    )
    from cyberwave.calibration.frames import rotation_angle_deg
    from cyberwave.calibration.handeye import leave_one_out_stability, solve_hand_eye

    results: Dict[str, Any] = {}
    for method in sorted(methods or [DEFAULT_METHOD]):
        try:
            result = solve_hand_eye(
                gripper_poses,
                target_poses,
                method=method,
            )
        except Exception as exc:
            results[method] = {"error": type(exc).__name__, "message": str(exc)}
            logger.warning("Solver %s failed: %s", method, exc)
            continue

        # Where each sample lands relative to the others. The board did not move,
        # so every sample's implied board pose should agree; the one that does not
        # is the sample to go and look at.
        implied = [a @ result.gripper_to_camera @ b for a, b in zip(gripper_poses, target_poses)]
        reference = _mean_transform(implied)
        inverse_rotation = reference[:3, :3].T
        per_sample = [
            {
                "index": indices[i],
                "translation_m": round(float(np.linalg.norm(t[:3, 3] - reference[:3, 3])), 6),
                "rotation_deg": round(rotation_angle_deg(inverse_rotation @ t[:3, :3]), 4),
            }
            for i, t in enumerate(implied)
        ]

        # Leave-one-out stability: re-solve with each sample dropped and see how far
        # the answer moves. This is the statistic the dashboard's verdict is keyed
        # on, so computing it here is what lets the two agree -- without it this
        # tool could only ever report "marginal", however good the run was.
        # ``None`` below MIN_LEAVE_ONE_OUT_SAMPLES, which the verdict reads as
        # "cannot say" rather than as a pass.
        try:
            loo = leave_one_out_stability(
                gripper_poses,
                target_poses,
                method=method,
            )
        except Exception:
            logger.debug("Leave-one-out failed for %s", method, exc_info=True)
            loo = None

        attach_offset = optical_to_attach_offset(result.gripper_to_camera, sensor_offset)
        position, rotation = matrix_to_pose(attach_offset)
        results[method] = {
            **result.to_metadata(),
            "stability_m": (
                None if loo is None else round(float(loo.max_stddev_m), 6)
            ),
            "gripper_to_camera_optical": result.gripper_to_camera.tolist(),
            "gripper_to_camera_pose": describe_pose(result.gripper_to_camera),
            "board_pose_in_base": describe_pose(reference),
            "per_sample_residual": per_sample,
            "worst_sample": max(per_sample, key=lambda s: s["translation_m"]),
            # Precisely the fields CameraCalibrationHandle.set would PATCH onto the
            # camera twin, so this can be diffed against what the platform stored.
            "attach_offset": {
                "attach_offset_x": position["x"],
                "attach_offset_y": position["y"],
                "attach_offset_z": position["z"],
                "attach_offset_rotation_w": rotation["w"],
                "attach_offset_rotation_x": rotation["x"],
                "attach_offset_rotation_y": rotation["y"],
                "attach_offset_rotation_z": rotation["z"],
            },
        }
    return results


def sensor_offset_from_arg(value: Optional[str]) -> np.ndarray:
    """The camera twin's own sensor offset, as JSON, or identity.

    Identity is right for most camera assets, which is exactly why a non-identity
    one is easy to forget -- and forgetting it moves the stored offset by however
    far the sensor sits from the twin origin.
    """
    from cyberwave.calibration import pose_to_matrix

    if not value:
        return np.eye(4)
    offset = json.loads(value)
    return pose_to_matrix(offset.get("position"), offset.get("rotation"))


def report(results: Dict[str, Any], default: str) -> None:
    """Print the summary a human reads before opening any JSON."""
    from utils.cw_handeye import _verdict

    print("\n=== solve ===")
    for method in sorted(results):
        data = results[method]
        mark = "*" if method == default else " "
        if "error" in data:
            print(f"{mark} {method:<11} FAILED  {data['error']}: {data['message']}")
            continue
        # Stability is the *primary* input to the verdict; passing only the
        # residual and spread pinned every run to "marginal" at best, so this tool
        # could never agree with the dashboard about a good calibration.
        stability_m = data.get("stability_m")
        stability_text = (
            f"stability {stability_m * 1000:5.2f} mm   "
            if stability_m is not None
            else "stability     n/a   "
        )
        print(
            f"{mark} {method:<11} residual {data['residual_translation_m'] * 1000:7.2f} mm "
            f"(max {data['max_residual_translation_m'] * 1000:7.2f}) / "
            f"{data['residual_rotation_deg']:6.3f} deg   "
            + stability_text
            + f"verdict {_verdict(data['residual_translation_m'], stability_m)}"
        )

    print(
        "\nThe residual only measures whether the samples agree with each other. It "
        "cannot see a wrong focal length, a wrong --square-size or a wrong --fk-frame: "
        "each of those is self-consistent and lands a confident, wrong answer. Use "
        "'intrinsics' to measure the focal length and 'selftest' to confirm the "
        "pipeline before believing a verdict."
    )

    best = results.get(default)
    if best and "error" not in best:
        pose = best["gripper_to_camera_pose"]
        x, y, z = pose["position_m"]
        print(
            f"\n{default}: camera at ({x:+.4f}, {y:+.4f}, {z:+.4f}) m on the "
            f"link, rotated {pose['rotation_angle_deg']:.2f} deg"
        )
        worst = best["worst_sample"]
        print(
            f"worst sample: #{worst['index']} off by {worst['translation_m'] * 1000:.2f} mm / "
            f"{worst['rotation_deg']:.3f} deg -- drop it and re-solve to see if it is the cause"
        )


# --- subcommands ----------------------------------------------------------


def cmd_board(args: argparse.Namespace) -> int:
    """Render the board spec to a printable PNG."""
    import cv2

    spec = board_spec_from_args(args)
    board = build_board(spec)
    if not hasattr(board, "generate_image"):
        raise SystemExit(
            "Only charuco boards can be rendered. A checkerboard prints from anywhere; "
            "just confirm --inner-corners counts *interior* corners, not squares."
        )
    image = board.generate_image(args.pixels_per_square)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(out), image)
    print(json.dumps(board.to_metadata(), indent=2))
    print(f"\nwrote {out}")
    print(
        "Hold this against the print. If the markers differ, --dictionary is wrong and "
        "nothing will ever be detected. Then measure a few squares with calipers and "
        f"divide: --square-size {spec['square_size_m']} is the only scale input to the "
        "whole calibration."
    )
    return 0


def cmd_capture(args: argparse.Namespace) -> int:
    """Interactive live run: capture, solve, and save everything."""
    import cv2

    from utils.config import get_so101_urdf_path
    from utils.cw_handeye import HANDEYE_CAPTURE_SIZE, HANDEYE_MIN_SAMPLES

    urdf_path = args.urdf or get_so101_urdf_path()
    if urdf_path is None:
        raise SystemExit("No SO101 URDF found under ~/.cyberwave/so101_lib/urdf. Pass --urdf.")

    run_dir = Path(args.out or DEFAULT_RUN_ROOT / time.strftime("%Y%m%dT%H%M%S"))
    samples_dir = run_dir / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)
    attach_log_file(run_dir / "run.log")
    logger.info("Run directory: %s", run_dir)

    spec = board_spec_from_args(args)
    board = build_board(spec)
    dist_coeffs = json.loads(args.dist_coeffs) if args.dist_coeffs else None

    camera = Camera(args.device)
    arm: Optional[Arm] = None
    try:
        arm = Arm(args.port, args.follower_id)
        kinematics = build_kinematics(str(urdf_path), args.fk_frame)

        # Intrinsics are sized from the first real frame, never from a requested
        # mode: a camera that quietly serves VGA when asked for 720p would
        # otherwise put the principal point outside the image.
        first = camera.grab()
        cv2.imwrite(str(run_dir / "probe_frame.png"), first)
        intrinsics, intrinsics_source = resolve_intrinsics(args, first.shape)
        logger.info("Intrinsics (%s): %s", intrinsics_source, intrinsics)

        config = {
            "created_at": utc_now(),
            "fk_frame": args.fk_frame,
            "urdf_path": str(urdf_path),
            "urdf_joint_names": list(URDF_JOINT_NAMES),
            "board": board.to_metadata(),
            "intrinsics": intrinsics,
            "intrinsics_source": intrinsics_source,
            "dist_coeffs": dist_coeffs,
            "frame_shape": list(first.shape),
            "camera": {
                "device": args.device,
                "requested": list(HANDEYE_CAPTURE_SIZE),
                "granted": list(camera.granted),
            },
            "arm": {
                "port": args.port,
                "follower_id": args.follower_id,
                "calibrated": arm.calibrated,
            },
            "versions": {
                "python": sys.version.split()[0],
                "cv2": cv2.__version__,
                "numpy": np.__version__,
            },
        }
        write_json(run_dir / "run.json", config)

        print(f"\nrun: {run_dir}")
        print(f"frame {first.shape[1]}x{first.shape[0]}, intrinsics {intrinsics_source}")
        print(
            "\nHand-guide the wrist to a new ORIENTATION between captures -- tilt it "
            "about genuinely different axes. Sliding it around at a fixed orientation "
            "leaves the problem unsolvable."
        )
        print("[Enter] capture   s solve   d drop last   c clear   q quit\n")

        samples: List[Dict[str, Any]] = []
        while True:
            try:
                command = input(f"[{len(samples)} kept] > ").strip().lower()
            except EOFError:
                command = "q"

            if command == "q":
                break
            if command == "c":
                samples.clear()
                print("cleared (files kept on disk)")
                continue
            if command == "d":
                if samples:
                    dropped = samples.pop()
                    print(f"dropped #{dropped['index']} (files kept on disk)")
                continue
            if command == "s":
                if len(samples) < HANDEYE_MIN_SAMPLES:
                    print(f"need {HANDEYE_MIN_SAMPLES} samples, have {len(samples)}")
                    continue
                _solve_and_write(run_dir, config, samples, args)
                continue
            if command:
                print("unknown command")
                continue

            index = len(list(samples_dir.glob("[0-9]*.json")))
            record = _capture_one(
                index, samples_dir, camera, arm, kinematics, board, intrinsics, dist_coeffs
            )
            if record["detected"]:
                samples.append(record)
                print(
                    f"  kept #{index}: {record['correspondences']} corners at "
                    f"{record['board_distance_m']:.3f} m, reprojection "
                    f"{record['reprojection_error_px']['mean']:.2f} px"
                )
            else:
                print(f"  REJECTED #{index}: {_reject_hint(record)}")

        if len(samples) >= HANDEYE_MIN_SAMPLES and not (run_dir / "solve.json").exists():
            _solve_and_write(run_dir, config, samples, args)
        print(f"\nrun saved: {run_dir}")
        return 0
    finally:
        camera.close()
        if arm is not None:
            arm.close()


def _reject_hint(record: Dict[str, Any]) -> str:
    """Why a capture missed, phrased as the thing to go and change."""
    found = record.get("markers_detected")
    if record.get("reject_reason") == "pnp_failed":
        return "corners found but solvePnP failed -- check the intrinsics"
    if found == 0:
        return (
            f"no {record.get('dictionary', 'ArUco')} markers at all -- wrong "
            "--dictionary, or the board is out of frame / too far / blurred"
        )
    if isinstance(found, int) and found > 0:
        transposed = record.get("transposed_corners")
        if isinstance(transposed, int) and transposed >= 6:
            return (
                f"{found} markers read but --squares looks transposed -- swapping it "
                f"places the board with {transposed} corners"
            )
        return (
            f"{found} markers but not enough grid to place the board -- bring more of "
            "it into view, or --squares does not match the print"
        )
    return "board not found -- check it is fully in frame, in focus and evenly lit"


def _capture_one(
    index: int,
    samples_dir: Path,
    camera: Camera,
    arm: Arm,
    kinematics: Any,
    board: Any,
    intrinsics: Dict[str, float],
    dist_coeffs: Optional[Sequence[float]],
) -> Dict[str, Any]:
    """One (frame, pose) pair, saved whether or not the board was found.

    The frame is grabbed before the joints are read, and both are local and on
    demand, so the pair is genuinely simultaneous -- the reason this flow lives on
    the edge rather than going through a transport at all.
    """
    import cv2

    frame = camera.grab()
    joints = arm.read()
    base_to_gripper = np.asarray(kinematics.fk(dict(joints["fk_joints"])), dtype=float)

    detection = detect(frame, board, intrinsics, dist_coeffs)
    raw_path = samples_dir / f"{index:03d}_raw.png"
    cv2.imwrite(str(raw_path), frame)
    write_overlay(
        samples_dir / f"{index:03d}_overlay.png", frame, detection, intrinsics, dist_coeffs
    )

    record = {
        "index": index,
        "captured_at": utc_now(),
        "raw_frame": raw_path.name,
        "overlay_frame": f"{index:03d}_overlay.png",
        "frame_shape": list(frame.shape),
        "joints": joints,
        "base_to_gripper": base_to_gripper.tolist(),
        "base_to_gripper_pose": describe_pose(base_to_gripper),
        **{k: v for k, v in detection.items() if not k.startswith("_")},
    }
    write_json(samples_dir / f"{index:03d}.json", record)
    logger.info(
        "sample %03d detected=%s corners=%s",
        index,
        record["detected"],
        record.get("correspondences"),
    )
    # Keep the correspondences on the in-memory copy only; they are large and the
    # overlay PNG already shows them.
    record.update({k: v for k, v in detection.items() if k.startswith("_")})
    return record


def _solve_and_write(
    run_dir: Path,
    config: Dict[str, Any],
    samples: List[Dict[str, Any]],
    args: argparse.Namespace,
) -> None:
    from cyberwave.calibration import DEFAULT_METHOD

    gripper_poses = [np.asarray(s["base_to_gripper"], dtype=float) for s in samples]
    target_poses = [np.asarray(s["camera_to_target"], dtype=float) for s in samples]
    indices = [int(s["index"]) for s in samples]
    sensor_offset = sensor_offset_from_arg(args.sensor_offset)

    results = solve_methods(gripper_poses, target_poses, indices, sensor_offset)
    payload = {
        "solved_at": utc_now(),
        "samples_used": indices,
        "board": config["board"],
        "intrinsics": config["intrinsics"],
        "intrinsics_source": config["intrinsics_source"],
        "dist_coeffs": config.get("dist_coeffs"),
        "sensor_offset": sensor_offset.tolist(),
        "default_method": DEFAULT_METHOD,
        "methods": results,
    }
    write_json(run_dir / "solve.json", payload)
    report(results, DEFAULT_METHOD)
    print(f"\nwrote {run_dir / 'solve.json'}")


def synthetic_camera_to_target(board: Any, index: int, distance_m: float) -> np.ndarray:
    """A board pose that is well inside the frame and tilted about a fresh axis.

    Rotation axes are cycled deliberately: hand-eye is constrained by rotation
    about *non-parallel* axes, so a generator that only varies one axis would
    produce a degenerate set and prove nothing.
    """
    from cyberwave.calibration import make_transform

    axes = [
        (1.0, 0.0, 0.0),
        (0.0, 1.0, 0.0),
        (0.0, 0.0, 1.0),
        (1.0, 1.0, 0.0),
        (1.0, 0.0, 1.0),
        (0.0, 1.0, 1.0),
        (1.0, -1.0, 0.0),
    ]
    axis = np.asarray(axes[index % len(axes)], dtype=float)
    axis /= np.linalg.norm(axis)
    angle = np.radians(18.0 + 6.0 * (index % 4)) * (1.0 if index % 2 == 0 else -1.0)

    import cv2

    rotation, _ = cv2.Rodrigues((axis * angle).reshape(3, 1))
    # Put the board centre on the optical axis at *distance_m*, so every view is
    # fully visible regardless of the tilt.
    centre = np.array([*board_extent_m(board), 0.0]) / 2.0
    translation = np.array([0.0, 0.0, distance_m]) - rotation @ centre
    return make_transform(rotation, translation)


def board_extent_m(board: Any) -> Tuple[float, float]:
    """Printed ``(width, height)`` of the board, in metres."""
    if hasattr(board, "squares"):
        cols, rows = board.squares
    else:
        # A checkerboard's object points span its interior corners, one square in
        # from each edge -- which is what generate/detect agree on.
        cols, rows = (v - 1 for v in board.inner_corners)
    return (cols * board.square_size_m, rows * board.square_size_m)


def render_board_view(
    board: Any,
    board_image: Any,
    pixels_per_square: int,
    camera_to_target: np.ndarray,
    intrinsics: Dict[str, float],
    frame_shape: Tuple[int, int],
    noise_sigma: float,
) -> Any:
    """Render the board as a camera at *camera_to_target* would see it.

    The board is planar, so one homography is exact -- no renderer needed. The
    board frame maps to the generated image linearly at
    ``pixels_per_square / square_size_m`` px per metre, with the origin half a
    pixel outside the top-left corner (measured against ``matchImagePoints``).
    """
    import cv2
    from cyberwave.calibration.board import camera_matrix

    scale = pixels_per_square / board.square_size_m
    width_m, height_m = board_extent_m(board)
    plane = np.array(
        [[0.0, 0.0], [width_m, 0.0], [width_m, height_m], [0.0, height_m]], dtype=float
    )
    source = (plane * scale - 0.5).astype(np.float32)

    points = np.c_[plane, np.zeros(len(plane))]
    in_camera = (camera_to_target[:3, :3] @ points.T).T + camera_to_target[:3, 3]
    projected = (camera_matrix(intrinsics) @ in_camera.T).T
    destination = (projected[:, :2] / projected[:, 2:3]).astype(np.float32)

    homography = cv2.getPerspectiveTransform(source, destination)
    height_px, width_px = frame_shape
    frame = cv2.warpPerspective(
        board_image,
        homography,
        (width_px, height_px),
        flags=cv2.INTER_LINEAR,
        borderMode=cv2.BORDER_CONSTANT,
        borderValue=255,
    )
    frame = cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
    if noise_sigma > 0.0:
        noise = np.random.default_rng(0).normal(0.0, noise_sigma, frame.shape)
        frame = np.clip(frame.astype(float) + noise, 0, 255).astype(np.uint8)
    return frame


def cmd_selftest(args: argparse.Namespace) -> int:
    """Solve a synthetic run whose answer is known, and report the recovery error.

    This is the tool checking itself. It renders real board images through a real
    camera model, runs them through the same detection and the same solvers as a
    hardware run, and compares the answer against the transform it built the data
    from. If the recovery here is tight and a hardware run is not, the difference
    is in the hardware run's inputs -- intrinsics, board measurement, follower
    calibration, fk_frame, or frame freshness -- and not in this pipeline.

    It also leaves behind a run directory shaped exactly like a captured one, so
    ``solve`` and ``intrinsics`` can be exercised without any hardware.
    """
    import cv2
    from cyberwave.calibration import DEFAULT_METHOD, invert, make_transform
    from cyberwave.calibration.frames import quat_wxyz_to_matrix, rotation_angle_deg

    run_dir = Path(args.out or DEFAULT_RUN_ROOT / f"selftest-{time.strftime('%Y%m%dT%H%M%S')}")
    samples_dir = run_dir / "samples"
    samples_dir.mkdir(parents=True, exist_ok=True)
    attach_log_file(run_dir / "run.log")

    spec = board_spec_from_args(args)
    board = build_board(spec)
    if not hasattr(board, "generate_image"):
        raise SystemExit("selftest renders charuco boards only.")
    board_image = board.generate_image(args.pixels_per_square)

    frame_shape = (720, 1280)
    # A synthetic camera, not a guess: selftest renders the board *and* detects it
    # through this same model, so what matters is that it is self-consistent and
    # plausible, never that it matches any real device. Roughly the focal length a
    # 720p webcam of this class actually has.
    intrinsics = {"fx": 1030.0, "fy": 1030.0, "cx": 640.0, "cy": 360.0}
    if all(v is not None for v in (args.fx, args.fy, args.cx, args.cy)):
        intrinsics = {"fx": args.fx, "fy": args.fy, "cx": args.cx, "cy": args.cy}

    # Ground truth: a plausible wrist mount, 2 cm across and 5 cm forward of the
    # link origin, tilted off axis so a solver that only recovers translation
    # cannot pass by accident.
    truth = make_transform(
        quat_wxyz_to_matrix([0.9469, 0.2391, -0.1503, 0.1503]),
        [0.021, -0.034, 0.052],
    )
    base_to_target = make_transform(
        quat_wxyz_to_matrix([0.7071, 0.7071, 0.0, 0.0]), [0.28, -0.05, 0.11]
    )

    records: List[Dict[str, Any]] = []
    for index in range(args.samples):
        camera_to_target = synthetic_camera_to_target(board, index, args.distance)
        # base_to_gripper is *derived* so the data is exactly consistent with
        # `truth`, which is what makes the recovery error meaningful.
        base_to_gripper = base_to_target @ invert(camera_to_target) @ invert(truth)

        frame = render_board_view(
            board,
            board_image,
            args.pixels_per_square,
            camera_to_target,
            intrinsics,
            frame_shape,
            args.noise_sigma,
        )
        detection = detect(frame, board, intrinsics, None)
        raw_path = samples_dir / f"{index:03d}_raw.png"
        cv2.imwrite(str(raw_path), frame)
        write_overlay(samples_dir / f"{index:03d}_overlay.png", frame, detection, intrinsics, None)
        record = {
            "index": index,
            "captured_at": utc_now(),
            "raw_frame": raw_path.name,
            "overlay_frame": f"{index:03d}_overlay.png",
            "frame_shape": list(frame.shape),
            "synthetic": True,
            "base_to_gripper": base_to_gripper.tolist(),
            "base_to_gripper_pose": describe_pose(base_to_gripper),
            "ground_truth_camera_to_target": camera_to_target.tolist(),
            **{k: v for k, v in detection.items() if not k.startswith("_")},
        }
        write_json(samples_dir / f"{index:03d}.json", record)
        if detection["detected"]:
            records.append(record)
        else:
            print(f"  sample {index}: NOT DETECTED -- {_reject_hint(record)}")

    config = {
        "created_at": utc_now(),
        "synthetic": True,
        "fk_frame": "<synthetic>",
        "urdf_path": None,
        "urdf_joint_names": list(URDF_JOINT_NAMES),
        "board": board.to_metadata(),
        "intrinsics": intrinsics,
        "intrinsics_source": "synthetic",
        "dist_coeffs": None,
        "frame_shape": list(frame_shape),
        "ground_truth": {
            "gripper_to_camera_optical": truth.tolist(),
            "gripper_to_camera_pose": describe_pose(truth),
            "base_to_target": base_to_target.tolist(),
        },
        "noise_sigma": args.noise_sigma,
        "versions": {
            "python": sys.version.split()[0],
            "cv2": cv2.__version__,
            "numpy": np.__version__,
        },
    }
    write_json(run_dir / "run.json", config)

    if len(records) < 3:
        raise SystemExit(
            f"Only {len(records)} of {args.samples} synthetic views were detected -- the "
            "renderer and the detector disagree, so this build cannot be trusted."
        )

    results = solve_methods(
        [np.asarray(r["base_to_gripper"], dtype=float) for r in records],
        [np.asarray(r["camera_to_target"], dtype=float) for r in records],
        [int(r["index"]) for r in records],
        np.eye(4),
    )

    print("\n=== recovery against ground truth ===")
    # Individual solvers are allowed to fail here. METHODS documents that OpenCV's
    # implementations differ in robustness across versions, so one refusing on a
    # well-conditioned set is an OpenCV quirk, not a fault in this pipeline. What
    # must hold is that the default method solves and that the survivors agree
    # with truth.
    worst_error = 0.0
    failed: List[str] = []
    for method in sorted(results):
        data = results[method]
        if "error" in data:
            print(f"  {method:<11} FAILED  {data['error']}: {data['message'].strip()}")
            failed.append(method)
            continue
        solved = np.asarray(data["gripper_to_camera_optical"], dtype=float)
        translation = float(np.linalg.norm(solved[:3, 3] - truth[:3, 3]))
        rotation = rotation_angle_deg((invert(solved) @ truth)[:3, :3])
        data["ground_truth_error"] = {
            "translation_m": round(translation, 6),
            "rotation_deg": round(rotation, 4),
        }
        print(
            f"  {method:<11} off by {translation * 1000:6.3f} mm / {rotation:6.4f} deg   "
            f"(residual {data['residual_translation_m'] * 1000:6.3f} mm)"
        )
        worst_error = max(worst_error, translation)

    payload = {
        "solved_at": utc_now(),
        "synthetic": True,
        "samples_used": [int(r["index"]) for r in records],
        "board": board.to_metadata(),
        "intrinsics": intrinsics,
        "intrinsics_source": "synthetic",
        "ground_truth": config["ground_truth"],
        "default_method": DEFAULT_METHOD,
        "methods": results,
    }
    write_json(run_dir / "solve.json", payload)
    report(results, DEFAULT_METHOD)
    print(f"\nrun saved: {run_dir}")

    if DEFAULT_METHOD in failed:
        print(
            f"\nFAIL: {DEFAULT_METHOD!r} did not solve this synthetic set at all. "
            "Check the OpenCV version before looking at any hardware run."
        )
        return 1
    if worst_error > args.tolerance:
        print(
            f"\nFAIL: worst recovery error {worst_error * 1000:.3f} mm exceeds the "
            f"{args.tolerance * 1000:.1f} mm tolerance. The pipeline itself is off -- "
            "check the OpenCV version before looking at any hardware run."
        )
        return 1
    if failed:
        print(
            f"\nnote: {', '.join(failed)} refused this set. Known OpenCV variation "
            f"between solvers -- {DEFAULT_METHOD!r} is the default precisely because it "
            "has the wider margin."
        )
    print(
        f"\nPASS: {DEFAULT_METHOD!r} recovered the mount to within "
        f"{worst_error * 1000:.3f} mm of truth. The detection, FK convention, solver "
        "and frame handling in this tool are sound, so a bad hardware run is a bad "
        "input: intrinsics, --square-size, follower calibration, --fk-frame, or a "
        "board/frame that moved."
    )
    return 0


def cmd_intrinsics(args: argparse.Namespace) -> int:
    """Fit real intrinsics from a run's saved frames.

    FOV-derived intrinsics are a guess, and a wrong focal length scales every
    solved translation. The frames a run already saved are enough to measure the
    real ones, so this turns the biggest error term into a known quantity without
    capturing anything new.
    """
    import cv2

    run_dir = Path(args.run)
    config, samples = load_run(run_dir)
    board = build_board(args.board_override or config["board"])

    object_points: List[np.ndarray] = []
    image_points: List[np.ndarray] = []
    used: List[int] = []
    shape: Optional[Tuple[int, int]] = None
    for sample in samples:
        frame = cv2.imread(str(run_dir / "samples" / sample["raw_frame"]))
        if frame is None:
            logger.warning("Missing frame for sample %s", sample["index"])
            continue
        shape = (frame.shape[1], frame.shape[0])
        correspondences = board.correspondences(cv2, frame)
        if correspondences is None:
            continue
        objects, images = correspondences
        object_points.append(objects.astype(np.float32).reshape(-1, 1, 3))
        image_points.append(images.astype(np.float32).reshape(-1, 1, 2))
        used.append(int(sample["index"]))

    if len(used) < 4 or shape is None:
        raise SystemExit(
            f"Only {len(used)} usable views. cv2.calibrateCamera needs a handful of "
            "well-spread views of the board; capture more, then re-run."
        )

    rms, matrix, distortion, _rvecs, _tvecs = cv2.calibrateCamera(
        object_points, image_points, shape, None, None
    )
    fitted = {
        "fx": float(matrix[0, 0]),
        "fy": float(matrix[1, 1]),
        "cx": float(matrix[0, 2]),
        "cy": float(matrix[1, 2]),
    }
    payload = {
        "fitted_at": utc_now(),
        "views_used": used,
        "image_size": list(shape),
        "rms_reprojection_error_px": round(float(rms), 4),
        "intrinsics": fitted,
        "dist_coeffs": [round(float(v), 8) for v in np.asarray(distortion).ravel()],
        "compared_to_run": {
            "intrinsics": config["intrinsics"],
            "source": config["intrinsics_source"],
        },
    }
    write_json(run_dir / "intrinsics.json", payload)

    guess = run_intrinsics(config, run_dir)
    print(f"\nfitted from {len(used)} views, RMS {rms:.3f} px")
    for key in ("fx", "fy", "cx", "cy"):
        delta = fitted[key] - float(guess[key])
        print(
            f"  {key}: {fitted[key]:9.2f}   run used {float(guess[key]):9.2f}   "
            f"delta {delta:+8.2f} ({delta / float(guess[key]) * 100:+6.2f}%)"
        )
    print(
        "\nA focal-length error scales every solved translation by the same fraction. "
        "Re-solve against these with:\n"
        f"  python -m scripts.cw_handeye_debug solve --run {run_dir} --intrinsics-from-fit"
    )
    print(f"wrote {run_dir / 'intrinsics.json'}")
    return 0


def cmd_solve(args: argparse.Namespace) -> int:
    """Re-solve a saved run offline, optionally changing one input at a time."""
    import cv2
    from cyberwave.calibration import DEFAULT_METHOD

    from utils.cw_handeye import HANDEYE_MIN_SAMPLES

    run_dir = Path(args.run)
    config, samples = load_run(run_dir)
    attach_log_file(run_dir / "run.log")

    board_spec = args.board_override or config["board"]
    board = build_board(board_spec)

    # Precedence, highest first: a previous fit, explicit CLI values, then whatever
    # the run itself recorded. The run's own value is resolved *last* so a run that
    # recorded none (the driver solved them at solve time) can still be re-solved by
    # supplying them, instead of being refused before the override is even read.
    dist_coeffs = config.get("dist_coeffs")
    if args.intrinsics_from_fit:
        fit_path = run_dir / "intrinsics.json"
        if not fit_path.exists():
            raise SystemExit(
                f"{fit_path} does not exist. Fit the intrinsics first:\n"
                f"    python -m scripts.cw_handeye_debug intrinsics --run {run_dir}"
            )
        fit = json.loads(fit_path.read_text())
        intrinsics = fit["intrinsics"]
        intrinsics_source = "fitted"
        if args.use_fitted_distortion:
            dist_coeffs = fit["dist_coeffs"]
    elif all(v is not None for v in (args.fx, args.fy, args.cx, args.cy)):
        intrinsics = {"fx": args.fx, "fy": args.fy, "cx": args.cx, "cy": args.cy}
        intrinsics_source = "cli"
    else:
        intrinsics = run_intrinsics(config, run_dir)
        intrinsics_source = config["intrinsics_source"]
    if args.dist_coeffs:
        dist_coeffs = json.loads(args.dist_coeffs)
    # Say which source won. Several can be supplied at once, and silently applying
    # one of them is how a re-solve gets attributed to the wrong input.
    logger.info("Solving with %s intrinsics: %s", intrinsics_source, intrinsics)

    drop = set(args.drop or ())
    keep = set(args.keep) if args.keep else None

    kept: List[Dict[str, Any]] = []
    redetected: List[Dict[str, Any]] = []
    for sample in samples:
        index = int(sample["index"])
        if index in drop or (keep is not None and index not in keep):
            continue

        if args.no_redetect:
            if not sample.get("detected"):
                continue
            camera_to_target = np.asarray(sample["camera_to_target"], dtype=float)
        else:
            # Re-detect from the raw frame so a changed board spec or changed
            # intrinsics actually take effect. This is the whole point of keeping
            # the frames: the change is the only thing that differs.
            frame = cv2.imread(str(run_dir / "samples" / sample["raw_frame"]))
            if frame is None:
                logger.warning("Missing frame for sample %s", index)
                continue
            detection = detect(frame, board, intrinsics, dist_coeffs)
            redetected.append(
                {
                    "index": index,
                    "was_detected": bool(sample.get("detected")),
                    **{k: v for k, v in detection.items() if not k.startswith("_")},
                }
            )
            if not detection["detected"]:
                continue
            camera_to_target = np.asarray(detection["camera_to_target"], dtype=float)

        kept.append(
            {
                "index": index,
                "base_to_gripper": sample["base_to_gripper"],
                "camera_to_target": camera_to_target.tolist(),
            }
        )

    if len(kept) < HANDEYE_MIN_SAMPLES:
        raise SystemExit(
            f"Only {len(kept)} usable samples (need {HANDEYE_MIN_SAMPLES}). "
            + (
                "Re-detection rejected samples the run had accepted -- the board spec "
                "or intrinsics you passed do not match the frames."
                if redetected and any(r["was_detected"] and not r["detected"] for r in redetected)
                else "Capture more."
            )
        )

    sensor_offset = sensor_offset_from_arg(args.sensor_offset)
    results = solve_methods(
        [np.asarray(s["base_to_gripper"], dtype=float) for s in kept],
        [np.asarray(s["camera_to_target"], dtype=float) for s in kept],
        [s["index"] for s in kept],
        sensor_offset,
    )
    payload = {
        "solved_at": utc_now(),
        "source_run": str(run_dir),
        "redetected": not args.no_redetect,
        "samples_used": [s["index"] for s in kept],
        "samples_dropped": sorted(drop),
        "board": board.to_metadata(),
        "intrinsics": intrinsics,
        "intrinsics_source": intrinsics_source,
        "dist_coeffs": dist_coeffs,
        "sensor_offset": sensor_offset.tolist(),
        "default_method": DEFAULT_METHOD,
        "methods": results,
        "detection": redetected,
    }
    out = Path(args.out) if args.out else run_dir / "solve.json"
    write_json(out, payload)

    print(f"\nrun {run_dir}: {len(kept)}/{len(samples)} samples, intrinsics {intrinsics_source}")
    if redetected:
        lost = [r["index"] for r in redetected if r["was_detected"] and not r["detected"]]
        gained = [r["index"] for r in redetected if not r["was_detected"] and r["detected"]]
        if lost:
            print(f"re-detection LOST samples {lost} -- the board spec is not what was captured")
        if gained:
            print(f"re-detection GAINED samples {gained}")
        errors = [r["reprojection_error_px"]["mean"] for r in redetected if r.get("detected")]
        if errors:
            print(
                f"reprojection error: mean {np.mean(errors):.2f} px, worst "
                f"{max(errors):.2f} px  (>1 px on sharp frames means the projection "
                "model is wrong; a low value does NOT vouch for the focal length)"
            )
    report(results, DEFAULT_METHOD)
    print(f"\nwrote {out}")
    return 0


# --- cli ------------------------------------------------------------------


def add_board_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--board-type", choices=("charuco", "checkerboard"), default="charuco")
    parser.add_argument(
        "--squares",
        type=int,
        nargs=2,
        default=[23, 12],
        metavar=("COLS", "ROWS"),
        help="charuco: total square count, not interior corners. COLS then ROWS -- "
        "transposing these finds zero corners even when every marker is read",
    )
    parser.add_argument(
        "--inner-corners",
        type=int,
        nargs=2,
        default=[9, 6],
        metavar=("COLS", "ROWS"),
        help="checkerboard: INTERIOR corners (a 10x7-square board has 9x6)",
    )
    parser.add_argument(
        "--square-size",
        type=float,
        default=0.011,
        help="metres. Measure the print with calipers -- the only scale input",
    )
    parser.add_argument("--marker-size", type=float, default=0.008, help="metres")
    # 23x12 needs cols*rows//2 = 138 marker ids; DICT_4X4_250 holds 250. Too small
    # a dictionary fails only at image generation, not construction.
    parser.add_argument("--dictionary", default="DICT_4X4_250")


def add_intrinsics_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--fx", type=float)
    parser.add_argument("--fy", type=float)
    parser.add_argument("--cx", type=float)
    parser.add_argument("--cy", type=float)
    parser.add_argument("--dist-coeffs", help='JSON list, e.g. "[0.1,-0.2,0,0,0]". Default: none')


def add_solve_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sensor-offset",
        help='camera twin sensor offset as JSON, e.g. \'{"position":{"z":0.01}}\'. '
        "Default identity",
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="so101-handeye-debug",
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p_board = sub.add_parser("board", help="render the board spec to a printable PNG")
    add_board_args(p_board)
    p_board.add_argument("--out", default="charuco_board.png")
    p_board.add_argument("--pixels-per-square", type=int, default=120)
    p_board.set_defaults(func=cmd_board)

    p_capture = sub.add_parser("capture", help="live interactive run against the arm")
    add_board_args(p_capture)
    add_intrinsics_args(p_capture)
    add_solve_args(p_capture)
    p_capture.add_argument("--port", default="/dev/ttyACM0", help="follower serial port")
    p_capture.add_argument("--follower-id", default="follower1")
    p_capture.add_argument("--device", default="/dev/video0", help="wrist camera device")
    p_capture.add_argument(
        "--fk-frame",
        default="gripper",
        help="URDF link the camera is mounted on. Must be the real one: a wrong link "
        "yields a wrong transform with a good residual",
    )
    p_capture.add_argument("--urdf", help="override the SO101 URDF path")
    p_capture.add_argument("--out", help=f"run directory (default {DEFAULT_RUN_ROOT}/<ts>)")
    p_capture.set_defaults(func=cmd_capture)

    p_self = sub.add_parser(
        "selftest", help="solve a synthetic run with a known answer; proves the pipeline"
    )
    add_board_args(p_self)
    add_intrinsics_args(p_self)
    add_solve_args(p_self)
    p_self.add_argument("--samples", type=int, default=12)
    p_self.add_argument("--distance", type=float, default=0.30, help="board distance, m")
    p_self.add_argument("--pixels-per-square", type=int, default=200)
    p_self.add_argument(
        "--noise-sigma",
        type=float,
        default=0.0,
        help="gaussian grey-level noise (0-255 scale) to inject into the rendered "
        "frames, to see how sensor noise maps onto the residual",
    )
    p_self.add_argument("--tolerance", type=float, default=0.002, help="pass/fail bar in metres")
    p_self.add_argument("--out", help="run directory")
    p_self.set_defaults(func=cmd_selftest)

    p_intr = sub.add_parser("intrinsics", help="fit intrinsics from a run's saved frames")
    p_intr.add_argument("--run", required=True)
    p_intr.add_argument(
        "--board-override", type=json.loads, help="board spec as JSON, to re-detect with"
    )
    p_intr.set_defaults(func=cmd_intrinsics)

    p_solve = sub.add_parser("solve", help="re-solve a saved run offline")
    add_intrinsics_args(p_solve)
    add_solve_args(p_solve)
    p_solve.add_argument("--run", required=True)
    p_solve.add_argument(
        "--board-override", type=json.loads, help="board spec as JSON, to re-detect with"
    )
    p_solve.add_argument(
        "--intrinsics-from-fit",
        action="store_true",
        help="use intrinsics.json from the 'intrinsics' subcommand",
    )
    p_solve.add_argument(
        "--use-fitted-distortion",
        action="store_true",
        help="with --intrinsics-from-fit, also apply the fitted distortion",
    )
    p_solve.add_argument(
        "--no-redetect",
        action="store_true",
        help="reuse the stored board poses instead of re-detecting; isolates a solver "
        "question from a detection one",
    )
    p_solve.add_argument(
        "--drop", type=int, nargs="+", help="sample indices to exclude"
    )
    p_solve.add_argument(
        "--keep", type=int, nargs="+", help="only these sample indices"
    )
    p_solve.add_argument("--out", help="write the result here instead of <run>/solve.json")
    p_solve.set_defaults(func=cmd_solve)

    args = parser.parse_args(argv)
    console_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=console_level, format="%(levelname)-7s %(message)s")
    # attach_log_file drops the root logger to DEBUG so the run log is complete;
    # pin the console handler so that does not also turn the terminal into a firehose.
    for handler in logging.getLogger().handlers:
        handler.setLevel(console_level)
    try:
        return int(args.func(args))
    except KeyboardInterrupt:
        print("\ninterrupted")
        return 130
    except (RuntimeError, FileNotFoundError, OSError) as exc:
        # These are device and file problems, i.e. things the operator fixes. The
        # traceback adds nothing; anything genuinely unexpected still raises.
        logger.error("%s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
