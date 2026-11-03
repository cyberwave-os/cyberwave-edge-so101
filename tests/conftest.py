"""Pytest configuration and fixtures for SO101 tests."""

from __future__ import annotations

import math
import os
import site
import sys
import time
import types
from enum import Enum
from pathlib import Path


def _ensure_cmeel_lib_path() -> None:
    """Expose cmeel-bundled shared libs (urdfdom, tinyxml2) before importing pin."""
    for site_dir in site.getsitepackages():
        lib_dir = Path(site_dir) / "cmeel.prefix" / "lib"
        if not lib_dir.is_dir():
            continue
        lib_str = str(lib_dir.resolve())
        current = os.environ.get("LD_LIBRARY_PATH", "")
        if lib_str not in {part for part in current.split(":") if part}:
            os.environ["LD_LIBRARY_PATH"] = (
                f"{lib_str}:{current}" if current else lib_str
            )
        break


_ensure_cmeel_lib_path()

def _real_sdk_available() -> bool:
    """Whether the installed SDK carries the hand-eye flow.

    The doubles below let the suite run with no SDK at all, which is what CI on a
    bare checkout needs. But the hand-eye flow, its config and its alert rendering
    now *live* in ``cyberwave.calibration``, so faking that package would mean
    testing the driver's integration against a fiction -- a fake that drifted from
    the real flow would keep passing. So when the real SDK is importable, use it.
    """
    import importlib.util

    try:
        return importlib.util.find_spec("cyberwave.calibration.flow") is not None
    except (ImportError, ValueError):
        return False


# Install cyberwave mock BEFORE any project imports (main, so101, utils, etc.)
# so that tests can run without the real cyberwave SDK.
if "cyberwave" not in sys.modules and not _real_sdk_available():
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
    _utils.TimeReference = type(
        "TimeReference",
        (),
        {
            "update": lambda self: (time.time(), time.monotonic()),
            "read": lambda self: (time.time(), time.monotonic()),
        },
    )
    sys.modules["cyberwave.utils"] = _utils

    _edge = types.ModuleType("cyberwave.edge")
    sys.modules["cyberwave.edge"] = _edge

    _edge_health = types.ModuleType("cyberwave.edge.health")
    _edge_health.EdgeHealthCheck = type(
        "EdgeHealthCheck",
        (),
        {
            "__init__": lambda self, *a, **k: None,
            "start": lambda self: None,
            "stop": lambda self: None,
            "register_stream_config": lambda self, *a, **k: None,
            "update_frame_count": lambda self: None,
            "edge_id": "",
        },
    )
    _edge.health = _edge_health
    sys.modules["cyberwave.edge.health"] = _edge_health

    # --- cyberwave.exceptions / .calibration / .driver.kinematics.arm ---------
    #
    # Used by utils/cw_handeye.py. The board classes only record what they were
    # constructed with: the real ones' validation is covered by the SDK's own
    # tests/test_calibration_board.py, and duplicating it here would just couple
    # the driver suite to SDK internals. What the driver tests own is the flow --
    # state transitions, the alert contract, device lifecycle.
    _exceptions = types.ModuleType("cyberwave.exceptions")

    class _CyberwaveError(Exception):
        pass

    class _CyberwaveValidationError(_CyberwaveError):
        pass

    _exceptions.CyberwaveError = _CyberwaveError
    _exceptions.CyberwaveValidationError = _CyberwaveValidationError
    sys.modules["cyberwave.exceptions"] = _exceptions

    _calibration = types.ModuleType("cyberwave.calibration")

    class _FakeBoard:
        """Records its kwargs so board_from_spec's merge can be asserted on."""

        _kind = "charuco"

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            # Mirror the real board's attributes, not just the kwargs dict: the
            # SDK's boards are dataclasses, so callers read ``board.marker_size_m``
            # directly. A double that only kept ``kwargs`` silently made every such
            # read return nothing, which is a passing test for broken code.
            for key, value in kwargs.items():
                setattr(self, key, value)
            if kwargs.get("marker_size_m", 0) >= kwargs.get("square_size_m", 1):
                raise _CyberwaveValidationError("marker must be smaller than square")

        def to_metadata(self):
            meta = {"type": self._kind}
            for key, value in self.kwargs.items():
                meta[key] = list(value) if isinstance(value, tuple) else value
            return meta

    class _FakeCharucoBoard(_FakeBoard):
        _kind = "charuco"

    class _FakeCheckerBoard(_FakeBoard):
        _kind = "checkerboard"

    class _BoardNotDetectedError(_CyberwaveValidationError):
        pass

    class _HandEyeDegenerateError(_CyberwaveValidationError):
        pass

    _calibration.CharucoBoard = _FakeCharucoBoard
    _calibration.CheckerBoard = _FakeCheckerBoard
    _calibration.BoardNotDetectedError = _BoardNotDetectedError
    _calibration.HandEyeDegenerateError = _HandEyeDegenerateError

    class _FakeHandEyeSession:
        """Minimal working stand-in: the real one is built with kwargs and used.

        A bare ``type(...)`` stub rejects the constructor call, which forces tests
        to bypass the code path that builds it -- including the intrinsics-source
        selection that only happens there.
        """

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.sample_count = 0
            # None mirrors a session given intrinsics up front: nothing solved.
            self.solved_intrinsics = None

        def add_sample(self, **_kwargs):
            self.sample_count += 1

        def solve(self):
            raise AssertionError("tests should inject their own result")

        def clear(self):
            self.sample_count = 0

    _calibration.HandEyeSession = _FakeHandEyeSession
    _calibration.MIN_SAMPLES = 3
    # Mark it a package: the driver imports the ``.frames`` and ``.board``
    # submodules, and a plain module rejects that with a confusing "not a
    # package".
    _calibration.__path__ = []  # type: ignore[attr-defined]
    sys.modules["cyberwave.calibration"] = _calibration

    # These two carry real implementations rather than doubles. They are small,
    # dependency-free and pure (SE(3) algebra and a camera matrix), so faking them
    # would only let a caller's bug pass by agreeing with the fake.
    _frames = types.ModuleType("cyberwave.calibration.frames")

    def _matrix_to_quat_wxyz(rotation):
        import numpy as np

        m = np.asarray(rotation, dtype=float)[:3, :3]
        trace = m[0, 0] + m[1, 1] + m[2, 2]
        if trace > 0.0:
            s = math.sqrt(trace + 1.0) * 2.0
            q = (0.25 * s, (m[2, 1] - m[1, 2]) / s, (m[0, 2] - m[2, 0]) / s, (m[1, 0] - m[0, 1]) / s)
        elif m[0, 0] > m[1, 1] and m[0, 0] > m[2, 2]:
            s = math.sqrt(1.0 + m[0, 0] - m[1, 1] - m[2, 2]) * 2.0
            q = ((m[2, 1] - m[1, 2]) / s, 0.25 * s, (m[0, 1] + m[1, 0]) / s, (m[0, 2] + m[2, 0]) / s)
        elif m[1, 1] > m[2, 2]:
            s = math.sqrt(1.0 + m[1, 1] - m[0, 0] - m[2, 2]) * 2.0
            q = ((m[0, 2] - m[2, 0]) / s, (m[0, 1] + m[1, 0]) / s, 0.25 * s, (m[1, 2] + m[2, 1]) / s)
        else:
            s = math.sqrt(1.0 + m[2, 2] - m[0, 0] - m[1, 1]) * 2.0
            q = ((m[1, 0] - m[0, 1]) / s, (m[0, 2] + m[2, 0]) / s, (m[1, 2] + m[2, 1]) / s, 0.25 * s)
        return tuple(-v for v in q) if q[0] < 0.0 else q

    def _rotation_angle_deg(rotation):
        import numpy as np

        m = np.asarray(rotation, dtype=float)[:3, :3]
        cos_angle = (m[0, 0] + m[1, 1] + m[2, 2] - 1.0) / 2.0
        return float(math.degrees(math.acos(float(np.clip(cos_angle, -1.0, 1.0)))))

    def _rotation_axis(rotation):
        import numpy as np

        m = np.asarray(rotation, dtype=float)[:3, :3]
        axis = np.array([m[2, 1] - m[1, 2], m[0, 2] - m[2, 0], m[1, 0] - m[0, 1]])
        norm = float(np.linalg.norm(axis))
        return None if norm < 1e-9 else axis / norm

    def _describe_pose(transform):
        import numpy as np

        m = np.asarray(transform, dtype=float)
        axis = _rotation_axis(m[:3, :3])
        return {
            "position_m": [round(float(v), 6) for v in m[:3, 3]],
            "quaternion_wxyz": [
                round(float(v), 6) for v in _matrix_to_quat_wxyz(m[:3, :3])
            ],
            "rotation_angle_deg": round(_rotation_angle_deg(m[:3, :3]), 4),
            "rotation_axis": None if axis is None else [round(float(v), 6) for v in axis],
        }

    _frames.matrix_to_quat_wxyz = _matrix_to_quat_wxyz
    _frames.rotation_angle_deg = _rotation_angle_deg
    _frames.rotation_axis = _rotation_axis
    _frames.describe_pose = _describe_pose
    sys.modules["cyberwave.calibration.frames"] = _frames
    _calibration.frames = _frames

    _board = types.ModuleType("cyberwave.calibration.board")

    def _camera_matrix(intrinsics):
        import numpy as np

        fx, fy, cx, cy = (float(intrinsics[k]) for k in ("fx", "fy", "cx", "cy"))
        return np.array([[fx, 0.0, cx], [0.0, fy, cy], [0.0, 0.0, 1.0]])

    def _distortion_vector(dist_coeffs):
        import numpy as np

        if dist_coeffs is None:
            return np.zeros((1, 5))
        return np.asarray(dist_coeffs, dtype=float).reshape(1, -1)

    def _resolve_dictionary(cv2, name):
        return cv2.aruco.getPredefinedDictionary(getattr(cv2.aruco, str(name).upper()))

    _board.camera_matrix = _camera_matrix
    _board.distortion_vector = _distortion_vector
    _board.resolve_dictionary = _resolve_dictionary
    sys.modules["cyberwave.calibration.board"] = _board
    _calibration.board = _board

    _driver = types.ModuleType("cyberwave.driver")
    _kinematics = types.ModuleType("cyberwave.driver.kinematics")
    _kin_arm = types.ModuleType("cyberwave.driver.kinematics.arm")

    class _FakeKwargs:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        def fk(self, positions):
            raise AssertionError("tests should inject their own kinematics")

    _kin_arm.ArmKinematicsConfig = _FakeKwargs
    _kin_arm.BaseKinematicsManipulator = _FakeKwargs
    _kinematics.arm = _kin_arm
    _driver.kinematics = _kinematics
    sys.modules["cyberwave.driver"] = _driver
    sys.modules["cyberwave.driver.kinematics"] = _kinematics
    sys.modules["cyberwave.driver.kinematics.arm"] = _kin_arm
