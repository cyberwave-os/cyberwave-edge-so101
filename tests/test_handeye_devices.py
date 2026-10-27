"""Tests for the SO-101 hand-eye device adapters (``utils/cw_handeye_devices.py``).

These cover the device-specific behaviour that used to live inside the flow: the
V4L2 buffer drain, the backend pinning, the MJPG-before-size ordering, and the
bus connect failure. cv2 is stubbed -- what is under test is the sequence of
calls the adapter makes, because that sequence *is* the measured workaround.
"""

from __future__ import annotations

import sys
import types
from typing import Any, Dict, List, Optional, Tuple

import pytest
from cyberwave.calibration import HandEyeError

from utils.cw_handeye_devices import (
    FreedriveJointSource,
    V4L2FrameSource,
    _fourcc_name,
)

# --- cv2 stub --------------------------------------------------------------


class FakeVideoCapture:
    """Records every property set, in order, so ordering can be asserted."""

    def __init__(self, source: Any, backend: Any = None) -> None:
        self.source = source
        self.backend = backend
        self.opened = True
        self.released = False
        self.grabs = 0
        self.reads = 0
        #: ``(prop, value)`` in call order -- MJPG must precede the size.
        self.sets: List[Tuple[int, Any]] = []
        self.props: Dict[int, Any] = {
            _CAP_PROP_FRAME_WIDTH: 1280,
            _CAP_PROP_FRAME_HEIGHT: 720,
            _CAP_PROP_FOURCC: 1196444237,  # "MJPG"
        }
        self.read_ok = True

    def isOpened(self) -> bool:  # noqa: N802 - cv2's name
        return self.opened

    def set(self, prop: int, value: Any) -> bool:
        self.sets.append((prop, value))
        self.props[prop] = value
        return True

    def get(self, prop: int) -> Any:
        # -1, not 0, for a property this capture does not carry: that is what
        # OpenCV answers for one its backend cannot supply. The old default of 0
        # meant the suite could not see an unsupported control at all, which is
        # how a preflight that accuses fixed-focus cameras of autofocus shipped.
        return self.props.get(prop, -1)

    def grab(self) -> bool:
        self.grabs += 1
        return True

    def read(self):
        import numpy as np

        self.reads += 1
        if not self.read_ok:
            return False, None
        return True, np.zeros((720, 1280, 3), dtype=np.uint8)

    def release(self) -> None:
        self.released = True


_CAP_V4L2 = 200
_CAP_PROP_FRAME_WIDTH = 3
_CAP_PROP_FRAME_HEIGHT = 4
_CAP_PROP_FOURCC = 6
_CAP_PROP_BUFFERSIZE = 38


@pytest.fixture
def fake_cv2(monkeypatch):
    """Install a stub ``cv2`` for the duration of a test.

    The adapter imports cv2 inside ``open()``, so patching sys.modules is enough
    and no real OpenCV is needed.
    """
    created: List[FakeVideoCapture] = []

    def _video_capture(source, backend=None):
        cap = FakeVideoCapture(source, backend)
        created.append(cap)
        return cap

    module = types.SimpleNamespace(
        VideoCapture=_video_capture,
        CAP_V4L2=_CAP_V4L2,
        CAP_PROP_FRAME_WIDTH=_CAP_PROP_FRAME_WIDTH,
        CAP_PROP_FRAME_HEIGHT=_CAP_PROP_FRAME_HEIGHT,
        CAP_PROP_FOURCC=_CAP_PROP_FOURCC,
        CAP_PROP_BUFFERSIZE=_CAP_PROP_BUFFERSIZE,
        VideoWriter_fourcc=lambda *cc: 1196444237,
    )
    monkeypatch.setitem(sys.modules, "cv2", module)
    module.created = created  # type: ignore[attr-defined]
    return module


def make_source(fake_cv2, device="/dev/video0", **kw) -> V4L2FrameSource:
    return V4L2FrameSource(
        device=device,
        capture_size=kw.pop("capture_size", (1280, 720)),
        error_factory=HandEyeError,
        **kw,
    )


# --- V4L2FrameSource -------------------------------------------------------


class TestV4L2Open:
    def test_a_dev_node_pins_the_v4l2_backend(self, fake_cv2):
        """Letting OpenCV choose sends a string path to FFmpeg's libavdevice V4L2
        demuxer, which spams QBUF errors and makes CAP_PROP_FOURCC a no-op."""
        source = make_source(fake_cv2, "/dev/video2")
        source.open()

        (cap,) = fake_cv2.created
        assert cap.source == "/dev/video2"
        assert cap.backend == _CAP_V4L2

    def test_an_http_url_keeps_the_default_backend(self, fake_cv2):
        """_resolve_camera_device_for_twin can legitimately return an MJPEG URL,
        which V4L2 cannot open."""
        source = make_source(fake_cv2, "http://camera.local/stream.mjpg")
        source.open()

        (cap,) = fake_cv2.created
        assert cap.backend is None

    def test_an_integer_index_keeps_the_default_backend(self, fake_cv2):
        source = make_source(fake_cv2, 0)
        source.open()

        (cap,) = fake_cv2.created
        assert cap.source == 0
        assert cap.backend is None

    def test_mjpg_is_requested_before_the_size(self, fake_cv2):
        """Order matters: V4L2 renegotiates the format, and setting the size first
        lets the driver pick a YUYV mode the later fourcc cannot change.

        Uncompressed 720p YUYV is ~27 MB/s -- at the USB 2.0 ceiling, where frame
        grabs measured 500-1250 ms and the arm can drift mid-capture.
        """
        source = make_source(fake_cv2)
        source.open()

        (cap,) = fake_cv2.created
        props = [prop for prop, _ in cap.sets]
        assert props.index(_CAP_PROP_FOURCC) < props.index(_CAP_PROP_FRAME_WIDTH)
        assert props.index(_CAP_PROP_FOURCC) < props.index(_CAP_PROP_FRAME_HEIGHT)

    def test_the_driver_queue_is_shrunk_to_one_frame(self, fake_cv2):
        """So a capture cannot be served an image from before the arm moved."""
        source = make_source(fake_cv2)
        source.open()

        (cap,) = fake_cv2.created
        assert (_CAP_PROP_BUFFERSIZE, 1) in cap.sets

    def test_the_granted_size_is_reported_not_the_requested_one(self, fake_cv2):
        """V4L2 substitutes the nearest supported mode silently, and the
        difference decides whether a marker is decodable at working distance."""

        def _substituting(src, backend=None):
            cap = FakeVideoCapture(src, backend)
            # However the size is set, this camera only does VGA -- exactly the
            # silent substitution the adapter must report rather than hide.
            cap.set = lambda prop, value: cap.sets.append((prop, value)) or True
            cap.props[_CAP_PROP_FRAME_WIDTH] = 640
            cap.props[_CAP_PROP_FRAME_HEIGHT] = 480
            fake_cv2.created.append(cap)
            return cap

        fake_cv2.VideoCapture = _substituting
        source = make_source(fake_cv2, capture_size=(1280, 720))
        source.open()

        assert source.granted_size == (640, 480)

    def test_a_camera_that_will_not_open_raises_a_coded_error(self, fake_cv2):
        source = make_source(fake_cv2)

        def _closed(src, backend=None):
            cap = FakeVideoCapture(src, backend)
            cap.opened = False
            fake_cv2.created.append(cap)
            return cap

        fake_cv2.VideoCapture = _closed

        with pytest.raises(HandEyeError) as exc:
            source.open()

        assert exc.value.code == "camera_unavailable"
        # Released rather than leaked, even on the failure path.
        assert fake_cv2.created[-1].released

    def test_opening_twice_reuses_the_handle(self, fake_cv2):
        """Held open for the run so grabs stay cheap."""
        source = make_source(fake_cv2)
        source.open()
        source.open()

        assert len(fake_cv2.created) == 1


class TestV4L2Grab:
    def test_a_grab_drains_the_queue_first(self, fake_cv2):
        """THE staleness workaround. V4L2 hands back whatever is queued, which
        after a pause is an image from before the arm was moved."""
        source = make_source(fake_cv2)
        source.grab_fresh_frame()

        (cap,) = fake_cv2.created
        assert cap.grabs == 5
        assert cap.reads == 1

    def test_the_drain_depth_is_configurable(self, fake_cv2):
        """A different camera needs a different depth -- or none."""
        source = make_source(fake_cv2, drain_frames=2)
        source.grab_fresh_frame()

        (cap,) = fake_cv2.created
        assert cap.grabs == 2

    def test_a_grab_opens_the_device_on_demand(self, fake_cv2):
        source = make_source(fake_cv2)
        source.grab_fresh_frame()

        assert len(fake_cv2.created) == 1

    def test_a_failed_read_raises_a_coded_error(self, fake_cv2):
        source = make_source(fake_cv2)
        source.open()
        fake_cv2.created[0].read_ok = False

        with pytest.raises(HandEyeError) as exc:
            source.grab_fresh_frame()

        assert exc.value.code == "no_frame"

    def test_the_returned_frame_is_the_image(self, fake_cv2):
        source = make_source(fake_cv2)
        frame = source.grab_fresh_frame()

        assert frame.shape == (720, 1280, 3)


class TestV4L2Release:
    def test_release_frees_the_handle(self, fake_cv2):
        source = make_source(fake_cv2)
        source.open()
        source.release()

        assert fake_cv2.created[0].released

    def test_release_is_idempotent(self, fake_cv2):
        """close() runs on another thread than a capture, deliberately."""
        source = make_source(fake_cv2)
        source.open()
        source.release()
        source.release()  # must not raise

    def test_release_before_open_is_harmless(self, fake_cv2):
        make_source(fake_cv2).release()  # must not raise

    def test_a_released_source_reopens_on_the_next_grab(self, fake_cv2):
        source = make_source(fake_cv2)
        source.open()
        source.release()
        source.grab_fresh_frame()

        assert len(fake_cv2.created) == 2


# --- FreedriveJointSource --------------------------------------------------


class FakeFreedriveSession:
    instances: List["FakeFreedriveSession"] = []

    def __init__(self, *, client, robot, port, follower_id) -> None:
        self.port = port
        self.follower_id = follower_id
        self.connected = False
        self.disconnected = False
        self.connect_ok = True
        self.positions: Optional[Dict[str, float]] = {"_1": 0.5}
        FakeFreedriveSession.instances.append(self)

    def connect(self) -> bool:
        self.connected = self.connect_ok
        return self.connect_ok

    def read_joint_positions(self) -> Optional[Dict[str, float]]:
        return self.positions

    def disconnect(self) -> None:
        self.disconnected = True


@pytest.fixture
def fake_freedrive(monkeypatch):
    FakeFreedriveSession.instances = []
    module = types.SimpleNamespace(FreedriveSession=FakeFreedriveSession)
    monkeypatch.setitem(sys.modules, "utils.cw_freedrive", module)
    return FakeFreedriveSession


class FakeClient:
    def twin(self, twin_id=None, **kwargs):
        return object()


def make_joint_source() -> FreedriveJointSource:
    return FreedriveJointSource(
        client=FakeClient(),
        robot_twin_uuid="robot-uuid",
        port="/dev/ttyACM0",
        follower_id="follower1",
        error_factory=HandEyeError,
    )


class TestFreedriveJointSource:
    def test_connect_opens_the_named_port(self, fake_freedrive):
        source = make_joint_source()
        source.connect()

        (session,) = fake_freedrive.instances
        assert session.port == "/dev/ttyACM0"
        assert session.follower_id == "follower1"
        assert session.connected

    def test_a_bus_that_will_not_open_raises_a_coded_error(self, fake_freedrive):
        source = make_joint_source()

        original = fake_freedrive.connect

        def _refuse(self):
            self.connect_ok = False
            return original(self)

        fake_freedrive.connect = _refuse
        try:
            with pytest.raises(HandEyeError) as exc:
                source.connect()
        finally:
            fake_freedrive.connect = original

        assert exc.value.code == "bus_unavailable"
        # The message names the port, which is why connecting stays the node's job.
        assert "/dev/ttyACM0" in exc.value.message

    def test_reads_pass_straight_through(self, fake_freedrive):
        source = make_joint_source()
        source.connect()

        assert source.read_joint_positions() == {"_1": 0.5}

    def test_a_read_before_connect_reports_no_bus(self, fake_freedrive):
        """None means "the bus is gone", which the flow treats as a lost device."""
        assert make_joint_source().read_joint_positions() is None

    def test_a_lost_bus_is_forwarded_as_none(self, fake_freedrive):
        source = make_joint_source()
        source.connect()
        fake_freedrive.instances[0].positions = None

        assert source.read_joint_positions() is None

    def test_disconnect_releases_the_session(self, fake_freedrive):
        source = make_joint_source()
        source.connect()
        source.disconnect()

        assert fake_freedrive.instances[0].disconnected

    def test_disconnect_is_idempotent(self, fake_freedrive):
        source = make_joint_source()
        source.connect()
        source.disconnect()
        source.disconnect()  # must not raise

    def test_disconnect_before_connect_is_harmless(self, fake_freedrive):
        make_joint_source().disconnect()  # must not raise


# --- protocol conformance --------------------------------------------------


class TestProtocolConformance:
    """The adapters must satisfy the shapes the flow duck-types against.

    Structural, so there is no base class to inherit and nothing checks this at
    import. A second arm's adapters should assert the same way.
    """

    def test_the_joint_source_has_the_joint_source_surface(self):
        source = make_joint_source()
        assert callable(source.read_joint_positions)
        assert callable(source.disconnect)

    def test_the_frame_source_has_the_frame_source_surface(self, fake_cv2):
        source = make_source(fake_cv2)
        assert callable(source.grab_fresh_frame)
        assert callable(source.release)

    def test_the_frame_source_does_not_expose_cv2s_pair(self, fake_cv2):
        """grab()/read() would let the flow depend on a cv2-shaped source again,
        which is the coupling the single method exists to prevent."""
        source = make_source(fake_cv2)
        assert not hasattr(source, "grab")
        assert not hasattr(source, "read")


# --- fourcc decoding -------------------------------------------------------


class TestFourccName:
    @pytest.mark.parametrize(
        "code, expected",
        [
            (1196444237, "MJPG"),
            (1448695129, "YUYV"),
        ],
    )
    def test_a_real_fourcc_decodes(self, code, expected):
        assert _fourcc_name(code) == expected

    @pytest.mark.parametrize("value", [0, -1, None, "", "abc"])
    def test_anything_undecodable_is_unknown(self, value):
        assert _fourcc_name(value) == "unknown"


# --- preflight readout -----------------------------------------------------

_CAP_PROP_AUTOFOCUS = 39
_CAP_PROP_AUTO_EXPOSURE = 21


class TestDescribeSettings:
    def test_it_is_empty_before_open(self, fake_cv2):
        """Nothing to read back from a camera nobody has opened."""
        assert make_source(fake_cv2).describe_settings() == {}

    def test_it_reports_the_granted_mode_not_the_requested_one(self, fake_cv2):
        source = make_source(fake_cv2, capture_size=(1920, 1080))
        source.open()
        (cap,) = fake_cv2.created
        cap.props[_CAP_PROP_FRAME_WIDTH] = 640
        cap.props[_CAP_PROP_FRAME_HEIGHT] = 480
        source._granted_size = (640, 480)

        settings = source.describe_settings()
        assert settings["granted_size"] == (640, 480)
        assert settings["requested_size"] == (1920, 1080)
        assert settings["fourcc"] == "MJPG"

    def test_missing_constants_read_as_none_rather_than_raising(self, fake_cv2):
        """This OpenCV build has no autofocus property at all -- the stub is the
        case, and an AttributeError here would abort the run over a diagnostic."""
        source = make_source(fake_cv2)
        source.open()
        settings = source.describe_settings()
        assert settings["autofocus"] is None
        assert settings["auto_exposure"] is None

    def test_it_reads_the_controls_when_the_build_has_them(self, fake_cv2):
        fake_cv2.CAP_PROP_AUTOFOCUS = _CAP_PROP_AUTOFOCUS
        fake_cv2.CAP_PROP_AUTO_EXPOSURE = _CAP_PROP_AUTO_EXPOSURE
        source = make_source(fake_cv2)
        source.open()
        (cap,) = fake_cv2.created
        cap.props[_CAP_PROP_AUTOFOCUS] = 1
        cap.props[_CAP_PROP_AUTO_EXPOSURE] = 3

        settings = source.describe_settings()
        assert settings["autofocus"] == 1.0
        assert settings["auto_exposure"] == 3.0

    def test_an_unsupported_control_reads_as_none(self, fake_cv2):
        """The camera has no such control, so OpenCV answers its negative
        sentinel. Passing that on reads as "autofocus is on"."""
        fake_cv2.CAP_PROP_AUTOFOCUS = _CAP_PROP_AUTOFOCUS
        source = make_source(fake_cv2)
        source.open()
        (cap,) = fake_cv2.created
        cap.props[_CAP_PROP_AUTOFOCUS] = -1

        assert source.describe_settings()["autofocus"] is None

    def test_a_control_that_is_off_still_reads_as_zero(self, fake_cv2):
        """0 is ambiguous, not absent -- it stays a reading and the judgement
        upstream decides it proves nothing."""
        fake_cv2.CAP_PROP_AUTOFOCUS = _CAP_PROP_AUTOFOCUS
        source = make_source(fake_cv2)
        source.open()
        (cap,) = fake_cv2.created
        cap.props[_CAP_PROP_AUTOFOCUS] = 0

        assert source.describe_settings()["autofocus"] == 0.0

    def test_a_camera_that_refuses_a_get_reads_as_none(self, fake_cv2):
        """A UVC device may answer VIDIOC_G_CTRL with EINVAL for a control it
        advertises; OpenCV surfaces that as an exception, not a value."""
        fake_cv2.CAP_PROP_AUTOFOCUS = _CAP_PROP_AUTOFOCUS
        source = make_source(fake_cv2)
        source.open()
        (cap,) = fake_cv2.created

        def _boom(prop):
            if prop == _CAP_PROP_AUTOFOCUS:
                raise RuntimeError("EINVAL")
            return cap.props.get(prop, 0)

        cap.get = _boom
        assert source.describe_settings()["autofocus"] is None
