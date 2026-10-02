"""SO-101 device adapters for the hand-eye calibration flow.

The flow itself needs exactly two things from the hardware, and neither is
SO-101-specific as a *concept*:

* **A joint source** -- the arm's joint angles right now, read without energising
  it, so the arm stays hand-guidable between captures.
* **A frame source** -- an image captured *after* the arm came to rest.

Everything SO-101-specific about satisfying those lives here: the Feetech bus
opened through ``FreedriveSession``, and the V4L2/cv2 capture with its measured
bandwidth and buffering workarounds. The flow talks to both through the small
duck-typed protocols below, so a different arm or camera supplies its own pair
without the flow changing.

Why the frame source exposes one ``grab_fresh_frame()`` rather than cv2's
``grab()``/``read()`` pair: the freshness *technique* is device-specific. Here it
is a V4L2 buffer drain; a RealSense, a GigE camera or a ROS image topic each have
their own, and some need none at all. Putting one method in the contract keeps
the *obligation* ("this frame postdates the arm stopping") in the protocol and
the mechanism in the adapter -- which is the whole reason the flow can be reused.

Neither adapter is thread-safe on its own. The flow serialises access under its
own ``_device_lock``, and calls ``close()`` from a different thread than a
capture on purpose, so both teardown methods are idempotent and must stay so.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Protocol, Tuple

logger = logging.getLogger(__name__)


class JointSource(Protocol):
    """Reads the arm's joint angles now, without energising it.

    Keys are whatever the bus calls its joints; the flow maps them to URDF names.
    Returns ``None`` when the bus is gone (the flow treats that as a lost device)
    and ``{}`` on a transient empty read.

    ``FreedriveSession`` already satisfies this exactly -- the protocol was shaped
    around it rather than the other way round, so no wrapper is needed for the
    reading itself. ``connect()`` is deliberately absent: connecting is the node's
    business, because that is where the failure message knows which port it was.
    """

    def read_joint_positions(self) -> Optional[Dict[str, float]]: ...

    def disconnect(self) -> None: ...


class FrameSource(Protocol):
    """Grabs a frame captured after the arm came to rest.

    Raises rather than returning ``None`` on failure, so the flow gets a coded
    error it can put in front of the operator.
    """

    def grab_fresh_frame(self) -> Any: ...

    def release(self) -> None: ...


def _fourcc_name(value: Any) -> str:
    """Decode a V4L2 fourcc into its four characters, e.g. ``MJPG`` or ``YUYV``.

    The granted codec decides the frame-grab latency, so it belongs in the log
    next to the granted size. Falls back to the raw number for anything that does
    not decode to printable characters.
    """
    try:
        code = int(value)
    except (TypeError, ValueError):
        return "unknown"
    if code <= 0:
        return "unknown"
    chars = [chr((code >> shift) & 0xFF) for shift in (0, 8, 16, 24)]
    name = "".join(chars)
    return name if name.isprintable() and name.strip() else str(code)


class FreedriveJointSource:
    """Reads the SO-101 follower bus without ever enabling torque.

    ``FreedriveSession`` is the one bus path that never energises the arm, which
    is what keeps it hand-guidable between captures. This wrapper exists only to
    own the connect step and its error message; the reads pass straight through.
    """

    def __init__(
        self,
        *,
        client: Any,
        robot_twin_uuid: str,
        port: str,
        follower_id: str,
        error_factory: Any,
    ) -> None:
        self._client = client
        self._robot_twin_uuid = str(robot_twin_uuid)
        self._port = str(port)
        self._follower_id = str(follower_id)
        # Injected so this module does not import the flow it is consumed by.
        self._error = error_factory
        self._session: Any = None

    @property
    def port(self) -> str:
        """The bus this source is reading, for messages that name it."""
        return self._port

    def connect(self) -> None:
        """Open the bus. Raises the flow's own error type on failure."""
        from utils.cw_freedrive import FreedriveSession

        robot = self._client.twin(twin_id=self._robot_twin_uuid)
        session = FreedriveSession(
            client=self._client,
            robot=robot,
            port=self._port,
            follower_id=self._follower_id,
        )
        if not session.connect():
            raise self._error(
                "bus_unavailable",
                f"Could not open the follower bus on {self._port}. Check "
                "the arm is connected and powered, and that no other operation "
                "holds the port.",
            )
        self._session = session

    def read_joint_positions(self) -> Optional[Dict[str, float]]:
        if self._session is None:
            return None
        return self._session.read_joint_positions()

    def disconnect(self) -> None:
        session, self._session = self._session, None
        if session is not None:
            session.disconnect()


class V4L2FrameSource:
    """A local USB camera read through cv2, tuned for synchronous captures.

    The tuning here is measured, not defensive -- see the comments in
    :meth:`open`. Changing it changes whether a marker is decodable at working
    distance and whether a frame grab is fast enough that the arm has not drifted.
    """

    def __init__(
        self,
        *,
        device: Any,
        capture_size: Tuple[int, int],
        error_factory: Any,
        drain_frames: int = 5,
    ) -> None:
        self._device = device
        self._capture_size = capture_size
        self._error = error_factory
        #: Frames discarded before the one we keep. V4L2 hands back whatever is
        #: queued, which after a pause is an image from before the arm moved.
        self._drain_frames = int(drain_frames)
        self._capture: Any = None
        self._granted_size: Optional[Tuple[int, int]] = None

    @property
    def device(self) -> Any:
        return self._device

    @property
    def granted_size(self) -> Optional[Tuple[int, int]]:
        """``(width, height)`` the camera actually gave us, once opened.

        V4L2 substitutes the nearest supported mode silently, and the difference
        decides whether the board decodes -- so the caller reports this, never the
        requested size.
        """
        return self._granted_size

    def focus_value(self) -> Optional[float]:
        """The lens' current focus position, or ``None`` if it reports none.

        Read per capture, because what matters is whether the lens MOVED during a
        run -- that is what makes one fitted camera model wrong for some of the
        views. Asking the camera whether autofocus is off answers a different and
        weaker question: V4L2 reports 0 both for "off" and for "no such control",
        and an HTTP MJPEG source has no local lens to ask at all.

        A camera with no focus control returns a constant, which the caller reads
        as "never moved" and correctly acts on not at all.
        """
        capture = self._capture
        if capture is None:
            return None
        try:
            import cv2

            prop = getattr(cv2, "CAP_PROP_FOCUS", None)
            if prop is None:
                return None
            return float(capture.get(prop))
        except Exception:
            return None

    def open(self) -> None:
        if self._capture is not None:
            return

        import cv2

        device = self._device
        # Pin the V4L2 backend for local device nodes, matching
        # ``device_utils._probe_cv2_camera``: letting OpenCV choose sends a string
        # path to FFmpeg's libavdevice V4L2 demuxer first, which spams
        # ``ioctl(VIDIOC_QBUF): Bad file descriptor`` and probes a backend the SDK
        # never streams through. An HTTP MJPEG URL -- which
        # ``_resolve_camera_device_for_twin`` can legitimately return -- must keep
        # the default backend.
        v4l2_backend = getattr(cv2, "CAP_V4L2", None)
        if isinstance(device, int):
            capture = cv2.VideoCapture(device)
        elif str(device).startswith("/dev/video") and v4l2_backend is not None:
            capture = cv2.VideoCapture(str(device), v4l2_backend)
        else:
            capture = cv2.VideoCapture(str(device))
        if not capture.isOpened():
            capture.release()
            raise self._error(
                "camera_unavailable",
                f"Could not open the wrist camera at {device!r}. Another operation may "
                "still hold the device; stop it and restart the calibration.",
            )
        width, height = self._capture_size
        # Ask for MJPG *before* the size. Uncompressed 720p YUYV is ~27 MB/s,
        # right at the USB 2.0 ceiling: measured frame grabs of 500-1250 ms,
        # during which the arm can drift away from the joint angles the sample is
        # paired with. MJPG is roughly a tenth the bandwidth for the same frame.
        # Order matters -- V4L2 renegotiates the format, and setting the size
        # first lets the driver pick a YUYV mode that the later fourcc cannot
        # change. A camera without MJPG simply keeps its default, and the granted
        # mode is logged below either way.
        fourcc = getattr(cv2, "VideoWriter_fourcc", None)
        if fourcc is not None:
            capture.set(cv2.CAP_PROP_FOURCC, fourcc(*"MJPG"))
        capture.set(cv2.CAP_PROP_FRAME_WIDTH, width)
        capture.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
        # Shrink the driver queue to one frame so a capture cannot be served an
        # image from before the arm was repositioned. ``grab_fresh_frame`` still
        # drains defensively -- not every backend honours this -- but with a depth
        # of one the drain has almost nothing to discard, which is the difference
        # between "probably fresh" and "fresh".
        capture.set(cv2.CAP_PROP_BUFFERSIZE, 1)
        granted_w = int(capture.get(cv2.CAP_PROP_FRAME_WIDTH))
        granted_h = int(capture.get(cv2.CAP_PROP_FRAME_HEIGHT))
        # Log the granted mode rather than the requested one: V4L2 silently
        # substitutes the nearest supported size, and the difference decides
        # whether a marker is decodable at working distance.
        logger.info(
            "Hand-eye capture: requested %dx%d MJPG, camera granted %dx%d %s",
            width,
            height,
            granted_w,
            granted_h,
            _fourcc_name(capture.get(cv2.CAP_PROP_FOURCC)),
        )
        self._granted_size = (granted_w, granted_h)
        self._capture = capture

    def describe_settings(self) -> Dict[str, Any]:
        """Read back what the camera is actually set to, for the preflight check.

        Read back rather than remembered: V4L2 accepts every ``set()`` and honours
        the ones it feels like, so the only trustworthy account of a camera's state
        is what it reports after the fact.

        Returns ``{}`` before ``open()``. Every value may be ``None`` -- an HTTP
        MJPEG source has no V4L2 controls at all, and a UVC camera that does not
        implement one simply fails the ``get``.
        """
        capture = self._capture
        if capture is None:
            return {}

        settings: Dict[str, Any] = {
            # Carried so the alert can print a command the operator can paste,
            # rather than one with a placeholder they have to fill in.
            "device": str(self._device),
            "requested_size": tuple(self._capture_size),
            "granted_size": self._granted_size,
            "fourcc": _fourcc_name(self._get(capture, "CAP_PROP_FOURCC")),
            "autofocus": self._get(capture, "CAP_PROP_AUTOFOCUS"),
            "auto_exposure": self._get(capture, "CAP_PROP_AUTO_EXPOSURE"),
        }
        logger.info("Hand-eye camera preflight: %s", settings)
        return settings

    @staticmethod
    def _get(capture: Any, prop_name: str) -> Optional[float]:
        """One ``VideoCapture.get``, or ``None`` if the build or camera lacks it.

        ``getattr`` for the constant because the property set varies by OpenCV
        build, and an ``AttributeError`` here would abort a run over a readout
        nothing depends on.
        """
        import cv2

        prop = getattr(cv2, prop_name, None)
        if prop is None:
            return None
        try:
            value = capture.get(prop)
        except Exception:
            logger.debug("Camera did not answer %s", prop_name, exc_info=True)
            return None
        if value is None:
            return None
        # A negative is OpenCV's "the backend cannot supply this property" --
        # a control the camera does not implement, or a source with no V4L2
        # controls at all. That is the absence of a reading, not a reading, and
        # it is truthy: passed on, it makes the preflight accuse a fixed-focus
        # webcam of having autofocus switched on and hand over a v4l2-ctl command
        # that fails with "unknown control". ``_fourcc_name`` above already
        # treats a non-positive code the same way.
        #
        # 0 stays ambiguous -- "off, or a control this build does not map" -- so
        # it is still never proof of anything.
        reading = float(value)
        return None if reading < 0 else reading

    def grab_fresh_frame(self) -> Any:
        """Grab one frame, flushing the driver's buffer first.

        V4L2 hands back whatever is queued, which after a pause is an image from
        before the arm was moved. Discarding the queued frames is the difference
        between a synchronous sample and a silently corrupt one.
        """
        self.open()
        capture = self._capture
        for _ in range(self._drain_frames):
            capture.grab()
        ok, frame = capture.read()
        if not ok or frame is None:
            raise self._error(
                "no_frame",
                "The wrist camera returned no frame. Check the device is still "
                "connected, then capture again.",
            )
        return frame

    def release(self) -> None:
        capture, self._capture = self._capture, None
        if capture is not None:
            capture.release()
