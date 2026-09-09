"""The ``freedrive`` operation: publish measured joint state, drive nothing.

Backed by a ``freedrive`` controller policy, this is the read-only sibling of
``cw_teleoperate`` and ``cw_remoteoperate``. Where those publish joint state as
a side effect of a 100 Hz control loop, this polls ``Present_Position`` at a low
fixed rate and publishes the same aggregated payload -- so the arm can be moved
by hand and its real pose shows up on the twin.

**It releases torque and never re-engages it.** It talks to
:class:`FeetechMotorsBus` directly rather than going through
``SO101Follower.connect()``, which calls ``bus.enable_torque()`` and *forces a
calibration run* when no calibration file exists. Both would be unacceptable
for a controller whose whole contract is that it does not drive the robot.

Ownership follows ``remoteoperate``: the driver builds the device that holds the
port (:class:`FreedriveSession`, the role ``SO101Follower`` plays there),
connects it, and registers it as ``_current_follower`` so
``_stop_current_operation`` can force-release the port when the operation thread
will not exit. This module owns the loop -- its :class:`TimeReference` and its
camera streams.
"""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from cyberwave.constants import SOURCE_TYPE_EDGE_FOLLOWER
from cyberwave.sensor import CameraStreamManager
from cyberwave.utils import TimeReference

from motors import FeetechMotorsBus, Motor, MotorCalibration
from so101.robot import SO101_MOTORS
from utils.config import get_so101_lib_dir
from utils.cw_utils import build_joint_mappings
from utils.utils import load_calibration, normalized_to_radians
from utils.video_sync import ensure_cyberwave_video_sync

logger = logging.getLogger(__name__)

# Low enough to be invisible next to the 100 Hz control loop, high enough that
# dragging the arm by hand looks live in the UI.
DEFAULT_FREEDRIVE_HZ = 5.0

# Backoff after a failed connect or a read that killed the bus. It escalates so
# a permanently absent arm (unplugged, or the port owned by someone else)
# settles into a slow retry instead of hammering the serial layer.
_RECONNECT_BACKOFF_MIN = 5.0
_RECONNECT_BACKOFF_MAX = 60.0

# The backoff above is tuned for "the arm is absent", but the *first* connect of a
# session usually follows another operation on the same port, and that operation's
# sit-down can still own the port for a moment -- it may outlive the join in
# ``_stop_current_operation``. Treating that as an absent arm costs a spurious
# WARNING plus a full _RECONNECT_BACKOFF_MIN stall before the first sample, so
# until the first successful connect we retry fast and quietly for this long.
_HANDOVER_GRACE = 5.0
_HANDOVER_RETRY_INTERVAL = 0.25


def freedrive(
    *,
    client: Any,
    session: "FreedriveSession",
    stop_event: threading.Event,
    cameras: Optional[List[Dict[str, Any]]] = None,
) -> None:
    """Publish measured follower joint angles until *stop_event* is set.

    Blocking: the caller runs this on the driver's operation thread, mirroring
    ``remoteoperate(...)``.

    Args:
        session: The device that holds the serial port, already built and
            connected by the driver -- the same split as ``remoteoperate``, which
            is handed a connected ``SO101Follower``. The driver registers it as
            ``_current_follower`` so a wedged thread can be force-disconnected.
        cameras: Camera entries from ``materialize_camera_entries_for_edge_operation``.
            Streamed alongside the joint samples and stamped from the same
            :class:`TimeReference`, and each publish carries the cameras' frame
            counters -- the same contract as ``remoteoperate``/``teleoperate``, so
            a demonstration recorded under freedrive lines its video up with the
            joint stream instead of relying on timestamps alone.
    """
    ensure_cyberwave_video_sync()

    # One clock for joints and camera frames, exactly as the other operations do.
    time_reference = TimeReference()
    camera_manager = _start_camera_streaming(client, cameras, stop_event, time_reference)

    try:
        session.run(
            stop_event,
            time_reference=time_reference,
            frame_counters=(
                camera_manager.get_frame_counters if camera_manager is not None else None
            ),
        )
    finally:
        session.disconnect()
        if camera_manager is not None:
            # The streams watch the same stop_event; setting it here covers the
            # paths that end the loop without it (a raised error). Bounded join,
            # like remoteoperate: a wedged stream must not strand the operation
            # thread, because _stop_current_operation clears its bookkeeping
            # either way and the next operation would inherit the mess.
            stop_event.set()
            try:
                camera_manager.join(timeout=5.0)
            except Exception:
                logger.debug("Freedrive: error stopping camera streams", exc_info=True)


def _start_camera_streaming(
    client: Any,
    cameras: Optional[List[Dict[str, Any]]],
    stop_event: threading.Event,
    time_reference: TimeReference,
) -> Optional[CameraStreamManager]:
    """Start one stream per configured camera, or None when none are configured.

    ``camera_id`` is always present in entries built by
    ``materialize_camera_entries_for_edge_operation``, which is why this needs no
    ``follower.config.cameras`` fallback -- there is no follower here by design.
    """
    if not cameras:
        return None

    twins: List[Any] = []
    for cfg in cameras:
        overrides = {k: v for k, v in cfg.items() if k != "twin" and v is not None}
        twins.append((cfg["twin"], overrides) if overrides else cfg["twin"])

    try:
        manager = CameraStreamManager(
            client=client,
            twins=twins,
            stop_event=stop_event,
            time_reference=time_reference,
        )
        manager.start()
    except Exception:
        # Cameras are secondary to the joint stream: one that will not open must
        # not cost the user their live twin.
        logger.warning("Freedrive: camera streaming failed to start", exc_info=True)
        return None

    logger.info("Freedrive: streaming %d camera(s)", len(twins))
    return manager


class FreedriveSession:
    """Owns the motor bus and the normalized-to-radians conversion.

    The freedrive equivalent of ``SO101Follower``: the driver builds it, calls
    :meth:`connect`, and registers it as ``_current_follower``. Constructing it
    opens no port, and :meth:`disconnect` is safe to call from another thread at
    any point -- that is what lets ``_stop_current_operation`` release the port
    when the operation thread will not exit.
    """

    def __init__(
        self,
        client: Any,
        robot: Any,
        port: str,
        follower_id: str,
        rate_hz: float = DEFAULT_FREEDRIVE_HZ,
    ) -> None:
        self._client = client
        self._twin_uuid = str(robot.uuid)
        self._port = port
        self._period = 1.0 / rate_hz if rate_hz > 0 else 1.0 / DEFAULT_FREEDRIVE_HZ
        # Supplied by run(): they belong to the loop, not the device -- the same
        # reason remoteoperate_loop() takes them as arguments.
        self._time_reference: Optional[TimeReference] = None
        self._frame_counters: Optional[Callable[[], Dict[str, Dict[str, Any]]]] = None

        self._motors: Dict[str, Motor] = SO101_MOTORS
        self._calibration = _load_follower_calibration(follower_id)
        mappings = build_joint_mappings(robot, self._motors)
        self._motor_id_to_schema_joint: Dict[int, str] = mappings["motor_id_to_schema_joint"]

        self._bus: Optional[FeetechMotorsBus] = None
        self._backoff = _RECONNECT_BACKOFF_MIN
        # Set by run() and cleared by the first successful connect: the grace
        # covers the handover from the previous operation, nothing after it.
        self._handover_deadline: Optional[float] = None
        # Log the *first* failure of an outage at WARNING and stay quiet until
        # the port recovers. Silent retries would leave "why are there no joint
        # states?" undiagnosable; per-retry logging would bury the driver logs.
        self._failure_reported = False

    def run(
        self,
        stop_event: threading.Event,
        *,
        time_reference: Optional[TimeReference] = None,
        frame_counters: Optional[Callable[[], Dict[str, Dict[str, Any]]]] = None,
    ) -> None:
        self._time_reference = time_reference
        self._frame_counters = frame_counters
        self._handover_deadline = time.monotonic() + _HANDOVER_GRACE
        logger.info("Freedrive: publishing joint state for twin %s", self._twin_uuid)
        while not stop_event.is_set():
            if self._bus is None and not self.connect():
                self._wait_backoff(stop_event)
                continue

            if not self._poll_once():
                # The bus is gone (unplugged, or the port was taken from under
                # us). Drop it and let the next iteration reconnect.
                self.disconnect()
                self._wait_backoff(stop_event)
                continue

            self._backoff = _RECONNECT_BACKOFF_MIN
            stop_event.wait(self._period)

    def _in_handover_grace(self) -> bool:
        """True while a failed connect is likelier a port handover than an absent arm."""
        return self._handover_deadline is not None and time.monotonic() < self._handover_deadline

    def _wait_backoff(self, stop_event: threading.Event) -> None:
        if self._in_handover_grace():
            # Poll for the port instead of escalating: the previous owner is
            # expected to let go within the grace window.
            stop_event.wait(_HANDOVER_RETRY_INTERVAL)
            return
        stop_event.wait(self._backoff)
        self._backoff = min(self._backoff * 2, _RECONNECT_BACKOFF_MAX)

    def connect(self) -> bool:
        """Open the port and release the brakes. False if the port would not open.

        Returns a flag rather than raising: the loop retries with backoff when the
        arm is unplugged mid-operation, and the driver reports the *first* failure
        as an alert the way it does for ``follower.connect()``.
        """
        try:
            bus = FeetechMotorsBus(
                port=self._port,
                motors=self._motors,
                calibration=self._calibration,
            )
            # Skip the preflight handshake: our first sync_read is the same
            # liveness check, and preflight logs a full ERROR traceback on every
            # failure -- which, retried for the life of the operation, would
            # bury the driver's logs whenever the arm is absent or the port busy.
            bus.connect(preflight_check=False)
            # Release the brakes so the arm can be moved by hand. Must follow
            # connect(): disable_torque() goes through _ensure_connected().
            bus.disable_torque()
        except Exception as e:
            self._report_failure(f"cannot open {self._port}: {e}")
            return False

        self._bus = bus
        self._failure_reported = False
        # Handover is over; a later drop is a real outage and gets the full backoff.
        self._handover_deadline = None
        logger.info("Freedrive: reading %s at %.1f Hz", self._port, 1.0 / self._period)
        return True

    def disconnect(self) -> None:
        bus, self._bus = self._bus, None
        if bus is not None:
            try:
                bus.disconnect()
            except Exception:
                logger.debug("Freedrive: error closing %s", self._port, exc_info=True)

    def _report_failure(self, message: str) -> None:
        """Warn once per outage, then drop to debug until the port recovers.

        Stays quiet inside the handover grace: the port being briefly busy while
        the previous operation lets go is expected, not a fault worth warning about.
        """
        if self._failure_reported or self._in_handover_grace():
            logger.debug("Freedrive: %s", message)
            return
        self._failure_reported = True
        logger.warning(
            "Freedrive: %s. No joint states will be published until this clears; "
            "retrying with backoff (up to %.0fs).",
            message,
            _RECONNECT_BACKOFF_MAX,
        )

    def _camera_frame_counters(self) -> Optional[Dict[str, Dict[str, Any]]]:
        """Frame counters to attach to this sample, or None when there are no cameras.

        These are what let a recording tie each joint sample to the exact video
        frame; without them the backend falls back to timestamps alone. An empty
        snapshot (streams still opening) is sent as None so the payload stays in
        the same shape the other operations produce.
        """
        if self._frame_counters is None:
            return None
        try:
            return self._frame_counters() or None
        except Exception:
            logger.debug("Freedrive: could not read camera frame counters", exc_info=True)
            return None

    def _poll_once(self) -> bool:
        """Read and publish one sample. Returns False if the bus must be dropped."""
        bus = self._bus
        if bus is None:
            return False

        # Stamp joints and camera frames from the same clock so the samples line
        # up in a recorded dataset.
        timestamp = (
            self._time_reference.update()[0] if self._time_reference is not None else time.time()
        )

        try:
            normalized = bus.sync_read("Present_Position", normalize=True, num_retry=1)
        except Exception as e:
            self._report_failure(f"read failed on {self._port}: {e}")
            return False

        if not normalized:
            # Empty read is a transient bus hiccup, not a dead port.
            return True

        positions: Dict[str, float] = {}
        for motor_name, value in normalized.items():
            motor = self._motors.get(motor_name)
            if motor is None:
                continue
            calib = self._calibration.get(motor_name) if self._calibration else None
            schema_joint = self._motor_id_to_schema_joint.get(motor.id, f"_{motor.id}")
            positions[schema_joint] = normalized_to_radians(value, motor.norm_mode, calib)

        if not positions:
            return True

        try:
            self._client.mqtt.update_joints_state(
                twin_uuid=self._twin_uuid,
                joint_positions=positions,
                source_type=SOURCE_TYPE_EDGE_FOLLOWER,
                timestamp=timestamp,
                camera_frame_counters=self._camera_frame_counters(),
            )
        except Exception:
            # An MQTT failure says nothing about the serial bus; keep the port.
            logger.debug("Freedrive: publish failed", exc_info=True)
        return True


def _load_follower_calibration(follower_id: str) -> Optional[Dict[str, MotorCalibration]]:
    """Load ``calibrations/{follower_id}.json``, or None when absent.

    ``_evaluate_and_drive`` gates every operation on calibration, so in the
    normal flow this file exists. It can still be missing if the file is deleted
    underneath a running operation, in which case ``normalized_to_radians`` falls
    back to an approximate conversion rather than dropping the stream.
    """
    path: Path = get_so101_lib_dir() / "calibrations" / f"{follower_id}.json"
    if not path.exists():
        logger.warning("Freedrive: no calibration at %s; angles are approximate", path)
        return None
    try:
        return {name: MotorCalibration(**data) for name, data in load_calibration(path).items()}
    except Exception:
        logger.warning("Freedrive: could not read %s", path, exc_info=True)
        return None
