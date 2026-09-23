"""Tests for the read-only ``freedrive`` operation."""

from __future__ import annotations

import math
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

from utils.cw_freedrive import (
    _HANDOVER_GRACE,
    _HANDOVER_RETRY_INTERVAL,
    _RECONNECT_BACKOFF_MIN,
    FreedriveSession,
    freedrive,
)


class FakeBus:
    """Stands in for FeetechMotorsBus, recording connect/disconnect calls."""

    instances: list["FakeBus"] = []

    def __init__(self, port, motors, calibration=None):
        self.port = port
        self.motors = motors
        self.calibration = calibration
        self.connected = False
        self.disconnect_count = 0
        self.torque_disabled = False
        FakeBus.instances.append(self)

    def connect(self, preflight_check=True):
        # The freedrive loop must skip preflight: it logs an ERROR traceback per
        # failure, which retried for the life of the operation buries the logs.
        assert preflight_check is False
        self.connected = True

    def disable_torque(self, motor_ids=None):
        # The real bus routes this through _ensure_connected(), so calling it
        # before connect() would raise and freedrive would never start.
        assert self.connected, "disable_torque must be called after connect"
        self.torque_disabled = True

    def disconnect(self):
        self.connected = False
        self.disconnect_count += 1

    def sync_read(self, data_name, normalize=True, num_retry=0):
        assert data_name == "Present_Position"
        # Normalized units: 100 = full positive end of the calibrated range.
        return {"shoulder_pan": 100.0, "gripper": 0.0}


class RecordingBus(FakeBus):
    """A FakeBus that records call order and the motors each release targeted."""

    def __init__(self, port, motors, calibration=None):
        self.calls: list = []
        self.torque_calls: list = []
        super().__init__(port, motors, calibration)

    def connect(self, preflight_check=True):
        super().connect(preflight_check=preflight_check)
        self.calls.append("connect")

    def disable_torque(self, motor_ids=None):
        super().disable_torque(motor_ids)
        self.calls.append("disable_torque")
        self.torque_calls.append(motor_ids)

    def sync_read(self, data_name, normalize=True, num_retry=0):
        self.calls.append("sync_read")
        return super().sync_read(data_name, normalize=normalize, num_retry=num_retry)


@pytest.fixture(autouse=True)
def _reset_fake_bus():
    FakeBus.instances = []
    yield
    FakeBus.instances = []


@pytest.fixture
def robot():
    t = MagicMock()
    t.uuid = "twin-uuid-1"
    t.get_controllable_joint_names.return_value = [f"_{i}" for i in range(1, 7)]
    return t


def _session(robot, rate_hz=1000.0, frame_counters=None, time_reference=None):
    """A session as the driver builds it, with what run() would have supplied.

    Tests that drive connect()/_poll_once() directly bypass run(), which is where
    the loop hands those two in.
    """
    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
    ):
        session = FreedriveSession(
            MagicMock(),
            robot,
            "/dev/ttyACM0",
            "follower1",
            rate_hz,
        )
    session._frame_counters = frame_counters
    session._time_reference = time_reference
    return session


def test_publishes_radians_on_schema_joint_names(robot):
    session = _session(robot)
    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
        assert session._poll_once() is True

    call = session._client.mqtt.update_joints_state.call_args
    assert call.kwargs["twin_uuid"] == "twin-uuid-1"
    assert call.kwargs["source_type"] == "edge_follower"

    positions = call.kwargs["joint_positions"]
    # shoulder_pan is motor 1 (RANGE_M100_100), gripper is motor 6 (RANGE_0_100).
    assert set(positions) == {"_1", "_6"}
    assert positions["_1"] == pytest.approx(math.pi)  # 100% of ±180° fallback
    assert positions["_6"] == pytest.approx(0.0)


def test_never_writes_to_the_motor_bus(robot):
    """The contract of freedrive: it releases torque, reads, and never commands."""
    session = _session(robot)
    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        session.connect()
        session._poll_once()

    bus = FakeBus.instances[0]
    assert bus.torque_disabled, "freedrive must release torque so the arm moves by hand"
    for forbidden in ("sync_write", "enable_torque", "write", "sync_write_positions"):
        assert not hasattr(bus, forbidden), f"freedrive must not call {forbidden}"


def test_torque_is_released_on_every_connect_before_any_read(robot):
    """The arm must be limp for the whole session, reconnects included.

    A rigid arm is the one freedrive failure that still looks healthy: the twin
    keeps showing a live pose while the operator cannot move the robot. So the
    release must (a) happen on every connect, not just the first, (b) come
    before the first read, and (c) cover every motor rather than a subset.
    """
    session = _session(robot)
    with patch("utils.cw_freedrive.FeetechMotorsBus", RecordingBus):
        assert session.connect() is True
        assert session._poll_once() is True

        # The port dies mid-session (unplugged, or taken by another process);
        # run() drops the bus and reconnects, exactly as done here.
        session._bus.sync_read = MagicMock(side_effect=OSError("device gone"))
        assert session._poll_once() is False
        session.disconnect()
        assert session.connect() is True
        assert session._poll_once() is True

    assert len(FakeBus.instances) == 2, "expected a reconnect onto a fresh bus"
    for bus in FakeBus.instances:
        assert bus.calls[:2] == ["connect", "disable_torque"], (
            f"torque must be released right after connect, before reading: {bus.calls}"
        )
        assert "sync_read" in bus.calls, "the ordering above only holds if we did read"
        assert bus.torque_calls == [None], "every motor must be released, not a subset"


def test_mqtt_failure_does_not_drop_the_serial_port(robot):
    session = _session(robot)
    session._client.mqtt.update_joints_state.side_effect = RuntimeError("broker down")
    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        session.connect()
        assert session._poll_once() is True
    assert session._bus is not None


def test_read_failure_drops_the_serial_port(robot):
    session = _session(robot)
    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        session.connect()
        session._bus.sync_read = MagicMock(side_effect=OSError("device gone"))
        assert session._poll_once() is False


def test_connect_failure_warns_once_then_stays_quiet(robot, caplog):
    """An unreachable port must be diagnosable without flooding the driver logs."""
    session = _session(robot)
    failing = MagicMock(side_effect=PermissionError("Permission denied: '/dev/ttyACM0'"))

    with patch("utils.cw_freedrive.FeetechMotorsBus", failing):
        with caplog.at_level("WARNING", logger="utils.cw_freedrive"):
            assert session.connect() is False
            assert session.connect() is False
            assert session.connect() is False

    warnings = [r for r in caplog.records if r.levelname == "WARNING"]
    assert len(warnings) == 1
    assert "Permission denied" in warnings[0].getMessage()


def test_recovery_re_arms_the_warning(robot):
    session = _session(robot)
    with patch("utils.cw_freedrive.FeetechMotorsBus", MagicMock(side_effect=OSError)):
        session.connect()
    assert session._failure_reported is True

    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
    assert session._failure_reported is False


def test_inside_the_handover_grace_retries_fast_and_stays_quiet(robot, caplog):
    """A port still owned by the previous operation's sit-down is expected, not a
    fault: poll for it rather than escalating the absent-arm backoff, and say
    nothing -- so a slow release costs neither a spurious WARNING nor a full
    _RECONNECT_BACKOFF_MIN stall before the first sample."""
    session = _session(robot)
    session._handover_deadline = time.monotonic() + _HANDOVER_GRACE
    busy = MagicMock(side_effect=PermissionError("Permission denied: '/dev/ttyACM0'"))

    waits: list[float] = []
    stop_event = MagicMock()
    stop_event.wait.side_effect = lambda timeout=None: waits.append(timeout)

    with patch("utils.cw_freedrive.FeetechMotorsBus", busy):
        with caplog.at_level("WARNING", logger="utils.cw_freedrive"):
            assert session.connect() is False
            session._wait_backoff(stop_event)
            session._wait_backoff(stop_event)

    assert waits == [_HANDOVER_RETRY_INTERVAL, _HANDOVER_RETRY_INTERVAL]
    # The absent-arm escalation must not have started.
    assert session._backoff == _RECONNECT_BACKOFF_MIN
    assert [r for r in caplog.records if r.levelname == "WARNING"] == []
    # Not latched: an outage that outlives the grace must still get its warning.
    assert session._failure_reported is False


def test_past_the_handover_grace_warns_and_backs_off(robot, caplog):
    """Once the grace expires a failed connect is treated as an absent arm again."""
    session = _session(robot)
    session._handover_deadline = time.monotonic() - 0.01  # expired
    failing = MagicMock(side_effect=OSError("no such device"))

    with patch("utils.cw_freedrive.FeetechMotorsBus", failing):
        with caplog.at_level("WARNING", logger="utils.cw_freedrive"):
            assert session.connect() is False

    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1

    waits: list[float] = []
    stop_event = MagicMock()
    stop_event.wait.side_effect = lambda timeout=None: waits.append(timeout)
    session._wait_backoff(stop_event)
    assert waits == [_RECONNECT_BACKOFF_MIN]
    assert session._backoff > _RECONNECT_BACKOFF_MIN


def test_successful_connect_ends_the_handover_grace(robot):
    """A drop *after* the arm was reached is a real outage and gets the full backoff."""
    session = _session(robot)
    session._handover_deadline = time.monotonic() + _HANDOVER_GRACE

    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True

    assert session._handover_deadline is None
    assert session._in_handover_grace() is False


def test_slow_port_release_still_publishes_promptly_and_quietly(robot, caplog):
    """The reported case end to end: the previous op's sit-down outlives the join,
    so the first connects fail. Freedrive must ride it out without a warning and
    still publish quickly -- not stall for a full backoff interval."""
    stop_event = threading.Event()
    client = MagicMock()
    published = threading.Event()
    client.mqtt.update_joints_state.side_effect = lambda **kw: published.set()

    attempts: list[int] = []

    def busy_then_free(port, motors, calibration=None):
        attempts.append(1)
        if len(attempts) <= 2:
            raise PermissionError("Permission denied: '/dev/ttyACM0'")
        return FakeBus(port, motors, calibration)

    session = _session(robot, rate_hz=100.0)
    session._client = client

    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", busy_then_free),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
        caplog.at_level("WARNING", logger="utils.cw_freedrive"),
    ):
        thread = threading.Thread(
            target=freedrive,
            kwargs={"client": client, "session": session, "stop_event": stop_event},
        )
        thread.start()
        try:
            # Two grace retries cost ~0.5s; the old 5s backoff would blow this.
            assert published.wait(timeout=3.0), "never published after the port freed"
        finally:
            stop_event.set()
            thread.join(timeout=5.0)

    assert not thread.is_alive()
    assert len(attempts) == 3
    # Scoped to this module's logger: freedrive() also warns when the optional
    # cyberwave-video-sync package is absent, which is unrelated to the handover.
    freedrive_warnings = [
        r
        for r in caplog.records
        if r.levelname == "WARNING" and r.name == "utils.cw_freedrive"
    ]
    assert freedrive_warnings == []


def test_freedrive_returns_and_releases_the_port_on_stop_event(robot):
    """The driver's _stop_current_operation sets the stop event; freedrive must exit."""
    stop_event = threading.Event()
    client = MagicMock()
    published = threading.Event()
    client.mqtt.update_joints_state.side_effect = lambda **kw: published.set()

    session = _session(robot, rate_hz=100.0)
    session._client = client

    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
    ):
        thread = threading.Thread(
            target=freedrive,
            kwargs={
                "client": client,
                "session": session,
                "stop_event": stop_event,
            },
        )
        thread.start()
        try:
            assert published.wait(timeout=5.0), "never published"
        finally:
            stop_event.set()
            thread.join(timeout=5.0)

    assert not thread.is_alive()
    assert FakeBus.instances[0].connected is False
    assert FakeBus.instances[0].disconnect_count >= 1


def test_force_disconnect_releases_the_port_while_the_thread_is_wedged(robot):
    """The third escalation step of _stop_current_operation: the loop is stuck in
    sync_read on a half-dead port and ignores stop_event, so the driver releases
    the port out from under it through the session it registered."""

    class WedgedBus(FakeBus):
        """A bus whose read blocks until the test lets it go."""

        reading = threading.Event()
        release = threading.Event()

        def sync_read(self, data_name, normalize=True, num_retry=0):
            WedgedBus.reading.set()
            WedgedBus.release.wait(timeout=10.0)
            return super().sync_read(data_name, normalize=normalize, num_retry=num_retry)

    stop_event = threading.Event()
    session = _session(robot, rate_hz=100.0)

    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", WedgedBus),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
    ):
        thread = threading.Thread(
            target=freedrive,
            kwargs={
                "client": MagicMock(),
                "session": session,
                "stop_event": stop_event,
            },
        )
        thread.start()
        try:
            assert WedgedBus.reading.wait(timeout=5.0), "never reached the blocking read"

            # Steps 1-2: signal, then fail to join.
            stop_event.set()
            thread.join(timeout=0.5)
            assert thread.is_alive(), "the read was supposed to stay wedged"

            # Step 3: force disconnect via the registered device.
            session.disconnect()

            bus = FakeBus.instances[0]
            assert bus.connected is False
            assert bus.disconnect_count >= 1
            # The point of the exercise: the port is free before the thread dies.
            assert thread.is_alive()
        finally:
            WedgedBus.release.set()
            thread.join(timeout=5.0)

    assert not thread.is_alive()


# --- camera streaming ------------------------------------------------------


def test_frame_counters_ride_along_with_every_joint_sample(robot):
    """The whole point of wiring cameras in: a recording can tie joints to frames.

    Without this the backend has only timestamps to align on, which is exactly
    what ``remoteoperate``/``teleoperate`` avoid by passing the same snapshot.
    """
    counters = {"track-1": {"frame_count": 42, "sensor_id": "wrist"}}
    session = _session(robot, frame_counters=lambda: counters)

    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
        assert session._poll_once() is True

    call = session._client.mqtt.update_joints_state.call_args
    assert call.kwargs["camera_frame_counters"] == counters


def test_no_cameras_publishes_none_not_an_empty_dict(robot):
    session = _session(robot, frame_counters=None)

    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
        assert session._poll_once() is True

    assert (
        session._client.mqtt.update_joints_state.call_args.kwargs["camera_frame_counters"] is None
    )


def test_empty_snapshot_is_sent_as_none(robot):
    """Streams that have not opened yet report {}; the payload shape must not change."""
    session = _session(robot, frame_counters=dict)

    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
        assert session._poll_once() is True

    assert (
        session._client.mqtt.update_joints_state.call_args.kwargs["camera_frame_counters"] is None
    )


def test_a_failing_counter_snapshot_does_not_cost_the_joint_sample(robot):
    """Cameras are secondary: a broken snapshot must not drop the joint stream."""

    def _boom():
        raise RuntimeError("streamer registry is mid-teardown")

    session = _session(robot, frame_counters=_boom)

    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
        assert session._poll_once() is True

    call = session._client.mqtt.update_joints_state.call_args
    assert call.kwargs["camera_frame_counters"] is None
    assert call.kwargs["joint_positions"], "the joint sample must still be published"


def test_joint_timestamp_comes_from_the_shared_clock(robot):
    """Joints and camera frames must be stamped from one clock, as the siblings do."""
    clock = MagicMock()
    clock.update.return_value = (1234.5, 99.0)

    session = _session(robot, time_reference=clock)
    with patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus):
        assert session.connect() is True
        assert session._poll_once() is True

    assert session._client.mqtt.update_joints_state.call_args.kwargs["timestamp"] == 1234.5


def test_cameras_are_streamed_on_the_shared_clock(robot):
    """freedrive() hands CameraStreamManager the same TimeReference and stop_event."""
    stop_event = threading.Event()
    stop_event.set()  # run() returns immediately; we only inspect the wiring
    cam_twin = MagicMock()

    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
        patch("utils.cw_freedrive.ensure_cyberwave_video_sync"),
        patch("utils.cw_freedrive.CameraStreamManager") as manager_cls,
    ):
        freedrive(
            client=MagicMock(),
            session=_session(robot),
            stop_event=stop_event,
            cameras=[{"twin": cam_twin, "camera_id": "/dev/video0"}],
        )

    kwargs = manager_cls.call_args.kwargs
    assert kwargs["twins"] == [(cam_twin, {"camera_id": "/dev/video0"})]
    assert kwargs["stop_event"] is stop_event
    manager_cls.return_value.start.assert_called_once()
    # Bounded join, like remoteoperate: a wedged stream must not strand the thread.
    manager_cls.return_value.join.assert_called_once_with(timeout=5.0)


def test_camera_failure_does_not_cost_the_joint_stream(robot):
    """A camera that will not open must not take the live twin down with it."""
    stop_event = threading.Event()
    stop_event.set()

    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
        patch("utils.cw_freedrive.ensure_cyberwave_video_sync"),
        patch(
            "utils.cw_freedrive.CameraStreamManager",
            MagicMock(side_effect=OSError("/dev/video0 busy")),
        ),
    ):
        # Must return normally rather than propagating the camera error.
        freedrive(
            client=MagicMock(),
            session=_session(robot),
            stop_event=stop_event,
            cameras=[{"twin": MagicMock()}],
        )


def test_no_cameras_configured_starts_no_manager(robot):
    stop_event = threading.Event()
    stop_event.set()

    with (
        patch("utils.cw_freedrive.FeetechMotorsBus", FakeBus),
        patch("utils.cw_freedrive._load_follower_calibration", return_value=None),
        patch("utils.cw_freedrive.ensure_cyberwave_video_sync"),
        patch("utils.cw_freedrive.CameraStreamManager") as manager_cls,
    ):
        freedrive(
            client=MagicMock(),
            session=_session(robot),
            stop_event=stop_event,
            cameras=None,
        )

    manager_cls.assert_not_called()
