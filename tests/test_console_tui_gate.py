"""Headless-safe status rendering.

Two properties: without a TTY nothing reaches stdout, and sampling/alerting
keep their 1 Hz cadence regardless — only the drawing is gated.
"""

from __future__ import annotations

import io
import logging
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.console import (  # noqa: E402
    DEFAULT_SUMMARY_INTERVAL_SECONDS,
    summary_interval_seconds,
    tui_enabled,
)
from utils.trackers import (  # noqa: E402
    StatusTracker,
    _format_status_summary,
    run_status_logging_thread,
)


class TestTuiEnabled:
    def test_disabled_when_stdout_is_not_a_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        monkeypatch.setattr(sys, "stdout", io.StringIO())
        assert tui_enabled() is False

    def test_enabled_when_stdout_is_a_tty(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        fake = io.StringIO()
        monkeypatch.setattr(fake, "isatty", lambda: True, raising=False)
        monkeypatch.setattr(sys, "stdout", fake)
        assert tui_enabled() is True

    @pytest.mark.parametrize("value,expected", [("1", True), ("true", True), ("YES", True)])
    def test_env_can_force_on(
        self, monkeypatch: pytest.MonkeyPatch, value: str, expected: bool
    ) -> None:
        monkeypatch.setattr(sys, "stdout", io.StringIO())  # not a tty
        monkeypatch.setenv("CYBERWAVE_STATUS_TUI", value)
        assert tui_enabled() is expected

    @pytest.mark.parametrize("value", ["0", "false", "off", "no", ""])
    def test_env_can_force_off(self, monkeypatch: pytest.MonkeyPatch, value: str) -> None:
        fake = io.StringIO()
        monkeypatch.setattr(fake, "isatty", lambda: True, raising=False)
        monkeypatch.setattr(sys, "stdout", fake)
        monkeypatch.setenv("CYBERWAVE_STATUS_TUI", value)
        assert tui_enabled() is False

    def test_closed_stdout_does_not_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A closed stream raises ValueError from isatty(); treat it as no TTY."""
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        closed = io.StringIO()
        closed.close()
        monkeypatch.setattr(sys, "stdout", closed)
        assert tui_enabled() is False


class TestSummaryInterval:
    def test_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CYBERWAVE_STATUS_SUMMARY_SECONDS", raising=False)
        assert summary_interval_seconds() == DEFAULT_SUMMARY_INTERVAL_SECONDS

    @pytest.mark.parametrize("raw", ["0", "-1", "junk"])
    def test_rejects_values_that_would_reintroduce_volume(
        self, monkeypatch: pytest.MonkeyPatch, raw: str
    ) -> None:
        monkeypatch.setenv("CYBERWAVE_STATUS_SUMMARY_SECONDS", raw)
        assert summary_interval_seconds() == DEFAULT_SUMMARY_INTERVAL_SECONDS

    def test_accepts_positive_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("CYBERWAVE_STATUS_SUMMARY_SECONDS", "5")
        assert summary_interval_seconds() == 5.0


def _make_tracker() -> StatusTracker:
    tracker = StatusTracker()
    tracker.camera_enabled = True
    tracker.set_camera_infos([{"uuid": "c1", "name": "wrist"}, {"uuid": "c2", "name": "top"}])
    tracker.update_camera_status("wrist", detected=True, started=True)
    tracker.set_joint_index_to_name({"1": "shoulder_pan", "2": "elbow_flex"})
    tracker.update_joint_states({"1": 0.5, "2": -0.25})
    tracker.update_mqtt_status(True)
    return tracker


def _run_ticks(monkeypatch: pytest.MonkeyPatch, ticks: int, **kwargs: object) -> None:
    """Run the status loop for exactly *ticks* iterations, synchronously.

    The loop's only sleep is its 1 Hz pacer, so counting sleeps gives an exact
    iteration count with no wall-clock wait. monotonic advances with it so the
    summary cadence stays deterministic.
    """
    import utils.trackers as trackers

    stop = threading.Event()
    clock = {"now": 1000.0}
    count = {"n": 0}

    def fake_sleep(seconds: float) -> None:
        clock["now"] += seconds
        count["n"] += 1
        if count["n"] >= ticks:
            stop.set()

    monkeypatch.setattr(trackers.time, "sleep", fake_sleep)
    monkeypatch.setattr(trackers.time, "monotonic", lambda: clock["now"])
    run_status_logging_thread(_make_tracker(), stop, 30, 30, **kwargs)  # type: ignore[arg-type]


class TestHeadlessStatusThread:
    def test_writes_nothing_to_stdout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        monkeypatch.setenv("CYBERWAVE_STATUS_SUMMARY_SECONDS", "1")
        captured = io.StringIO()
        monkeypatch.setattr(sys, "stdout", captured)  # not a tty

        _run_ticks(monkeypatch, 5, mode="remoteoperate")

        assert captured.getvalue() == ""

    def test_summary_logged_once_per_interval_not_once_per_tick(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ) -> None:
        """10 ticks at 1 Hz with a 5 s summary interval => 2 lines, not 10."""
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        monkeypatch.setenv("CYBERWAVE_STATUS_SUMMARY_SECONDS", "5")
        monkeypatch.setattr(sys, "stdout", io.StringIO())

        with caplog.at_level(logging.INFO, logger="utils.trackers"):
            _run_ticks(monkeypatch, 10, mode="remoteoperate")

        summaries = [r for r in caplog.records if r.message.startswith("status[")]
        assert len(summaries) == 2

    def test_still_publishes_motor_telemetry_every_tick(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Gating the render must not slow alerting or telemetry.

        Regression guard: an obvious but wrong fix is to drop the whole loop to
        the summary cadence, which would also throttle overheat detection.
        """
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        monkeypatch.setenv("CYBERWAVE_STATUS_SUMMARY_SECONDS", "3600")
        monkeypatch.setattr(sys, "stdout", io.StringIO())

        robot = MagicMock()
        robot.uuid = "twin-abc"
        publish = MagicMock()

        with patch("utils.motor_telemetry.publish_robot_motor_status", publish):
            _run_ticks(
                monkeypatch,
                4,
                mode="remoteoperate",
                robot=robot,
                mqtt_client=MagicMock(),
            )

        # Exactly once per tick, even though the summary interval is an hour.
        assert publish.call_count == 4


class TestCalibrationRecorderHeadless:
    """record_ranges_of_motion's 10 Hz display must also stay off a pipe.

    Guided calibration runs it headless, advancing it over the subprocess's
    stdin, so it is the driver's loudest writer while active.
    """

    def _make_bus(self, monkeypatch: pytest.MonkeyPatch):
        from motors.feetech_bus import FeetechMotorsBus

        bus = FeetechMotorsBus.__new__(FeetechMotorsBus)
        motor = MagicMock()
        motor.id = 1
        bus.motors = {"shoulder_pan": motor}
        monkeypatch.setattr(bus, "_ensure_connected", lambda: None, raising=False)
        monkeypatch.setattr(
            bus, "sync_read_positions", lambda ids, **kw: {1: 2048}, raising=False
        )
        monkeypatch.setattr(
            bus,
            "_format_calibration_display",
            lambda *a, **kw: "TABLE\n",
            raising=False,
        )
        return bus

    def test_writes_nothing_and_skips_formatting(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("CYBERWAVE_STATUS_TUI", raising=False)
        bus = self._make_bus(monkeypatch)

        formatted = [0]

        def _count_format(*_a: object, **_kw: object) -> str:
            formatted[0] += 1
            return "TABLE\n"

        monkeypatch.setattr(bus, "_format_calibration_display", _count_format, raising=False)

        captured = io.StringIO()
        monkeypatch.setattr(sys, "stdout", captured)  # not a tty

        # input() blocks until "Enter"; release it once the display loop has
        # really iterated. Waiting on the condition, not the clock, so a slow
        # machine slows the test rather than flaking it.
        ticked = threading.Event()
        progress_calls = [0]

        def _on_progress(*_a: object) -> None:
            progress_calls[0] += 1
            if progress_calls[0] >= 3:
                ticked.set()

        monkeypatch.setattr("builtins.input", lambda: ticked.wait(10) and "")

        bus.record_ranges_of_motion(["shoulder_pan"], on_progress=_on_progress)

        assert ticked.is_set(), "display loop never ran; the test proved nothing"

        assert captured.getvalue() == ""
        # That block holds display_lock, contending with the 100 Hz sampler.
        assert formatted[0] == 0
        # Progress callbacks still fire — that is how the UI tracks calibration.
        assert progress_calls[0] >= 1


class TestStatusSummary:
    def test_remoteoperate_counters(self) -> None:
        status = {
            "mqtt_connected": True,
            "camera_enabled": True,
            "camera_states": {
                "wrist": {"detected": True, "started": True},
                "top": {"detected": True, "started": False},
            },
            "fps": 10,
            "camera_fps": 30,
            "messages_received": 7,
            "messages_processed": 6,
            "messages_filtered": 1,
            "errors_motor": 0,
            "errors_mqtt": 0,
            "joint_temperatures": {"follower_1": 51.2, "follower_2": None},
        }
        line = _format_status_summary(status, "remoteoperate")
        assert "\n" not in line
        assert "mqtt=up" in line
        assert "cameras=1/2" in line
        assert "received=7" in line
        assert "processed=6" in line
        assert "hottest=51C" in line

    def test_teleoperate_uses_produced_counter(self) -> None:
        line = _format_status_summary(
            {"mqtt_connected": False, "messages_produced": 42, "joint_temperatures": {}},
            "teleoperate",
        )
        assert "produced=42" in line
        assert "mqtt=DOWN" in line
        assert "hottest=n/a" in line
        assert "cameras=none" in line
