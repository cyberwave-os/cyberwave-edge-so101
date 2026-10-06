"""Ending a hand-eye run must hand the robot back to its controller.

A run preempts whatever control operation was assigned. Without an explicit
resume the twin sits with nothing running until someone re-assigns the controller
in the dashboard -- which is exactly what happened on real hardware: freedrive
stayed down after a cancel until the controller was detached and re-attached.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import main as main_module


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture(autouse=True)
def _isolate():
    main_module._handeye_runner = None
    yield
    main_module._handeye_runner = None


def _flow():
    flow = MagicMock()
    flow.alert_uuid = "alert-1"
    # The runner reads both when deciding whether a run is live or reapable, and a
    # bare MagicMock compares as neither True nor a number.
    flow.holds_devices = True
    flow.idle_seconds.return_value = 0.0
    # Real numbers where the alert rendering does arithmetic on them.
    flow.sample_count = 0
    flow.captured_count = 0
    flow.target_samples = 12
    flow.board_spec = {}
    flow.snapshot.return_value = {"state": flow.state}
    return flow


def test_cancel_resumes_the_assigned_controller(mock_client):
    main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
    with patch.object(main_module, "_resolve_alert_by_uuid"):
        with patch.object(main_module, "_evaluate_and_drive") as mock_drive:
            with patch.object(main_module, "_is_control_operation_running", return_value=True):
                with patch.object(main_module, "_start_idle_camera_streaming") as mock_idle:
                    main_module._end_handeye_flow(mock_client, "arm-uuid")
    mock_drive.assert_called_once_with(mock_client, "arm-uuid")
    # The resumed operation runs its own camera streams; idle would contend.
    mock_idle.assert_not_called()


def test_cancel_falls_back_to_idle_streaming_with_no_controller(mock_client):
    """_evaluate_and_drive starts nothing when no controller is assigned."""
    main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
    with patch.object(main_module, "_resolve_alert_by_uuid"):
        with patch.object(main_module, "_evaluate_and_drive"):
            with patch.object(main_module, "_is_control_operation_running", return_value=False):
                with patch.object(main_module, "_start_idle_camera_streaming") as mock_idle:
                    main_module._end_handeye_flow(mock_client, "arm-uuid")
    mock_idle.assert_called_once()


def test_applying_resumes_too(mock_client):
    flow = _flow()
    flow.state = "applied"
    main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
    with patch.object(main_module, "_publish_handeye_alert"):
        with patch.object(main_module, "_evaluate_and_drive") as mock_drive:
            with patch.object(main_module, "_is_control_operation_running", return_value=True):
                main_module._handeye_step(mock_client, "arm-uuid", "apply")
    flow.close.assert_called_once()
    mock_drive.assert_called_once_with(mock_client, "arm-uuid")


def test_a_still_capturing_flow_keeps_the_devices(mock_client):
    """Mid-run steps must not hand the camera back."""
    flow = _flow()
    flow.state = "capturing"
    main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
    with patch.object(main_module, "_publish_handeye_alert"):
        with patch.object(main_module, "_evaluate_and_drive") as mock_drive:
            main_module._handeye_step(mock_client, "arm-uuid", "capture")
    flow.close.assert_not_called()
    mock_drive.assert_not_called()


class TestIdleReaping:
    """An abandoned run holds the wrist camera, so the twin shows no stream."""

    def test_reaps_a_run_idle_past_the_limit(self, mock_client):
        flow = _flow()
        flow.holds_devices = True
        flow.idle_seconds.return_value = main_module.HANDEYE_IDLE_TIMEOUT_S + 1
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        with patch.object(main_module, "_evaluate_and_drive") as mock_drive:
            with patch.object(main_module, "_is_control_operation_running", return_value=True):
                assert main_module._reap_idle_handeye_flow(mock_client, "arm-uuid") is True
        # The devices go back and the robot resumes -- that is what reaping is for.
        flow.close.assert_called_once()
        mock_drive.assert_called_once_with(mock_client, "arm-uuid")

    def test_leaves_an_active_run_alone(self, mock_client):
        flow = _flow()
        flow.holds_devices = True
        flow.idle_seconds.return_value = 5.0
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        assert main_module._reap_idle_handeye_flow(mock_client, "arm-uuid") is False
        flow.close.assert_not_called()

    def test_does_not_reap_an_applied_run_waiting_on_done(self, mock_client):
        """Its devices are already released, so it costs nothing to leave up."""
        flow = _flow()
        flow.holds_devices = False
        flow.idle_seconds.return_value = main_module.HANDEYE_IDLE_TIMEOUT_S * 10
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        assert main_module._reap_idle_handeye_flow(mock_client, "arm-uuid") is False
        flow.close.assert_not_called()

    def test_no_flow_is_a_no_op(self, mock_client):
        main_module._handeye_runner = None
        assert main_module._reap_idle_handeye_flow(mock_client, "arm-uuid") is False


class TestOperationPredicate:
    """Hand-eye owns the same devices a control operation does."""

    def test_a_live_run_counts_as_an_operation(self):
        flow = _flow()
        flow.holds_devices = True
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        assert main_module._is_handeye_running() is True
        with patch.object(main_module, "_is_control_operation_running", return_value=False):
            with patch.object(main_module, "_is_calibration_running", return_value=False):
                assert main_module._is_any_operation_running() is True

    def test_an_applied_run_does_not_block_idle_streaming(self):
        """Its camera is already released; idle streaming must be allowed to take it."""
        flow = _flow()
        flow.holds_devices = False
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        assert main_module._is_handeye_running() is False

    def test_hand_eye_is_not_an_actuating_operation(self):
        """Like freedrive it never enables torque, so an MQTT outage needs no trip."""
        flow = _flow()
        flow.holds_devices = True
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        with patch.object(main_module, "_is_control_operation_running", return_value=False):
            with patch.object(main_module, "_is_calibration_running", return_value=False):
                assert main_module._is_actuating_operation_running() is False


def test_a_refused_restart_still_releases_the_previous_run(mock_client):
    """A config bail-out must not leave the old run holding camera and bus.

    ``flow.fail`` only moves the state -- it does not close devices -- and
    "Restart calibration" is offered in exactly that errored state. So at the
    moment a restart arrives, the previous run still owns the follower bus and the
    wrist camera. If the restart is then refused for a configuration reason, the
    teardown must already have happened: every bail-out returns without touching
    it, so anything left held stays held until the 900s idle reaper, with the
    alert that explained it already resolved client-side by the restart press.

    Refused here with the emptiest bail-out there is -- no camera twin named --
    which is the first check in the function and therefore the strictest possible
    statement of "released before *any* validation".
    """
    flow = _flow()
    flow.state = "error"
    main_module._install_handeye_flow(mock_client, "arm-uuid", flow)

    with patch.object(main_module, "_end_handeye_flow") as mock_end:
        with patch.object(main_module, "_create_error_alert"):
            main_module._handle_handeye_start(
                mock_client, "arm-uuid", {"action": "restart"}
            )

    mock_end.assert_called_once_with(mock_client, "arm-uuid", resume=False)


# --- a press that arrives after the run ended ------------------------------


@pytest.fixture
def unthrottled():
    """Clear the alert throttle between tests.

    ``_should_create_alert`` keeps process-wide state keyed by alert name, so the
    first test through this path consumes the 30s window and every later one is
    correctly suppressed -- which would make these tests pass or fail on ordering
    rather than on behaviour.
    """
    import utils.cw_alerts as cw_alerts

    cw_alerts._last_alert_times.clear()
    cw_alerts._alert_active_counts.clear()
    yield
    cw_alerts._last_alert_times.clear()
    cw_alerts._alert_active_counts.clear()


class TestOrphanedCancel:
    """Cancel is the button an operator presses on a card that looks stale.

    It routes through _end_handeye_flow, which returns early with no runner and
    publishes nothing -- so before this guard the press left every button on the
    card disabled for good, which is the dead end the orphan guard exists to
    remove.
    """

    def _press(self, client, alert_uuid="alert-1"):
        return main_module._handle_handeye_button(
            client,
            "arm-uuid",
            {"flow": "so101_handeye", "action": "cancel", "alert_uuid": alert_uuid},
        )

    def test_cancel_with_no_run_resolves_the_stale_alert(self, mock_client, unthrottled):
        with patch.object(main_module, "_resolve_alert_by_uuid") as resolve:
            with patch.object(main_module, "_create_error_alert"):
                assert self._press(mock_client) is True
        resolve.assert_called_once_with(mock_client, "arm-uuid", "alert-1")

    def test_cancel_with_no_run_tells_the_operator_why(self, mock_client, unthrottled):
        with patch.object(main_module, "_resolve_alert_by_uuid"):
            with patch.object(main_module, "_create_error_alert") as alert:
                self._press(mock_client)
        assert alert.call_args.kwargs.get("severity") == "warning"

    def test_a_live_run_still_ends_normally(self, mock_client, unthrottled):
        """The guard must not swallow the ordinary end-of-calibration press."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        runner = main_module._handeye_runner
        with patch.object(runner, "end") as end:
            with patch.object(main_module, "_create_error_alert") as orphan:
                assert self._press(mock_client) is True
        end.assert_called_once()
        orphan.assert_not_called()

    def test_a_press_with_no_alert_uuid_still_answers(self, mock_client, unthrottled):
        """An older dashboard may omit it; the follow-up alert is still worth
        raising even when there is no stale card to resolve."""
        with patch.object(main_module, "_resolve_alert_by_uuid"):
            with patch.object(main_module, "_create_error_alert") as alert:
                main_module._handle_handeye_button(
                    mock_client,
                    "arm-uuid",
                    {"flow": "so101_handeye", "action": "cancel"},
                )
        alert.assert_called_once()


class TestOrphanedAlert:
    """A button press the driver cannot answer must not wedge the whole card.

    The dashboard clears a pressed button's "Waiting..." only when the alert's
    ``updated_at`` changes, and it disables the entire button group while one is
    pending -- so a step that returns without publishing disables Cancel too, and
    the only way out is to leave the page. The press itself does not bump
    ``updated_at``: press_alert_button dispatches and returns without saving.
    """

    def test_a_step_with_no_run_resolves_the_stale_alert(self, mock_client):
        """Resolving is what unsticks the UI: the card unmounts and takes the
        pressed-button state with it."""
        main_module._handeye_runner = None
        with patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve:
            with patch.object(main_module, "_create_error_alert"):
                main_module._handeye_step(mock_client, "arm-uuid", "capture", "alert-9")
        mock_resolve.assert_called_once_with(mock_client, "arm-uuid", "alert-9")

    def test_it_says_why_the_buttons_vanished(self, mock_client, unthrottled):
        main_module._handeye_runner = None
        with patch.object(main_module, "_resolve_alert_by_uuid"):
            with patch.object(main_module, "_create_error_alert") as mock_alert:
                main_module._handeye_step(mock_client, "arm-uuid", "capture", "alert-9")
        assert mock_alert.call_count == 1
        assert mock_alert.call_args.args[2] == "handeye_run_gone"

    def test_a_runner_whose_flow_ended_is_the_same_case(self, mock_client):
        """end() detaches the flow before resolving the alert and swallows a
        failed resolve, so a live runner with no flow is the reachable shape."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        main_module._handeye_runner._flow = None
        with patch.object(main_module, "_resolve_alert_by_uuid") as mock_resolve:
            with patch.object(main_module, "_create_error_alert"):
                main_module._handeye_step(mock_client, "arm-uuid", "solve", "alert-3")
        mock_resolve.assert_called_once_with(mock_client, "arm-uuid", "alert-3")

    def test_a_live_run_still_steps_normally(self, mock_client):
        flow = _flow()
        main_module._install_handeye_flow(mock_client, "arm-uuid", flow)
        with patch.object(main_module, "_publish_handeye_alert"):
            with patch.object(main_module, "_create_error_alert") as mock_alert:
                main_module._handeye_step(mock_client, "arm-uuid", "capture", "alert-1")
        flow.capture_sample.assert_called_once()
        mock_alert.assert_not_called()

    def test_a_press_with_no_alert_uuid_still_warns(self, mock_client, unthrottled):
        """Older payloads carry no uuid. Nothing to resolve, but the operator
        still gets told rather than watching a dead card."""
        main_module._handeye_runner = None
        with patch.object(main_module, "_resolve_alert_by_uuid"):
            with patch.object(main_module, "_create_error_alert") as mock_alert:
                main_module._handeye_step(mock_client, "arm-uuid", "capture", None)
        assert mock_alert.call_count == 1

    def test_the_button_router_passes_the_alert_uuid_through(self, mock_client):
        """The uuid only helps if it survives the hop from the MQTT payload."""
        from utils.cw_handeye import HANDEYE_BUTTON_FLOW

        with patch.object(main_module, "_handeye_step") as mock_step:
            main_module._handle_handeye_button(
                mock_client,
                "arm-uuid",
                {"flow": HANDEYE_BUTTON_FLOW, "action": "capture", "alert_uuid": "a-7"},
            )
        mock_step.assert_called_once_with(mock_client, "arm-uuid", "capture", "a-7")

    def test_a_double_click_leaves_one_alert_not_two(self, mock_client, unthrottled):
        """_create_error_alert never auto-resolves, so an unthrottled path would
        leave one permanent alert per click."""
        main_module._handeye_runner = None
        with patch.object(main_module, "_resolve_alert_by_uuid"):
            with patch.object(main_module, "_create_error_alert") as mock_alert:
                main_module._handeye_step(mock_client, "arm-uuid", "capture", "a-1")
                main_module._handeye_step(mock_client, "arm-uuid", "capture", "a-1")
        assert mock_alert.call_count == 1
