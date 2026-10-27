"""Closing the hand-eye alert from the dashboard must end the run.

Cancel and Done reach the driver as button presses, but resolving or silencing
the alert is a backend state transition on a topic the driver never read. The run
kept the wrist camera and the serial bus until the 900s idle reaper fired, so the
operator saw a twin with no stream and no alert explaining it.
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


def _flow(alert_uuid="alert-1"):
    flow = MagicMock()
    flow.alert_uuid = alert_uuid
    flow.holds_devices = True
    flow.idle_seconds.return_value = 0.0
    return flow


def _resolved(uuid="alert-1"):
    return {"uuid": uuid, "status": "resolved"}


class TestNetworkThreadMatch:
    """_on_twin_alert_update runs on the paho thread: it may only match and enqueue."""

    def test_resolving_the_active_alert_enqueues_the_teardown(self, mock_client):
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(mock_client, "arm-uuid", _resolved())
        enqueue.assert_called_once_with(
            mock_client,
            "arm-uuid",
            main_module.HANDEYE_ALERT_CLOSED_COMMAND,
            {"alert_uuid": "alert-1"},
        )

    def test_silencing_counts_as_closing(self, mock_client):
        """A silenced alert is just as invisible, so the run is just as undrivable."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(
                mock_client, "arm-uuid", {"uuid": "alert-1", "status": "silenced"}
            )
        enqueue.assert_called_once()

    def test_the_runs_own_alert_being_created_is_ignored(self, mock_client):
        """Every alert this twin raises comes back on this topic, our own included."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(
                mock_client, "arm-uuid", {"uuid": "alert-1", "status": "active"}
            )
        enqueue.assert_not_called()

    def test_another_alert_resolving_is_ignored(self, mock_client):
        """An unrelated error alert closing must not tear down the calibration."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(mock_client, "arm-uuid", _resolved("other"))
        enqueue.assert_not_called()

    def test_a_uuid_object_on_the_flow_still_matches(self, mock_client):
        """The alerts API hands back a UUID; the payload carries text."""
        import uuid as uuid_module

        value = uuid_module.uuid4()
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow(value))
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(mock_client, "arm-uuid", _resolved(str(value)))
        enqueue.assert_called_once()

    def test_no_run_is_ignored(self, mock_client):
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(mock_client, "arm-uuid", _resolved())
        enqueue.assert_not_called()

    @pytest.mark.parametrize("payload", ["not a dict", None, {}, {"status": "resolved"}])
    def test_junk_payloads_are_ignored_without_raising(self, mock_client, payload):
        """This runs on the network thread; raising there kills the subscription."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        with patch.object(main_module, "_enqueue_command") as enqueue:
            main_module._on_twin_alert_update(mock_client, "arm-uuid", payload)
        enqueue.assert_not_called()


class TestWorkerThreadTeardown:
    def test_it_ends_the_run_without_resolving_the_alert_again(self, mock_client):
        """The backend already resolved it -- that publish is what got us here."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
        runner = main_module._handeye_runner
        with patch.object(runner, "end") as end:
            main_module._end_handeye_flow_for_closed_alert(mock_client, "arm-uuid", "alert-1")
        end.assert_called_once_with(resolve_alert=False)

    def test_a_restarted_run_is_left_alone(self, mock_client):
        """Matched on the network thread, then the operator pressed Start again."""
        main_module._install_handeye_flow(mock_client, "arm-uuid", _flow("alert-2"))
        runner = main_module._handeye_runner
        with patch.object(runner, "end") as end:
            main_module._end_handeye_flow_for_closed_alert(mock_client, "arm-uuid", "alert-1")
        end.assert_not_called()

    def test_no_run_left_is_a_no_op(self, mock_client):
        main_module._end_handeye_flow_for_closed_alert(mock_client, "arm-uuid", "alert-1")

    def test_the_command_dispatcher_routes_it(self, mock_client):
        with patch.object(main_module, "_end_handeye_flow_for_closed_alert") as teardown:
            main_module.handle_command(
                mock_client,
                "arm-uuid",
                main_module.HANDEYE_ALERT_CLOSED_COMMAND,
                {"alert_uuid": "alert-1"},
            )
        teardown.assert_called_once_with(mock_client, "arm-uuid", "alert-1")


def test_the_drivers_own_cancel_does_not_bounce_back(mock_client):
    """Cancel resolves the alert too, so the backend echoes it onto this topic.

    Nothing ends twice because ``HandEyeRunner.end`` detaches the flow *before* it
    resolves -- by the time the echo arrives there is no run to match. That
    ordering is load-bearing, hence this test rather than a comment.
    """
    main_module._install_handeye_flow(mock_client, "arm-uuid", _flow())
    with patch.object(main_module, "_resume_after_handeye"):
        main_module._end_handeye_flow(mock_client, "arm-uuid")

    with patch.object(main_module, "_enqueue_command") as enqueue:
        main_module._on_twin_alert_update(mock_client, "arm-uuid", _resolved())
    enqueue.assert_not_called()
