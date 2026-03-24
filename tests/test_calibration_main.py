"""Unit tests for main.py calibration-related functionality."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add project root for imports (conftest mocks cyberwave before this)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def mock_client():
    """Create a mock Cyberwave client with MQTT and alert update chain."""
    client = MagicMock()
    client.mqtt.publish_command_message = MagicMock()
    mock_alert = MagicMock()
    mock_alert.metadata = {}
    mock_robot = MagicMock()
    mock_robot.alerts.get.return_value = mock_alert
    client.twin.return_value = mock_robot
    return client


@pytest.fixture
def valid_calibration_data():
    """Valid calibration start payload."""
    return {
        "step": "start_calibration",
        "type": "follower",
        "follower_port": "/dev/ttyACM0",
        "follower_id": "follower1",
        "alert_uuid": "alert-uuid-123",
    }


class TestCalibrationConcurrencyGuard:
    """Tests for calibration concurrency guard in _handle_calibration_start."""

    def test_rejects_concurrent_calibration_when_proc_running(
        self, mock_client, valid_calibration_data
    ):
        """When _calibration_active_count > 0, start_calibration should be rejected."""
        import main as main_module

        with patch.object(main_module, "_calibration_active_count", 1):
            with patch.object(main_module, "threading") as mock_threading:
                mock_threading.Thread = MagicMock()

                main_module._handle_calibration_start(
                    mock_client,
                    "twin-uuid-456",
                    valid_calibration_data,
                )

        mock_client.mqtt.publish_command_message.assert_called_once_with(
            "twin-uuid-456",
            {"status": "error", "reason": "calibration_already_running"},
        )
        mock_threading.Thread.return_value.start.assert_not_called()

    def test_allows_calibration_when_no_proc_running(
        self, mock_client, valid_calibration_data
    ):
        """When no calibration is running, start_calibration should proceed."""
        import main as main_module

        mock_alert = MagicMock()
        mock_alert.uuid = "alert-uuid-123"
        mock_client.twin.return_value.alerts.create.return_value = mock_alert

        with patch.object(main_module, "_calibration_active_count", 0):
            with patch.object(main_module, "_stop_current_operation", MagicMock()):
                with patch.object(
                    main_module, "_get_hardware_config",
                    return_value={
                        "follower_port": "/dev/ttyACM0",
                        "leader_port": "/dev/ttyACM1",
                        "follower_id": "follower1",
                        "leader_id": "leader1",
                    },
                ):
                    with patch.object(main_module, "threading") as mock_threading:
                        mock_thread = MagicMock()
                        mock_threading.Thread.return_value = mock_thread

                        main_module._handle_calibration_start(
                            mock_client,
                            "twin-uuid-456",
                            valid_calibration_data,
                        )

        mock_threading.Thread.assert_called_once()
        call_kwargs = mock_threading.Thread.call_args.kwargs
        assert call_kwargs["kwargs"]["alert_uuid"] is not None
        mock_thread.start.assert_called_once()

    def test_allows_calibration_when_proc_finished(
        self, mock_client, valid_calibration_data
    ):
        """When _calibration_active_count is 0 (proc finished), allow new start."""
        import main as main_module

        mock_alert = MagicMock()
        mock_alert.uuid = "alert-uuid-123"
        mock_client.twin.return_value.alerts.create.return_value = mock_alert

        with patch.object(main_module, "_calibration_flow_step", None):
            with patch.object(main_module, "_calibration_active_count", 0):
                with patch.object(main_module, "_stop_current_operation", MagicMock()):
                    with patch.object(
                        main_module, "_get_hardware_config",
                        return_value={
                            "follower_port": "/dev/ttyACM0",
                            "leader_port": "/dev/ttyACM1",
                            "follower_id": "follower1",
                            "leader_id": "leader1",
                        },
                    ):
                        with patch.object(main_module, "threading") as mock_threading:
                            mock_thread = MagicMock()
                            mock_threading.Thread.return_value = mock_thread

                            main_module._handle_calibration_start(
                                mock_client,
                                "twin-uuid-456",
                                valid_calibration_data,
                            )

        mock_threading.Thread.assert_called_once()
        mock_thread.start.assert_called_once()

    def test_rejects_calibration_start_when_operation_running(
        self, mock_client, valid_calibration_data
    ):
        """Calibration cannot start while teleoperate/remoteoperate is active."""
        import main as main_module

        with patch.object(main_module, "_calibration_flow_step", None):
            with patch.object(main_module, "_is_control_operation_running", return_value=True):
                with patch.object(main_module, "threading") as mock_threading:
                    main_module._handle_calibration_start(
                        mock_client,
                        "twin-uuid-456",
                        valid_calibration_data,
                    )

        mock_client.mqtt.publish_command_message.assert_called_once_with(
            "twin-uuid-456",
            {"status": "error", "reason": "operation_running"},
        )
        mock_threading.Thread.assert_not_called()

    def test_restart_calibration_terminates_existing_and_starts_new(
        self, mock_client, valid_calibration_data
    ):
        """restart_calibration should stop the running process and launch a new one."""
        import main as main_module

        fake_proc = MagicMock()
        fake_proc.poll.return_value = None  # Running calibration

        with patch.object(main_module, "_is_control_operation_running", return_value=False):
            with patch.object(main_module, "_calibration_proc", fake_proc):
                with patch.object(main_module, "_stop_current_operation", MagicMock()) as mock_stop:
                    with patch.object(main_module, "threading") as mock_threading:
                        mock_thread = MagicMock()
                        mock_threading.Thread.return_value = mock_thread

                        main_module._handle_calibration_restart(
                            mock_client,
                            "twin-uuid-456",
                            valid_calibration_data,
                        )

        mock_stop.assert_called_once()
        mock_threading.Thread.assert_called_once()
        mock_thread.start.assert_called_once()

    def test_restart_calibration_creates_operation_interrupted_alert_when_control_op_running(
        self, mock_client, valid_calibration_data
    ):
        """restart_calibration creates operation_interrupted alert when control op is running."""
        import main as main_module

        mock_alert = MagicMock()
        mock_robot = MagicMock()
        mock_robot.alerts.create.return_value = mock_alert
        mock_client.twin.return_value = mock_robot

        with patch.object(main_module, "_is_control_operation_running", return_value=True):
            with patch.object(main_module, "_calibration_proc", None):
                with patch.object(main_module, "_stop_current_operation", MagicMock()):
                    with patch.object(main_module, "threading") as mock_threading:
                        mock_threading.Thread.return_value = MagicMock()

                        main_module._handle_calibration_restart(
                            mock_client,
                            "twin-uuid-456",
                            {**valid_calibration_data, "follower_port": "/dev/ttyACM0"},
                        )

        create_calls = mock_robot.alerts.create.call_args_list
        assert len(create_calls) >= 1
        first_call_kwargs = create_calls[0].kwargs
        assert first_call_kwargs["name"] == "Operation stopped for calibration"
        assert first_call_kwargs["alert_type"] == "operation_interrupted"
        assert "assign a controller" in first_call_kwargs["description"]

    def test_restart_calibration_preserves_recovery_command(
        self, mock_client, valid_calibration_data
    ):
        """restart_calibration should preserve _pending_recovery_command so operation resumes after."""
        import main as main_module

        fake_proc = MagicMock()
        fake_proc.poll.return_value = None  # Running calibration

        with patch.object(main_module, "_is_control_operation_running", return_value=False):
            with patch.object(main_module, "_calibration_proc", fake_proc):
                with patch.object(main_module, "_stop_current_operation", MagicMock()):
                    with patch.object(main_module, "threading") as mock_threading:
                        mock_threading.Thread.return_value = MagicMock()

                        # Simulate user had started teleoperate, calibration needed, recovery stored
                        main_module._pending_recovery_command = "teleoperate"

                        main_module._handle_calibration_restart(
                            mock_client,
                            "twin-uuid-456",
                            valid_calibration_data,
                        )

        # Recovery command must be preserved for when new calibration completes
        assert main_module._pending_recovery_command == "teleoperate"


class TestCalibrationAlertPayload:
    """Tests for calibration alert payload emitted from main flow."""

    def test_trigger_alert_starts_calibration_at_zero_step(self, mock_client):
        """Trigger from teleop/remoteop starts calibration at zero position (no Start calibration step)."""
        import main as main_module

        mock_alert = MagicMock()
        mock_alert.uuid = "alert-uuid-123"
        mock_robot = MagicMock()
        mock_robot.alerts.create.return_value = mock_alert
        mock_robot.alerts.get.return_value = MagicMock(metadata={})
        mock_client.twin.return_value = mock_robot

        previous_recovery = main_module._pending_recovery_command
        try:
            main_module._calibration_active_count = 0
            with patch.object(main_module, "_calibration_flow_step", None):
                with patch.object(main_module, "_stop_current_operation", MagicMock()):
                    with patch.object(
                        main_module, "_get_hardware_config",
                        return_value={
                            "follower_port": "/dev/ttyACM0",
                            "leader_port": "/dev/ttyACM1",
                            "follower_id": "follower1",
                            "leader_id": "leader1",
                        },
                    ):
                        with patch.object(main_module, "threading") as mock_threading:
                            mock_threading.Thread.return_value = MagicMock()
                            main_module._trigger_alert_and_switch_to_calibration(
                                client=mock_client,
                                twin_uuid="twin-uuid-456",
                                follower_port="/dev/ttyACM0",
                                follower_id="follower1",
                                recovery_command="remoteoperate",
                                device_type="follower",
                            )
        finally:
            main_module._clear_calibration_flow_state(clear_recovery=False)
            main_module._pending_recovery_command = previous_recovery

        kwargs = mock_robot.alerts.create.call_args.kwargs
        assert kwargs["alert_type"] == "calibration_needed"
        assert kwargs["media"] == main_module.get_calibration_media_url("follower", "zero")
        assert kwargs["metadata"]["calibration"]["device_type"] == "follower"
        assert kwargs["metadata"]["calibration"]["follower_port"] == "/dev/ttyACM0"
        assert kwargs["metadata"]["calibration"]["state"] == "zero_pose_waiting"
        assert kwargs["metadata"]["buttons"][0]["label"] == "Next"
        assert kwargs["metadata"]["buttons"][0]["payload"]["flow"] == "so101_calibration"
        assert kwargs["metadata"]["buttons"][0]["payload"]["action"] == "next"


class TestCalibrationCommandPassthrough:
    """Tests that calibration commands pass alert_uuid and twin_uuid to subprocess."""

    def test_run_calibration_with_advance_includes_alert_and_twin_uuid(self):
        """_run_calibration_with_advance builds cmd with --alert-uuid and --twin-uuid."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        with patch.object(main_module, "_calibration_active_count", 1):
            with patch.object(main_module, "subprocess") as mock_subprocess:
                mock_popen = MagicMock()
                mock_popen.wait.return_value = 0
                mock_popen.stdin = None
                mock_subprocess.Popen.return_value = mock_popen

                main_module._run_calibration_with_advance(
                    mock_client,
                    twin_uuid="twin-123",
                    device_type="follower",
                    port="/dev/ttyACM0",
                    device_id="follower1",
                    alert_uuid="alert-456",
                )

        call_args = mock_subprocess.Popen.call_args[0][0]
        assert "--alert-uuid" in call_args
        assert "alert-456" in call_args
        assert "--twin-uuid" in call_args
        assert "twin-123" in call_args

    def test_run_calibration_without_alert_uuid_omits_alert_args(self):
        """When alert_uuid is None, --alert-uuid and --twin-uuid are not passed."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        with patch.object(main_module, "_calibration_active_count", 1):
            with patch.object(main_module, "subprocess") as mock_subprocess:
                mock_popen = MagicMock()
                mock_popen.wait.return_value = 0
                mock_popen.stdin = None
                mock_subprocess.Popen.return_value = mock_popen

                main_module._run_calibration_with_advance(
                    mock_client,
                    twin_uuid="twin-123",
                    device_type="follower",
                    port="/dev/ttyACM0",
                    device_id="follower1",
                    alert_uuid=None,
                )

        call_args = mock_subprocess.Popen.call_args[0][0]
        assert "--alert-uuid" not in call_args
        assert "--twin-uuid" not in call_args


class TestHandleCommandCalibrate:
    """Tests for handle_command with calibrate command."""

    def test_calibrate_start_dispatches_to_handle_calibration_start(
        self, mock_client, valid_calibration_data
    ):
        """handle_command with calibrate/start_calibration calls _handle_calibration_start."""
        import main as main_module

        with patch.object(
            main_module, "_handle_calibration_start", MagicMock()
        ) as mock_start:
            with patch.object(main_module, "_calibration_proc", None):
                with patch.object(main_module, "threading") as mock_threading:
                    mock_threading.Thread.return_value = MagicMock()

                    main_module.handle_command(
                        mock_client,
                        "twin-uuid-456",
                        "calibrate",
                        {"data": valid_calibration_data},
                    )

        mock_start.assert_called_once_with(
            mock_client, "twin-uuid-456", valid_calibration_data
        )

    def test_calibrate_restart_dispatches_to_handle_calibration_restart(
        self, mock_client, valid_calibration_data
    ):
        """handle_command with calibrate/restart_calibration calls restart handler."""
        import main as main_module

        restart_payload = dict(valid_calibration_data)
        restart_payload["step"] = "restart_calibration"

        with patch.object(
            main_module, "_handle_calibration_restart", MagicMock()
        ) as mock_restart:
            main_module.handle_command(
                mock_client,
                "twin-uuid-456",
                "calibrate",
                {"data": restart_payload},
            )

        mock_restart.assert_called_once_with(
            mock_client, "twin-uuid-456", restart_payload
        )

    def test_calibrate_advance_dispatches_with_script_args(
        self, mock_client, valid_calibration_data
    ):
        """handle_command with calibrate/advance forwards script args to next handler."""
        import main as main_module

        advance_payload = dict(valid_calibration_data)
        advance_payload["step"] = "advance"

        with patch.object(
            main_module, "_handle_calibration_next", MagicMock()
        ) as mock_next:
            main_module.handle_command(
                mock_client,
                "twin-uuid-456",
                "calibrate",
                {"data": advance_payload},
            )

        mock_next.assert_called_once_with(
            mock_client,
            "twin-uuid-456",
            advance_payload,
        )

    def test_calibrate_step_blocked_while_operation_running(
        self, mock_client, valid_calibration_data
    ):
        """Calibration MQTT steps are rejected while control operation is active."""
        import main as main_module

        with patch.object(main_module, "_is_control_operation_running", return_value=True):
            main_module.handle_command(
                mock_client,
                "twin-uuid-456",
                "calibrate",
                {"data": valid_calibration_data},
            )

        mock_client.mqtt.publish_command_message.assert_called_once_with(
            "twin-uuid-456",
            {"status": "error", "reason": "operation_running"},
        )

    def test_calibrate_restart_allowed_when_operation_running(self, mock_client):
        """restart_calibration is allowed when control operation is running (stops it gracefully)."""
        import main as main_module

        restart_payload = {
            "step": "restart_calibration",
            "type": "follower",
            "follower_id": "follower1",
        }
        with patch.object(main_module, "_is_control_operation_running", return_value=True):
            with patch.object(
                main_module, "_handle_calibration_restart", MagicMock()
            ) as mock_restart:
                main_module.handle_command(
                    mock_client,
                    "twin-uuid-456",
                    "calibrate",
                    {"data": restart_payload},
                )
        mock_restart.assert_called_once_with(
            mock_client, "twin-uuid-456", restart_payload
        )
        mock_client.mqtt.publish_command_message.assert_not_called()

    def test_calibrate_restart_accepts_frontend_payload_format(
        self, mock_client
    ):
        """handle_command accepts frontend TwinEditorPanel Run Again payload format.

        Frontend sends: { step, type, id, follower_id|leader_id } (no port - from config).
        """
        import main as main_module

        # Exact payload format from twin-editor-panel runCalibrationAgain
        follower_payload = {
            "step": "restart_calibration",
            "type": "follower",
            "id": "follower1",
            "follower_id": "follower1",
        }
        leader_payload = {
            "step": "restart_calibration",
            "type": "leader",
            "id": "leader1",
            "leader_id": "leader1",
        }

        for payload in (follower_payload, leader_payload):
            with patch.object(
                main_module, "_handle_calibration_restart", MagicMock()
            ) as mock_restart:
                main_module.handle_command(
                    mock_client,
                    "twin-uuid-456",
                    "calibrate",
                    {"command": "calibrate", "data": payload, "source_type": "edit"},
                )
            mock_restart.assert_called_once_with(
                mock_client, "twin-uuid-456", payload
            )


class TestStopCurrentOperationGracefulShutdown:
    """Tests for _stop_current_operation graceful shutdown behavior."""

    def test_sets_stop_event_before_force_disconnect(self):
        """_stop_current_operation sets stop_event first, then waits before force disconnect."""
        import main as main_module

        stop_event = __import__("threading").Event()
        mock_follower = MagicMock()
        mock_thread = MagicMock()
        mock_thread.is_alive.side_effect = [True, True, False]

        with patch.object(main_module, "_current_thread", mock_thread):
            with patch.object(main_module, "_current_follower", mock_follower):
                with patch.object(main_module, "_operation_stop_event", stop_event):
                    with patch.object(main_module, "_calibration_proc", None):
                        main_module._stop_current_operation()

        assert stop_event.is_set()
        mock_thread.join.assert_called()
        calls = mock_thread.join.call_args_list
        assert len(calls) >= 1
        assert calls[0][1]["timeout"] == main_module.GRACEFUL_JOIN_TIMEOUT

    def test_does_not_force_disconnect_when_thread_exits_gracefully(self):
        """When thread exits after stop_event, no force disconnect is needed."""
        import main as main_module

        stop_event = __import__("threading").Event()
        mock_follower = MagicMock()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False

        with patch.object(main_module, "_current_thread", mock_thread):
            with patch.object(main_module, "_current_follower", mock_follower):
                with patch.object(main_module, "_operation_stop_event", stop_event):
                    with patch.object(main_module, "_calibration_proc", None):
                        main_module._stop_current_operation()

        mock_follower.disconnect.assert_not_called()

    def test_force_disconnects_when_thread_does_not_exit_gracefully(self):
        """When thread does not exit within graceful timeout, force disconnect is called."""
        import main as main_module

        stop_event = __import__("threading").Event()
        mock_follower = MagicMock()
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = True  # Thread never exits

        with patch.object(main_module, "_current_thread", mock_thread):
            with patch.object(main_module, "_current_follower", mock_follower):
                with patch.object(main_module, "_operation_stop_event", stop_event):
                    with patch.object(main_module, "_calibration_proc", None):
                        main_module._stop_current_operation()

        mock_follower.disconnect.assert_called_once()

    def test_stops_calibration_subprocess_gracefully(self):
        """_stop_current_operation terminates calibration first, then kill if needed."""
        import main as main_module

        mock_proc = MagicMock()
        mock_proc.wait.return_value = None
        mock_thread = MagicMock()
        mock_thread.is_alive.return_value = False

        with patch.object(main_module, "_current_thread", mock_thread):
            with patch.object(main_module, "_current_follower", None):
                with patch.object(main_module, "_operation_stop_event", None):
                    with patch.object(main_module, "_calibration_proc", mock_proc):
                        main_module._stop_current_operation()

        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once_with(timeout=5)
        mock_proc.kill.assert_not_called()


class TestCalibrationAdvanceRecovery:
    """Tests for advance recovery flow when process is missing."""

    def test_advance_restarts_from_alert_when_process_missing(self):
        """When no process is running, advance recovers by restarting from alert metadata."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        payload = {"step": "advance", "alert_uuid": "alert-123", "type": "follower"}

        with patch.object(main_module, "_calibration_proc", None):
            with patch.object(
                main_module, "_handle_calibration_start", MagicMock()
            ) as mock_start:
                with patch.object(
                    main_module,
                    "_build_calibration_start_data_from_alert",
                    return_value={
                        "step": "start_calibration",
                        "type": "follower",
                        "alert_uuid": "alert-123",
                    },
                ):
                    with patch.object(
                        main_module,
                        "_wait_for_running_calibration_proc",
                        return_value=None,
                    ):
                        main_module._handle_calibration_advance(
                            mock_client,
                            "twin-123",
                            payload,
                        )

        mock_start.assert_called_once_with(
            mock_client,
            "twin-123",
            {
                "step": "start_calibration",
                "type": "follower",
                "alert_uuid": "alert-123",
            },
            publish_status=False,
        )
        mock_client.mqtt.publish_command_message.assert_called_once_with(
            "twin-123",
            {"status": "error", "reason": "calibration_not_running"},
        )

    def test_advance_returns_calibration_not_running_without_alert_uuid(self):
        """When no process/alert context exists, advance returns an explicit error reason."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        with patch.object(main_module, "_calibration_proc", None):
            with patch.object(
                main_module,
                "_build_calibration_start_data_from_alert",
                return_value=None,
            ):
                main_module._handle_calibration_advance(
                    mock_client,
                    "twin-123",
                    {"step": "advance"},
                )

        mock_client.mqtt.publish_command_message.assert_called_once_with(
            "twin-123",
            {"status": "error", "reason": "calibration_not_running"},
        )

    def test_advance_resolves_stale_alert_before_recovery(self):
        """When advance recovers, stale alert is resolved first to avoid alert spam."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()
        mock_robot = MagicMock()
        mock_alert = MagicMock()
        mock_robot.alerts.get.return_value = mock_alert
        mock_client.twin.return_value = mock_robot

        payload = {"step": "advance", "alert_uuid": "stale-alert-123", "type": "follower"}
        with patch.object(main_module, "_calibration_proc", None):
            with patch.object(
                main_module,
                "_build_calibration_start_data_from_alert",
                return_value={
                    "step": "start_calibration",
                    "type": "follower",
                    "alert_uuid": "stale-alert-123",
                    "follower_port": "/dev/ttyACM0",
                },
            ):
                with patch.object(
                    main_module, "_handle_calibration_start", MagicMock()
                ) as mock_start:
                    with patch.object(
                        main_module,
                        "_wait_for_running_calibration_proc",
                        return_value=None,
                    ):
                        main_module._handle_calibration_advance(
                            mock_client,
                            "twin-123",
                            payload,
                        )

        mock_robot.alerts.get.assert_called_with("stale-alert-123")
        mock_alert.resolve.assert_called_once()
        mock_start.assert_called_once()


class TestSingleScriptAtATime:
    """Tests for single-script-at-a-time concurrency guards."""

    def test_run_script_command_calls_stop_before_running(self):
        """_run_script_command calls _stop_current_operation before running script."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        with patch.object(main_module, "_stop_current_operation", MagicMock()) as mock_stop:
            with patch.object(main_module, "subprocess") as mock_subprocess:
                mock_subprocess.run.return_value = MagicMock(returncode=0)

                main_module._run_script_command(
                    mock_client,
                    twin_uuid="twin-123",
                    script_name="find_port",
                    data={},
                )

        mock_stop.assert_called_once()

    def test_run_script_calibrate_rejected_while_operation_running(self):
        """_run_script_command for calibrate is blocked while teleop/remoteop is active."""
        import main as main_module

        mock_client = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        with patch.object(main_module, "_is_control_operation_running", return_value=True):
            with patch.object(main_module, "_stop_current_operation", MagicMock()) as mock_stop:
                main_module._run_script_command(
                    mock_client,
                    twin_uuid="twin-123",
                    script_name="calibrate",
                    data={},
                )

        mock_stop.assert_not_called()
        mock_client.mqtt.publish_command_message.assert_called_once_with(
            "twin-123",
            {"status": "error", "reason": "operation_running"},
        )

    def test_handle_calibration_start_calls_stop_when_not_rejecting(self):
        """_handle_calibration_start calls _stop_current_operation before starting."""
        import main as main_module

        valid_data = {
            "type": "follower",
            "follower_port": "/dev/ttyACM0",
            "follower_id": "follower1",
        }

        with patch.object(main_module, "_calibration_proc", None):
            with patch.object(main_module, "_stop_current_operation", MagicMock()) as mock_stop:
                with patch.object(main_module, "threading") as mock_threading:
                    mock_threading.Thread.return_value = MagicMock()
                    mock_client = MagicMock()
                    mock_client.mqtt.publish_command_message = MagicMock()
                    mock_robot = MagicMock()
                    mock_alert = MagicMock()
                    mock_alert.metadata = {}
                    mock_robot.alerts.get.return_value = mock_alert
                    mock_client.twin.return_value = mock_robot

                    main_module._handle_calibration_start(
                        mock_client,
                        "twin-123",
                        valid_data,
                    )

        mock_stop.assert_called_once()

    def test_handle_calibration_start_uses_leader_port_when_both_ports_in_payload(
        self, mock_client, valid_calibration_data
    ):
        """When both follower_port and leader_port are in payload, use port matching type."""
        import main as main_module

        data_with_both_ports = {
            **valid_calibration_data,
            "type": "leader",
            "follower_port": "/dev/ttyACM0",
            "leader_port": "/dev/ttyACM1",
            "leader_id": "leader1",
        }

        mock_alert = MagicMock()
        mock_alert.uuid = "alert-uuid-123"
        mock_alert.metadata = {}
        mock_robot = MagicMock()
        mock_robot.alerts.get.return_value = mock_alert
        mock_robot.alerts.create.return_value = mock_alert
        mock_client.twin.return_value = mock_robot

        with patch.object(main_module, "_calibration_flow_step", None):
            with patch.object(main_module, "_calibration_active_count", 0):
                with patch.object(main_module, "_stop_current_operation", MagicMock()):
                    with patch.object(
                        main_module, "_get_hardware_config",
                        return_value={
                            "follower_port": "/dev/ttyACM0",
                            "leader_port": "/dev/ttyACM1",
                            "follower_id": "follower1",
                            "leader_id": "leader1",
                        },
                    ):
                        with patch.object(main_module, "threading") as mock_threading:
                            mock_threading.Thread.return_value = MagicMock()

                            main_module._handle_calibration_start(
                                mock_client,
                                "twin-123",
                                data_with_both_ports,
                            )

        assert mock_threading.Thread.called
        call_kwargs = mock_threading.Thread.call_args.kwargs
        thread_args = call_kwargs["args"]
        # _run_calibration_with_advance(client, twin_uuid, device_type, port, device_id)
        device_type, port, device_id = thread_args[2], thread_args[3], thread_args[4]
        assert device_type == "leader"
        assert port == "/dev/ttyACM1"
        assert device_id == "leader1"


class TestCalibrationCompleteInsufficientRange:
    """Tests for _handle_calibration_complete when calibration fails with insufficient range."""

    def test_creates_error_alert_with_warnings_when_exit_code_2(self, mock_client):
        """When exit code is 2 (insufficient range), create error alert with joint warnings."""
        import main as main_module

        mock_alert = MagicMock()
        mock_alert.uuid = "new-alert-uuid"
        mock_alert.metadata = {}
        mock_robot = MagicMock()
        mock_robot.alerts.create.return_value = mock_alert
        mock_robot.alerts.get.return_value = MagicMock(
            metadata={
                "calibration": {
                    "warnings": [
                        "shoulder_pan: Insufficient range - min (2052) should be <= 1638",
                    ],
                    "severity": "error",
                },
            }
        )
        mock_client.twin.return_value = mock_robot

        with patch.object(
            main_module, "_calibration_flow_step", main_module.CALIBRATION_STEP_RANGE
        ):
            with patch.object(main_module, "_calibration_alert_uuid", "alert-456"):
                with patch.object(main_module, "_calibration_proc", None):
                    with patch.object(main_module, "_calibration_last_exit_code", 2):
                        with patch.object(main_module, "_calibration_finished_event") as mock_event:
                            mock_event.wait.return_value = True
                            with patch.object(
                                main_module, "_handle_calibration_advance", return_value=True
                            ):
                                with patch.object(main_module, "_set_calibration_flow_state"):
                                    with patch.object(main_module, "_resolve_alert_by_uuid"):
                                        with patch.object(
                                            main_module,
                                            "_calibration_context",
                                            {
                                                "device_type": "follower",
                                                "follower_port": "/dev/ttyACM0",
                                                "follower_id": "follower1",
                                            },
                                        ):
                                            main_module._handle_calibration_complete(
                                                mock_client,
                                                "twin-123",
                                                {"alert_uuid": "alert-456"},
                                            )

        mock_robot.alerts.create.assert_called_once()
        create_kwargs = mock_robot.alerts.create.call_args.kwargs
        assert create_kwargs["severity"] == "error"
        assert create_kwargs["metadata"]["error_type"] == "insufficient_range"
        assert "calibration" in create_kwargs["metadata"]
        assert create_kwargs["metadata"]["calibration"]["device_type"] == "follower"

    def test_exit_code_2_creates_error_alert_regardless_of_metadata_severity(self, mock_client):
        """When exit code 2, create error alert (metadata severity is only used for exit code 0)."""
        import main as main_module

        mock_alert = MagicMock()
        mock_alert.uuid = "new-alert-uuid"
        mock_alert.metadata = {}
        mock_robot = MagicMock()
        mock_robot.alerts.create.return_value = mock_alert
        mock_robot.alerts.get.return_value = MagicMock(
            metadata={
                "calibration": {
                    "warnings": ["elbow_flex: Insufficient range - move further"],
                    "severity": "warning",
                },
            }
        )
        mock_client.twin.return_value = mock_robot

        with patch.object(
            main_module, "_calibration_flow_step", main_module.CALIBRATION_STEP_RANGE
        ):
            with patch.object(main_module, "_calibration_alert_uuid", "alert-456"):
                with patch.object(main_module, "_calibration_proc", None):
                    with patch.object(main_module, "_calibration_last_exit_code", 2):
                        with patch.object(main_module, "_calibration_finished_event") as mock_event:
                            mock_event.wait.return_value = True
                            with patch.object(
                                main_module, "_handle_calibration_advance", return_value=True
                            ):
                                with patch.object(main_module, "_set_calibration_flow_state"):
                                    with patch.object(main_module, "_resolve_alert_by_uuid"):
                                        with patch.object(
                                            main_module,
                                            "_calibration_context",
                                            {
                                                "device_type": "follower",
                                                "follower_port": "/dev/ttyACM0",
                                                "follower_id": "follower1",
                                            },
                                        ):
                                            main_module._handle_calibration_complete(
                                                mock_client,
                                                "twin-123",
                                                {"alert_uuid": "alert-456"},
                                            )

        create_kwargs = mock_robot.alerts.create.call_args.kwargs
        # Exit code 2 always creates error alert; metadata severity is only used for exit code 0
        assert create_kwargs["severity"] == "error"


class TestCalibrationFailurePreservesRecoveryCommand:
    """Tests that recovery command is preserved when calibration fails."""

    def test_handle_calibration_complete_preserves_recovery_on_failure(self, mock_client):
        """When calibration fails, _pending_recovery_command should NOT be cleared."""
        import main as main_module

        # Set up a pending recovery command (as if user tried teleoperate but needed calibration)
        original_recovery_command = "teleoperate"

        with patch.object(main_module, "_pending_recovery_command", original_recovery_command):
            with patch.object(main_module, "_calibration_proc", None):
                with patch.object(main_module, "_calibration_last_exit_code", 2):  # Insufficient range
                    with patch.object(main_module, "_calibration_finished_event") as mock_event:
                        mock_event.wait.return_value = True  # Process finished
                        with patch.object(main_module, "_handle_calibration_advance", return_value=True):
                            with patch.object(main_module, "_create_guided_calibration_alert", return_value="new-alert"):
                                with patch.object(main_module, "_set_calibration_flow_state"):
                                    with patch.object(main_module, "_resolve_alert_by_uuid"):
                                        main_module._handle_calibration_complete(
                                            mock_client,
                                            "twin-123",
                                            {"alert_uuid": "alert-456"},
                                        )

            # Recovery command should still be set (not cleared)
            assert main_module._pending_recovery_command == original_recovery_command

    def test_handle_calibration_complete_preserves_recovery_on_timeout(self, mock_client):
        """When calibration times out, _pending_recovery_command should NOT be cleared."""
        import main as main_module

        original_recovery_command = "remoteoperate"

        with patch.object(main_module, "_pending_recovery_command", original_recovery_command):
            with patch.object(main_module, "_calibration_proc", MagicMock()) as mock_proc:
                mock_proc.terminate = MagicMock()
                mock_proc.wait.return_value = -1
                mock_proc.poll.return_value = None
                with patch.object(main_module, "_calibration_finished_event") as mock_event:
                    mock_event.wait.return_value = False  # Timeout
                    with patch.object(main_module, "_handle_calibration_advance", return_value=True):
                        with patch.object(main_module, "_create_guided_calibration_alert", return_value="new-alert"):
                            with patch.object(main_module, "_set_calibration_flow_state"):
                                main_module._handle_calibration_complete(
                                    mock_client,
                                    "twin-123",
                                    {"alert_uuid": "alert-456"},
                                )

            # Recovery command should still be set (not cleared)
            assert main_module._pending_recovery_command == original_recovery_command

    def test_handle_calibration_next_preserves_recovery_on_failure(self, mock_client):
        """When calibration fails during next step, _pending_recovery_command should NOT be cleared."""
        import main as main_module

        original_recovery_command = "teleoperate"

        with patch.object(main_module, "_pending_recovery_command", original_recovery_command):
            with patch.object(main_module, "_calibration_proc", MagicMock()) as mock_proc:
                mock_proc.poll.return_value = 1  # Non-zero exit = failure
                with patch.object(main_module, "_calibration_last_exit_code", 1):
                    with patch.object(main_module, "_handle_calibration_advance", return_value=True):
                        with patch.object(main_module, "_create_guided_calibration_alert", return_value="new-alert"):
                            with patch.object(main_module, "_set_calibration_flow_state"):
                                with patch.object(main_module, "time"):
                                    main_module._handle_calibration_next(
                                        mock_client,
                                        "twin-123",
                                        {"alert_uuid": "alert-456"},
                                    )

            # Recovery command should still be set (not cleared)
            assert main_module._pending_recovery_command == original_recovery_command

    def test_handle_calibration_cancel_clears_recovery_command(self, mock_client):
        """When user explicitly cancels calibration, _pending_recovery_command SHOULD be cleared."""
        import main as main_module

        original_recovery_command = "teleoperate"

        # Create a mock calibration proc that is running
        mock_proc = MagicMock()
        mock_proc.terminate = MagicMock()
        mock_proc.wait.return_value = 0

        with patch.object(main_module, "_pending_recovery_command", original_recovery_command):
            with patch.object(main_module, "_calibration_proc", mock_proc):
                with patch.object(main_module, "_calibration_finished_event"):
                    with patch.object(main_module, "_resolve_alert_by_uuid"):
                        with patch.object(main_module, "_update_calibration_alert_metadata"):
                            main_module._handle_calibration_cancel(
                                mock_client,
                                "twin-123",
                                {"alert_uuid": "alert-456"},
                            )

            # Recovery command SHOULD be cleared on explicit cancel
            assert main_module._pending_recovery_command is None

    def test_handle_calibration_complete_clears_recovery_on_success(self, mock_client):
        """When calibration succeeds, recovery command runs and is cleared."""
        import main as main_module

        original_recovery_command = "teleoperate"

        with patch.object(
            main_module, "_calibration_flow_step", main_module.CALIBRATION_STEP_RANGE
        ):
            with patch.object(main_module, "_calibration_alert_uuid", "alert-456"):
                with patch.object(main_module, "_pending_recovery_command", original_recovery_command):
                    with patch.object(main_module, "_calibration_proc", None):
                        with patch.object(main_module, "_calibration_last_exit_code", 0):  # Success
                            with patch.object(main_module, "_calibration_finished_event") as mock_event:
                                mock_event.wait.return_value = True
                                with patch.object(main_module, "_handle_calibration_advance", return_value=True):
                                    with patch.object(main_module, "_clear_calibration_flow_state"):
                                        with patch.object(main_module, "_resolve_alert_by_uuid"):
                                            with patch.object(main_module, "start_teleoperate") as mock_start:
                                                main_module._handle_calibration_complete(
                                                    mock_client,
                                                    "twin-123",
                                                    {"alert_uuid": "alert-456"},
                                                )

                                                # Recovery command should run
                                                mock_start.assert_called_once_with(mock_client, "twin-123")

                    # Recovery command should be cleared after running
                    assert main_module._pending_recovery_command is None

    def test_handle_calibration_complete_runs_check_startup_calibration_recovery(self, mock_client):
        """When calibration succeeds with check_startup_calibration recovery, _check_startup_calibration is called."""
        import main as main_module

        with patch.object(
            main_module, "_calibration_flow_step", main_module.CALIBRATION_STEP_RANGE
        ):
            with patch.object(main_module, "_calibration_alert_uuid", "alert-456"):
                with patch.object(
                    main_module, "_pending_recovery_command", "check_startup_calibration"
                ):
                    with patch.object(main_module, "_calibration_proc", None):
                        with patch.object(main_module, "_calibration_last_exit_code", 0):
                            with patch.object(main_module, "_calibration_finished_event") as mock_event:
                                mock_event.wait.return_value = True
                                with patch.object(main_module, "_handle_calibration_advance", return_value=True):
                                    with patch.object(main_module, "_clear_calibration_flow_state"):
                                        with patch.object(main_module, "_resolve_alert_by_uuid"):
                                            with patch.object(
                                                main_module, "_check_startup_calibration"
                                            ) as mock_check:
                                                main_module._handle_calibration_complete(
                                                    mock_client,
                                                    "twin-123",
                                                    {"alert_uuid": "alert-456"},
                                                )

                                                mock_check.assert_called_once_with(mock_client, "twin-123")


class TestCheckStartupCalibration:
    """Tests for _check_startup_calibration (startup calibration file check)."""

    def test_no_trigger_when_both_calibrated(self, mock_client):
        """When follower and leader are calibrated, _trigger_alert_and_switch_to_calibration is not called."""
        import main as main_module

        cfg = {
            "follower_port": "/dev/ttyACM0",
            "leader_port": "/dev/ttyACM1",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_follower_calibrated", return_value=True):
                with patch.object(main_module, "_is_leader_calibrated", return_value=True):
                    with patch.object(
                        main_module, "_trigger_alert_and_switch_to_calibration"
                    ) as mock_trigger:
                        main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_not_called()

    def test_trigger_follower_with_recovery_when_both_missing(self, mock_client):
        """When both follower and leader calibration are missing, trigger follower with recovery=check_startup_calibration so leader runs next."""
        import main as main_module

        cfg = {
            "follower_port": "/dev/ttyACM0",
            "leader_port": "/dev/ttyACM1",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_follower_calibrated", return_value=False):
                with patch.object(main_module, "_is_leader_calibrated", return_value=False):
                    with patch.object(
                        main_module, "_trigger_alert_and_switch_to_calibration"
                    ) as mock_trigger:
                        main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_called_once_with(
            mock_client,
            "twin-123",
            follower_port="/dev/ttyACM0",
            follower_id="follower1",
            leader_port="/dev/ttyACM1",
            leader_id="leader1",
            recovery_command="check_startup_calibration",
            device_type="follower",
        )

    def test_trigger_follower_without_recovery_when_only_follower_missing(self, mock_client):
        """When only follower is missing (leader calibrated), trigger follower with no recovery."""
        import main as main_module

        cfg = {
            "follower_port": "/dev/ttyACM0",
            "leader_port": "/dev/ttyACM1",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_follower_calibrated", return_value=False):
                with patch.object(main_module, "_is_leader_calibrated", return_value=True):
                    with patch.object(
                        main_module, "_trigger_alert_and_switch_to_calibration"
                    ) as mock_trigger:
                        main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_called_once_with(
            mock_client,
            "twin-123",
            follower_port="/dev/ttyACM0",
            follower_id="follower1",
            leader_port="/dev/ttyACM1",
            leader_id="leader1",
            recovery_command=None,
            device_type="follower",
        )

    def test_trigger_leader_when_follower_ok_leader_missing(self, mock_client):
        """When follower is calibrated but leader is missing, trigger leader calibration."""
        import main as main_module

        cfg = {
            "follower_port": "/dev/ttyACM0",
            "leader_port": "/dev/ttyACM1",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_follower_calibrated", return_value=True):
                with patch.object(main_module, "_is_leader_calibrated", return_value=False):
                    with patch.object(
                        main_module, "_trigger_alert_and_switch_to_calibration"
                    ) as mock_trigger:
                        main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_called_once_with(
            mock_client,
            "twin-123",
            follower_port="/dev/ttyACM0",
            follower_id="follower1",
            leader_port="/dev/ttyACM1",
            leader_id="leader1",
            recovery_command=None,
            device_type="leader",
        )

    def test_no_trigger_when_no_ports_configured(self, mock_client):
        """When neither follower_port nor leader_port is set, no trigger."""
        import main as main_module

        cfg = {
            "follower_port": None,
            "leader_port": None,
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(
                main_module, "_trigger_alert_and_switch_to_calibration"
            ) as mock_trigger:
                main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_not_called()

    def test_no_trigger_when_only_leader_port_set_but_leader_calibrated(self, mock_client):
        """When only leader_port is set and leader is calibrated, no trigger."""
        import main as main_module

        cfg = {
            "follower_port": None,
            "leader_port": "/dev/ttyACM1",
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_leader_calibrated", return_value=True):
                with patch.object(
                    main_module, "_trigger_alert_and_switch_to_calibration"
                ) as mock_trigger:
                    main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_not_called()

    def test_trigger_follower_only_when_remoteoperate_config(self, mock_client):
        """When only follower_port is set (remoteoperate-only), trigger follower if missing."""
        import main as main_module

        cfg = {
            "follower_port": "/dev/ttyACM0",
            "leader_port": None,
            "follower_id": "follower1",
            "leader_id": "leader1",
        }
        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_follower_calibrated", return_value=False):
                with patch.object(
                    main_module, "_trigger_alert_and_switch_to_calibration"
                ) as mock_trigger:
                    main_module._check_startup_calibration(mock_client, "twin-123")

        mock_trigger.assert_called_once_with(
            mock_client,
            "twin-123",
            follower_port="/dev/ttyACM0",
            follower_id="follower1",
            leader_port=None,
            leader_id="leader1",
            recovery_command=None,
            device_type="follower",
        )
