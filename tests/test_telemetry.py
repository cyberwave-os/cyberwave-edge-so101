"""Unit tests for telemetry messages (start, end, initial observation, disconnect)."""

from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from motors import MotorNormMode


class TestPublishInitialObservations:
    """Tests for publish_initial_observations (telemetry_start with initial observations)."""

    def test_calls_publish_telemetry_start_message_with_follower_observations(self):
        """publish_initial_observations calls mqtt_client.publish_telemetry_start_message with metadata."""
        from utils.cw_teleoperate_helpers import publish_initial_observations

        mock_mqtt = MagicMock()
        mock_robot = MagicMock()
        mock_robot.uuid = "twin-abc-123"

        mock_follower = MagicMock()
        mock_follower.get_observation.return_value = {"j1.pos": 0.5, "j2.pos": -0.3}
        mock_follower.motors = {
            "j1": MagicMock(id=1),
            "j2": MagicMock(id=2),
        }

        joint_name_to_norm_mode = {"j1": MotorNormMode.RANGE_M100_100, "j2": MotorNormMode.RANGE_M100_100}
        motor_id_to_schema_joint = {1: "_1", 2: "_2"}

        publish_initial_observations(
            leader=None,
            follower=mock_follower,
            robot=mock_robot,
            mqtt_client=mock_mqtt,
            leader_calibration=None,
            follower_calibration=None,
            joint_name_to_norm_mode=joint_name_to_norm_mode,
            motor_id_to_schema_joint=motor_id_to_schema_joint,
            fps=100,
        )

        mock_mqtt.publish_telemetry_start_message.assert_called_once()
        call_args = mock_mqtt.publish_telemetry_start_message.call_args
        assert call_args[0][0] == "twin-abc-123"
        metadata = call_args[0][1]
        assert metadata["fps"] == 100
        assert "edge_follower" in metadata["observations"]
        assert "_1" in metadata["observations"]["edge_follower"]
        assert "_2" in metadata["observations"]["edge_follower"]

    def test_calls_publish_telemetry_start_message_with_leader_and_follower(self):
        """publish_initial_observations includes both leader and follower when both provided."""
        from utils.cw_teleoperate_helpers import publish_initial_observations

        mock_mqtt = MagicMock()
        mock_robot = MagicMock()
        mock_robot.uuid = "twin-xyz"

        mock_leader = MagicMock()
        mock_leader.get_observation.return_value = {"j1.pos": 0.0}
        mock_leader.motors = {"j1": MagicMock(id=1)}

        mock_follower = MagicMock()
        mock_follower.get_observation.return_value = {"j1.pos": 0.1}
        mock_follower.motors = {"j1": MagicMock(id=1)}

        joint_name_to_norm_mode = {"j1": MotorNormMode.RANGE_M100_100}
        motor_id_to_schema_joint = {1: "_1"}

        publish_initial_observations(
            leader=mock_leader,
            follower=mock_follower,
            robot=mock_robot,
            mqtt_client=mock_mqtt,
            leader_calibration=None,
            follower_calibration=None,
            joint_name_to_norm_mode=joint_name_to_norm_mode,
            motor_id_to_schema_joint=motor_id_to_schema_joint,
            fps=100,
        )

        metadata = mock_mqtt.publish_telemetry_start_message.call_args[0][1]
        assert "edge_leader" in metadata["observations"]
        assert "edge_follower" in metadata["observations"]

    def test_does_not_call_publish_telemetry_start_message_when_no_observations(self):
        """publish_initial_observations does not call when leader and follower are None."""
        from utils.cw_teleoperate_helpers import publish_initial_observations

        mock_mqtt = MagicMock()
        mock_robot = MagicMock()
        mock_robot.uuid = "twin-empty"

        publish_initial_observations(
            leader=None,
            follower=None,
            robot=mock_robot,
            mqtt_client=mock_mqtt,
            leader_calibration=None,
            follower_calibration=None,
            joint_name_to_norm_mode={},
            motor_id_to_schema_joint={},
        )

        mock_mqtt.publish_telemetry_start_message.assert_not_called()


class TestMainStopCurrentOperationTelemetry:
    """Tests for main._stop_current_operation sending disconnect."""

    def test_sends_disconnect_when_stopping_operation(self):
        """_stop_current_operation calls publish_disconnected when thread was running."""
        import main as main_module

        mock_client = MagicMock()
        mock_thread = MagicMock()
        mock_thread.is_alive.side_effect = [True, False]  # alive, then dead after join

        with patch.object(main_module, "_current_thread", mock_thread):
            with patch.object(main_module, "_current_follower", None):
                with patch.object(main_module, "_operation_stop_event", MagicMock()):
                    with patch.object(main_module, "_current_client", mock_client):
                        with patch.object(main_module, "_current_twin_uuid", "twin-123"):
                            with patch.object(main_module, "_calibration_proc", None):
                                main_module._stop_current_operation()

        mock_client.mqtt.publish_disconnected.assert_called_once_with("twin-123")

    def test_does_not_send_disconnect_when_no_thread(self):
        """_stop_current_operation does not call disconnect when no operation was running."""
        import main as main_module

        mock_client = MagicMock()

        with patch.object(main_module, "_current_thread", None):
            with patch.object(main_module, "_current_client", mock_client):
                with patch.object(main_module, "_calibration_proc", None):
                    main_module._stop_current_operation()

        mock_client.mqtt.publish_disconnected.assert_not_called()

    def test_does_not_send_disconnect_when_client_or_twin_missing(self):
        """_stop_current_operation does not crash when _current_client or _current_twin_uuid is None."""
        import main as main_module

        mock_thread = MagicMock()
        mock_thread.is_alive.side_effect = [True, False]

        with patch.object(main_module, "_current_thread", mock_thread):
            with patch.object(main_module, "_current_follower", None):
                with patch.object(main_module, "_operation_stop_event", MagicMock()):
                    with patch.object(main_module, "_current_client", None):
                        with patch.object(main_module, "_current_twin_uuid", "twin-123"):
                            with patch.object(main_module, "_calibration_proc", None):
                                main_module._stop_current_operation()

        # No assert - we're checking it doesn't crash. _publish_disconnect_message not called
        # because _current_client is None (the if check fails).


class TestRemoteoperateTelemetryEnd:
    """Tests for remoteoperate publishing telemetry_end on exit."""

    def test_remoteoperate_calls_publish_telemetry_end_in_finally(self):
        """remoteoperate calls publish_telemetry_end when exiting (stop_event or error)."""
        from scripts.cw_remoteoperate import remoteoperate

        mock_client = MagicMock()
        mock_client.mqtt.connected = True
        mock_client.mqtt.publish_telemetry_end = MagicMock()

        mock_follower = MagicMock()
        mock_follower.connected = True
        mock_follower.torque_enabled = True
        mock_follower.calibration = None
        mock_follower.get_observation.return_value = {}
        mock_follower.motors = {}
        mock_follower.bus = MagicMock()

        mock_robot = MagicMock()
        mock_robot.uuid = "robot-twin-456"

        stop_event = __import__("threading").Event()
        stop_event.set()  # Exit immediately

        with patch("scripts.cw_remoteoperate.check_calibration_required"):
            with patch("scripts.cw_remoteoperate.build_joint_mappings") as mock_build:
                mock_build.return_value = {
                    "joint_index_to_name": {},
                    "joint_name_to_norm_mode": {},
                    "motor_id_to_schema_joint": {},
                    "schema_joint_to_motor_id": {},
                    "joint_name_to_index": {},
                }
            with patch("scripts.cw_remoteoperate.resolve_calibration_for_edge", return_value={}):
                with patch("scripts.cw_remoteoperate.create_joint_state_callback"):
                    with patch("scripts.cw_remoteoperate.joint_position_heartbeat_thread"):
                        with patch("scripts.cw_remoteoperate.motor_writer_worker"):
                            with patch("scripts.cw_remoteoperate.CameraStreamManager"):
                                remoteoperate(
                                    client=mock_client,
                                    follower=mock_follower,
                                    robot=mock_robot,
                                    cameras=None,
                                    stop_event=stop_event,
                                )

        mock_client.mqtt.publish_telemetry_end.assert_called_once_with("robot-twin-456")


class TestTeleoperateTelemetryEnd:
    """Tests for teleoperate publishing telemetry_end on exit."""

    def test_teleoperate_calls_publish_telemetry_end_in_finally(self):
        """teleoperate calls publish_telemetry_end when exiting (stop_event or error)."""
        from scripts.cw_teleoperate import teleoperate

        mock_client = MagicMock()
        mock_client.mqtt.connected = True
        mock_client.mqtt.publish_telemetry_end = MagicMock()

        mock_leader = MagicMock()
        mock_leader.connected = True
        mock_leader.calibration = None
        mock_leader.get_observation.return_value = {}
        mock_leader.motors = {}

        mock_follower = MagicMock()
        mock_follower.connected = True
        mock_follower.torque_enabled = True
        mock_follower.calibration = None
        mock_follower.get_observation.return_value = {}
        mock_follower.motors = {}
        mock_follower.send_action = MagicMock()

        mock_robot = MagicMock()
        mock_robot.uuid = "teleop-twin-789"

        stop_event = __import__("threading").Event()
        stop_event.set()

        with patch("scripts.cw_teleoperate.check_calibration_required"):
            with patch("scripts.cw_teleoperate.build_joint_mappings") as mock_build:
                mock_build.return_value = {
                    "joint_index_to_name": {},
                    "joint_name_to_index": {},
                    "joint_name_to_norm_mode": {},
                    "motor_id_to_schema_joint": {},
                    "joint_index_to_name_str": {},
                }
            with patch("scripts.cw_teleoperate.resolve_calibration_for_edge", return_value={}):
                with patch("scripts.cw_teleoperate.upload_calibration_to_twin"):
                    with patch("scripts.cw_teleoperate.cyberwave_update_worker"):
                        with patch("scripts.cw_teleoperate.CameraStreamManager"):
                            with patch("scripts.cw_teleoperate.keyboard_input_thread"):
                                teleoperate(
                                    leader=mock_leader,
                                    cyberwave_client=mock_client,
                                    follower=mock_follower,
                                    robot=mock_robot,
                                    cameras=[],
                                    stop_event=stop_event,
                                )

        mock_client.mqtt.publish_telemetry_end.assert_called_once_with("teleop-twin-789")


class TestMainTeleoperateIntegration:
    """Integration test: main starts teleoperate, stops it, verifies telemetry messages."""

    def test_main_start_stop_teleoperate_produces_telemetry_messages(self):
        """handle_command(teleoperate) then handle_command(stop) produces expected MQTT messages."""
        import main as main_module

        twin_uuid = "integration-twin-001"
        mock_client = MagicMock()
        mock_client.mqtt.connected = True
        mock_client.mqtt.publish_telemetry_start_message = MagicMock()
        mock_client.mqtt.publish_telemetry_end = MagicMock()
        mock_client.mqtt.publish_disconnected = MagicMock()
        mock_client.mqtt.publish_command_message = MagicMock()

        mock_robot = MagicMock()
        mock_robot.uuid = twin_uuid
        mock_client.twin.return_value = mock_robot

        cfg = {
            "follower_port": "/dev/ttyUSB0",
            "leader_port": "/dev/ttyUSB1",
            "follower_id": "follower1",
            "leader_id": "leader1",
            "max_relative_target": None,
            "cameras": [],
        }

        mock_leader = MagicMock()
        mock_follower = MagicMock()

        def _minimal_teleoperate(leader, cyberwave_client, follower, robot, cameras=None, stop_event=None, **kwargs):
            """Minimal teleoperate that emits telemetry messages for integration test."""
            mqtt = cyberwave_client.mqtt
            mqtt.publish_telemetry_start_message(str(robot.uuid), {"fps": 100, "observations": {}})
            try:
                if stop_event:
                    stop_event.wait(timeout=2.0)
            finally:
                mqtt.publish_telemetry_end(str(robot.uuid))

        with patch.object(main_module, "_get_hardware_config", return_value=cfg):
            with patch.object(main_module, "_is_follower_calibrated", return_value=True):
                with patch.object(main_module, "_is_leader_calibrated", return_value=True):
                    with patch("so101.follower.SO101Follower", return_value=mock_follower):
                        with patch("so101.leader.SO101Leader", return_value=mock_leader):
                            with patch(
                                "scripts.cw_teleoperate.teleoperate",
                                _minimal_teleoperate,
                            ):
                                # Start teleoperate
                                main_module.handle_command(
                                    mock_client, twin_uuid, "teleoperate", {}
                                )

                                time.sleep(0.3)

                                # Stop
                                main_module.handle_command(
                                    mock_client, twin_uuid, "stop", {}
                                )

        # Verify telemetry messages
        mock_client.mqtt.publish_telemetry_start_message.assert_called()
        mock_client.mqtt.publish_telemetry_end.assert_called_with(twin_uuid)
        mock_client.mqtt.publish_disconnected.assert_called_with(twin_uuid)
