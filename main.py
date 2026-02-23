"""Main entry point for the SO101 edge node.

Connects to the Cyberwave MQTT broker and listens for command events.
Runs as a long-running process inside the Docker container.
"""

import asyncio
import logging
import os
import signal
import sys
import threading
from typing import Optional
from config import FollowerConfig
from follower import SO101Follower

from cyberwave import Cyberwave

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("so101-edge")

# ---------------------------------------------------------------------------
# Module-level state for tracking the currently running operation.
# Only one operation (teleoperate / localop) runs at a time.
# ---------------------------------------------------------------------------
_current_thread: Optional[threading.Thread] = None
_current_follower: Optional[object] = (
    None  # SO101Follower – typed loosely to avoid top-level import
)


def _trigger_alert_and_switch_to_calibration(
    client: Cyberwave,
    twin_uuid: str,
    follower_port: str,
    follower_id: str = "follower1",
    leader_port: Optional[str] = None,
    leader_id: str = "leader1",
) -> None:
    """
    We ended up here because:
    - The user tried to start local teleop or remote teleop AND
    - No calibration file was found
    So what happens is:
    - We should trigger an alert (check the alert example in the SDK)
    - We should switch to calibration mode

    Then once the calibration is done: We should resolve the alert and switch back to the original mode.
    """

    # Create a robot twin to access the alerts API
    robot = client.twin(
        twin_id=twin_uuid,
    )

    # Trigger an alert to notify the user that calibration is needed
    alert = robot.alerts.create(
        name="Calibration Needed",
        description="No calibration file found for the follower arm. Switching to calibration mode.",
        severity="warning",
        alert_type="calibration_needed",
    )
    logger.info("Created calibration alert %s for twin %s", alert.uuid, twin_uuid)

    # TODO:Notify the system that we are switching to calibration mode
    # client.mqtt.publish_command_message(twin_uuid, "calibrating")

    try:
        import subprocess

        # Run the calibration script as a subprocess
        calibrate_cmd = [
            sys.executable,
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "calibrate.py"),
            "--type",
            "follower",
            "--port",
            follower_port,
            "--id",
            follower_id,
        ]
        logger.info("Starting calibration subprocess: %s", " ".join(calibrate_cmd))

        result = subprocess.run(calibrate_cmd)

        if result.returncode != 0:
            raise RuntimeError(f"Calibration subprocess exited with code {result.returncode}")

        logger.info("Calibration completed for follower %s", follower_id)

        # If there is also a leader, calibrate it too
        if leader_port is not None:
            leader_cmd = [
                sys.executable,
                os.path.join(os.path.dirname(os.path.abspath(__file__)), "calibrate.py"),
                "--type",
                "leader",
                "--port",
                leader_port,
                "--id",
                leader_id,
            ]
            logger.info("Starting leader calibration subprocess: %s", " ".join(leader_cmd))

            result = subprocess.run(leader_cmd)

            if result.returncode != 0:
                raise RuntimeError(
                    f"Leader calibration subprocess exited with code {result.returncode}"
                )

            logger.info("Calibration completed for leader %s", leader_id)

        # Resolve the alert now that calibration is done
        alert.resolve()
        logger.info("Calibration alert %s resolved", alert.uuid)

    except Exception:
        logger.exception("Calibration failed for follower %s", follower_id)
        # Update the alert to reflect the failure
        alert.update(
            severity="error",
            description="Automatic calibration failed. Please calibrate manually.",
        )
        client.mqtt.publish_command_message(twin_uuid, "error")
        raise


def _is_follower_calibrated(follower_id: str = "follower1") -> bool:
    """Check if the follower is calibrated."""
    from pathlib import Path

    calibration_path = (
        Path.home() / ".cyberwave" / "so101_lib" / "calibrations" / f"{follower_id}.json"
    )
    return calibration_path.exists()


def _is_leader_calibrated(leader_id: str = "leader1") -> bool:
    """Check if the leader is calibrated."""
    from pathlib import Path

    calibration_path = (
        Path.home() / ".cyberwave" / "so101_lib" / "calibrations" / f"{leader_id}.json"
    )
    return calibration_path.exists()


def _stop_current_operation() -> None:
    """Stop any currently running operation and disconnect the follower."""
    global _current_thread, _current_follower

    if _current_thread is not None and _current_thread.is_alive():
        logger.info("Stopping current operation …")
        # The remoteoperate loop checks a threading.Event internally; disconnecting
        # the follower and interrupting the thread is the pragmatic way to stop it
        # from outside without modifying the remoteoperate module.
        if _current_follower is not None:
            try:
                _current_follower.disconnect()  # type: ignore[union-attr]
            except Exception:
                logger.exception("Error disconnecting follower")
        _current_thread.join(timeout=10.0)
        if _current_thread.is_alive():
            logger.warning("Operation thread did not stop in time")

    _current_thread = None
    _current_follower = None


def handle_controller_changed(client: Cyberwave, twin_uuid: str, data: dict) -> None:
    logger.info("Controller changed: %s", data)
    controller = data.get("controller")
    if not controller:
        raise ValueError("Controller is required")

    # Stop whatever is currently running before switching
    _stop_current_operation()

    controller_type = controller.get("controller_type")
    if controller_type == "teleop":
        start_teleoperate(client, twin_uuid, controller)
    elif controller_type == "localop":
        start_localop(client, twin_uuid, controller)


def start_teleoperate(client: Cyberwave, twin_uuid: str, controller: dict) -> None:
    """Start remote operation (teleop) for the SO101 follower.

    Reads hardware configuration from ``CYBERWAVE_METADATA_*`` environment
    variables, creates the follower + twins, and launches the ``remoteoperate``
    loop from :pymod:`remoteoperate` in a background thread so the MQTT command
    listener keeps running.
    """
    global _current_thread, _current_follower

    logger.info("Starting teleoperate: %s", controller)

    # -- hardware config from environment -----------------------------------
    follower_port = os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
    if not follower_port:
        logger.error("CYBERWAVE_METADATA_FOLLOWER_PORT is not set – cannot start teleoperate")
        return

    follower_id = os.getenv("CYBERWAVE_METADATA_FOLLOWER_ID", "follower1")
    max_relative_target_str = os.getenv("CYBERWAVE_METADATA_MAX_RELATIVE_TARGET")
    max_relative_target = float(max_relative_target_str) if max_relative_target_str else None

    # Check if the follower is calibrated. if not, trigger an alert and switch to calibration mode
    if not _is_follower_calibrated(follower_port):
        _trigger_alert_and_switch_to_calibration(client, twin_uuid, follower_port, follower_id)
        return

    # -- camera config (deprecated) -------------------------------------------
    camera_twin_uuid = os.getenv("CYBERWAVE_METADATA_CAMERA_TWIN_UUID")
    camera_type = os.getenv("CYBERWAVE_METADATA_CAMERA_TYPE", "cv2")
    camera_fps = int(os.getenv("CYBERWAVE_METADATA_CAMERA_FPS", "30"))
    camera_resolution_str = os.getenv("CYBERWAVE_METADATA_CAMERA_RESOLUTION", "VGA")

    def _run() -> None:
        global _current_follower

        from config import FollowerConfig
        from follower import SO101Follower
        from remoteoperate import _parse_resolution, remoteoperate

        # Robot twin
        robot = client.twin(
            asset_key="the-robot-studio/so101",
            twin_id=twin_uuid,
        )

        # Camera twin (only when explicitly configured)
        camera = None
        if camera_twin_uuid:
            camera_asset = (
                "intel/realsensed455"
                if "realsense" in camera_type.lower()
                else "cyberwave/standard-cam"
            )
            camera = client.twin(
                asset_key=camera_asset,
                twin_id=camera_twin_uuid,
            )

        # Follower
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=[0] if camera is not None else None,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()
        _current_follower = follower

        resolution = _parse_resolution(camera_resolution_str)

        try:
            remoteoperate(
                client=client,
                follower=follower,
                robot=robot,
                camera=camera,
                camera_fps=camera_fps,
                camera_type=camera_type,
                camera_resolution=resolution,
            )
        except Exception:
            logger.exception("Teleoperate loop failed")
        finally:
            try:
                follower.disconnect()
            except Exception:
                logger.exception("Error disconnecting follower after teleoperate")
            _current_follower = None

    _current_thread = threading.Thread(target=_run, daemon=True, name="teleoperate")
    _current_thread.start()
    logger.info("Teleoperate thread started")


def start_localop(client: Cyberwave, twin_uuid: str, controller: dict) -> None:
    """Start local operation (localop) for the SO101 leader+follower.

    Reads hardware configuration from ``CYBERWAVE_METADATA_*`` environment
    variables, creates the leader + follower + twins, and launches the
    ``teleoperate`` loop from :pymod:`teleoperate` in a background thread so
    the MQTT command listener keeps running.
    """
    global _current_thread, _current_follower

    logger.info("Starting localop: %s", controller)

    # -- hardware config from environment -----------------------------------
    follower_port = os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
    if not follower_port:
        logger.error("CYBERWAVE_METADATA_FOLLOWER_PORT is not set – cannot start localop")
        return

    leader_port = os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
    if not leader_port:
        logger.error("CYBERWAVE_METADATA_LEADER_PORT is not set – cannot start localop")
        return

    follower_id = os.getenv("CYBERWAVE_METADATA_FOLLOWER_ID", "follower1")
    leader_id = os.getenv("CYBERWAVE_METADATA_LEADER_ID", "leader1")
    max_relative_target_str = os.getenv("CYBERWAVE_METADATA_MAX_RELATIVE_TARGET")
    max_relative_target = float(max_relative_target_str) if max_relative_target_str else None

    # Check if the follower and leader are calibrated. if not, trigger an alert and switch to calibration mode
    if not _is_follower_calibrated(follower_id) or not _is_leader_calibrated(leader_id):
        _trigger_alert_and_switch_to_calibration(
            client, twin_uuid, follower_port, follower_id, leader_port, leader_id
        )
        return

    # -- camera config (optional) -------------------------------------------
    camera_twin_uuid = os.getenv("CYBERWAVE_METADATA_CAMERA_TWIN_UUID")
    camera_type = os.getenv("CYBERWAVE_METADATA_CAMERA_TYPE", "cv2")
    camera_fps = int(os.getenv("CYBERWAVE_METADATA_CAMERA_FPS", "30"))
    camera_resolution_str = os.getenv("CYBERWAVE_METADATA_CAMERA_RESOLUTION", "VGA")

    def _run() -> None:
        global _current_follower

        from config import FollowerConfig, LeaderConfig
        from follower import SO101Follower
        from leader import SO101Leader
        from teleoperate import _parse_resolution, teleoperate

        # Robot twin
        robot = client.twin(
            asset_key="the-robot-studio/so101",
            twin_id=twin_uuid,
        )

        # Camera twin (only when explicitly configured)
        camera = None
        if camera_twin_uuid:
            camera_asset = (
                "intel/realsensed455"
                if "realsense" in camera_type.lower()
                else "cyberwave/standard-cam"
            )
            camera = client.twin(
                asset_key=camera_asset,
                twin_id=camera_twin_uuid,
            )

        # Leader
        leader_config = LeaderConfig(port=leader_port)
        leader = SO101Leader(config=leader_config)
        leader.connect()

        # Follower
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=[0] if camera is not None else None,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()
        _current_follower = follower

        resolution = _parse_resolution(camera_resolution_str)

        try:
            teleoperate(
                leader=leader,
                cyberwave_client=client,
                follower=follower,
                robot=robot,
                camera=camera,
                camera_fps=camera_fps,
                camera_type=camera_type,
                camera_resolution=resolution,
            )
        except Exception:
            logger.exception("Localop loop failed")
        finally:
            try:
                follower.disconnect()
            except Exception:
                logger.exception("Error disconnecting follower after localop")
            try:
                leader.disconnect()
            except Exception:
                logger.exception("Error disconnecting leader after localop")
            _current_follower = None

    _current_thread = threading.Thread(target=_run, daemon=True, name="localop")
    _current_thread.start()
    logger.info("Localop thread started")


async def main() -> None:
    token = os.getenv("CYBERWAVE_API_KEY")
    twin_uuid = os.getenv("CYBERWAVE_TWIN_UUID")

    if not token:
        logger.error("CYBERWAVE_API_KEY environment variable is required")
        sys.exit(1)
    if not twin_uuid:
        logger.error("CYBERWAVE_TWIN_UUID environment variable is required")
        sys.exit(1)

    logger.info("Initializing SO101 edge node for twin %s", twin_uuid)

    # Initialize the Cyberwave SDK client (reads remaining config from env vars)
    client = Cyberwave(token=token, source_type="edge")

    # Connect to the MQTT broker
    client.mqtt.connect()
    logger.info("Connected to MQTT broker")

    # --- command handler ------------------------------------------------
    def on_command(data: dict) -> None:
        """Handle incoming command messages for this twin.

        Status messages (responses) are published on the same topic, so we
        skip any payload that already contains a ``status`` key.
        """
        # Ignore our own status responses echoed back
        if "status" in data:
            return

        command = data.get("command")
        logger.info("Received command: %s (payload: %s)", command, data)

        if not command:
            logger.warning("Received message with no 'command' field: %s", data)
            client.mqtt.publish_command_message(twin_uuid, "error")
            return

        match command:
            case "controller-changed":
                handle_controller_changed(client, twin_uuid, data)
            # case "calibrate":
            #     handle_calibrate(data)
            # case "teleoperate":
            #     handle_teleoperate(data)
            # case "remoteoperate":
            #     handle_remoteoperate(data)
            case _:
                raise NotImplementedError(f"Command {command} not implemented")

    # Subscribe to the command topic for this twin
    client.mqtt.subscribe_command_message(twin_uuid, on_command)
    logger.info("Subscribed to command topic for twin %s", twin_uuid)

    # --- graceful shutdown ---------------------------------------------
    stop_event = asyncio.Event()

    def _handle_signal() -> None:
        logger.info("Shutdown signal received, stopping...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    # Keep the process alive until a shutdown signal is received
    logger.info("SO101 edge node is running. Waiting for commands...")
    await stop_event.wait()

    # Cleanup
    logger.info("Stopping any running operation...")
    _stop_current_operation()
    logger.info("Disconnecting from MQTT broker...")
    client.mqtt.disconnect()
    logger.info("SO101 edge node stopped.")


if __name__ == "__main__":
    asyncio.run(main())
