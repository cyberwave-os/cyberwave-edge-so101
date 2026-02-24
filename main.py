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
            "-m",
            "scripts.cw_calibrate",
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
                "-m",
                "scripts.cw_calibrate",
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


# Commands that map to so101-* scripts (pyproject.toml [project.scripts])
# Long-running: remoteoperate, teleoperate (run in background thread)
# One-off: calibrate, find_port, read_device, setup, write_position (run as subprocess)
# stop: stop current operation (not a script, but supported)
SUPPORTED_COMMANDS = frozenset({
    "remoteoperate",
    "teleoperate",
    "calibrate",
    "find_port",
    "read_device",
    "setup",
    "write_position",
    "stop",
})


def _run_script_command(
    client: Cyberwave,
    twin_uuid: str,
    script_name: str,
    data: dict,
    extra_cli: list[str] | None = None,
) -> None:
    """Run a one-off script (calibrate, find_port, read_device, setup, write_position) as subprocess.

    extra_cli: raw CLI args from payload.args when it's a list (e.g. ["--find-port"]).
    """
    import subprocess

    module_map = {
        "find_port": "scripts.cw_find_port",
        "read_device": "scripts.cw_read_device",
        "calibrate": "scripts.cw_calibrate",
        "setup": "scripts.cw_setup",
        "write_position": "scripts.cw_write_position",
    }
    module = module_map.get(script_name)
    if not module:
        client.mqtt.publish_command_message(twin_uuid, "error")
        return

    cmd = [sys.executable, "-m", module]
    # Build args from data; fall back to CYBERWAVE_METADATA_* env vars
    match script_name:
        case "calibrate":
            calib_type = data.get("type") or "follower"
            cmd.extend(["--type", str(calib_type)])
            port = data.get("port") or (
                os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
                if calib_type == "leader"
                else os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
            )
            if port:
                cmd.extend(["--port", str(port)])
            if data.get("id"):
                cmd.extend(["--id", str(data["id"])])
        case "read_device":
            port = data.get("port") or os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT") or os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
            if port:
                cmd.extend(["--port", str(port)])
        case "setup":
            if data.get("setup_path"):
                cmd.extend(["--setup-path", str(data["setup_path"])])
        case "write_position":
            if data.get("port"):
                cmd.extend(["--port", str(data["port"])])
            if data.get("position"):
                cmd.extend(["--position", str(data["position"])])
        case _:
            pass

    # Append extra CLI args from payload.args (when args is a list)
    if extra_cli:
        cmd.extend(str(a) for a in extra_cli)

    logger.info("Running script: %s", " ".join(cmd))
    try:
        result = subprocess.run(cmd)
        status = "ok" if result.returncode == 0 else "error"
        client.mqtt.publish_command_message(twin_uuid, status)
    except Exception:
        logger.exception("Script %s failed", script_name)
        client.mqtt.publish_command_message(twin_uuid, "error")


def handle_command(client: Cyberwave, twin_uuid: str, command: str, payload: dict) -> None:
    """Dispatch command to the corresponding so101-* script or operation.

    Payload may include:
    - data: dict of named args for the script
    - args: dict of additional args (merged with data, args override data)
    """
    # Normalize: accept "so101-remoteoperate" or "remoteoperate"
    cmd = command.removeprefix("so101-") if isinstance(command, str) else ""
    if not cmd or cmd not in SUPPORTED_COMMANDS:
        logger.debug("Unsupported command: %s", command)
        return

    # Merge data + args for script commands. args can be:
    # - dict: merged with data (args overrides data)
    # - list: raw CLI args (e.g. ["--port", "/dev/ttyACM0"]) appended after built-in args
    data = dict(payload.get("data") or {}) if isinstance(payload.get("data"), dict) else {}
    raw_args = payload.get("args")
    script_args = {**data, **raw_args} if isinstance(raw_args, dict) else data
    extra_cli = raw_args if isinstance(raw_args, list) else []

    match cmd:
        case "stop":
            _stop_current_operation()
            client.mqtt.publish_command_message(twin_uuid, "ok")
        case "remoteoperate":
            _stop_current_operation()
            try:
                start_remoteoperate(client, twin_uuid)
            except Exception:
                logger.exception("Failed to start remoteoperate")
                client.mqtt.publish_command_message(twin_uuid, "error")
        case "teleoperate":
            _stop_current_operation()
            try:
                start_teleoperate(client, twin_uuid)
            except Exception:
                logger.exception("Failed to start teleoperate")
                client.mqtt.publish_command_message(twin_uuid, "error")
        case "calibrate" | "find_port" | "read_device" | "setup" | "write_position":
            _run_script_command(client, twin_uuid, cmd, script_args, extra_cli or [])
        case _:
            logger.debug("Unsupported command: %s", cmd)


def start_remoteoperate(client: Cyberwave, twin_uuid: str) -> None:
    """Start remote operation (so101-remoteoperate) for the SO101 follower.

    Receives joint states from the frontend via MQTT and writes to the follower.
    Reads hardware configuration from ``CYBERWAVE_METADATA_*`` environment
    variables, creates the follower + twins, and launches the ``remoteoperate``
    loop from :pymod:`scripts.cw_remoteoperate` in a background thread so the
    MQTT command listener keeps running.
    """
    global _current_thread, _current_follower

    logger.info("Starting remoteoperate")

    # -- hardware config from environment -----------------------------------
    follower_port = os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
    if not follower_port:
        logger.error("CYBERWAVE_METADATA_FOLLOWER_PORT is not set – cannot start remoteoperate")
        return

    follower_id = os.getenv("CYBERWAVE_METADATA_FOLLOWER_ID", "follower1")
    max_relative_target_str = os.getenv("CYBERWAVE_METADATA_MAX_RELATIVE_TARGET")
    max_relative_target = float(max_relative_target_str) if max_relative_target_str else None

    # Check if the follower is calibrated. if not, trigger an alert and switch to calibration mode
    if not _is_follower_calibrated(follower_id):
        _trigger_alert_and_switch_to_calibration(client, twin_uuid, follower_port, follower_id)
        return

    # -- camera config (deprecated) -------------------------------------------
    camera_twin_uuid = os.getenv("CYBERWAVE_METADATA_CAMERA_TWIN_UUID")
    camera_type = os.getenv("CYBERWAVE_METADATA_CAMERA_TYPE", "cv2")
    camera_fps = int(os.getenv("CYBERWAVE_METADATA_CAMERA_FPS", "30"))
    camera_resolution_str = os.getenv("CYBERWAVE_METADATA_CAMERA_RESOLUTION", "VGA")

    def _run() -> None:
        global _current_follower

        from scripts.cw_remoteoperate import remoteoperate
        from so101.follower import SO101Follower
        from utils.config import FollowerConfig
        from utils.utils import parse_resolution_to_enum

        # Robot twin
        robot = client.twin(
            asset_key="the-robot-studio/so101",
            twin_id=twin_uuid,
        )

        # Camera config (only when explicitly configured)
        cameras_list = []
        if camera_twin_uuid:
            camera_asset = (
                "intel/realsensed455"
                if "realsense" in camera_type.lower()
                else "cyberwave/standard-cam"
            )
            camera_twin = client.twin(
                asset_key=camera_asset,
                twin_id=camera_twin_uuid,
            )
            res_enum = parse_resolution_to_enum(camera_resolution_str)
            cameras_list.append({
                "twin": camera_twin,
                "camera_id": 0,
                "camera_type": camera_type,
                "camera_resolution": res_enum,
                "fps": camera_fps,
            })

        # Follower
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=[0] if cameras_list else None,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()
        _current_follower = follower

        try:
            remoteoperate(
                client=client,
                follower=follower,
                robot=robot,
                cameras=cameras_list if cameras_list else None,
            )
        except Exception:
            logger.exception("Remoteoperate loop failed")
        finally:
            try:
                follower.disconnect()
            except Exception:
                logger.exception("Error disconnecting follower after remoteoperate")
            _current_follower = None

    _current_thread = threading.Thread(target=_run, daemon=True, name="remoteoperate")
    _current_thread.start()
    logger.info("Remoteoperate thread started")


def start_teleoperate(client: Cyberwave, twin_uuid: str) -> None:
    """Start local teleoperation (so101-teleoperate) for the SO101 leader+follower.

    Reads hardware configuration from ``CYBERWAVE_METADATA_*`` environment
    variables, creates the leader + follower + twins, and launches the
    ``teleoperate`` loop from :pymod:`scripts.cw_teleoperate` in a background
    thread so the MQTT command listener keeps running.
    """
    global _current_thread, _current_follower

    logger.info("Starting teleoperate")

    # -- hardware config from environment -----------------------------------
    follower_port = os.getenv("CYBERWAVE_METADATA_FOLLOWER_PORT")
    if not follower_port:
        logger.error("CYBERWAVE_METADATA_FOLLOWER_PORT is not set – cannot start teleoperate")
        return

    leader_port = os.getenv("CYBERWAVE_METADATA_LEADER_PORT")
    if not leader_port:
        logger.error("CYBERWAVE_METADATA_LEADER_PORT is not set – cannot start teleoperate")
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

        from scripts.cw_teleoperate import teleoperate
        from so101.follower import SO101Follower
        from so101.leader import SO101Leader
        from utils.config import FollowerConfig, LeaderConfig
        from utils.utils import parse_resolution_to_enum

        # Robot twin
        robot = client.twin(
            asset_key="the-robot-studio/so101",
            twin_id=twin_uuid,
        )

        # Camera config (only when explicitly configured)
        cameras_list = []
        if camera_twin_uuid:
            camera_asset = (
                "intel/realsensed455"
                if "realsense" in camera_type.lower()
                else "cyberwave/standard-cam"
            )
            camera_twin = client.twin(
                asset_key=camera_asset,
                twin_id=camera_twin_uuid,
            )
            res_enum = parse_resolution_to_enum(camera_resolution_str)
            cameras_list.append({
                "twin": camera_twin,
                "camera_id": 0,
                "camera_type": camera_type,
                "camera_resolution": res_enum,
                "fps": camera_fps,
            })

        # Leader
        leader_config = LeaderConfig(port=leader_port)
        leader = SO101Leader(config=leader_config)
        leader.connect()

        # Follower
        follower_config = FollowerConfig(
            port=follower_port,
            max_relative_target=max_relative_target,
            id=follower_id,
            cameras=[0] if cameras_list else None,
        )
        follower = SO101Follower(config=follower_config)
        follower.connect()
        _current_follower = follower

        try:
            teleoperate(
                leader=leader,
                cyberwave_client=client,
                follower=follower,
                robot=robot,
                cameras=cameras_list if cameras_list else None,
            )
        except Exception:
            logger.exception("Teleoperate loop failed")
        finally:
            try:
                follower.disconnect()
            except Exception:
                logger.exception("Error disconnecting follower after teleoperate")
            try:
                leader.disconnect()
            except Exception:
                logger.exception("Error disconnecting leader after teleoperate")
            _current_follower = None

    _current_thread = threading.Thread(target=_run, daemon=True, name="teleoperate")
    _current_thread.start()
    logger.info("Teleoperate thread started")


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

        try:
            handle_command(client, twin_uuid, command, data)
        except Exception:
            logger.exception("Error handling command %s", command)
            client.mqtt.publish_command_message(twin_uuid, "error")

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
