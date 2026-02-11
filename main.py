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
        start_localop(controller)


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

    # -- camera config (optional) -------------------------------------------
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
            name="robot",
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
                name="camera",
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


def start_localop(controller: dict) -> None:
    logger.info("Starting localop: %s", controller)


async def main() -> None:
    token = os.getenv("CYBERWAVE_TOKEN")
    twin_uuid = os.getenv("CYBERWAVE_TWIN_UUID")

    if not token:
        logger.error("CYBERWAVE_TOKEN environment variable is required")
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
