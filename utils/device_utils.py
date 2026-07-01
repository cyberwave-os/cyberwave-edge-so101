"""Device discovery utilities for the SO101 edge driver.

Discovers USB cameras via v4l2-ctl (Linux). Used for camera port assignment.
"""

import logging
import importlib
import os
import re
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Raspberry Pi platform devices that v4l2-ctl lists but are not actual cameras.
# Keep them in discovered_devices for visibility, but mark them incompatible.
EXCLUDED_CAMERA_NAMES = frozenset(
    {"pispbe", "rpi-hevc-dec", "bcm2835-codec-decode", "bcm2835-isp"}
)


@dataclass
class CameraDevice:
    """Represents a discovered camera device."""

    card: str
    bus_info: str
    paths: list[str] = field(default_factory=list)
    driver: Optional[str] = None
    serial: Optional[str] = None
    is_compatible: Optional[bool] = None
    compatibility_reason: Optional[str] = None
    last_checked_at: Optional[str] = None

    @property
    def primary_path(self) -> Optional[str]:
        """The primary /dev/video* path (usually the first one)."""
        return self.paths[0] if self.paths else None

    @property
    def index(self) -> Optional[int]:
        """Extract the numeric index from the primary path (e.g. /dev/video2 -> 2)."""
        if not self.primary_path:
            return None
        match = re.search(r"/dev/video(\d+)", self.primary_path)
        return int(match.group(1)) if match else None

    def to_dict(self) -> dict:
        """Convert to a JSON-serializable dict."""
        result = {
            "card": self.card,
            "bus_info": self.bus_info,
            "paths": self.paths,
            "primary_path": self.primary_path,
            "index": self.index,
            "driver": self.driver,
            "serial": self.serial,
        }
        if self.is_compatible is not None:
            result["is_compatible"] = self.is_compatible
        if self.compatibility_reason:
            result["compatibility_reason"] = self.compatibility_reason
        if self.last_checked_at:
            result["last_checked_at"] = self.last_checked_at
        return result


def _checked_at_timestamp() -> str:
    """Return an RFC3339 UTC timestamp for compatibility checks."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _is_realsense_device(card: str, driver: Optional[str] = None) -> bool:
    """True when the device should be handled by the RealSense runtime."""
    haystack = " ".join(part for part in (card, driver or "") if part).lower()
    return "realsense" in haystack or "intel" in haystack


def _is_excluded_virtual_device(card: str, driver: Optional[str] = None) -> bool:
    """True for V4L2 platform nodes that are not streamable user cameras."""
    names = {part.lower().strip() for part in (card, driver or "") if part and part.strip()}
    return bool(names & EXCLUDED_CAMERA_NAMES)


def _device_path_from_dict(device: dict[str, Any]) -> Optional[str]:
    """Resolve a usable /dev/video* path from cached metadata."""
    primary_path = device.get("primary_path")
    if isinstance(primary_path, str) and primary_path.strip():
        return primary_path.strip()

    paths = device.get("paths")
    if isinstance(paths, list):
        for path in paths:
            if isinstance(path, str) and path.strip():
                return path.strip()

    index = device.get("index")
    if isinstance(index, int):
        return f"/dev/video{index}"
    if isinstance(index, str) and index.isdigit():
        return f"/dev/video{index}"
    return None


def _probe_cv2_camera(device_path: str) -> tuple[bool, str]:
    """Try opening a V4L2 camera and reading a frame."""
    try:
        cv2 = importlib.import_module("cv2")
    except Exception as exc:
        logger.warning("OpenCV unavailable while probing %s: %s", device_path, exc)
        return False, "opencv_unavailable"

    cap = None
    try:
        cap = cv2.VideoCapture(device_path)
        if not cap.isOpened():
            return False, "open_failed"

        for _ in range(3):
            ok, _frame = cap.read()
            if ok:
                return True, "ok"
            time.sleep(0.1)
        return False, "frame_read_failed"
    except Exception as exc:
        logger.debug("Camera probe failed for %s: %s", device_path, exc)
        return False, "probe_exception"
    finally:
        if cap is not None:
            cap.release()


def _probe_camera_compatibility(
    *,
    card: str,
    driver: Optional[str],
    device_path: Optional[str],
) -> tuple[bool, str]:
    """Return compatibility status for a discovered camera device."""
    if _is_excluded_virtual_device(card, driver):
        return False, "excluded_virtual_device"
    if _is_realsense_device(card, driver):
        return True, "realsense_device"
    if not device_path:
        return False, "missing_device_path"
    return _probe_cv2_camera(device_path)


def annotate_discovered_devices(devices: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add compatibility probe metadata to cached or discovered device dicts."""
    annotated: list[dict[str, Any]] = []
    checked_at = _checked_at_timestamp()

    for device in devices:
        current = dict(device)
        device_path = _device_path_from_dict(current)
        compatible, reason = _probe_camera_compatibility(
            card=str(current.get("card") or "").strip(),
            driver=str(current.get("driver") or "").strip() or None,
            device_path=device_path,
        )
        if device_path and not current.get("primary_path"):
            current["primary_path"] = device_path
        current["is_compatible"] = compatible
        current["compatibility_reason"] = reason
        current["last_checked_at"] = checked_at
        annotated.append(current)

    return annotated


def _parse_v4l2_list_devices(output: str) -> list[CameraDevice]:
    """Parse the output of `v4l2-ctl --list-devices`."""
    devices: list[CameraDevice] = []
    current_device: Optional[CameraDevice] = None

    for line in output.splitlines():
        line = line.rstrip()
        if not line:
            continue

        if line.startswith("\t") or line.startswith(" "):
            path = line.strip()
            if current_device and path.startswith("/dev/video"):
                current_device.paths.append(path)
        else:
            match = re.match(r"^(.+?)\s*\(([^)]+)\):\s*$", line)
            if match:
                card = match.group(1).strip()
                bus_info = match.group(2).strip()
                current_device = CameraDevice(card=card, bus_info=bus_info)
                devices.append(current_device)
            else:
                card = line.rstrip(":").strip()
                current_device = CameraDevice(card=card, bus_info="")
                devices.append(current_device)

    return [d for d in devices if d.paths]


def _get_v4l2_device_info(device_path: str) -> dict:
    """Get detailed info for a specific v4l2 device using `v4l2-ctl --device=X --all`."""
    if not shutil.which("v4l2-ctl"):
        return {}

    try:
        result = subprocess.run(
            ["v4l2-ctl", f"--device={device_path}", "--all"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return {}

        key_aliases = {
            "driver_name": "driver",
            "driver": "driver",
            "card_type": "card",
            "card": "card",
            "bus_info": "bus_info",
            "serial": "serial",
            "serial_number": "serial",
        }
        info: dict = {}
        for line in result.stdout.splitlines():
            if ":" in line:
                key, _, value = line.partition(":")
                raw_key = key.strip().lower().replace(" ", "_")
                value = value.strip()
                if raw_key in key_aliases and value:
                    info[key_aliases[raw_key]] = value
        return info
    except Exception as exc:
        logger.debug("Failed to get v4l2 device info for %s: %s", device_path, exc)
        return {}


def _ensure_video_device_permissions() -> None:
    """Set read/write permissions on /dev/video* so v4l2-ctl and camera drivers can access them."""
    dev = Path("/dev")
    if not dev.exists():
        return
    try:
        for path in dev.glob("video*"):
            if path.is_char_device():
                os.chmod(path, 0o666)
    except PermissionError:
        logger.warning(
            "Cannot set permissions on /dev/video* (need root). "
            "Run: sudo chmod 666 /dev/video* to allow camera access."
        )


def discover_usb_cameras_v4l2() -> list[CameraDevice]:
    """Discover USB cameras using v4l2-ctl (Linux only)."""
    if not shutil.which("v4l2-ctl"):
        logger.warning("v4l2-ctl not found; cannot enumerate cameras (install v4l-utils)")
        return []

    _ensure_video_device_permissions()

    try:
        result = subprocess.run(
            ["v4l2-ctl", "--list-devices"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 and result.stderr:
            logger.warning(
                "v4l2-ctl reported errors but continuing: %s",
                result.stderr.strip(),
            )

        devices = _parse_v4l2_list_devices(result.stdout or "")

        for device in devices:
            if device.primary_path:
                info = _get_v4l2_device_info(device.primary_path)
                if info.get("driver"):
                    device.driver = info["driver"]
                if info.get("serial"):
                    device.serial = info["serial"]

        logger.info("Discovered %d camera device(s) via v4l2-ctl", len(devices))
        return devices

    except subprocess.TimeoutExpired:
        logger.warning("v4l2-ctl timed out")
        return []
    except Exception as exc:
        logger.warning("Failed to discover USB cameras: %s", exc)
        return []


def discover_usb_cameras() -> list[CameraDevice]:
    """Discover USB cameras on the system. Linux: v4l2-ctl. Other platforms: not implemented."""
    import platform

    system = platform.system()
    if system == "Linux":
        return discover_usb_cameras_v4l2()
    logger.warning("Camera discovery not implemented for %s", system)
    return []
