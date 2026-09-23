"""Unit tests for entrypoint env var export behavior."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ENTRYPOINT_PATH = Path(__file__).resolve().parent.parent / "entrypoint.sh"


def _extract_embedded_python() -> str:
    """Extract the Python snippet used by entrypoint.sh for env exports."""
    lines = ENTRYPOINT_PATH.read_text().splitlines()
    start_marker = 'eval "$(python3 -c "'
    end_marker = '")"'

    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if line.strip() == start_marker:
            start_idx = i + 1
            break
    if start_idx is None:
        raise AssertionError("Could not find embedded python start marker in entrypoint.sh")

    for i in range(start_idx, len(lines)):
        if lines[i].strip() == end_marker:
            end_idx = i
            break
    if end_idx is None:
        raise AssertionError("Could not find embedded python end marker in entrypoint.sh")

    return "\n".join(lines[start_idx:end_idx]) + "\n"


def _run_export_script(twin_payload: dict, extra_env: dict[str, str] | None = None) -> list[str]:
    """Run entrypoint embedded exporter and return emitted export lines."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        twin_file = Path(tmp_dir) / "entrypoint_test_twin.json"
        twin_file.write_text(json.dumps(twin_payload))

        env = os.environ.copy()
        env["CYBERWAVE_TWIN_JSON_FILE"] = str(twin_file)
        if extra_env:
            env.update(extra_env)

        code = _extract_embedded_python()
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        return [line for line in result.stdout.splitlines() if line.strip()]


def test_entrypoint_sanitizes_special_chars_in_nested_keys():
    """Special chars in nested keys are sanitized before env var construction."""
    payload = {
        "uuid": "abc-123",
        "metadata": {
            "drivers": {
                "intel/realsensed455": {
                    "docker-image": "cyberwaveos/so101-driver:latest-realsense",
                    "driver.version": "1.0.0",
                }
            }
        },
    }

    exports = _run_export_script(payload)

    assert "export CYBERWAVE_TWIN_UUID=abc-123" in exports
    assert any(
        line.startswith(
            "export CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DOCKER_IMAGE="
        )
        for line in exports
    )
    assert any(
        line.startswith(
            "export CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DRIVER_VERSION="
        )
        for line in exports
    )
    assert all("/" not in line.split("=", 1)[0] for line in exports)


def test_entrypoint_respects_existing_sanitized_env_var():
    """Exporter should not override explicitly provided env vars."""
    payload = {
        "metadata": {
            "drivers": {
                "intel/realsensed455": {
                    "docker-image": "new-image:tag",
                }
            }
        }
    }
    existing = {
        "CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DOCKER_IMAGE": "pre-set-image:tag",
    }

    exports = _run_export_script(payload, extra_env=existing)

    assert not any(
        line.startswith("export CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DOCKER_IMAGE=")
        for line in exports
    )


def test_entrypoint_uses_python3_for_main():
    """Debian bookworm image has python3 only; exec must not call bare 'python'."""
    content = ENTRYPOINT_PATH.read_text()
    assert "exec python3 main.py" in content
    assert "exec python main.py" not in content


def test_entrypoint_usbip_section_waits_for_video_devices():
    """Entrypoint USB/IP block polls for /dev/video* after serial devices."""
    content = ENTRYPOINT_PATH.read_text()
    assert "/dev/video*" in content, "entrypoint must poll for video devices"
    assert "CYBERWAVE_USBIP_VIDEO_TIMEOUT_SECS" in content, (
        "entrypoint must support configurable video timeout"
    )
    serial_pos = content.index("/dev/ttyACM*")
    video_pos = content.index("/dev/video*")
    assert serial_pos < video_pos, "video device polling must come after serial check"


def _extract_busid_filter_awk() -> str:
    """Extract the awk program entrypoint.sh uses to build BUSIDS from `usbip list`."""
    lines = ENTRYPOINT_PATH.read_text().splitlines()
    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if line.rstrip().endswith("| awk '"):
            start_idx = i + 1
            break
    if start_idx is None:
        raise AssertionError("Could not find BUSIDS awk start marker in entrypoint.sh")

    for i in range(start_idx, len(lines)):
        if lines[i].strip() == "' || true)":
            end_idx = i
            break
    if end_idx is None:
        raise AssertionError("Could not find BUSIDS awk end marker in entrypoint.sh")

    return "\n".join(lines[start_idx:end_idx]) + "\n"


def _run_busid_filter(usbip_list_output: str) -> list[str]:
    program = _extract_busid_filter_awk()
    result = subprocess.run(
        ["awk", program],
        input=usbip_list_output,
        capture_output=True,
        text=True,
        check=True,
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


# Real `usbip list -r <host>` output captured against a live cyberwave usbip
# host server on macOS (jiegec/usbip v0.8.0), exporting a real Mac's internal
# hub tree plus an attached webcam.
#
# Regression context: attaching a Hub-class (bDeviceClass 09) device via
# `usbip attach` panics the host server with `unimplemented!("control out")`
# in src/device.rs — hub-class requests (e.g. SetPortFeature, sent to the
# "Other" recipient) aren't handled by that match arm. The panic kills the
# worker thread handling that connection and leaves the server's exportable
# device list empty until the launchd service is restarted, which is
# indistinguishable from "USB/IP crashed" and made the SO101 arm unusable.
# Confirmed live: attaching only the non-hub devices below never panics the
# server; attaching every listed bus ID (the pre-fix behavior) does, reliably.
_USBIP_LIST_MIXED_HUBS_AND_LEAVES = """Exportable USB devices
======================
 - host.docker.internal
     8-17-3: Logitech, Inc. : unknown product (046d:085b)
           : /sys/bus/8/17/3
           : Miscellaneous Device / ? / Interface Association (ef/02/01)
           :  0 - Video / Video Control / unknown protocol (0e/01/00)
           :  1 - Video / Video Streaming / unknown protocol (0e/02/00)
           :  2 - Audio / Control Device / unknown protocol (01/01/00)
           :  3 - Audio / Streaming / unknown protocol (01/02/00)

      0-2-5: Texas Instruments, Inc. : unknown product (0451:82ff)
           : /sys/bus/0/2/5
           : (Defined at Interface level) (00/00/00)
           :  0 - Human Interface Device / No Subclass / None (03/00/00)

      0-1-1: Texas Instruments, Inc. : unknown product (0451:8442)
           : /sys/bus/0/1/1
           : Hub / Unused / TT per port (09/00/02)
           :  0 - Hub / Unused / Single TT (09/00/01)

      8-3-1: unknown vendor : unknown product (3434:d031)
           : /sys/bus/8/3/1
           : (Defined at Interface level) (00/00/00)
           :  0 - Human Interface Device / Boot Interface Subclass / Mouse (03/01/02)
           :  1 - Human Interface Device / Boot Interface Subclass / Keyboard (03/01/01)
           :  2 - Human Interface Device / Boot Interface Subclass / Keyboard (03/01/01)
           :  3 - Human Interface Device / No Subclass / None (03/00/00)

      8-2-3: Apple, Inc. : unknown product (05ac:8009)
           : /sys/bus/8/2/3
           : Hub / Unused / TT per port (09/00/02)
           :  0 - Hub / Unused / Single TT (09/00/01)

      8-1-1: Apple, Inc. : unknown product (05ac:800a)
           : /sys/bus/8/1/1
           : Hub / Unused / unknown protocol (09/00/03)
           :  0 - Hub / Unused / Full speed (or root) hub (09/00/00)
"""


def test_busid_filter_skips_hub_class_devices():
    """Auto-discovery must never attach Hub-class bus IDs (see module docstring)."""
    busids = _run_busid_filter(_USBIP_LIST_MIXED_HUBS_AND_LEAVES)

    assert busids == ["8-17-3", "0-2-5", "8-3-1"]
    assert "0-1-1" not in busids
    assert "8-2-3" not in busids
    assert "8-1-1" not in busids


def test_busid_filter_keeps_so101_arm_serial_device():
    """A real SO101 arm (WCH CDC-ACM composite) must still be attached."""
    usbip_list_output = """Exportable USB devices
======================
 - host.docker.internal
      1-1-1: QinHeng Electronics : USB Single Serial (1a86:55d3)
           : /sys/bus/1/1/1
           : Communications Device Class / Abstract (modem) / AT-commands (v.25ter) (02/02/01)
           :  0 - Communications / Abstract (modem) / AT-commands (v.25ter) (02/02/01)
           :  1 - CDC Data / Unused / unknown protocol (0a/00/00)

      2-1-1: Apple, Inc. : unknown product (05ac:800a)
           : /sys/bus/2/1/1
           : Hub / Unused / unknown protocol (09/00/03)
           :  0 - Hub / Unused / Full speed (or root) hub (09/00/00)
"""

    busids = _run_busid_filter(usbip_list_output)

    assert busids == ["1-1-1"]
