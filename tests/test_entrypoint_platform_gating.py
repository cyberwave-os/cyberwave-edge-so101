"""The USB/IP path is macOS-only and must stay inert everywhere else.

The so101 image is published for linux/amd64 + linux/arm64 only; macOS Docker
Desktop runs that same Linux image, so the macOS-specific USB/IP code cannot be
split out at build time. It is gated at *runtime* instead — edge-core sets
CYBERWAVE_USBIP_ENABLED only on Darwin. These tests pin that gate so a Linux
edge host never runs nsenter, mknod, or even reads the helper library.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ENTRYPOINT = Path(__file__).resolve().parent.parent / "entrypoint.sh"

# Records every invocation so the assertions can prove absence, not just
# "nothing crashed".
SPY = """#!/bin/sh
echo "$0 $*" >> "$SPY_LOG"
exit 0
"""

# Stands in for the driver so `exec python3 main.py` terminates the run.
FAKE_PYTHON = """#!/bin/sh
echo "python3 $*" >> "$SPY_LOG"
exit 0
"""


@pytest.fixture
def sandbox(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name in ("nsenter", "mknod", "usbip"):
        (bin_dir / name).write_text(SPY)
    (bin_dir / "python3").write_text(FAKE_PYTHON)
    for name in ("nsenter", "mknod", "usbip", "python3"):
        os.chmod(bin_dir / name, 0o755)

    log = tmp_path / "spy.log"
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["SPY_LOG"] = str(log)
    env["CYBERWAVE_EDGE_CONFIG_DIR"] = str(tmp_path / "cfg")
    env.pop("CYBERWAVE_USBIP_ENABLED", None)
    env.pop("CYBERWAVE_TWIN_JSON_FILE", None)
    return env, log


def _run_entrypoint(env, *args):
    return subprocess.run(
        ["sh", str(ENTRYPOINT), *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(ENTRYPOINT.parent),
        timeout=60,
    )


def test_linux_run_never_invokes_usbip_machinery(sandbox):
    """With CYBERWAVE_USBIP_ENABLED unset, nothing USB/IP-related may run."""
    env, log = sandbox

    result = _run_entrypoint(env)

    assert result.returncode == 0, result.stderr
    calls = log.read_text() if log.exists() else ""
    assert "nsenter" not in calls
    assert "mknod" not in calls
    assert "usbip" not in calls
    assert "[usbip]" not in result.stdout


def test_linux_run_still_starts_the_driver(sandbox):
    """Gating must not cost the normal startup path."""
    env, log = sandbox

    result = _run_entrypoint(env)

    assert result.returncode == 0, result.stderr
    assert "python3 main.py" in log.read_text()


def test_usbip_helpers_are_not_sourced_on_the_linux_path(sandbox, tmp_path):
    """The helper library is macOS-only, so a Linux container must not even
    read it. Verified with a stub library that records being sourced —
    entrypoint.sh resolves it relative to its own directory."""
    env, log = sandbox
    stage = tmp_path / "app"
    stage.mkdir()
    (stage / "entrypoint.sh").write_text(ENTRYPOINT.read_text())
    (stage / "usbip_lib.sh").write_text('echo "SOURCED usbip_lib" >> "$SPY_LOG"\n')

    result = subprocess.run(
        ["sh", str(stage / "entrypoint.sh")],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(stage),
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert "SOURCED usbip_lib" not in (log.read_text() if log.exists() else "")


def test_usbip_helpers_are_sourced_for_attach_only_mode(sandbox, tmp_path):
    """...but the macOS pre-attach entrypoint must still get them."""
    env, log = sandbox
    stage = tmp_path / "app"
    stage.mkdir()
    (stage / "entrypoint.sh").write_text(ENTRYPOINT.read_text())
    (stage / "usbip_lib.sh").write_text(
        'echo "SOURCED usbip_lib" >> "$SPY_LOG"\n'
        "_usbip_vm_serial_nodes() { :; }\n"
        "_usbip_vm_serial_nodes_for_vidpid() { :; }\n"
        "_usbip_mirror_nodes() { :; }\n"
        "_usbip_print_device_lines() { :; }\n"
    )

    subprocess.run(
        ["sh", str(stage / "entrypoint.sh"), "--usbip-attach-only"],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(stage),
        timeout=60,
    )

    assert "SOURCED usbip_lib" in (log.read_text() if log.exists() else "")


def test_attach_only_fails_when_not_all_exported_arms_enumerate(sandbox, tmp_path):
    """Edge-core marks any successful helper run as fully pre-attached. A
    timeout with only one of two exported arms must therefore be non-zero,
    otherwise the real driver starts permanently missing its second arm."""
    env, _ = sandbox
    stage = tmp_path / "app"
    stage.mkdir()
    (stage / "entrypoint.sh").write_text(ENTRYPOINT.read_text())
    (stage / "usbip_lib.sh").write_text(
        '_usbip_resolve_host() { echo "$1"; }\n'
        "_usbip_attach_device() { :; }\n"
        "_usbip_vm_serial_nodes() { echo /dev/ttyACM0; }\n"
        "_usbip_vm_serial_nodes_for_vidpid() { echo /dev/ttyACM0; }\n"
        "_usbip_mirror_nodes() { :; }\n"
        "_usbip_print_device_lines() { echo USBIP_DEVICE=/dev/ttyACM0; }\n"
    )

    bin_dir = Path(env["PATH"].split(":", 1)[0])
    (bin_dir / "cat").write_text(
        '#!/bin/sh\nif [ "$1" = "/proc/1/comm" ]; then '
        'echo init; else exec /bin/cat "$@"; fi\n'
    )
    (bin_dir / "nsenter").write_text(
        """#!/bin/sh
while [ "$1" != "--" ] && [ "$#" -gt 0 ]; do shift; done
[ "$1" = "--" ] && shift
if [ "$1" = "usbip" ] && [ "$2" = "list" ]; then
    cat <<'EOF'
  1-1: WCH : USB Serial (1a86:55d3)
       : /sys/bus/usb/devices/1-1
       : Vendor Specific Class / unknown subclass / unknown protocol (ff/00/00)
  1-2: WCH : USB Serial (1a86:55d3)
       : /sys/bus/usb/devices/1-2
       : Vendor Specific Class / unknown subclass / unknown protocol (ff/00/00)
EOF
fi
exit 0
"""
    )
    for name in ("cat", "nsenter"):
        os.chmod(bin_dir / name, 0o755)
    env["CYBERWAVE_USBIP_WAIT_SECS"] = "0"

    result = subprocess.run(
        ["sh", str(stage / "entrypoint.sh"), "--usbip-attach-only"],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(stage),
        timeout=60,
    )

    assert result.returncode != 0
    assert "Expected 2 arm serial device(s); found 1" in (result.stdout + result.stderr)


def test_attach_only_does_not_report_serial_nodes_from_other_devices(sandbox, tmp_path):
    """A camera-only USB/IP export may coexist with an unrelated VM serial
    adapter. The SO101 preflight must not map that adapter into its container."""
    env, _ = sandbox
    stage = tmp_path / "app"
    stage.mkdir()
    (stage / "entrypoint.sh").write_text(ENTRYPOINT.read_text())
    (stage / "usbip_lib.sh").write_text(
        '_usbip_resolve_host() { echo "$1"; }\n'
        "_usbip_attach_device() { :; }\n"
        "_usbip_vm_serial_nodes() { echo /dev/ttyUSB0; }\n"
        "_usbip_vm_serial_nodes_for_vidpid() { :; }\n"
        "_usbip_mirror_nodes() { :; }\n"
        "_usbip_print_device_lines() {\n"
        '    if [ "$#" -gt 0 ]; then _nodes="$1"; '
        "else _nodes=$(_usbip_vm_serial_nodes); fi\n"
        '    for _node in $_nodes; do echo "USBIP_DEVICE=$_node"; done\n'
        "}\n"
    )

    bin_dir = Path(env["PATH"].split(":", 1)[0])
    (bin_dir / "cat").write_text(
        '#!/bin/sh\nif [ "$1" = "/proc/1/comm" ]; then '
        'echo init; else exec /bin/cat "$@"; fi\n'
    )
    (bin_dir / "nsenter").write_text(
        """#!/bin/sh
while [ "$1" != "--" ] && [ "$#" -gt 0 ]; do shift; done
[ "$1" = "--" ] && shift
if [ "$1" = "usbip" ] && [ "$2" = "list" ]; then
    cat <<'EOF'
  1-3: Camera : UVC Camera (1234:5678)
       : /sys/bus/usb/devices/1-3
       : Video / Video Control (0e/01/00)
EOF
fi
exit 0
"""
    )
    for name in ("cat", "nsenter"):
        os.chmod(bin_dir / name, 0o755)
    env["CYBERWAVE_USBIP_WAIT_SECS"] = "0"

    result = subprocess.run(
        ["sh", str(stage / "entrypoint.sh"), "--usbip-attach-only"],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(stage),
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert "USBIP_DEVICE=" not in result.stdout


def _stage_entrypoint(tmp_path):
    """Copy the entrypoint plus stub libs that record being sourced/called."""
    stage = tmp_path / "app"
    stage.mkdir()
    (stage / "entrypoint.sh").write_text(ENTRYPOINT.read_text())
    (stage / "usbip_lib.sh").write_text(
        'echo "SOURCED usbip_lib" >> "$SPY_LOG"\n'
        "_usbip_vm_serial_nodes() { :; }\n"
        "_usbip_vm_serial_nodes_for_vidpid() { :; }\n"
        "_usbip_mirror_nodes() { :; }\n"
        "_usbip_print_device_lines() { :; }\n"
        "_usbip_resolve_host() { echo \"$1\"; }\n"
        "_usbip_attach_device() { :; }\n"
    )
    (stage / "serial_bridge_lib.sh").write_text(
        'echo "SOURCED serial_bridge_lib" >> "$SPY_LOG"\n'
        '_start_serial_bridges() { echo "BRIDGES $1 $2" >> "$SPY_LOG"; }\n'
    )
    return stage


def test_serial_bridge_starts_when_ports_are_configured(sandbox, tmp_path):
    """macOS cannot pass the SO101's CDC data through USB/IP, so a configured
    bridge is what actually reaches the arms."""
    env, log = sandbox
    stage = _stage_entrypoint(tmp_path)
    env["CYBERWAVE_SERIAL_BRIDGE_PORTS"] = "8100,8101"

    result = subprocess.run(
        ["sh", str(stage / "entrypoint.sh")],
        capture_output=True, text=True, env=env, cwd=str(stage), timeout=60,
    )

    assert result.returncode == 0, result.stderr
    calls = log.read_text()
    assert "SOURCED serial_bridge_lib" in calls
    assert "BRIDGES 8100,8101" in calls


def test_usbip_is_skipped_when_the_serial_bridge_is_in_use(sandbox, tmp_path):
    """Both paths create /dev/ttyACM*; running them together would collide."""
    env, log = sandbox
    stage = _stage_entrypoint(tmp_path)
    env["CYBERWAVE_SERIAL_BRIDGE_PORTS"] = "8100"
    env["CYBERWAVE_USBIP_ENABLED"] = "1"

    result = subprocess.run(
        ["sh", str(stage / "entrypoint.sh")],
        capture_output=True, text=True, env=env, cwd=str(stage), timeout=60,
    )

    assert result.returncode == 0, result.stderr
    calls = log.read_text()
    assert "SOURCED serial_bridge_lib" in calls
    assert "SOURCED usbip_lib" not in calls


def test_no_bridge_lib_sourced_when_no_ports_configured(sandbox, tmp_path):
    """Linux edge hosts must not touch the macOS-only bridge."""
    env, log = sandbox
    stage = _stage_entrypoint(tmp_path)
    env.pop("CYBERWAVE_SERIAL_BRIDGE_PORTS", None)

    result = subprocess.run(
        ["sh", str(stage / "entrypoint.sh")],
        capture_output=True, text=True, env=env, cwd=str(stage), timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert "SOURCED serial_bridge_lib" not in (log.read_text() if log.exists() else "")
