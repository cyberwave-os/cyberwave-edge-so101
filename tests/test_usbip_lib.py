"""Tests for ``usbip_lib.sh`` — the in-container USB/IP device-node mirror.

Docker snapshots the VM's device list into a privileged container's private
tmpfs ``/dev`` at container-*creation* time, so ``/dev/ttyACM*`` nodes that the
entrypoint attaches afterwards exist in the VM but never inside the container.
``_usbip_mirror_nodes`` recreates them locally with ``mknod``.

The fakes below stand in for ``nsenter`` (which needs a real VM) and ``mknod``
(which needs root), so the shell logic itself is what gets exercised.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

LIB = Path(__file__).resolve().parent.parent / "usbip_lib.sh"

# 0xa6 == 166, the ttyACM major.
FAKE_NSENTER = """#!/bin/sh
# Emulate `nsenter -t 1 -m [-n] -- <cmd...>` against a stub VM /dev.
[ -n "$NSENTER_LOG" ] && echo "$*" >> "$NSENTER_LOG"
while [ "$1" != "--" ] && [ $# -gt 0 ]; do shift; done
[ "$1" = "--" ] && shift
case "$1" in
  stat)
    # `stat -c %t %T <dev>` -> hex major minor
    case "$4" in
      */ttyACM0) echo "a6 0" ;;
      */ttyACM1) echo "a6 1" ;;
      *) exit 1 ;;
    esac
    ;;
  sh)
    case "$3" in
      *idVendor*)
        # VID:PID-filtered helper walks VM sysfs and emits matching tty nodes.
        printf '%s\\n' $FAKE_VM_MATCHING_NODES
        ;;
      *)
        # `sh -c 'ls -d /dev/ttyACM* ...'`
        printf '%s\\n' $FAKE_VM_NODES
        ;;
    esac
    ;;
  usbip) exit 0 ;;
  *) exit 1 ;;
esac
"""

FAKE_MKNOD = """#!/bin/sh
echo "$*" >> "$MKNOD_LOG"
"""


@pytest.fixture
def shell_env(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "nsenter").write_text(FAKE_NSENTER)
    (bin_dir / "mknod").write_text(FAKE_MKNOD)
    for name in ("nsenter", "mknod"):
        os.chmod(bin_dir / name, 0o755)

    log = tmp_path / "mknod.log"
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["MKNOD_LOG"] = str(log)
    return env, log


def _run(env, func: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["sh", "-c", f". '{LIB}'; {func}"],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )


def test_mirror_creates_nodes_with_matching_major_and_minor(shell_env):
    """A mirrored node is useless unless its major/minor match the VM's."""
    env, log = shell_env
    env["FAKE_VM_NODES"] = "/dev/ttyACM0 /dev/ttyACM1"

    result = _run(env, "_usbip_mirror_nodes")

    assert result.returncode == 0, result.stderr
    calls = log.read_text().strip().splitlines()
    assert "/dev/ttyACM0 c 166 0" in calls
    assert "/dev/ttyACM1 c 166 1" in calls


def test_mirror_is_a_noop_when_vm_has_no_nodes(shell_env):
    env, log = shell_env
    env["FAKE_VM_NODES"] = ""

    result = _run(env, "_usbip_mirror_nodes")

    assert result.returncode == 0, result.stderr
    assert not log.exists() or log.read_text().strip() == ""


def test_mirror_skips_nodes_already_present_in_the_container(shell_env):
    """/dev/null always exists locally, so it must never be re-created."""
    env, log = shell_env
    env["FAKE_VM_NODES"] = "/dev/null"

    result = _run(env, "_usbip_mirror_nodes")

    assert result.returncode == 0, result.stderr
    assert not log.exists() or "/dev/null" not in log.read_text()


def test_attach_runs_in_the_vm_network_namespace(shell_env):
    """The kernel keeps the attach socket for the whole device session. With
    `nsenter -m` alone that socket lives in *this container's* network
    namespace and dies when the container exits, killing the import. It must be
    created in the VM's netns (-n) so it outlives any container."""
    env, log = shell_env
    nsenter_log = log.parent / "nsenter.log"
    env["NSENTER_LOG"] = str(nsenter_log)

    result = subprocess.run(
        ["sh", "-c", f". '{LIB}'; _usbip_attach_device 10.0.0.1 1-1-1"],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    call = nsenter_log.read_text()
    assert "-n" in call.split(), f"attach must enter the VM netns: {call}"
    assert "-m" in call.split()
    assert "1-1-1" in call


def test_resolve_host_passes_through_a_bare_ipv4(shell_env):
    """DNS for host.docker.internal is container-scoped and does not resolve in
    the VM netns, so the caller resolves first and passes an address."""
    env, _ = shell_env

    result = subprocess.run(
        ["sh", "-c", f". '{LIB}'; _usbip_resolve_host 192.168.65.254"],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )

    assert result.stdout.strip() == "192.168.65.254"


def test_resolve_host_falls_back_to_the_name_when_unresolvable(shell_env):
    """A resolution failure must not abort the entrypoint under `set -e`."""
    env, _ = shell_env

    result = subprocess.run(
        ["sh", "-c", f". '{LIB}'; _usbip_resolve_host no-such-host.invalid"],
        capture_output=True,
        text=True,
        env=env,
        timeout=30,
    )

    assert result.returncode == 0
    assert result.stdout.strip() == "no-such-host.invalid"


def test_vm_serial_nodes_lists_paths_from_the_vm_namespace(shell_env):
    env, _ = shell_env
    env["FAKE_VM_NODES"] = "/dev/ttyACM0 /dev/ttyACM1"

    result = _run(env, "_usbip_vm_serial_nodes")

    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["/dev/ttyACM0", "/dev/ttyACM1"]


def test_vm_serial_nodes_for_vidpid_excludes_unrelated_serial_devices(shell_env):
    """The Docker VM may contain serial devices unrelated to SO101. They must
    neither satisfy the expected-arm count nor be mapped into the driver."""
    env, _ = shell_env
    env["FAKE_VM_NODES"] = "/dev/ttyACM0 /dev/ttyUSB0 /dev/ttyACM1"
    env["FAKE_VM_MATCHING_NODES"] = "/dev/ttyACM0 /dev/ttyACM1"

    result = _run(env, "_usbip_vm_serial_nodes_for_vidpid 1a86:55d3")

    assert result.returncode == 0, result.stderr
    assert result.stdout.split() == ["/dev/ttyACM0", "/dev/ttyACM1"]
