"""Tests for ``serial_bridge_lib.sh`` — the container half of the macOS serial bridge.

macOS keeps AppleUSBACM attached to the SO101's CDC interface, so USB/IP can
enumerate the device but never carries its bulk data. The host therefore runs a
``socat`` TCP listener per arm (same shape as the camera MJPEG and audio
bridges) and the container turns each TCP endpoint back into a PTY the driver
can open as an ordinary serial port.
"""

from __future__ import annotations

import os
import signal
import subprocess
import time
from pathlib import Path

import pytest

LIB = Path(__file__).resolve().parent.parent / "serial_bridge_lib.sh"

FAKE_SOCAT = """#!/bin/sh
echo "$*" >> "$SOCAT_LOG"
# Emulate socat creating the PTY symlink it was asked for, then staying up.
for arg in "$@"; do
    case "$arg" in
        PTY,link=*)
            _p=${arg#PTY,link=}
            _p=${_p%%,*}
            : > "$_p"
            ;;
    esac
done
# Stay alive briefly so the caller sees a running bridge, but do not hold the
# test's stdout pipe open for long.
exec >/dev/null 2>&1
if [ -n "$FAKE_SOCAT_EXIT_AFTER" ]; then
    sleep "$FAKE_SOCAT_EXIT_AFTER"
    exit "${FAKE_SOCAT_EXIT_STATUS:-0}"
fi
sleep 3
"""


_RUNNING_GROUPS: list[int] = []


@pytest.fixture(autouse=True)
def _reap_bridge_groups():
    yield
    for pgid in _RUNNING_GROUPS:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            pass
    _RUNNING_GROUPS.clear()


@pytest.fixture
def sandbox(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "socat").write_text(FAKE_SOCAT)
    os.chmod(bin_dir / "socat", 0o755)

    log = tmp_path / "socat.log"
    env = dict(os.environ)
    env["PATH"] = f"{bin_dir}:{env['PATH']}"
    env["SOCAT_LOG"] = str(log)
    # Keep PTY nodes inside tmp_path so the test never touches a real /dev.
    env["CYBERWAVE_SERIAL_BRIDGE_DEV_PREFIX"] = str(tmp_path / "ttyACM")
    env["CYBERWAVE_SERIAL_BRIDGE_WAIT_SECS"] = "5"
    return env, log, tmp_path


def _run(env, func: str) -> subprocess.CompletedProcess:
    """Run *func* and reap the whole process group.

    The bridges are supervised by backgrounded loops that never exit by design,
    so they must not share the parent's stdio (a pipe would never reach EOF) and
    must be killed as a group once the assertions are done.
    """
    out = Path(env["SOCAT_LOG"]).parent / "run.out"
    proc = subprocess.Popen(
        ["/bin/sh", "-c", f". '{LIB}'; {func} > '{out}' 2>&1"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
        start_new_session=True,
    )
    try:
        rc = proc.wait(timeout=60)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        raise
    _RUNNING_GROUPS.append(proc.pid)
    return subprocess.CompletedProcess(
        args=func, returncode=rc, stdout=out.read_text() if out.exists() else "", stderr=""
    )


def test_one_pty_is_created_per_bridge_port(sandbox):
    env, log, tmp_path = sandbox

    result = _run(env, '_start_serial_bridges "8100,8101" example.host')

    assert result.returncode == 0, result.stderr
    calls = log.read_text()
    assert "TCP:example.host:8100" in calls
    assert "TCP:example.host:8101" in calls
    assert f"PTY,link={tmp_path}/ttyACM0" in calls
    assert f"PTY,link={tmp_path}/ttyACM1" in calls


def test_ptys_become_available_before_returning(sandbox):
    """The driver opens these immediately after the entrypoint returns, so the
    helper must not hand back before the nodes exist."""
    env, _, tmp_path = sandbox

    result = _run(env, '_start_serial_bridges "8100,8101" example.host')

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "ttyACM0").exists()
    assert (tmp_path / "ttyACM1").exists()


def test_empty_port_list_is_a_noop(sandbox):
    env, log, _ = sandbox

    result = _run(env, '_start_serial_bridges "" example.host')

    assert result.returncode == 0, result.stderr
    assert not log.exists() or log.read_text().strip() == ""


def test_whitespace_and_blank_entries_are_ignored(sandbox):
    """A trailing comma in config must not spawn a bridge to port ''."""
    env, log, tmp_path = sandbox

    result = _run(env, '_start_serial_bridges " 8100 , ,8101, " example.host')

    assert result.returncode == 0, result.stderr
    calls = log.read_text()
    assert "TCP:example.host:8100" in calls
    assert "TCP:example.host:8101" in calls
    assert "TCP:example.host:" not in calls.replace("TCP:example.host:8100", "").replace(
        "TCP:example.host:8101", ""
    )


def test_missing_socat_fails_loudly_without_aborting(sandbox):
    """Without socat the arms are simply unreachable; say so rather than
    dying under `set -e` and taking the whole driver down."""
    env, _, _ = sandbox
    env["PATH"] = "/nonexistent"

    result = _run(env, '_start_serial_bridges "8100" example.host')

    assert result.returncode == 0
    assert "socat" in (result.stdout + result.stderr).lower()


def test_bridge_survives_the_host_side_restarting(sandbox):
    """The host wrapper deliberately restarts on device replug (~1s). Each of
    those restarts closes the TCP peer, and a bare `socat ... TCP:` exits when
    that happens — deleting the PTY symlink for the container's whole remaining
    life. The container side needs its own retry, like the host's while-loop."""
    env, log, tmp_path = sandbox
    env["FAKE_SOCAT_EXIT_AFTER"] = "1"
    env["FAKE_SOCAT_EXIT_STATUS"] = "1"

    result = _run(env, 'set -e; _start_serial_bridges "8100" example.host')

    assert result.returncode == 0, result.stderr
    node = tmp_path / "ttyACM0"
    assert node.exists()

    # Simulate the host restarting: socat exits and removes its link.
    node.unlink()
    deadline = time.time() + 8
    while time.time() < deadline and not node.exists():
        time.sleep(0.2)

    assert node.exists(), "PTY must be re-created after the peer goes away"
