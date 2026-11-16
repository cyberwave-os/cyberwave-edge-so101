"""A persisted serial port that no longer exists must not be trusted.

setup.json survives container recreation (it is bind-mounted from the host), and
``_apply_so101_ports_env_and_discovery`` only *overwrites* ports when the
environment supplies one or voltage discovery succeeds. A path that has since
disappeared therefore lives forever and gets handed to calibration, which fails
with a bare FileNotFoundError and raises a ``device_disconnected`` alert — it
reads like broken hardware rather than stale config.

This is especially sharp on macOS, where a host path such as
``/dev/tty.usbmodem5B141129631`` can never exist inside the Linux container.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def no_discovery(monkeypatch):
    """Voltage discovery finds nothing — the case where stale values survive."""
    import utils.utils as uu

    monkeypatch.setattr(
        uu,
        "discover_so101_ports_by_voltage",
        lambda *a, **k: {"leader_port": None, "follower_port": None},
    )
    monkeypatch.delenv("CYBERWAVE_METADATA_LEADER_PORT", raising=False)
    monkeypatch.delenv("CYBERWAVE_METADATA_FOLLOWER_PORT", raising=False)


def test_persisted_port_that_no_longer_exists_is_dropped(no_discovery, tmp_path):
    import main

    # Paths under tmp_path are guaranteed absent — using a literal
    # /dev/tty.usbmodem* would pass or fail depending on whether the machine
    # running the tests happens to have an arm plugged in.
    cfg = {
        "leader_port": str(tmp_path / "tty.usbmodem5B141128621"),
        "follower_port": str(tmp_path / "tty.usbmodem5B141129631"),
    }

    main._apply_so101_ports_env_and_discovery(cfg, phase="test")

    assert not cfg.get("leader_port"), "stale macOS host path must not survive"
    assert not cfg.get("follower_port")


def test_persisted_port_that_still_exists_is_kept(no_discovery, tmp_path):
    """Only *missing* paths are pruned; a live device is left alone."""
    import main

    live = tmp_path / "ttyACM0"
    live.write_text("")
    cfg = {"follower_port": str(live)}

    main._apply_so101_ports_env_and_discovery(cfg, phase="test")

    assert cfg.get("follower_port") == str(live)


def test_env_supplied_port_is_never_pruned(monkeypatch, no_discovery):
    """An explicit operator override must reach the driver even if absent, so
    the resulting error names the port they configured."""
    import main

    monkeypatch.setenv("CYBERWAVE_METADATA_FOLLOWER_PORT", "/dev/ttyDOESNOTEXIST")
    cfg: dict = {}

    main._apply_so101_ports_env_and_discovery(cfg, phase="test")

    assert cfg.get("follower_port") == "/dev/ttyDOESNOTEXIST"
