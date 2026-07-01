"""Tests for cyberwave-video-sync bootstrap helper."""

from __future__ import annotations

import builtins
import importlib
import sys
from typing import Any

import pytest


@pytest.fixture(autouse=True)
def _reset_video_sync_state() -> None:
    """Isolate module state; CI installs the real extension globally."""
    from utils import video_sync

    importlib.reload(video_sync)
    video_sync._installed = False
    yield
    importlib.reload(video_sync)


def _block_cyberwave_video_sync_import(monkeypatch: pytest.MonkeyPatch) -> None:
    """Simulate a missing extension even when CI pre-installed cyberwave-video-sync."""
    real_import = builtins.__import__

    def _import(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "cyberwave_video_sync":
            raise ImportError("simulated missing cyberwave_video_sync")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _import)
    monkeypatch.delitem(sys.modules, "cyberwave_video_sync", raising=False)


def test_ensure_cyberwave_video_sync_idempotent(monkeypatch: pytest.MonkeyPatch) -> None:
    from utils import video_sync

    calls: list[object] = []

    class _FakeSync:
        __version__ = "0.1.0-test"

        @staticmethod
        def install() -> None:
            calls.append(True)

    monkeypatch.setitem(sys.modules, "cyberwave_video_sync", _FakeSync())

    assert video_sync.ensure_cyberwave_video_sync() is True
    assert video_sync.ensure_cyberwave_video_sync() is True
    assert len(calls) == 1


def test_ensure_cyberwave_video_sync_optional_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from utils import video_sync

    _block_cyberwave_video_sync_import(monkeypatch)

    assert video_sync.ensure_cyberwave_video_sync(required=False) is False


def test_ensure_cyberwave_video_sync_required_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    from utils import video_sync

    _block_cyberwave_video_sync_import(monkeypatch)

    with pytest.raises(RuntimeError, match="cyberwave-video-sync is required"):
        video_sync.ensure_cyberwave_video_sync(required=True)
