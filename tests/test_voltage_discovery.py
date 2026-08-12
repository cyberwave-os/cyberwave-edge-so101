"""Tests for SO101 port autodiscovery via Present_Voltage (detect_voltage_rating, discover_so101_ports_by_voltage)."""

from __future__ import annotations

import sys
import types
from pathlib import Path
from unittest.mock import patch

import pytest

# Project root on path (same pattern as test_imports)
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _install_fake_scservo(
    *,
    open_ok: bool | list[bool] = True,
    present_raw_tenths: int = 54,
) -> None:
    """Put a fake scservo_sdk in sys.modules for detect_voltage_rating."""

    open_states: list[bool]
    if isinstance(open_ok, bool):
        open_states = [open_ok]
    else:
        open_states = list(open_ok)

    class _FakePortHandler:
        _open_idx = 0

        def __init__(self, port: str) -> None:
            self.port = port

        def setBaudRate(self, _baud: int) -> None:
            pass

        def openPort(self) -> bool:
            idx = min(_FakePortHandler._open_idx, len(open_states) - 1)
            ok = open_states[idx]
            _FakePortHandler._open_idx += 1
            return ok

        def closePort(self) -> None:
            pass

    class _FakePacketHandler:
        def __init__(self, _protocol: float) -> None:
            pass

        def read1ByteTxRx(self, _port_handler: object, _motor_id: int, _addr: int) -> tuple[int, int, None]:
            return (present_raw_tenths, 0, None)

    fake = types.ModuleType("scservo_sdk")
    fake.PortHandler = _FakePortHandler
    fake.PacketHandler = _FakePacketHandler
    sys.modules["scservo_sdk"] = fake


def _reset_fake_open_counter() -> None:
    import scservo_sdk

    ph = scservo_sdk.PortHandler
    ph._open_idx = 0  # type: ignore[attr-defined]


@pytest.fixture
def fake_scservo_module():
    """Replace scservo_sdk for the duration of the test."""
    saved = sys.modules.get("scservo_sdk")
    yield
    if saved is not None:
        sys.modules["scservo_sdk"] = saved
    else:
        sys.modules.pop("scservo_sdk", None)


class TestDetectVoltageRating:
    def test_returns_5v_when_present_voltage_low(self, fake_scservo_module) -> None:
        _install_fake_scservo(open_ok=True, present_raw_tenths=54)
        from utils import utils as uu

        with patch.object(uu, "time") as m_time:
            m_time.sleep = lambda _s: None
            assert uu.detect_voltage_rating("/dev/ttyFAKE") == 5

    def test_returns_12v_when_present_voltage_high(self, fake_scservo_module) -> None:
        _install_fake_scservo(open_ok=True, present_raw_tenths=125)
        from utils import utils as uu

        with patch.object(uu, "time") as m_time:
            m_time.sleep = lambda _s: None
            assert uu.detect_voltage_rating("/dev/ttyFAKE") == 12

    def test_retries_then_succeeds(self, fake_scservo_module) -> None:
        _install_fake_scservo(open_ok=[False, False, True], present_raw_tenths=125)
        _reset_fake_open_counter()
        from utils import utils as uu

        with patch.object(uu, "time") as m_time:
            m_time.sleep = lambda _s: None
            assert uu.detect_voltage_rating("/dev/ttyFAKE") == 12

    def test_returns_none_when_never_opens(self, fake_scservo_module) -> None:
        _install_fake_scservo(open_ok=False, present_raw_tenths=125)
        _reset_fake_open_counter()
        from utils import utils as uu

        with patch.object(uu, "time") as m_time:
            m_time.sleep = lambda _s: None
            assert uu.detect_voltage_rating("/dev/ttyFAKE") is None

    def test_returns_none_when_no_motor_reads(self, fake_scservo_module) -> None:
        _install_fake_scservo(open_ok=True, present_raw_tenths=54)

        class _BadPacket:
            def __init__(self, _p: float) -> None:
                pass

            def read1ByteTxRx(self, _ph: object, _mid: int, _addr: int) -> tuple[int, int, None]:
                return (0, 1, None)  # non-zero = failure

        fake = sys.modules["scservo_sdk"]
        fake.PacketHandler = _BadPacket  # type: ignore[attr-defined]
        _reset_fake_open_counter()
        from utils import utils as uu

        with patch.object(uu, "time") as m_time:
            m_time.sleep = lambda _s: None
            assert uu.detect_voltage_rating("/dev/ttyFAKE") is None


class TestDiscoverSo101PortsByVoltage:
    def test_two_ports_both_classified(self) -> None:
        from utils import utils as uu

        with patch.object(uu, "_find_so101_candidate_ports", return_value=["/dev/a", "/dev/b"]):
            with patch.object(uu, "detect_voltage_rating", side_effect=[5, 12]):
                out = uu.discover_so101_ports_by_voltage()
        assert out == {"leader_port": "/dev/a", "follower_port": "/dev/b"}

    def test_one_port_fails_no_inference(self) -> None:
        from utils import utils as uu

        with patch.object(uu, "_find_so101_candidate_ports", return_value=["/dev/a", "/dev/b"]):
            with patch.object(uu, "detect_voltage_rating", side_effect=[5, None]):
                out = uu.discover_so101_ports_by_voltage()
        assert out["leader_port"] == "/dev/a"
        assert out["follower_port"] is None

    def test_empty_candidates(self) -> None:
        from utils import utils as uu

        with patch.object(uu, "_find_so101_candidate_ports", return_value=[]):
            out = uu.discover_so101_ports_by_voltage()
        assert out == {"leader_port": None, "follower_port": None}
