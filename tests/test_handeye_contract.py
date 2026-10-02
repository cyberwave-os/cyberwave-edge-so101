"""The hand-eye alert contract now lives with the flow, in the SDK.

This file used to carry the characterisation suite that pinned ``snapshot()``,
``describe_state()``, ``buttons_for_state()`` and the verdict truth table exactly.
When the flow moved into ``cyberwave.calibration`` those assertions moved with it
-- to ``tests/test_calibration_flow.py`` in the SDK -- because a contract test
belongs next to the code that produces the contract. Leaving a copy here would
have meant two baselines to keep in step, and the copy would drift.

What remains here is the check that the move did not simply lose the coverage,
plus the values the *driver* still restates locally and could drift on. The
driver-side integration tests are in ``test_handeye.py``.
"""

from __future__ import annotations

import pytest


def test_the_sdk_owns_the_flow_and_its_rendering():
    """The driver must not have grown its own copy of either."""
    from cyberwave.calibration import HandEyeFlow, HandEyeRunner, describe_state

    assert HandEyeFlow is not None
    assert HandEyeRunner is not None
    assert callable(describe_state)


def test_the_driver_re_exports_the_moved_names():
    """Existing call sites import hand-eye from one place, and still can."""
    import utils.cw_handeye as m

    for name in (
        "HandEyeError",
        "HandEyeFlow",
        "HandEyeRunner",
        "HandEyeFlowConfig",
        "buttons_for_state",
        "describe_state",
        "resolve_docked_fk_frame",
    ):
        assert getattr(m, name) is not None, name


def test_an_unknown_name_still_raises_attribute_error():
    """The lazy re-export must not turn a typo into a silent None."""
    import utils.cw_handeye as m

    with pytest.raises(AttributeError):
        _ = m.no_such_name


def test_the_alert_contract_values_have_not_drifted():
    """Alert type and state strings are a frontend contract: the dashboard gates
    on them literally, so a drift breaks the UI silently rather than failing."""
    from utils.cw_handeye import assert_constants_match_sdk

    assert_constants_match_sdk()


def test_the_verdict_thresholds_are_still_this_arms_measured_numbers():
    """Reproducible runs on this arm land at 1.9-3.5 mm; ill-conditioned ones
    reach 7.7-24 mm. The frontend duplicates these two numbers, so changing one
    here needs the dashboard changed with it."""
    from utils.cw_handeye import HANDEYE_BAD_STABILITY_M, HANDEYE_GOOD_STABILITY_M

    assert HANDEYE_GOOD_STABILITY_M == 0.004
    assert HANDEYE_BAD_STABILITY_M == 0.008
