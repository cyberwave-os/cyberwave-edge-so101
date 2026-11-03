"""Terminal-awareness helpers for the SO101 driver's live displays.

The status panel (:mod:`utils.trackers`) and calibration recorder
(:mod:`motors.feetech_bus`) repaint a full screen with ANSI escapes. Without a
TTY — every containerised run — each repaint is appended to the container log
instead of overwriting a screen, ~244 MB/day for the panel alone.
Callers gate only the drawing on :func:`tui_enabled`; sampling and alerting keep
their original cadence.
"""

import os
import sys

TUI_ENV_VAR = "CYBERWAVE_STATUS_TUI"
SUMMARY_INTERVAL_ENV_VAR = "CYBERWAVE_STATUS_SUMMARY_SECONDS"
DEFAULT_SUMMARY_INTERVAL_SECONDS = 60.0

_FALSEY = {"", "0", "false", "no", "off"}


def tui_enabled() -> bool:
    """Return True when full-screen ANSI repaints are appropriate.

    Set ``CYBERWAVE_STATUS_TUI=1`` to force the display on under
    ``docker run -it``, or ``0`` to suppress it on a terminal.
    """
    override = os.getenv(TUI_ENV_VAR)
    if override is not None:
        return override.strip().lower() not in _FALSEY
    try:
        return bool(sys.stdout.isatty())
    except (AttributeError, ValueError):
        # Closed or replaced stdout — either way, don't draw.
        return False


def summary_interval_seconds() -> float:
    """Seconds between headless one-line status summaries."""
    raw = os.getenv(SUMMARY_INTERVAL_ENV_VAR)
    if raw is None:
        return DEFAULT_SUMMARY_INTERVAL_SECONDS
    try:
        value = float(raw)
    except ValueError:
        return DEFAULT_SUMMARY_INTERVAL_SECONDS
    # Non-positive would log every tick, reintroducing the volume we're avoiding.
    return value if value > 0 else DEFAULT_SUMMARY_INTERVAL_SECONDS
