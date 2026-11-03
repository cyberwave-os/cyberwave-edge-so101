"""Bootstrap for the optional ``cyberwave-video-sync`` extension."""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_installed = False


def ensure_cyberwave_video_sync(*, required: bool = False) -> bool:
    """Install video sync hooks when ``cyberwave-video-sync`` is available.

    The SDK's :class:`~cyberwave.sensor.base_video.BaseVideoStreamer` also
    calls ``install()`` during WebRTC setup; invoking it here makes startup
    failures visible before the first camera stream and keeps standalone
    scripts aligned with the container image.

    Args:
        required: When ``True``, raise ``RuntimeError`` if the extension is
            missing. Edge Docker images should set this; local dev without the
            extension can leave the default ``False``.

    Returns:
        ``True`` when the extension is installed, ``False`` otherwise.
    """
    global _installed
    if _installed:
        return True

    try:
        import cyberwave_video_sync
    except ImportError:
        if required:
            raise RuntimeError(
                "cyberwave-video-sync is required for SO101 camera streaming. "
                "Install it with the driver image or `pip install cyberwave-video-sync`."
            )
        logger.warning(
            "cyberwave-video-sync not installed; camera/video frame sync disabled"
        )
        return False

    cyberwave_video_sync.install()
    _installed = True
    logger.info(
        "cyberwave-video-sync %s installed (camera frame sync enabled)",
        getattr(cyberwave_video_sync, "__version__", "unknown"),
    )
    return True
