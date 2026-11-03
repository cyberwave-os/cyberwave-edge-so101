"""Standalone SO101 Robot Library."""

__version__ = "0.2.0"

__all__ = [
    "SO101Leader",
    "SO101Follower",
    "LeaderConfig",
    "FollowerConfig",
    "teleoperate",
    "find_available_ports",
    "find_port",
    "is_port_available",
    "test_device_connection",
    "detect_voltage_rating",
]


def __getattr__(name: str):
    """Lazy imports to avoid loading cyberwave/scripts during pytest collection."""
    if name == "teleoperate":
        from scripts.cw_teleoperate import teleoperate
        return teleoperate
    if name == "SO101Follower":
        from so101.follower import SO101Follower
        return SO101Follower
    if name == "SO101Leader":
        from so101.leader import SO101Leader
        return SO101Leader
    if name in ("FollowerConfig", "LeaderConfig"):
        from utils.config import FollowerConfig, LeaderConfig
        return FollowerConfig if name == "FollowerConfig" else LeaderConfig
    if name in ("detect_voltage_rating", "find_available_ports", "find_port", "is_port_available", "test_device_connection"):
        from utils.utils import (
            detect_voltage_rating,
            find_available_ports,
            find_port,
            is_port_available,
            test_device_connection,
        )
        return {
            "detect_voltage_rating": detect_voltage_rating,
            "find_available_ports": find_available_ports,
            "find_port": find_port,
            "is_port_available": is_port_available,
            "test_device_connection": test_device_connection,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

