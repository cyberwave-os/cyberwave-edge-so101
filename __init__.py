"""Standalone SO101 Robot Library."""

from scripts.cw_teleoperate import teleoperate
from so101.follower import SO101Follower
from so101.leader import SO101Leader
from utils.config import FollowerConfig, LeaderConfig
from utils.utils import (
    detect_voltage_rating,
    find_available_ports,
    find_port,
    is_port_available,
    test_device_connection,
)

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

__version__ = "0.2.0"

