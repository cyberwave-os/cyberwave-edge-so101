"""Standalone SO101 Robot Library."""

from config import FollowerConfig, LeaderConfig
from follower import SO101Follower
from leader import SO101Leader
from teleoperate import teleoperate
from utils import (
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

__version__ = "0.1.0"

