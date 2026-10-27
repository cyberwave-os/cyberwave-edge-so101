"""Unit tests for StatusTracker behavior."""

from utils.trackers import StatusTracker


def test_update_joint_temperatures_replaces_snapshot() -> None:
    tracker = StatusTracker()
    tracker.update_joint_temperatures({"leader_1": 72.0, "follower_1": 48.0})
    assert tracker.get_status()["joint_temperatures"] == {"leader_1": 72.0, "follower_1": 48.0}

    tracker.update_joint_temperatures({"follower_1": 49.0})
    assert tracker.get_status()["joint_temperatures"] == {"follower_1": 49.0}

    tracker.update_joint_temperatures({})
    assert tracker.get_status()["joint_temperatures"] == {}
