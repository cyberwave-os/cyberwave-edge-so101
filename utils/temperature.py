"""Temperature reading utilities for SO101 motors."""

from typing import Any, Dict, Optional


def read_temperatures(
    leader: Optional[Any] = None,
    follower: Optional[Any] = None,
    joint_index_to_name: Optional[Dict[str, str]] = None,
) -> Dict[str, float]:
    """
    Read temperatures from leader and/or follower motors.

    Args:
        leader: SO101Leader instance (optional)
        follower: SO101Follower instance (optional)
        joint_index_to_name: Mapping from joint index to joint name (unused, for API consistency)

    Returns:
        Dictionary mapping "leader_{motor_id}" or "follower_{motor_id}" to temperature in Celsius
    """
    temperatures: Dict[str, float] = {}

    try:
        # Read temperatures from leader
        if leader is not None and getattr(leader, "connected", False):
            motor_names = list(getattr(leader, "motors", {}).keys())
            readings = leader.bus.sync_read(
                "Present_Temperature", motors=motor_names, normalize=False
            )
            for motor_name, temperature in readings.items():
                motor = leader.motors.get(motor_name)
                if motor is not None:
                    temperatures[f"leader_{motor.id}"] = float(temperature)
    except Exception:
        pass

    try:
        # Read temperatures from follower
        if follower is not None and getattr(follower, "connected", False):
            motor_names = list(getattr(follower, "motors", {}).keys())
            readings = follower.bus.sync_read(
                "Present_Temperature", motors=motor_names, normalize=False
            )
            for motor_name, temperature in readings.items():
                motor = follower.motors.get(motor_name)
                if motor is not None:
                    temperatures[f"follower_{motor.id}"] = float(temperature)
    except Exception:
        pass

    return temperatures
