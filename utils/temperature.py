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
        from motors.tables import ADDR_PRESENT_TEMPERATURE

        addr = ADDR_PRESENT_TEMPERATURE[0]  # Temperature is 1 byte

        # Read temperatures from leader
        if leader is not None and getattr(leader, "connected", False):
            motors = getattr(leader, "motors", {})
            for motor in motors.values():
                motor_id = motor.id
                try:
                    temperature, result, error = leader.bus._packet_handler.read1ByteTxRx(
                        leader.bus._port_handler, motor_id, addr
                    )
                    if result == 0:  # COMM_SUCCESS
                        temperatures[f"leader_{motor_id}"] = float(temperature)
                except Exception:
                    pass

        # Read temperatures from follower
        if follower is not None and getattr(follower, "connected", False):
            motors = getattr(follower, "motors", {})
            for motor in motors.values():
                motor_id = motor.id
                try:
                    temperature, result, error = follower.bus._packet_handler.read1ByteTxRx(
                        follower.bus._port_handler, motor_id, addr
                    )
                    if result == 0:  # COMM_SUCCESS
                        temperatures[f"follower_{motor_id}"] = float(temperature)
                except Exception:
                    pass
    except Exception:
        pass

    return temperatures
