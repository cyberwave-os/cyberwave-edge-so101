"""Motors map to twin schema joints by name, not by position in the joint list.

``twin.get_controllable_joint_names()`` returns the schema's joints sorted by name,
and schemas list joints in any order (the catalog SO-101 lists ``_6`` .. ``_1``).
Whatever the order, motor 1 is the shoulder pan. Each case also runs against the
list in schema order, so the mapping does not lean on the SDK sorting it.
"""

from __future__ import annotations

import json
import queue
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from so101.robot import SO101_MOTORS
from utils.cw_remoteoperate_helpers import (
    create_joint_state_callback,
    upload_calibration_to_twin,
)
from utils.cw_utils import build_joint_mappings, build_motor_id_to_schema_joint

NAMED_IN_MOTOR_ORDER = list(SO101_MOTORS)  # shoulder_pan .. gripper

SCHEMA_ORDERS = {
    "numbered_6_to_1": [f"_{i}" for i in range(6, 0, -1)],
    "numbered_1_to_6": [f"_{i}" for i in range(1, 7)],
    "named_schema_order": NAMED_IN_MOTOR_ORDER,
    "named_shuffled": [
        "wrist_roll",
        "gripper",
        "elbow_flex",
        "shoulder_pan",
        "wrist_flex",
        "shoulder_lift",
    ],
}

NUMBERED = {motor.id: f"_{motor.id}" for motor in SO101_MOTORS.values()}
NAMED = {motor.id: name for name, motor in SO101_MOTORS.items()}
EXPECTED = {
    "numbered_6_to_1": NUMBERED,
    "numbered_1_to_6": NUMBERED,
    "named_schema_order": NAMED,
    "named_shuffled": NAMED,
}


LIST_ORDERS = {
    # What the SDK returns: the schema's controllable joints, sorted by name.
    "sdk_sorted": sorted,
    "schema_order": list,
}


def _twin(schema_joints: list[str], list_order: str = "sdk_sorted") -> MagicMock:
    twin = MagicMock()
    twin.uuid = "twin-uuid-1"
    twin.get_controllable_joint_names.return_value = LIST_ORDERS[list_order](schema_joints)
    return twin


cases = pytest.mark.parametrize(
    ("case", "list_order"),
    [(c, o) for c in sorted(SCHEMA_ORDERS) for o in LIST_ORDERS],
)


@cases
def test_motor_ids_map_to_schema_joints_by_name(case: str, list_order: str) -> None:
    mapping = build_motor_id_to_schema_joint(_twin(SCHEMA_ORDERS[case], list_order))
    assert mapping == EXPECTED[case]
    assert mapping[SO101_MOTORS["shoulder_pan"].id] in ("_1", "shoulder_pan")


@cases
def test_incoming_commands_route_to_the_matching_motor(case: str, list_order: str) -> None:
    mappings = build_joint_mappings(_twin(SCHEMA_ORDERS[case], list_order))
    shoulder_pan_joint = EXPECTED[case][1]
    assert mappings["schema_joint_to_motor_id"][shoulder_pan_joint] == 1


MOTOR_NAME = {motor.id: name for name, motor in SO101_MOTORS.items()}


def _command(fmt: str, joint: str, radians: float) -> dict:
    if fmt == "single_joint":
        return {"source_type": "tele", "joint_name": joint, "joint_state": {"position": radians}}
    return {"source_type": "tele", "positions": {joint: radians}}


@pytest.mark.parametrize("fmt", ["single_joint", "multi_joint"])
@cases
def test_commands_move_only_the_matching_motor(case: str, list_order: str, fmt: str) -> None:
    mappings = build_joint_mappings(_twin(SCHEMA_ORDERS[case], list_order))
    calibration = {name: SimpleNamespace(range_min=1000, range_max=3000) for name in SO101_MOTORS}
    for motor_id, joint in EXPECTED[case].items():
        state = {f"{name}.pos": 10.0 for name in SO101_MOTORS}
        actions: queue.Queue = queue.Queue()
        callback = create_joint_state_callback(
            current_state=state,
            action_queue=actions,
            joint_index_to_name=mappings["joint_index_to_name"],
            joint_name_to_norm_mode=mappings["joint_name_to_norm_mode"],
            follower=SimpleNamespace(motors=SO101_MOTORS),
            follower_calibration=calibration,
            schema_joint_to_motor_id=mappings["schema_joint_to_motor_id"],
        )
        callback("cyberwave/twin/twin-uuid-1/update", _command(fmt, joint, 0.1))

        action = actions.get_nowait()
        moved = {key for key, value in action.items() if value != 10.0}
        assert moved == {f"{MOTOR_NAME[motor_id]}.pos"}, (joint, moved)


# Schemas whose names only partly match the motors. The position fallback must
# not hand a motor a joint another motor already matched by name. Each case lists
# the motors whose joint is certain; the rest may get an unclaimed joint or "_{id}".
PARTIAL_SCHEMAS = {
    "named_with_jaw": (
        NAMED_IN_MOTOR_ORDER[:5] + ["jaw"],
        {i: NAMED[i] for i in range(1, 6)},
    ),
    "numbered_without_1": (
        [f"_{i}" for i in range(6, 1, -1)],
        {i: f"_{i}" for i in range(2, 7)},
    ),
}


@pytest.mark.parametrize("list_order", list(LIST_ORDERS))
@pytest.mark.parametrize("case", sorted(PARTIAL_SCHEMAS))
def test_partly_matching_schema_never_maps_two_motors_to_one_joint(
    case: str, list_order: str
) -> None:
    schema, expected = PARTIAL_SCHEMAS[case]
    mapping = build_motor_id_to_schema_joint(_twin(schema, list_order))
    assert len(set(mapping.values())) == len(mapping)
    assert {motor_id: mapping[motor_id] for motor_id in expected} == expected


def test_unknown_joint_names_fall_back_to_position() -> None:
    mapping = build_motor_id_to_schema_joint(_twin(["j0", "j1", "j2", "j3", "j4", "j5"]))
    assert mapping == {i: f"j{i - 1}" for i in range(1, 7)}


def test_unreadable_twin_joints_fall_back_to_motor_id() -> None:
    twin = MagicMock()
    twin.get_controllable_joint_names.side_effect = RuntimeError("no schema")
    assert build_motor_id_to_schema_joint(twin) == NUMBERED


def _device(tmp_path) -> SimpleNamespace:
    calib = {
        name: {
            "id": motor.id,
            "drive_mode": 0,
            "homing_offset": 10 * motor.id,
            "range_min": 1000 + motor.id,
            "range_max": 3000 + motor.id,
        }
        for name, motor in SO101_MOTORS.items()
    }
    (tmp_path / "follower1.json").write_text(json.dumps(calib))
    config = SimpleNamespace(calibration_dir=tmp_path, id="follower1")
    return SimpleNamespace(config=config, motors=SO101_MOTORS)


@cases
def test_calibration_uploads_to_the_matching_joint(case: str, list_order: str, tmp_path) -> None:
    twin = _twin(SCHEMA_ORDERS[case], list_order)
    upload_calibration_to_twin(_device(tmp_path), twin, robot_type="follower")

    twin.update_calibration.assert_called_once()
    uploaded = twin.update_calibration.call_args.args[0]
    expected = EXPECTED[case]
    assert set(uploaded) == set(expected.values())
    for motor_id, joint in expected.items():
        assert uploaded[joint]["id"] == str(motor_id)
    assert uploaded[expected[1]]["range_min"] == 1001  # shoulder pan's own range
