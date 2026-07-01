#!/usr/bin/env python3
"""Test Pinocchio collision detection for SO101 robot.

Tests both floor collision and self-collision detection using various poses.

Usage:
    # Auto-detect URDF path
    python scripts/test_collision.py

    # Specify URDF path
    python scripts/test_collision.py --urdf /path/to/so101.urdf

    # From project root
    cd cyberwave-edge-so101
    python -m scripts.test_collision

Example output:
    ✅ PASS - Collision detected: Self-collision detected: upper_arm ↔ wrist
    ❌ FAIL - Expected collision but pose is safe
"""

import argparse
import sys
import time
from pathlib import Path

import numpy as np

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from so101.follower import SO101Follower
from utils.config import FollowerConfig, get_so101_urdf_path


def run_collision_detection(urdf_path: str):
    """Test Pinocchio collision detection with various poses."""

    print("=" * 80)
    print("SO101 Collision Detection Test")
    print("=" * 80)
    print()

    # Create a follower instance (no need to connect to hardware for kinematics testing)
    config = FollowerConfig(
        port="/dev/null",  # Dummy port, we won't connect
        max_relative_target=10.0,
    )
    follower = SO101Follower(config=config)

    # Initialize Pinocchio
    print(f"Loading URDF from: {urdf_path}")
    success = follower.init_pin(urdf_path, floor_z=0.0, collision_margin=0.01)

    if not success:
        print("❌ Failed to load Pinocchio model")
        return False

    print("✅ Pinocchio model loaded successfully")
    print(f"   - Joints: {follower._pin_model.nq}")
    print(
        f"   - Collision geometries: {'enabled' if follower._pin_collision_model else 'disabled'}"
    )
    if follower._pin_collision_model:
        print(f"   - Collision pairs: {len(follower._pin_collision_model.collisionPairs)}")
    print(f"   - Floor geom: {'added' if follower._pin_floor_geom_id is not None else 'NOT ADDED'}")
    print(f"   - Collision margin: {follower._pin_collision_margin}m")
    print()

    # Define test poses (shoulder_pan, shoulder_lift, elbow_flex, wrist_flex, wrist_roll, gripper)
    test_cases = [
        {
            "name": "Safe Home Pose (all zeros)",
            "pose": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "expected": "safe",
            "description": "Robot at home position - should be safe",
        },
        {
            "name": "Extended Arm",
            "pose": [0.0, 0.0, 0.0, 0.0, 0.0, 0.5],
            "expected": "safe",
            "description": "Arm extended forward - should be safe",
        },
        {
            "name": "User-Reported Collision Pose",
            "pose": [0.250, 0.090, 1.600, 1.410, -1.153, 0.051],
            "expected": "collision",
            "description": "Pose that user reported causes collision",
        },
        {
            "name": "Extreme Elbow Flex",
            "pose": [0.0, 0.5, 1.5, 0.0, 0.0, 0.0],
            "expected": "collision",
            "description": "Elbow bent back - likely self-collision",
        },
        {
            "name": "Floor Collision Test",
            "pose": [0.0, 1.5, -1.5, -1.5, 0.0, 0.0],
            "expected": "safe_or_collision",
            "description": "End-effector pointing down - floor check depends on EE frame and threshold",
        },
        {
            "name": "Folded Configuration",
            "pose": [0.0, 1.0, 1.5, 1.5, 0.0, 0.5],
            "expected": "collision",
            "description": "Robot folded in on itself - likely self-collision",
        },
        {
            "name": "Shoulder Pan Extreme",
            "pose": [1.8, 0.0, 0.5, 0.0, 0.0, 0.0],
            "expected": "safe_or_collision",
            "description": "Near joint limit - may or may not collide",
        },
    ]

    # Run tests
    print("Running collision detection tests:")
    print("-" * 80)

    results = {"passed": 0, "failed": 0, "warnings": 0}

    for i, test in enumerate(test_cases, 1):
        print(f"\nTest {i}/{len(test_cases)}: {test['name']}")
        print(f"Description: {test['description']}")
        print(f"Pose (rad): {test['pose']}")

        # Test with both methods
        is_safe_dist, message_dist = follower._validate_pose_pinocchio(
            test["pose"], use_distance_check=True
        )
        is_safe_bool, message_bool = follower._validate_pose_pinocchio(
            test["pose"], use_distance_check=False
        )

        # Show comparison
        print(f"  computeDistances (margin): {'SAFE' if is_safe_dist else 'COLLISION'}")
        if not is_safe_dist:
            print(f"    → {message_dist}")
        print(f"  computeCollisions (bool):  {'SAFE' if is_safe_bool else 'COLLISION'}")
        if not is_safe_bool:
            print(f"    → {message_bool}")

        # Flag discrepancies
        if is_safe_dist != is_safe_bool:
            print(f"  ⚠️  DISCREPANCY: distance check {'allows' if is_safe_dist else 'blocks'}, collision check {'allows' if is_safe_bool else 'blocks'}")

        # Use distance check result for pass/fail (default behavior)
        is_safe = is_safe_dist
        message = message_dist

        # Determine test result
        if test["expected"] == "safe":
            if is_safe:
                print("✅ PASS - Pose is safe (as expected)")
                results["passed"] += 1
            else:
                print(f"❌ FAIL - Expected safe but got collision: {message}")
                results["failed"] += 1
        elif test["expected"] == "collision":
            if not is_safe:
                print(f"✅ PASS - Collision detected (as expected): {message}")
                results["passed"] += 1
            else:
                print("❌ FAIL - Expected collision but pose is reported as SAFE - collision detection NOT working!")
                results["failed"] += 1
        else:  # safe_or_collision
            if is_safe:
                print("ℹ️  INFO - Pose is safe")
            else:
                print(f"ℹ️  INFO - Collision detected: {message}")
            results["passed"] += 1

    # Summary
    print()
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)
    print(f"Total tests:  {len(test_cases)}")
    print(f"✅ Passed:    {results['passed']}")
    print(f"❌ Failed:    {results['failed']}")
    print(f"⚠️  Warnings:  {results['warnings']}")
    print()

    if results["failed"] == 0:
        print("🎉 All tests passed!")
        return True
    else:
        print("💥 Tests failed - collision detection is NOT working correctly")
        print("   This means Pinocchio collision geometries did not load properly.")
        print("   Check that:")
        print("   1. The assets/ folder with STL files is next to the URDF")
        print("   2. Collision geometries loaded successfully (should show 'enabled' above)")
        print("   3. The URDF has <collision> tags with mesh references")
        return False


def run_random_poses(urdf_path: str, num_poses: int = 1000, seed: int = 42):
    """Test Pinocchio collision detection with randomly generated poses.

    Uses Pinocchio's joint limits to sample poses within valid ranges.

    IMPORTANT: This test measures what Pinocchio REPORTS as collisions.
    It does NOT have ground truth - we cannot verify if reported collisions
    are true positives or if safe poses are true negatives without:
    - Visual verification in a simulator
    - Cross-validation with another physics engine (PyBullet, MuJoCo)
    - Physical testing on the real robot

    What this test DOES validate:
    1. Pinocchio model loads correctly with collision geometries
    2. Collision checking runs without errors
    3. Some collisions are detected (sanity check - random poses should have some)
    4. Which link pairs most frequently collide (useful for understanding robot geometry)

    Args:
        urdf_path: Path to the URDF file
        num_poses: Number of random poses to test (default 1000)
        seed: Random seed for reproducibility

    Returns:
        True if test completed successfully, False otherwise
    """
    print()
    print("=" * 80)
    print(f"Random Pose Collision Analysis ({num_poses} poses)")
    print("=" * 80)
    print()
    print("NOTE: This test reports what Pinocchio detects as collisions.")
    print("      It does NOT verify accuracy against ground truth.")
    print("      Use a visualizer (MuJoCo/RViz) to verify specific poses.")
    print()

    # Set random seed for reproducibility
    np.random.seed(seed)

    # Create follower instance
    config = FollowerConfig(
        port="/dev/null",
        max_relative_target=10.0,
    )
    follower = SO101Follower(config=config)

    # Initialize Pinocchio
    print(f"Loading URDF from: {urdf_path}")
    success = follower.init_pin(urdf_path)

    if not success:
        print("❌ Failed to load Pinocchio model")
        return False

    print("✅ Pinocchio model loaded successfully")

    # Get joint limits from Pinocchio model
    model = follower._pin_model
    collision_model = follower._pin_collision_model
    lower_limits = model.lowerPositionLimit
    upper_limits = model.upperPositionLimit

    print()
    print("Model Information:")
    print(f"   - Number of joints (nq): {model.nq}")
    print("   - Joint limits (rad):")
    for i in range(model.nq):
        joint_name = model.names[i + 1] if i + 1 < len(model.names) else f"joint_{i}"
        print(f"      {joint_name}: [{lower_limits[i]:.3f}, {upper_limits[i]:.3f}]")

    # Print collision geometry details
    print()
    print("Collision Geometry Information:")
    if not collision_model:
        print("   ⚠️  NO collision geometries loaded!")
        print("   Only floor collision will be checked.")
    else:
        print(f"   - Number of geometry objects: {len(collision_model.geometryObjects)}")
        print(f"   - Number of collision pairs: {len(collision_model.collisionPairs)}")
        print()
        print("   Geometry objects (links with collision meshes):")
        for i, geom in enumerate(collision_model.geometryObjects):
            parent_joint = geom.parentJoint
            parent_name = model.names[parent_joint] if parent_joint < len(model.names) else "world"
            print(f"      [{i}] {geom.name} (parent: {parent_name})")

        print()
        print("   Collision pairs being checked:")
        pair_descriptions = []
        for pair in collision_model.collisionPairs:
            geom1 = collision_model.geometryObjects[pair.first]
            geom2 = collision_model.geometryObjects[pair.second]
            pair_descriptions.append(f"{geom1.name} ↔ {geom2.name}")
        # Show first 10 pairs
        for desc in pair_descriptions[:10]:
            print(f"      - {desc}")
        if len(pair_descriptions) > 10:
            print(f"      ... and {len(pair_descriptions) - 10} more pairs")

    print()
    if follower._pin_floor_geom_id is not None:
        print(f"Floor collision: enabled (geom_id={follower._pin_floor_geom_id})")
    else:
        print("Floor collision: disabled")
    print(f"Collision margin: {follower._pin_collision_margin}m")
    print()

    # Generate random poses within joint limits
    print("-" * 80)
    print(f"Testing {num_poses} random poses within joint limits...")
    print("-" * 80)

    # Stats tracking
    stats = {
        "total": num_poses,
        "safe": 0,
        "collision": 0,
        "self_collision": 0,
        "floor_collision": 0,
        "errors": 0,
    }

    # Track which collision pairs are triggered most often
    collision_pair_counts: dict[str, int] = {}

    start_time = time.time()

    # Progress tracking
    progress_interval = max(1, num_poses // 10)  # Report every 10%

    for i in range(num_poses):
        # Generate random pose within joint limits
        pose = np.random.uniform(lower_limits, upper_limits).tolist()

        try:
            is_safe, message = follower._validate_pose_pinocchio(pose)

            if is_safe:
                stats["safe"] += 1
            else:
                stats["collision"] += 1
                if "Self-collision" in message:
                    stats["self_collision"] += 1
                    # Extract collision pair from message (format: "Self-collision detected: X ↔ Y")
                    if "↔" in message:
                        pair = message.split(": ", 1)[-1].strip()
                        collision_pair_counts[pair] = collision_pair_counts.get(pair, 0) + 1
                if "floor" in message.lower():
                    stats["floor_collision"] += 1
                    collision_pair_counts["floor"] = collision_pair_counts.get("floor", 0) + 1
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                print(f"   Error at pose {i}: {e}")

        # Progress report
        if (i + 1) % progress_interval == 0:
            pct = (i + 1) / num_poses * 100
            print(
                f"   Progress: {i + 1}/{num_poses} ({pct:.0f}%) - "
                f"Safe: {stats['safe']}, Collisions: {stats['collision']}"
            )

    elapsed_time = time.time() - start_time

    # Print results
    print()
    print("=" * 80)
    print("Results Summary")
    print("=" * 80)
    print()
    print(f"Total poses tested: {stats['total']}")
    print(f"Time elapsed: {elapsed_time:.2f}s ({num_poses / elapsed_time:.1f} poses/sec)")
    print()

    safe_pct = stats["safe"] / stats["total"] * 100
    collision_pct = stats["collision"] / stats["total"] * 100
    print("Pinocchio Reports:")
    print(f"   ✅ Safe poses:        {stats['safe']:5d} ({safe_pct:5.1f}%)")
    print(f"   ⚠️  Collision poses:   {stats['collision']:5d} ({collision_pct:5.1f}%)")
    print(f"      - Self-collisions: {stats['self_collision']:5d}")
    print(f"      - Floor collisions: {stats['floor_collision']:5d}")
    if stats["errors"] > 0:
        print(f"   ❌ Errors:            {stats['errors']:5d}")

    # Show collision pair frequency
    if collision_pair_counts:
        print()
        print("Most Frequent Collision Pairs:")
        sorted_pairs = sorted(collision_pair_counts.items(), key=lambda x: -x[1])
        for pair, count in sorted_pairs[:10]:
            pct = count / stats["collision"] * 100 if stats["collision"] > 0 else 0
            print(f"   {pair}: {count} times ({pct:.1f}% of collisions)")

    # Interpretation
    print()
    print("=" * 80)
    print("Interpretation")
    print("=" * 80)
    print()

    if stats["collision"] == 0 and collision_model:
        print("⚠️  WARNING: No collisions detected!")
        print("   This is suspicious - random joint angles should produce some collisions.")
        print("   Possible issues:")
        print("   - Collision meshes may be too small or missing")
        print("   - Collision pairs may not be set up correctly")
        print()
        return False
    elif stats["collision"] == 0 and not collision_model:
        print("ℹ️  No self-collisions detected (no collision model loaded)")
        print("   Only floor collision is being checked.")
        print()
    else:
        print(f"✅ Pinocchio detected collisions in {collision_pct:.1f}% of random poses.")
        print()
        print("What this means:")
        print(f"   - {collision_pct:.1f}% of the robot's joint space is marked as collision")
        print(f"   - {safe_pct:.1f}% of random configurations are considered safe")
        print()

        if collision_pct < 5:
            print("   ⚠️  Low collision rate (<5%) - robot has large collision-free workspace")
            print("      OR collision meshes may be undersized")
        elif collision_pct > 50:
            print("   ⚠️  High collision rate (>50%) - robot has limited safe workspace")
            print("      OR collision meshes may be oversized / too conservative")
        else:
            print("   ℹ️  Collision rate seems reasonable for a 6-DOF arm")

    print()
    print("To verify accuracy, visualize sample collision poses in MuJoCo/RViz")
    print("and confirm the reported links actually intersect.")

    # Print some example collision poses for manual verification
    if stats["collision"] > 0:
        print()
        print("-" * 80)
        print("Sample Collision Poses (for manual verification in visualizer):")
        print("-" * 80)
        np.random.seed(seed)  # Reset seed for reproducibility
        sample_count = 0
        for i in range(num_poses):
            pose = np.random.uniform(lower_limits, upper_limits).tolist()
            is_safe, message = follower._validate_pose_pinocchio(pose)
            if not is_safe:
                print(f"\n   Pose #{i}:")
                print(f"      Joints (rad): {[round(p, 4) for p in pose]}")
                print(f"      Collision: {message}")
                sample_count += 1
                if sample_count >= 5:
                    break
        print()

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Test Pinocchio collision detection for SO101 robot"
    )
    parser.add_argument(
        "--urdf",
        type=str,
        help="Path to SO101 URDF file (auto-detected if not provided)",
    )
    parser.add_argument(
        "--random",
        action="store_true",
        help="Run random pose test (1000 poses)",
    )
    parser.add_argument(
        "--num-poses",
        type=int,
        default=1000,
        help="Number of random poses to test (default: 1000)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Run both predefined and random pose tests",
    )
    parser.add_argument(
        "--sit-down",
        action="store_true",
        help="Run sit-down position movement test",
    )

    args = parser.parse_args()

    # Get URDF path
    urdf_path = args.urdf
    if not urdf_path:
        urdf_path = str(get_so101_urdf_path())
        if not urdf_path:
            print("❌ Error: Could not find SO101 URDF file")
            print("   Please specify --urdf path or ensure URDF is in expected location")
            return 1

    # Determine which tests to run
    run_predefined = (not args.random and not args.sit_down) or args.all
    run_random_test = args.random or args.all
    run_sit_down = args.sit_down or args.all

    success = True

    # Run tests
    try:
        if run_predefined:
            predefined_success = run_collision_detection(urdf_path)
            success = success and predefined_success

        if run_random_test:
            random_success = run_random_poses(
                urdf_path, num_poses=args.num_poses, seed=args.seed
            )
            success = success and random_success
        
        if run_sit_down:
            sit_down_success = test_sit_down_position_movements()
            success = success and sit_down_success

        return 0 if success else 1
    except Exception as e:
        print(f"❌ Error during testing: {e}")
        import traceback

        traceback.print_exc()
        return 1


def test_collision_callback_is_triggered():
    """Test that collision callback is triggered when collision is detected."""
    urdf_path = get_so101_urdf_path()
    if urdf_path is None:
        print("⚠️  No URDF found, skipping callback test")
        return

    config = FollowerConfig(
        port="/dev/null",
        max_relative_target=10.0,
    )
    follower = SO101Follower(config=config)
    success = follower.init_pin(urdf_path, floor_z=0.0, collision_margin=0.01)
    if not success:
        print("⚠️  Failed to load Pinocchio model, skipping callback test")
        return

    # Track callback invocations
    callback_calls = []

    def on_collision(collision_type, message, joint_angles):
        callback_calls.append({
            "collision_type": collision_type,
            "message": message,
            "joint_angles": joint_angles,
        })

    follower.set_collision_callback(on_collision)

    # Create a fake calibration for testing
    from motors import MotorCalibration

    follower.calibration = {
        name: MotorCalibration(
            id=motor.id,
            homing_offset=0.0,
            drive_mode=0,
            range_min=-100.0 if name != "gripper" else 0.0,
            range_max=100.0,
        )
        for name, motor in follower.motors.items()
    }

    # Test with a safe pose - callback should NOT be triggered
    safe_action = {
        "shoulder_pan.pos": 0.0,
        "shoulder_lift.pos": 0.0,
        "elbow_flex.pos": 0.0,
        "wrist_flex.pos": 0.0,
        "wrist_roll.pos": 0.0,
        "gripper.pos": 0.0,
    }
    is_safe, msg = follower.validate_action_kinematics(safe_action)
    if is_safe:
        assert len(callback_calls) == 0, "Callback should NOT be triggered for safe pose"
        print("✅ Safe pose: callback not triggered (correct)")
    else:
        print(f"⚠️  Safe pose rejected: {msg}")

    # Test with an unsafe pose (arm reaching down) - callback SHOULD be triggered
    # This pose should cause floor collision
    unsafe_action = {
        "shoulder_pan.pos": 0.0,
        "shoulder_lift.pos": 50.0,  # Lift up significantly
        "elbow_flex.pos": -80.0,  # Bend elbow down
        "wrist_flex.pos": 0.0,
        "wrist_roll.pos": 0.0,
        "gripper.pos": 0.0,
    }
    is_safe, msg = follower.validate_action_kinematics(unsafe_action)
    if not is_safe:
        assert len(callback_calls) == 1, "Callback should be triggered for unsafe pose"
        assert callback_calls[0]["collision_type"] in ["floor", "self"]
        assert callback_calls[0]["joint_angles"] is not None
        print(f"✅ Unsafe pose: callback triggered with type={callback_calls[0]['collision_type']}")
    else:
        print("⚠️  Unsafe pose was not detected as collision (may be within limits)")

    # Test that callback can be disabled
    follower.set_collision_callback(None)
    callback_calls.clear()
    follower.validate_action_kinematics(unsafe_action)
    assert len(callback_calls) == 0, "Callback should NOT be triggered when disabled"
    print("✅ Callback disabled: no callback triggered")

    print()
    print("✅ Collision callback tests passed!")


def test_sit_down_position_movements():
    """Test movements from sit-down position for each joint.
    
    Sit-down position: shoulder_pan=0, shoulder_lift=-95, elbow_flex=+95,
                       wrist_flex=0, wrist_roll=0, gripper=small open % (see follower.GRIPPER_HOME_OPEN_NORM)
    
    Test that each joint can move from sit-down:
    - Joint 0 (shoulder_pan): Should move freely in both directions
    - Joint 1 (shoulder_lift): Can only increase (unfold upward from -95)
    - Joint 2 (elbow_flex): Can only decrease (unfold from +95)
    - Joint 3 (wrist_flex): Should move freely within range
    - Joint 4 (wrist_roll): Should move freely
    - Joint 5 (gripper): Should move freely
    """
    urdf_path = get_so101_urdf_path()
    if urdf_path is None:
        print("⚠️  No URDF found, skipping sit-down movement test")
        return False
    
    print()
    print("=" * 80)
    print("Sit-Down Position Movement Test")
    print("=" * 80)
    print()
    
    config = FollowerConfig(
        port="/dev/null",
        max_relative_target=10.0,
    )
    follower = SO101Follower(config=config)
    success = follower.init_pin(str(urdf_path), floor_z=0.0, collision_margin=0.01)
    
    if not success:
        print("❌ Failed to load Pinocchio model")
        return False
    
    print("✅ Pinocchio model loaded")
    print()
    
    # Import SIT_DOWN_POSITION from follower module
    from so101.follower import GRIPPER_HOME_OPEN_NORM, SIT_DOWN_POSITION
    
    # Create fake calibration to enable validation
    from motors import MotorCalibration
    follower.calibration = {
        name: MotorCalibration(
            id=motor.id,
            homing_offset=0.0,
            drive_mode=0,
            range_min=-100.0 if name != "gripper" else 0.0,
            range_max=100.0,
        )
        for name, motor in follower.motors.items()
    }
    
    # Define sit-down position variants to test (gripper matches SIT_DOWN home open %)
    _gh = GRIPPER_HOME_OPEN_NORM
    sit_down_variants = [
        {"shoulder_pan": 0.0, "shoulder_lift": -95.0, "elbow_flex": 95.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
        {"shoulder_pan": 0.0, "shoulder_lift": -97.0, "elbow_flex": 95.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
        {"shoulder_pan": 0.0, "shoulder_lift": -95.0, "elbow_flex": 95.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
        {"shoulder_pan": 0.0, "shoulder_lift": -95.0, "elbow_flex": 97.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
        {"shoulder_pan": 0.0, "shoulder_lift": -95.0, "elbow_flex": 99.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
        {"shoulder_pan": 0.0, "shoulder_lift": -99.0, "elbow_flex": 99.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
        {"shoulder_pan": 0.0, "shoulder_lift": -100.0, "elbow_flex": 100.0, "wrist_flex": 0.0, "wrist_roll": 0.0, "gripper": _gh},
    ]
    
    # Define movement test cases
    joint_names = ["shoulder_pan", "shoulder_lift", "elbow_flex", "wrist_flex", "wrist_roll", "gripper"]
    
    test_cases = [
        {
            "joint": "shoulder_pan",
            "movements": [
                ("left -30", -30.0),
                ("right +30", +30.0),
            ],
            "description": "Should move freely in both directions",
        },
        {
            "joint": "shoulder_lift",
            "movements": [
                ("increase to -70", -70.0),
                ("increase to -50", -50.0),
                ("increase to 0", 0.0),
            ],
            "description": "Can only increase (unfold upward from -95)",
        },
        {
            "joint": "elbow_flex",
            "movements": [
                ("to +100", +100.0),
                ("to +98", +98.0),
                ("to +95", +95.0),
                ("to +92", +92.0),
                ("to +90", +90.0),
            ],
            "description": "Test range 90-100 from sit-down (+95)",
        },
        {
            "joint": "wrist_flex",
            "movements": [
                ("to +100", +100.0),
                ("to +98", +98.0),
                ("to +95", +95.0),
                ("to +92", +92.0),
                ("to +90", +90.0),
            ],
            "description": "Test range 90-100 from sit-down (0)",
        },
        {
            "joint": "wrist_roll",
            "movements": [
                ("rotate -30", -30.0),
                ("rotate +30", +30.0),
            ],
            "description": "Should move freely",
        },
        {
            "joint": "gripper",
            "movements": [
                ("open to 50", 50.0),
                ("close to -50", -50.0),
            ],
            "description": "Should move freely",
        },
    ]
    
    results = {"passed": 0, "failed": 0}
    
    # Test each sit-down variant
    for variant_idx, sit_down_pose in enumerate(sit_down_variants, 1):
        print(f"\n{'='*80}")
        print(f"Testing variant {variant_idx}/{len(sit_down_variants)}")
        print(f"Starting pose: {sit_down_pose}")
        print("-" * 80)
        
        for test in test_cases:
            joint = test["joint"]
            print(f"\n{joint.upper()} - {test['description']}")
            
            for movement_desc, target_value in test["movements"]:
                # Create action with this sit-down variant, then modify the target joint
                action = {f"{k}.pos": v for k, v in sit_down_pose.items()}
                action[f"{joint}.pos"] = target_value
                
                # Validate the action
                is_safe, message = follower.validate_action_kinematics(action)
                
                if is_safe:
                    print(f"  ✅ {movement_desc}: SAFE")
                    results["passed"] += 1
                else:
                    print(f"  ❌ {movement_desc}: BLOCKED - {message}")
                    results["failed"] += 1
    
    # Summary
    print()
    print("=" * 80)
    print("Test Summary")
    print("=" * 80)
    total = results["passed"] + results["failed"]
    print(f"Total movements tested: {total}")
    print(f"✅ Passed (safe):       {results['passed']}")
    print(f"❌ Failed (blocked):    {results['failed']}")
    print()
    
    if results["failed"] == 0:
        print("🎉 All movements from sit-down position are safe!")
        return True
    else:
        print("⚠️  Some movements from sit-down position are blocked by collision detection")
        print("   Consider:")
        print("   1. Adjusting sit-down position to be less compact")
        print("   2. Reducing collision margin")
        print("   3. Adding more disabled collision pairs")
        return False


if __name__ == "__main__":
    sys.exit(main())
