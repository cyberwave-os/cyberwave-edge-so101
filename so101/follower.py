"""SO101 Follower class for teleoperation."""

import logging
import time
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union

import numpy as np
import pinocchio as pin
from serial.serialutil import SerialException

try:
    import hppfcl
except ImportError:
    hppfcl = None

from motors import (
    FeetechMotorsBus,
    MotorCalibration,
)
from motors.tables import MODE_POSITION
from so101.robot import SO101Robot
from utils.config import FollowerConfig
from utils.errors import DeviceNotConnectedError
from utils.utils import ensure_safe_goal_position, load_calibration, normalized_to_radians

logger = logging.getLogger(__name__)

# Normalized % of calibrated range (-100..100 for arm joints; 0..100 for gripper).
# Gripper at 0 is fully closed; use a small positive value (~5% of span) for home/zero/sit-down
# so the jaw is slightly open and less likely to stall or overheat when holding closed.
GRIPPER_HOME_OPEN_NORM = 5.0

SIT_DOWN_POSITION = {
    "shoulder_pan": 0.0,
    "shoulder_lift": -95.0,
    "elbow_flex": +95.0,
    "wrist_flex": 0.0,
    "wrist_roll": 0.0,
    "gripper": GRIPPER_HOME_OPEN_NORM,
}

# Max distance in normalized units from target per joint to consider sit-down complete.
_SIT_DOWN_POSITION_TOLERANCE = 5.0


class SO101Follower(SO101Robot):
    """SO101 Follower device for teleoperation."""

    def __init__(
        self,
        config: Optional[FollowerConfig] = None,
        port: Optional[str] = None,
        use_degrees: bool = False,
        id: str = "follower1",
        calibration_dir: Optional[Path] = None,
        max_relative_target: Optional[float] = None,
        cameras: Optional[List] = None,
    ):
        """
        Initialize SO101 Follower.

        Args:
            config: FollowerConfig object (if provided, other parameters are ignored)
            port: Serial port path (e.g., "/dev/tty.usbmodem456")
            use_degrees: Whether to use degrees for position values (default: False, uses RANGE_M100_100)
            id: Device identifier for calibration file management
            calibration_dir: Directory for calibration files (default: ~/.cyberwave/so101_lib/calibrations)
            max_relative_target: Max change per command in normalized units (None = disabled; motor limits apply)
            cameras: Optional list of camera configurations (not implemented yet)
        """
        super().__init__()
        if config is not None:
            self.config = config
        else:
            if port is None:
                raise ValueError("Either 'config' or 'port' must be provided")
            self.config = FollowerConfig(
                port=port,
                use_degrees=use_degrees,
                id=id,
                calibration_dir=calibration_dir,
                max_relative_target=max_relative_target,
                cameras=cameras,
            )

        self.bus = FeetechMotorsBus(port=self.config.port, motors=self.motors, calibration=None)

        self.calibration: Optional[Dict[str, MotorCalibration]] = None
        calibration_path = self.config.calibration_dir / f"{self.config.id}.json"
        if calibration_path.exists():
            logger.info(f"Loading calibration from {calibration_path}")
            calib_data = load_calibration(calibration_path)
            self.calibration = {}
            for name, data in calib_data.items():
                self.calibration[name] = MotorCalibration(**data)
            self.bus.calibration = self.calibration
        else:
            logger.info("No calibration file found - follower will work without calibration")

        self._current_positions: Dict[str, float] = {}

        self._pin_model = None
        self._pin_data = None
        self._pin_collision_model = None
        self._pin_collision_data = None
        self._pin_floor_geom_id: Optional[int] = None
        self._pin_collision_margin: float = 0.01  # 1cm safety margin for distance-based check

        # Collision callback: called with (collision_type, message, joint_angles_dict)
        self._on_collision_callback: Optional[
            Callable[[str, str, Optional[Dict[str, float]]], None]
        ] = None

    def __str__(self) -> str:
        """String representation of the follower."""
        return f"SO101Follower(id={self.config.id}, port={self.config.port})"

    def set_collision_callback(
        self,
        callback: Optional[Callable[[str, str, Optional[Dict[str, float]]], None]],
    ) -> None:
        """
        Set a callback to be invoked when collision is detected.

        The callback receives:
            - collision_type: "floor" or "self"
            - message: Detailed collision message
            - joint_angles: Optional dict of joint angles at collision time

        Args:
            callback: Callback function or None to disable
        """
        self._on_collision_callback = callback

    @property
    def connected(self) -> bool:
        """Check if follower is connected."""
        return self._connected and self.bus is not None and self.bus.connected

    @property
    def is_calibrated(self) -> bool:
        """Check if follower is calibrated."""
        return self.calibration is not None

    def move_to_sit_down_position(self, timeout: Optional[float] = None) -> bool:
        """
        Move follower to compact sit-down pose and wait until close to target or timeout.

        SIT_DOWN_POSITION values are normalized percentages of the calibrated range.

        Returns:
            True if the commanded move succeeded and joints reached within tolerance.
            False if the move was rejected, timed out, or an error occurred (best-effort).
        """
        if not self.connected:
            return False

        timeout_sec = self.config.sit_down_timeout if timeout is None else timeout
        sit_down_action = {f"{k}.pos": v for k, v in SIT_DOWN_POSITION.items()}

        try:
            # Skip pre-movement load check - arm may be under gravity stress in stretched pose.
            # Skip collision check - sit-down is a rescue motion, disabled pairs may not cover all cases.
            # Moving TO sit-down relieves stress. Use infinite threshold for stepping safety checks.
            sent, rejection = self.process_and_send_action(
                sit_down_action,
                load_threshold=float("inf"),
                check_load_before=False,
                check_collision=False,
            )
        except (DeviceNotConnectedError, SerialException) as e:
            logger.warning("Sit down: failed to send action: %s", e)
            return False
        except Exception as e:
            logger.warning("Sit down: unexpected error sending action: %s", e)
            return False

        if not sent:
            logger.warning(
                "Sit down: move rejected: %s",
                rejection or "unknown",
            )
            return False

        deadline = time.monotonic() + timeout_sec
        poll_interval = 0.1

        while time.monotonic() < deadline:
            try:
                obs = self.get_observation()
            except (DeviceNotConnectedError, SerialException) as e:
                logger.debug("Sit down: position poll failed: %s", e)
                time.sleep(poll_interval)
                continue

            if not obs:
                time.sleep(poll_interval)
                continue

            close_enough = True
            for name, target in SIT_DOWN_POSITION.items():
                key = f"{name}.pos"
                cur = obs.get(key)
                if cur is None:
                    close_enough = False
                    break
                if abs(cur - target) > _SIT_DOWN_POSITION_TOLERANCE:
                    close_enough = False
                    break

            if close_enough:
                return True

            time.sleep(poll_interval)

        logger.warning(
            "Sit down: timed out after %.1fs waiting for pose (tolerance=%.1f)",
            timeout_sec,
            _SIT_DOWN_POSITION_TOLERANCE,
        )
        return False

    def disconnect(self, torque: bool = False, skip_sit_down: bool = False) -> None:
        """
        Disconnect from the follower, optionally moving to sit-down pose first.

        Args:
            torque: If True, keep torque enabled on the bus until the port closes (see base).
            skip_sit_down: If True, skip sit-down motion (e.g. emergency shutdown).
        """
        if not self.connected:
            return

        if not skip_sit_down and self._torque_enabled and self.bus is not None:
            logger.info("Moving to sit down position before disconnect...")
            try:
                moved = self.move_to_sit_down_position()
            except Exception as e:
                logger.warning("Sit down before disconnect failed: %s", e)
            else:
                if moved:
                    logger.info("Reached sit down position")
                else:
                    logger.warning("Could not reach sit down position, disconnecting anyway")

        super().disconnect(torque=torque)

    def connect(
        self,
        calibrate: bool = False,
        on_state_change: Optional[Callable[[str], None]] = None,
        on_joint_progress: Optional[
            Callable[[Dict[str, float], Dict[str, float], Dict[str, float]], None]
        ] = None,
    ) -> None:
        """
        Connect to the follower device.

        We assume that at connection time, arm is in a rest position,
        and torque can be safely disabled to run calibration.

        Forces calibration if not already calibrated.

        Args:
            calibrate: Whether to calibrate motors on connection if not already calibrated.
                      If False and not calibrated, will still force calibration
            on_state_change: Optional callback(state) when calibration step changes.
                Passed to calibrate() when running calibration. Used by frontend to show
                "Go ahead" button at zero_pose_waiting / joint_calibration_waiting.
            on_joint_progress: Optional callback for joint progress during calibration.
        """
        if self.connected:
            logger.warning("Follower is already connected")
            return

        self.bus.connect()
        self._connected = True

        # Configure motors (set operating mode, PID coefficients)
        self.configure()

        # Force calibration if not already calibrated
        if not self.is_calibrated:
            logger.info(f"{self} is not calibrated. Running calibration...")
            self.calibrate(
                on_state_change=on_state_change,
                on_joint_progress=on_joint_progress,
            )
        elif calibrate:
            logger.info(f"{self} is already calibrated, but re-calibrating as requested...")
            self.calibrate(
                on_state_change=on_state_change,
                on_joint_progress=on_joint_progress,
            )
        elif self.calibration:
            logger.info("Restoring calibration to follower motors")
            self.bus.write_calibration(self.calibration)

        # Enable torque for all motors (follower needs torque to move)
        self.bus.enable_torque()
        self._torque_enabled = True
        self._current_positions = self.get_observation()

        logger.info(f"{self} connected.")

    def configure(self) -> None:
        """
        Configure motors: set operating mode and PID coefficients.

        This is called after connection/calibration to set up motor parameters.
        """
        if not self.connected:
            raise DeviceNotConnectedError("Follower is not connected")

        was_torque_enabled = self._torque_enabled
        if was_torque_enabled:
            self.bus.disable_torque()
            self._torque_enabled = False

        try:
            for motor in self.motors:
                self.bus.write("Operating_Mode", motor, MODE_POSITION)

            for motor in self.motors:
                self.bus.write("P_Coefficient", motor, 16)
                self.bus.write("I_Coefficient", motor, 0)
                self.bus.write("D_Coefficient", motor, 32)

                self.bus.write("Vmax", motor, 65)
                self.bus.write("Vmin", motor, 1)
                self.bus.write("Velocity_Unit_Factor", motor, 50)
                self.bus.write("Amax", motor, 50)
                self.bus.write("KAcc", motor, 1)

                if motor == "gripper":
                    self.bus.write("Max_Torque_Limit", motor, 500)
                    self.bus.write("Protection_Current", motor, 250)
                    self.bus.write("Overload_Torque", motor, 25)

        finally:
            if was_torque_enabled:
                self.bus.enable_torque()
                self._torque_enabled = True

    def init_pin(
        self,
        urdf_path: Union[str, Path],
        floor_z: float = 0.04,
        collision_margin: float = 0.01,
    ) -> bool:
        """
        Load Pinocchio model from URDF for kinematics safety checks.

        Call once at remoteoperate startup. The model is reused for fast validation.
        Loads collision geometries (STL meshes) for accurate self-collision detection.
        Adds a floor plane as a collision geometry for floor collision detection.

        Args:
            urdf_path: Path to SO101 URDF file
            floor_z: Z offset for floor plane below base (default: -0.03m)
            collision_margin: Safety margin for distance-based collision check (default: 0.01m)

        Returns:
            True if Pinocchio was loaded successfully, False otherwise (e.g. pin not installed)
        """

        path = Path(urdf_path)
        if not path.is_file():
            logger.warning("URDF not found at %s. Kinematics safety disabled.", path)
            return False

        self._pin_collision_margin = collision_margin

        try:
            self._pin_model = pin.buildModelFromUrdf(str(path))
            self._pin_data = self._pin_model.createData()

            try:
                mesh_dir = path.parent
                try:
                    package_dirs = [str(mesh_dir)]
                    self._pin_collision_model = pin.buildGeomFromUrdf(
                        self._pin_model,
                        str(path),
                        pin.GeometryType.COLLISION,
                        package_dirs=package_dirs,
                    )
                except TypeError:
                    logger.info("Trying buildGeomFromUrdf without package_dirs (older Pinocchio)")
                    self._pin_collision_model = pin.buildGeomFromUrdf(
                        self._pin_model,
                        str(path),
                        pin.GeometryType.COLLISION,
                    )

                self._pin_collision_data = self._pin_collision_model.createData()

                # Add floor geometry for collision checking
                self._pin_floor_geom_id = self._add_floor_geometry(floor_z)

                num_arm_geoms = len(self._pin_collision_model.geometryObjects)
                if self._pin_floor_geom_id is not None:
                    num_arm_geoms -= 1  # Don't count floor in arm geometry count

                # Build a set of valid collision pairs:
                # 1. Self-collision: only between links > 2 joints apart (adjacent links are naturally close)
                # 2. Floor collision: should be managed as self-collision as we added the floor geometry to the collision model

                # Explicitly disabled collision pairs (adjacent or never-touching links)
                # Based on URDF geometry names (e.g., "base_link_0", "shoulder_link_0", etc.)
                disabled_pairs_by_name = {
                    ("base_link", "shoulder_link"),  # Adjacent
                    ("shoulder_link", "upper_arm_link"),  # Adjacent
                    ("upper_arm_link", "forearm_link"),  # Adjacent
                    ("forearm_link", "wrist_link"),  # Adjacent
                    ("base_link", "upper_arm_link"),  # Adjacent / Never Touching
                    ("shoulder_link", "wrist_link"),  # Gets close in sit-down position
                    ("shoulder_link", "gripper_link"),  # Gets close in sit-down position
                }

                # Map geometry names to indices for filtering
                geom_name_to_idx = {}
                for i in range(num_arm_geoms):
                    geom = self._pin_collision_model.geometryObjects[i]
                    # Extract base name (e.g., "base_link_0" -> "base_link")
                    base_name = geom.name.rsplit("_", 1)[0] if "_" in geom.name else geom.name
                    if base_name not in geom_name_to_idx:
                        geom_name_to_idx[base_name] = []
                    geom_name_to_idx[base_name].append(i)

                # Build set of disabled geometry index pairs
                disabled_geom_pairs = set()
                for name_a, name_b in disabled_pairs_by_name:
                    indices_a = geom_name_to_idx.get(name_a, [])
                    indices_b = geom_name_to_idx.get(name_b, [])
                    for idx_a in indices_a:
                        for idx_b in indices_b:
                            disabled_geom_pairs.add((min(idx_a, idx_b), max(idx_a, idx_b)))

                valid_pairs = set()

                # Self-collision pairs (non-adjacent links only, excluding disabled pairs)
                for i in range(num_arm_geoms):
                    for j in range(i + 1, num_arm_geoms):
                        # Skip explicitly disabled pairs
                        if (i, j) in disabled_geom_pairs:
                            continue

                        geom_i = self._pin_collision_model.geometryObjects[i]
                        geom_j = self._pin_collision_model.geometryObjects[j]
                        parent_i = geom_i.parentJoint
                        parent_j = geom_j.parentJoint
                        # Only include links that are > 2 joints apart in kinematic chain
                        if abs(parent_i - parent_j) > 2:
                            valid_pairs.add((min(i, j), max(i, j)))

                # Floor collision pairs (only distal links that can actually reach the floor)
                # Exclude base and shoulder links - they are structurally attached to the base
                # and physically cannot collide with the floor (shoulder only rotates sideways)
                if self._pin_floor_geom_id is not None:
                    for i in range(num_arm_geoms):
                        geom = self._pin_collision_model.geometryObjects[i]
                        geom_name = geom.name.lower()
                        # Skip base, shoulder, and gripper geometries
                        if "base" in geom_name or "shoulder" in geom_name or "gripper" in geom_name:
                            continue
                        valid_pairs.add(
                            (min(i, self._pin_floor_geom_id), max(i, self._pin_floor_geom_id))
                        )

                # Remove invalid collision pairs from URDF-loaded pairs
                pairs_to_remove = []
                for pair in self._pin_collision_model.collisionPairs:
                    pair_key = (min(pair.first, pair.second), max(pair.first, pair.second))
                    if pair_key not in valid_pairs:
                        pairs_to_remove.append(pair)

                for pair in pairs_to_remove:
                    self._pin_collision_model.removeCollisionPair(pair)

                # Add any missing valid pairs
                existing_pairs = set()
                for pair in self._pin_collision_model.collisionPairs:
                    existing_pairs.add((min(pair.first, pair.second), max(pair.first, pair.second)))

                for i, j in valid_pairs:
                    if (i, j) not in existing_pairs:
                        self._pin_collision_model.addCollisionPair(pin.CollisionPair(i, j))

                logger.info(
                    "Collision pairs: %d total (%d self-collision, %d floor), %d explicitly disabled",
                    len(self._pin_collision_model.collisionPairs),
                    len([p for p in valid_pairs if self._pin_floor_geom_id not in p]),
                    len([p for p in valid_pairs if self._pin_floor_geom_id in p])
                    if self._pin_floor_geom_id
                    else 0,
                    len(disabled_geom_pairs),
                )

                self._pin_collision_data = self._pin_collision_model.createData()

            except Exception as e:
                logger.warning(
                    "Could not load collision geometries: %s. Self-collision check disabled.", e
                )
                logger.warning(
                    "To enable collision detection, ensure STL mesh files are in assets/ folder "
                    "next to the URDF, or use absolute paths in the URDF."
                )
                self._pin_collision_model = None
                self._pin_collision_data = None

            logger.info(
                "Pinocchio model loaded from %s (nq=%d, collisions=%s, floor=%s)",
                path,
                self._pin_model.nq,
                "enabled" if self._pin_collision_model else "disabled",
                "enabled" if self._pin_floor_geom_id is not None else "disabled",
            )
            return True
        except Exception as e:
            logger.warning("Failed to load Pinocchio model from %s: %s", path, e)
            self._pin_model = None
            self._pin_data = None
            self._pin_collision_model = None
            self._pin_collision_data = None
            self._pin_floor_geom_id = None
            return False

    def _add_floor_geometry(self, floor_z: float) -> Optional[int]:
        """
        Add a floor plane as a collision geometry attached to the base joint.

        Args:
            floor_z: Z offset for floor plane below base

        Returns:
            Geometry ID of the floor, or None if failed
        """
        if self._pin_collision_model is None or hppfcl is None:
            logger.warning("Cannot add floor geometry: collision model or hppfcl not available")
            return None

        try:
            # Get the base frame and its parent joint
            base_frame_id = self._pin_model.getFrameId("base_link")
            if base_frame_id >= self._pin_model.nframes:
                # Try alternative name
                base_frame_id = self._pin_model.getFrameId("base")
            if base_frame_id >= self._pin_model.nframes:
                # Fall back to universe joint (joint 0)
                base_joint_id = 0
                logger.info("Base frame not found, using universe joint for floor")
            else:
                base_joint_id = self._pin_model.frames[base_frame_id].parentJoint

            # Create floor pose (slightly below the base)
            floor_pose = pin.SE3.Identity()
            floor_pose.translation = np.array([0.0, 0.0, floor_z])  # Center of box

            # Create a large flat box as the floor (2m x 2m x 2cm)
            floor_shape = hppfcl.Box(2.0, 2.0, 0.02)

            # Create geometry object
            floor_go = pin.GeometryObject(
                "floor",
                base_joint_id,
                floor_shape,
                floor_pose,
            )
            floor_go.meshColor = np.array([0.5, 0.5, 0.5, 1.0])

            # Add to collision model
            floor_geom_id = self._pin_collision_model.addGeometryObject(floor_go)
            logger.info("Added floor geometry at Z=%.3f (geom_id=%d)", floor_z, floor_geom_id)
            return floor_geom_id

        except Exception as e:
            logger.warning("Failed to add floor geometry: %s", e)
            return None

    def _validate_pose_pinocchio(
        self,
        joint_angles_rad: List[float],
        use_distance_check: bool = True,
    ) -> Tuple[bool, str]:
        """
        Validate joint angles for geometry collisions (floor and self-collision).

        Args:
            joint_angles_rad: Joint angles in radians
            use_distance_check: If True (default), use computeDistances with collision margin.
                               If False, use computeCollisions for boolean collision check.

        DOES NOT check URDF joint limits - calibration limits are used instead.
        Pinocchio is used for collision detection (all links vs floor, self-collision).

        Requires init_pin() to have been called successfully.

        Returns:
            (is_safe, message)
        """
        if self._pin_model is None or self._pin_data is None:
            return True, ""

        q = np.array(joint_angles_rad, dtype=np.float64)
        if q.size != self._pin_model.nq:
            return False, f"Expected {self._pin_model.nq} joints, got {q.size}"

        pin.forwardKinematics(self._pin_model, self._pin_data, q)

        if self._pin_collision_model is not None and self._pin_collision_data is not None:
            pin.updateGeometryPlacements(
                self._pin_model,
                self._pin_data,
                self._pin_collision_model,
                self._pin_collision_data,
                q,
            )

            if use_distance_check:
                # Distance-based checking with safety margin
                pin.computeDistances(self._pin_collision_model, self._pin_collision_data)

                for k in range(len(self._pin_collision_model.collisionPairs)):
                    dist_result = self._pin_collision_data.distanceResults[k]
                    if dist_result.min_distance < self._pin_collision_margin:
                        pair = self._pin_collision_model.collisionPairs[k]
                        geom1_name = self._pin_collision_model.geometryObjects[pair.first].name
                        geom2_name = self._pin_collision_model.geometryObjects[pair.second].name

                        if geom1_name == "floor" or geom2_name == "floor":
                            other_name = geom2_name if geom1_name == "floor" else geom1_name
                            return False, (
                                f"Floor collision: {other_name} too close to floor "
                                f"(dist={dist_result.min_distance:.3f}m < {self._pin_collision_margin:.3f}m)"
                            )
                        else:
                            return False, (
                                f"Self-collision: {geom1_name} ↔ {geom2_name} too close "
                                f"(dist={dist_result.min_distance:.3f}m < {self._pin_collision_margin:.3f}m)"
                            )
            else:
                # Boolean collision check (actual penetration)
                is_colliding = pin.computeCollisions(
                    self._pin_collision_model,
                    self._pin_collision_data,
                    stop_at_first_collision=True,
                )
                if is_colliding:
                    for k in range(len(self._pin_collision_model.collisionPairs)):
                        cr = self._pin_collision_data.collisionResults[k]
                        if cr.isCollision():
                            pair = self._pin_collision_model.collisionPairs[k]
                            geom1_name = self._pin_collision_model.geometryObjects[pair.first].name
                            geom2_name = self._pin_collision_model.geometryObjects[pair.second].name

                            if geom1_name == "floor" or geom2_name == "floor":
                                other_name = geom2_name if geom1_name == "floor" else geom1_name
                                return False, f"Floor collision: {other_name} penetrating floor"
                            else:
                                return False, f"Self-collision: {geom1_name} ↔ {geom2_name}"
                    return False, "Collision detected"

        return True, ""

    def validate_action_kinematics(
        self,
        action: Dict[str, float],
        *,
        trigger_callback: bool = True,
        use_distance_check: bool = True,
    ) -> Tuple[bool, str]:
        """
        Validate action against URDF limits and geometry (no load check, no send).

        Use before sending when you need to validate without executing.

        Args:
            action: Dictionary mapping motor names to normalized positions
            trigger_callback: If True and collision detected, invoke collision callback
            use_distance_check: If True, use computeDistances with collision margin;
                if False, use computeCollisions (boolean, typically faster).
        """
        if self._pin_model is None or self.calibration is None:
            return True, ""

        goal_pos = {
            key.removesuffix(".pos"): val for key, val in action.items() if key.endswith(".pos")
        }
        for key, val in action.items():
            if not key.endswith(".pos") and key in self.motors:
                goal_pos[key] = val

        joint_order = [
            "shoulder_pan",
            "shoulder_lift",
            "elbow_flex",
            "wrist_flex",
            "wrist_roll",
            "gripper",
        ]
        joint_angles_rad: List[float] = []
        for name in joint_order:
            norm_val = goal_pos.get(name, 0.0)
            norm_mode = self.motors[name].norm_mode
            calib = self.calibration.get(name)
            rad = normalized_to_radians(norm_val, norm_mode, calib)
            joint_angles_rad.append(rad)

        is_safe, msg = self._validate_pose_pinocchio(
            joint_angles_rad, use_distance_check=use_distance_check
        )

        if not is_safe and trigger_callback and self._on_collision_callback is not None:
            collision_type = "floor" if "floor" in msg.lower() else "self"
            joint_angles_dict = dict(zip(joint_order, joint_angles_rad))
            try:
                self._on_collision_callback(collision_type, msg, joint_angles_dict)
            except Exception as e:
                logger.warning("Collision callback failed: %s", e)

        return is_safe, msg

    def send_action_safe(
        self,
        action: Dict[str, float],
        *,
        load_threshold: float = 900.0,
        check_load_before: bool = True,
        check_collision: bool = True,
    ) -> Tuple[bool, str]:
        """
        Validate and send action with load and kinematics safety checks.

        Performs: (1) load check before send (if enabled), (2) kinematics validation if
        check_collision, (3) send_action. If Pinocchio is not initialized, skips kinematics check.

        Args:
            action: Dictionary mapping motor names (with or without .pos) to normalized positions
            load_threshold: Load threshold for safety (default 900 = 90% of max torque)
            check_load_before: If True, reject when current load already exceeds threshold
            check_collision: If False, skip Pinocchio self/floor checks (e.g. sit-down rescue)

        Returns:
            (is_safe, message)
            - (True, ""): Action was sent successfully
            - (False, "reason"): Action was rejected, not sent
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected.")

        if check_load_before:
            high_loads = self._check_high_load(load_threshold)
            if high_loads:
                motor_names = {m.id: name for name, m in self.motors.items()}
                high_loads_named = {
                    motor_names.get(mid, f"motor_{mid}"): load for mid, load in high_loads.items()
                }
                msg = ", ".join(f"{n}={load:.1f}" for n, load in high_loads_named.items())
                return False, f"High motor load detected: {msg}"

        if check_collision:
            is_safe, msg = self.validate_action_kinematics(action)
            if not is_safe:
                return False, msg

        self.send_action(action)
        return True, ""

    def process_and_send_action(
        self,
        action: Dict[str, float],
        *,
        load_threshold: float = 900.0,
        check_load_before: bool = True,
        check_collision: bool = True,
    ) -> Tuple[bool, Optional[str]]:
        """
        Process action with stepping, validation, and send. Single entry point for motor writer.

        Handles: kinematics validation, max_relative_target stepping (or direct send_action_safe),
        returns whether action was sent and optional rejection reason.

        Safety checks performed:
        - Load check before send (if enabled)
        - Pinocchio kinematics validation for FINAL goal pose (if enabled)
        - Pinocchio kinematics validation for EACH intermediate step (if enabled)
        - Load check between steps (if stepping)

        Returns:
            (action_was_sent, rejection_reason)
            - (True, None): Action was sent
            - (False, "reason"): Action was rejected
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected.")

        goal_pos = {
            key.removesuffix(".pos"): val for key, val in action.items() if key.endswith(".pos")
        }
        for key, val in action.items():
            if not key.endswith(".pos") and key in self.motors:
                goal_pos[key] = val

        # Validate final goal pose with Pinocchio (floor + self-collision)
        if check_collision and self._pin_model is not None:
            goal_action = {f"{k}.pos": v for k, v in goal_pos.items()}
            is_safe, msg = self.validate_action_kinematics(goal_action)
            if not is_safe:
                return False, msg

        # Check load before starting (if enabled)
        if check_load_before:
            high_loads = self._check_high_load(load_threshold)
            if high_loads:
                motor_names = {m.id: name for name, m in self.motors.items()}
                high_loads_named = {
                    motor_names.get(mid, f"motor_{mid}"): load for mid, load in high_loads.items()
                }
                msg = ", ".join(f"{n}={load:.1f}" for n, load in high_loads_named.items())
                return False, f"High motor load before action: {msg}"

        if self.config.max_relative_target is not None:
            present = self.get_observation()
            if not present:
                return False, "Could not read current position"
            current_pos = {k.removesuffix(".pos"): v for k, v in present.items()}

            max_steps = 1
            for joint_name, goal_val in goal_pos.items():
                cur = current_pos.get(joint_name, 0.0)
                diff = abs(goal_val - cur)
                if diff > 0.01:
                    steps = int(diff / self.config.max_relative_target) + 1
                    max_steps = max(max_steps, steps)

            for step_idx in range(max_steps):
                remaining = {
                    j: g for j, g in goal_pos.items() if abs(g - current_pos.get(j, 0.0)) > 0.01
                }
                if not remaining:
                    break
                goal_present = {j: (goal_pos[j], current_pos.get(j, 0.0)) for j in remaining}
                safe_pos = ensure_safe_goal_position(goal_present, self.config.max_relative_target)
                for name, pos in safe_pos.items():
                    current_pos[name] = pos
                safe_action = {f"{n}.pos": p for n, p in safe_pos.items()}

                # Validate each intermediate step with Pinocchio
                if check_collision and self._pin_model is not None:
                    is_safe, msg = self.validate_action_kinematics(safe_action)
                    if not is_safe:
                        return (
                            False,
                            f"Unsafe intermediate pose (step {step_idx + 1}/{max_steps}): {msg}",
                        )

                orig = self.config.max_relative_target
                self.config.max_relative_target = None
                try:
                    self.send_action(safe_action)
                finally:
                    self.config.max_relative_target = orig

                if step_idx < max_steps - 1:
                    # Check load between steps
                    if check_load_before:
                        high_loads = self._check_high_load(load_threshold)
                        if high_loads:
                            motor_names = {m.id: name for name, m in self.motors.items()}
                            high_loads_named = {
                                motor_names.get(mid, f"motor_{mid}"): load
                                for mid, load in high_loads.items()
                            }
                            msg = ", ".join(
                                f"{n}={load:.1f}" for n, load in high_loads_named.items()
                            )
                            logger.warning(
                                f"High load during stepping (step {step_idx + 1}/{max_steps}): {msg}"
                            )
                            return False, f"High motor load during stepping: {msg}"

                    if self.config.step_delay_sec > 0:
                        time.sleep(self.config.step_delay_sec)
            return True, None
        else:
            is_safe, msg = self.send_action_safe(
                action,
                load_threshold=load_threshold,
                check_load_before=check_load_before,
                check_collision=check_collision,
            )
            return is_safe, None if is_safe else msg

    def _check_high_load(self, threshold: float) -> Optional[Dict[int, float]]:
        """Check if any motor load exceeds threshold. Returns dict of motor_id -> load or None."""
        try:
            motor_ids = [m.id for m in self.motors.values()]
            loads = self.bus.sync_read_loads(motor_ids, num_retry=1)
            if not loads:
                return None
            high = {mid: load for mid, load in loads.items() if abs(load) > threshold}
            return high if high else None
        except Exception as e:
            logger.debug("Failed to read loads for safety check: %s", e)
            return None

    def send_action(self, action: Dict[str, float]) -> Dict[str, float]:
        """
        Command arm to move to a target joint configuration.

        The relative action magnitude may be clipped depending on the configuration parameter
        `max_relative_target`. In this case, the action sent differs from original action.
        Thus, this function always returns the action actually sent.

        Args:
            action: Dictionary mapping motor names (with or without .pos suffix) to normalized positions

        Returns:
            Dictionary mapping motor names to the actual positions sent (normalized)
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected.")

        goal_pos = {
            key.removesuffix(".pos"): val for key, val in action.items() if key.endswith(".pos")
        }
        for key, val in action.items():
            if not key.endswith(".pos") and key in self.motors:
                goal_pos[key] = val

        if self.config.max_relative_target is not None:
            try:
                present_pos = self.bus.sync_read("Present_Position", normalize=True)
                # present_pos can be empty when read fails (e.g. Port is in use)
                if not present_pos and self._current_positions:
                    present_pos = {
                        k.removesuffix(".pos"): v for k, v in self._current_positions.items()
                    }
                if present_pos:
                    goal_present_pos = {
                        key: (g_pos, present_pos.get(key, g_pos)) for key, g_pos in goal_pos.items()
                    }
                    goal_pos = ensure_safe_goal_position(
                        goal_present_pos, self.config.max_relative_target
                    )
            except SerialException as e:
                logger.warning(
                    f"Serial communication error reading follower positions for safety check: {e}. Skipping safety check."
                )
                # Use last known positions if available for safety check
                if self._current_positions:
                    present_pos = {
                        k.removesuffix(".pos"): v for k, v in self._current_positions.items()
                    }
                    goal_present_pos = {
                        key: (g_pos, present_pos.get(key, g_pos)) for key, g_pos in goal_pos.items()
                    }
                    goal_pos = ensure_safe_goal_position(
                        goal_present_pos, self.config.max_relative_target
                    )

        self.bus.sync_write("Goal_Position", goal_pos, normalize=True)

        return {f"{motor}.pos": val for motor, val in goal_pos.items()}

    def get_observation(self) -> Dict[str, float]:
        """
        Get current motor positions as observation dictionary.

        Returns:
            Dictionary mapping motor names (with .pos suffix) to normalized position values
        """
        if not self.connected:
            raise DeviceNotConnectedError(f"{self} is not connected.")

        try:
            obs_dict = self.bus.sync_read("Present_Position", normalize=True)
            obs_dict = {f"{motor}.pos": val for motor, val in obs_dict.items()}
            if obs_dict:
                self._current_positions = obs_dict
                return obs_dict
            # Read failed (e.g. Port is in use) - return last known, don't overwrite
            return self._current_positions if self._current_positions else {}
        except SerialException as e:
            logger.warning(
                f"Serial communication error reading follower positions: {e}. Using last known positions."
            )
            return self._current_positions if self._current_positions else {}

    @property
    def torque_enabled(self) -> bool:
        """Check if torque is enabled."""
        return self._torque_enabled

    @property
    def calibration_fpath(self) -> Path:
        """Get calibration file path."""
        return self.config.calibration_dir / f"{self.config.id}.json"
