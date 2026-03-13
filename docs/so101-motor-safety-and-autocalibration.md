# SO101 Motor Safety, Effort-Aware Control, and Auto-Calibration Findings

## Purpose

This document summarizes the current SO101 motor-control implementation, the Feetech/SCServo capabilities relevant to motor safety, and a practical path toward safer effort-aware control and auto-calibration.

It is based on:

- the SO101 driver implementation in this repository
- the local STS3215 control table encoded in `motors/tables.py`
- public Feetech/SCServo protocol and STS3215 documentation

The focus here is not generic teleoperation behavior, but specifically:

- how to avoid overstressing or damaging the motors / gearbox / robot
- what safety-relevant telemetry and controls the servos expose
- what the current code already uses
- what is still missing for safe automated calibration

## Executive Summary

The current SO101 stack is strong in **position-domain safety** but weak in **effort-aware safety**.

Today, the system already does a good job of:

- clamping oversized target jumps
- constraining commands into calibrated joint ranges
- enforcing absolute encoder bounds
- persisting motor-side EEPROM min/max limits after calibration
- applying extra protection on the gripper

However, it does **not** yet make full use of the motor's feedback and protection features to answer questions like:

- Is the joint pushing into a hard stop?
- Is the motor stalled or jammed?
- Is the commanded motion causing excessive effort?
- Should calibration stop because resistance is too high?

For that reason, a naive fully automatic "move until the stop" calibration would be risky in the current implementation.

The best next step is **supervised, effort-aware auto-calibration**, not blind hard-stop probing.

## Hardware and Protocol Context

The SO101 is modeled in this repo as 6 Feetech `STS3215` bus servos:

- `shoulder_pan` -> ID 1
- `shoulder_lift` -> ID 2
- `elbow_flex` -> ID 3
- `wrist_flex` -> ID 4
- `wrist_roll` -> ID 5
- `gripper` -> ID 6

Relevant implementation files:

- `so101/robot.py`
- `so101/leader.py`
- `so101/follower.py`
- `motors/feetech_bus.py`
- `motors/tables.py`

The local control table in `motors/tables.py` matches the Feetech STS/SMS series layout and assumes:

- 12-bit magnetic encoder
- absolute position range `0..4095`
- half-duplex serial bus
- Feetech/SCServo packet protocol

Public protocol documentation and public STS3215 summaries confirm support for instruction-level operations such as:

- Ping
- Read
- Write
- Reg Write
- Action
- Sync Write
- Reset

## Safety-Relevant Motor Capabilities Exposed by STS3215 / SCServo

The STS3215 control table contains several classes of features relevant to motor protection and safer calibration.

### 1. Motion commands

- `Goal_Position`
- `Goal_Time`
- `Goal_Velocity`
- `Acceleration`
- `Maximum_Velocity_Limit`
- `Maximum_Acceleration`
- `Acceleration_Multiplier`

These are the main levers for making motion gentler and less likely to shock the gearbox.

### 2. Position and zeroing

- `Present_Position`
- `Min_Position_Limit`
- `Max_Position_Limit`
- `Homing_Offset`
- `Operating_Mode`

These are the basis of the current SO101 calibration flow and remain the most reliable controls already in active use.

### 3. Effort / protection / overload related fields

- `Present_Current`
- `Present_Load`
- `Torque_Enable`
- `Torque_Limit`
- `Max_Torque_Limit`
- `Protection_Current`
- `Protective_Torque`
- `Protection_Time`
- `Overload_Torque`
- `Over_Current_Protection_Time`

These are the most important controls for detecting or limiting motor stress during automatic probing.

### 4. Thermal / electrical health

- `Present_Temperature`
- `Max_Temperature_Limit`
- `Present_Voltage`
- `Min_Voltage_Limit`
- `Max_Voltage_Limit`

These are useful for thermal budget, power-quality monitoring, and calibration abort conditions.

### 5. State / fault / alarm signaling

- `Status`
- `Moving`
- `Unloading_Condition`
- `LED_Alarm_Condition`

These can help distinguish normal motion from stalled or protected motion.

## What the SO101 Code Actually Uses Today

There is a significant difference between what the servo supports and what the SO101 driver currently uses.

### Reads actively used today

#### Position

Primary runtime signal:

- `Present_Position`

Used in:

- leader observation
- follower observation
- teleoperation
- remote operation
- calibration

Files:

- `motors/feetech_bus.py`
- `so101/leader.py`
- `so101/follower.py`

#### Temperature

Read and surfaced for alerts / status only:

- `Present_Temperature`

Files:

- `utils/temperature.py`
- `utils/trackers.py`
- `utils/cw_alerts.py`

#### Diagnostic-only or utility-only reads

The diagnostic and helper code can also read:

- `Model_Number`
- `ID`
- `Goal_Position`
- `Present_Velocity`
- `Present_Load`
- `Present_Voltage`
- `Min_Voltage_Limit`
- `Max_Voltage_Limit`
- `Torque_Enable`
- `Moving`

Files:

- `scripts/cw_read_device.py`
- `utils/utils.py`

### Writes actively used today

#### Motion

- `Goal_Position`

File:

- `motors/feetech_bus.py`

#### Torque control

- `Torque_Enable`

Files:

- `motors/feetech_bus.py`
- `so101/leader.py`
- `so101/follower.py`
- `so101/robot.py`

#### General configuration currently exposed in the bus wrapper

- `Operating_Mode`
- `P_Coefficient`
- `I_Coefficient`
- `D_Coefficient`
- `Max_Torque_Limit`
- `Protection_Current`
- `Overload_Torque`

File:

- `motors/feetech_bus.py`

#### Calibration writes

- `Min_Position_Limit`
- `Max_Position_Limit`
- `Homing_Offset`

File:

- `motors/feetech_bus.py`

### Important dormant capabilities

The code defines but does not meaningfully use at runtime:

- `Present_Current`
- `Status`
- most torque/current/overload timing controls
- most motion-shaping registers
- servo-side thermal/alarm policy registers

This is the main reason the system remains position-centric instead of effort-aware.

## Current Safety Model in the Repository

The current safety stack is mostly layered around target position handling.

### 1. Relative target clamp

`ensure_safe_goal_position()` caps per-update delta between:

- desired goal position
- current position

The clamp is:

- `[-max_relative_target, +max_relative_target]`

Files:

- `utils/utils.py`
- `so101/follower.py`

### 2. Step splitting for remote operation

Remote operation can decompose a large target into multiple smaller substeps before writing to the motors.

File:

- `utils/cw_remoteoperate_helpers.py`

This is one of the strongest existing protections against abrupt movements.

### 3. Normalized-domain clamp

Before converting to raw encoder units:

- arm joints are clamped to `[-100, 100]`
- gripper is clamped to `[0, 100]`

File:

- `utils/utils.py`

### 4. Calibration-range clamp

After denormalization, each raw target is clamped to the joint's calibrated:

- `range_min`
- `range_max`

File:

- `utils/utils.py`

### 5. Absolute encoder clamp

Raw target values are finally clamped to:

- `0..4095`

Files:

- `utils/utils.py`
- `motors/feetech_bus.py`

### 6. Motor EEPROM limits after calibration

Once calibration exists, the driver writes these limits directly into the servo:

- `Min_Position_Limit`
- `Max_Position_Limit`
- `Homing_Offset`

This is one of the most valuable existing protections because the servo itself learns its usable range.

Files:

- `motors/feetech_bus.py`
- `so101/follower.py`
- `so101/leader.py`

### 7. Gripper-only protection tuning

The follower currently applies extra protection only to the gripper:

- `Max_Torque_Limit = 500`
- `Protection_Current = 250`
- `Overload_Torque = 25`

File:

- `so101/follower.py`

This is useful, but it is not yet generalized to the other joints.

## Gaps in the Current Safety Design

The following gaps matter if the goal is to better protect motors and enable safe automation.

### 1. No runtime current-based protection

The most important missing runtime signal is:

- `Present_Current`

The register exists in the control table, but the driver does not yet read or use it.

### 2. Load is available but not part of control decisions

`Present_Load` can be read, but only through lower-level helpers and diagnostics. It is not currently used for:

- jam detection
- contact detection
- endpoint finding
- automatic aborts

### 3. Temperature is passive, not active

Motor temperature is monitored and alerted, but it does not yet trigger:

- motion slowdowns
- torque disable
- calibration abort
- cooldown logic

### 4. Motion shaping registers are mostly unused

The driver currently relies almost entirely on small target deltas, not on:

- goal time
- goal velocity
- acceleration
- runtime max velocity / acceleration limiting

This leaves room for gentler, more controlled motion during probing.

### 5. Calibration remains warning-only

The calibration flow can detect suspicious ranges, but it still saves results too easily.

In particular:

- invalid or weak ranges are mostly warnings, not hard failures
- no-data cases are converted to broad defaults instead of failing hard

This is not suitable for future automatic calibration.

## Why Manual Calibration Is Safer Than Blind Auto-Probing

The current manual calibration has one large safety advantage:

- the human feels resistance before the motor does

During manual calibration:

- torque is disabled
- the user physically moves the joints
- the software only records observed positions

This avoids powered pushing into:

- hard stops
- self-collisions
- cable snags
- external obstacles

A blind automatic calibration cannot distinguish those cases unless it uses much richer motor-health telemetry.

## Deep Dive: Which Signals Matter Most for Protecting the Motors

### Highest-value signal: `Present_Current`

If only one additional signal is added for effort-aware control, it should be `Present_Current`.

Why it matters:

- closest proxy to real actuator effort
- strongest indicator of stall / jam / hard-stop contact
- better suited than temperature for immediate stop conditions

### Secondary signal: `Present_Load`

Useful as a corroborating effort signal, but likely less trustworthy as a sole hard-stop detector.

Best use:

- trend monitoring
- corroboration alongside current / velocity / movement state

### Critical context: `Present_Velocity` and `Moving`

High effort is not enough by itself. The system should ask:

- Is the joint still moving?
- Is the velocity near zero despite repeated commanded motion?

The most reliable stop condition is usually a combination:

- command issued
- little or no actual movement
- high current and/or load
- possible status/protection flags

### Thermal monitoring: `Present_Temperature`

Temperature is important for:

- accumulated motor stress
- cooldown budgeting
- refusing repeated aggressive calibration attempts

But it is too slow to be the primary endpoint detector.

### Fault signaling: `Status`

This should become a first-class abort gate:

- overload
- overcurrent
- thermal events
- motor-side protection state

At the moment it is defined in the table but not integrated into behavior.

## Assessment of Auto-Calibration Approaches

### Option A: Blind hard-stop probing

Description:

- command motion until motor can no longer progress
- infer min/max from the resistance point

Risk:

- highest stress on gearbox and linkages
- self-collision and cable drag can be misread as endpoints
- unsafe without current/status integration and conservative protection settings

Recommendation:

- not recommended as the first production approach

### Option B: Supervised, effort-aware auto-calibration

Description:

- move one joint at a time in very small increments
- use conservative protection settings
- stop based on effort-aware rules
- retreat from contact point
- store a safe margin inside the detected endpoint

Benefits:

- much safer than blind probing
- less manual work than full hand calibration
- compatible with the current position-based software design

Recommendation:

- best near-term target

### Option C: Assisted self-learning calibration

Description:

- infer usable min/max over time from real teleoperation and remote-operation data
- accept extrema only after repeated observation
- reject outliers
- set soft limits inward from observed extremes

Benefits:

- lowest stress on the robot
- no forced endpoint collisions
- can refine limits over time

Downside:

- converges slowly
- may never discover the full physical range

Recommendation:

- attractive long-term complement to supervised calibration

## Recommended Auto-Calibration Strategy

The recommended strategy is **supervised, one-joint-at-a-time, effort-aware auto-calibration in position mode**.

### Why position mode

Position mode is the safest practical mode for calibration because it allows:

- tiny bounded increments
- straightforward backoff after contact
- easy reuse of the current target clamp and normalization code

Velocity mode or PWM-style control would be riskier because the servo could keep pushing into resistance unless software reacts immediately.

### Proposed calibration flow

#### Stage 0: Preflight

Before probing:

- verify communication with every motor
- verify temperature is below threshold
- verify voltage is stable
- clear fault conditions if applicable
- refuse calibration if any critical status is present

#### Stage 1: Calibration-mode protection profile

Before any automated movement, apply conservative settings on **all joints**, not only the gripper:

- low torque limit
- conservative current protection
- conservative overload threshold
- low acceleration
- low max velocity
- gentle move timing

#### Stage 2: Safe parking pose

Move the robot into a known collision-reducing posture before probing any limit.

This is important because endpoint-seeking cannot otherwise distinguish:

- true hard stop
- self-collision
- cable snag
- accessory/camera interference

#### Stage 3: Probe one joint at a time

For each joint:

- hold all other joints fixed
- send a very small position increment
- wait for settle
- read:
  - present position
  - present velocity
  - moving
  - present current
  - present load
  - status
  - temperature

#### Stage 4: Detect a candidate endpoint

Treat the endpoint as a candidate only when multiple conditions agree, for example:

- commanded step issued
- actual position progress is very small
- velocity is near zero or moving is false
- current or load spikes above threshold
- status indicates protection / overload

No single signal should be trusted by itself.

#### Stage 5: Back off and store a soft limit

The first contact point should **not** become the saved limit.

Instead:

- retreat by a safety margin
- record the limit inward from the detected point

This reduces repeated hard-stop contact during future operation.

#### Stage 6: Repeat and confirm

Repeat endpoint detection more than once:

- multiple passes
- slightly different approach timing
- confirm that the detected contact zone is stable

Only then accept the result.

#### Stage 7: Persist limits

After both ends are confirmed:

- write `Min_Position_Limit`
- write `Max_Position_Limit`
- write `Homing_Offset`
- save the local calibration file
- optionally upload calibration to the backend

If any joint fails the process, fail that calibration run instead of inventing broad defaults.

## Proposed Abort Conditions for Safe Calibration

Auto-calibration should abort immediately when any of the following occurs:

- present current exceeds a hard threshold
- load remains high while motion progress is near zero
- servo status indicates protection or fault
- temperature exceeds a critical threshold
- voltage sags below a safe bound
- communication becomes unstable

It should also pause or slow down when:

- temperature is trending upward too quickly
- repeated medium-load events are observed
- settling time becomes abnormal

## Changes Needed in This Repository

The existing code already contains most of the structural pieces needed for a safer implementation, but several additions are required.

### 1. Expand the bus wrapper

Add readers or generic support for:

- `Present_Current`
- `Status`
- `Moving`
- `Present_Velocity`
- `Present_Load`

Main target file:

- `motors/feetech_bus.py`

### 2. Promote effort telemetry into runtime tracking

Extend status tracking so it can retain:

- current
- load
- velocity
- status/fault flags

Main target files:

- `utils/trackers.py`
- `utils/temperature.py` or a sibling telemetry helper

### 3. Turn thermal monitoring into active protection

Current behavior:

- creates alerts only

Desired behavior:

- warning -> slow or pause
- critical -> abort calibration and disable torque

Main target files:

- `utils/cw_alerts.py`
- `utils/trackers.py`
- calibration orchestration code

### 4. Generalize motor-side protection tuning beyond the gripper

Current behavior:

- only gripper gets extra torque/current/overload protection tuning

Desired behavior:

- define conservative per-joint protection profile for calibration mode

Main target file:

- `so101/follower.py`

### 5. Harden calibration validation

Current behavior:

- warnings are displayed, but calibration still proceeds too easily
- no-data samples can degrade into broad default limits

Desired behavior:

- fail hard on invalid or missing calibration ranges
- do not persist broad synthetic defaults

Main target files:

- `motors/feetech_bus.py`
- `so101/robot.py`
- `utils/utils.py`

## Recommended Stop-Condition Logic

For safe endpoint seeking, the recommended logic is:

1. command a small position step
2. wait for motion/settling
3. measure:
   - position delta
   - velocity / moving state
   - current
   - load
   - status
4. if:
   - progress is small, and
   - velocity is near zero, and
   - current or load is high, or status reports protection
   then:
   - stop
   - back off
   - mark candidate endpoint

This is much more robust than relying on any one metric alone.

## Practical Recommendation

The best near-term product strategy is:

1. keep manual calibration as the safety baseline
2. add effort-aware observability first
3. build supervised auto-calibration in position mode
4. use self-learning range refinement later

This balances:

- motor safety
- engineering complexity
- reliability
- user effort

## Key Repository References

Primary implementation files:

- `cyberwave-edge-nodes/cyberwave-edge-so101/motors/tables.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/motors/feetech_bus.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/so101/robot.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/so101/follower.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/utils/utils.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/utils/cw_remoteoperate_helpers.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/utils/temperature.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/utils/trackers.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/utils/cw_alerts.py`
- `cyberwave-edge-nodes/cyberwave-edge-so101/scripts/cw_read_device.py`

Public protocol/control-table references used for cross-checking:

- Feetech STS/SMS communication protocol manual
- public STS3215 control-table summaries, including:
  - `https://hexdocs.pm/feetech/Feetech.ControlTable.STS3215.html`
  - public STS3215 datasheet mirrors and vendor product pages

## Final Recommendation

Do **not** replace the current manual calibration with a first-pass fully blind hard-stop seeker.

Instead, build toward:

- conservative motor protection on all joints
- runtime current/load/status observability
- active thermal abort behavior
- supervised one-joint incremental probing
- repeated endpoint confirmation
- inward safety margins before writing EEPROM limits

That approach is much more likely to reduce user effort **without** increasing the chance of damaging the SO101 motors or the robot structure.
