# SO101 Calibration ↔ Controller Orchestration (internal)

Internal note describing how the SO101 driver, the frontend, and the backend
interact around calibration and the twin's controller policy. Not user-facing —
do not copy into the public README.

## Components involved

- **Driver** (`main.py`): owns calibration subprocesses and the
  teleoperate/remoteoperate operations. Reacts to `controller-changed` commands.
- **Backend** (`twins.py` → `send_controller_changed_mqtt`): publishes a
  `controller-changed` command on every twin PUT that includes
  `controller_policy_uuid` (note: fires even on a no-op write — there is no
  previous-vs-new diff).
- **Frontend**:
  - `useCalibrationAlertAutoDetach` — when a `calibration_needed` alert appears
    and the twin still has a controller, it PUTs the twin with no controller
    ("detach for calibration").
  - `useCalibrationAlertAutoAttach` — after calibration alerts clear, it assigns
    a keyboard controller to a controllerless twin.

## The loop this caused (fixed)

1. Driver needs calibration → creates a `calibration_needed` alert + starts the
   calibration subprocess, remembering the recovery op (`_pending_recovery_command`).
2. Frontend auto-detach sees the alert + attached controller → detaches it.
3. Backend echoes `controller-changed {controller: null}`.
4. Driver used to call `_stop_current_operation()` for that echo, which **killed
   the calibration subprocess and resolved the alert** → the alert "disappeared".
5. A re-attach / repeated PUT echoed `controller-changed {localop}` → back to step 1.

Result: the calibration alert flickered (a fresh alert UUID per cycle), the
leader stage never ran, and the robot could end up controllerless.

## State-driven recovery: `_evaluate_and_drive`

There is no stored "recovery script" token any more. A single evaluator,
`_evaluate_and_drive(client, twin_uuid)`, is the source of truth for *"given the
calibration files on disk and the assigned controller, what should be running?"*
It is called at three points (the spec):

1. **Startup** (`_check_startup_calibration`) — resolves the assigned controller
   fresh, pins it, then evaluates.
2. **End of every successful calibration stage** (`_handle_calibration_complete`,
   exit 0) — re-checks the files and either chains to the next missing
   calibration or resumes the assigned op. This is what fixes the
   **hang-after-successful-calibration** bug: previously a stored recovery token
   could be `None` (e.g. the leader stage was triggered with no controller
   resolved), so nothing ran and the driver sat idle even though the assigned
   controller op should have started.
3. **Controller assignment** (`_handle_controller_changed`) — pins the new
   controller's op, then evaluates (calibrate-then-resume).

Evaluator logic:

- Follower calibration is required for any operation.
- Leader calibration is required **only** for local teleoperation (`localop`);
  a remote/keyboard controller (or no controller) is never blocked on it.
- The op to resume is the **pinned** `_pending_recovery_command`
  (`teleoperate` | `remoteoperate` | `None`), captured when the controller was
  assigned / at startup so it survives the controller being detached for
  calibration **and** survives calibration failure + restart. When unset it is
  resolved fresh as a best effort. Chaining follower → leader is re-derived from
  the files, not from a token.
- Never starts anything while a calibration is in flight.

## Invariants the driver now enforces

- **Calibration is sticky against `controller-changed`.** While a calibration
  flow is active (`_is_calibration_running()` or `_calibration_flow_step` set),
  `_handle_controller_changed` never stops calibration or resolves its alert. A
  detach echo is ignored; a real controller in the payload only updates the
  remembered recovery op so the correct operation resumes when calibration ends.
  Real cancellation goes through the dedicated calibration cancel flow, not via
  detaching the controller.
- **The backend is authoritative for the assigned controller, read fresh.**
  `_resolve_assigned_controller_recovery_command` reads the controller via a
  fresh SDK round-trip first; the local twin JSON written by edge-core is only an
  offline fallback because it can be stale (e.g. right after a detach). This is
  why startup chains follower → leader calibration and resumes the right op
  instead of starting without a controller.

## "Restart calibration" button must preempt

The error alert raised on a failed calibration (insufficient range, device
disconnected, etc.) carries a `metadata.buttons` "Restart calibration" button.
Its payload uses the **`restart`** action so it maps to
`_handle_calibration_start(force_restart=True)`. The `start` action
(`force_restart=False`) is silently rejected by the idempotency /
`calibration_already_running` guards whenever residual state lingers — the
previous calibration thread not yet reaped (`_calibration_active_count > 0`) or a
flow left in the range step by a finalize timeout. Because the frontend resolves
the error alert as soon as the restart button is pressed, a rejected `start`
leaves the user with no alert and no new calibration. `restart` stops any
leftover operation first and always starts fresh.

## Known follow-up (frontend)

With the driver now sticky, the frontend `useCalibrationAlertAutoDetach` no
longer serves a purpose (calibration runs fine with the controller still
attached) and, paired with `useCalibrationAlertAutoAttach`, can leave a `localop`
twin on a keyboard controller after calibration. Consider retiring the
auto-detach/attach pair and letting the driver own the controller lifecycle.
