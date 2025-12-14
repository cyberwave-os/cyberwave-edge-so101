"""Command-line script to read and display data from SO101 device.

Example:
    python -m so101_lib.read_device --port /dev/tty.usbmodem123
    python -m so101_lib.read_device --port /dev/tty.usbmodem123 --continuous
    python -m so101_lib.read_device --find-port
"""

import argparse
import sys
import time
from typing import Any, Dict, List, Optional

from leader import SO101_LEADER_MOTORS
from utils import (
    detect_voltage_rating,
    find_available_ports,
    find_port,
)

# Optional (soft) imports for Rich used by the TUI and styled help.
# Keep these at module scope so the rest of the module can remain
# usable even if `rich` isn't installed.
try:
    from rich.live import Live
    from rich.table import Table
    from rich.panel import Panel
    from rich import box
    from rich.console import Console, Group
    from rich.text import Text
    from rich.align import Align
    from rich.padding import Padding
except Exception:
    Live = Table = Panel = box = Console = Group = Text = Align = Padding = None

try:
    from scservo_sdk import PacketHandler, PortHandler
    import scservo_sdk as scservo
except ImportError as err:
    raise ImportError(
        "scservo_sdk is required. Install it with: pip install feetech-servo-sdk"
    ) from err

from motors.encoding import decode_sign_magnitude, encode_sign_magnitude
from motors.tables import (
    ADDR_ID,
    ADDR_MODEL_NUMBER,
    ADDR_MAX_VOLTAGE_LIMIT,
    ADDR_MIN_VOLTAGE_LIMIT,
    ADDR_GOAL_POSITION,
    ADDR_GOAL_VELOCITY,
    ADDR_ACCELERATION,
    ADDR_PRESENT_VELOCITY,
    ADDR_PRESENT_LOAD,
    ADDR_PRESENT_POSITION,
    ADDR_PRESENT_TEMPERATURE,
    ADDR_PRESENT_VOLTAGE,
    ADDR_TORQUE_ENABLE,
    ADDR_MOVING,
    ENCODING_BIT_VELOCITY,
)

def read_motor_data(
    port: str,
    motor_ids: List[int],
    baudrate: int = 1000000,
    read_voltage_limits: bool = True,
) -> Dict:
    """
    Read comprehensive data from motors.

    Args:
        port: Serial port path
        motor_ids: List of motor IDs to read from
        baudrate: Serial communication baudrate

    Returns:
        Dictionary with motor data
    """
    port_handler = PortHandler(port)
    port_handler.setBaudRate(baudrate)

    if not port_handler.openPort():
        raise ConnectionError(f"Failed to open port: {port}")

    # Initialize packet handler
    packet_handler = PacketHandler(0)   # SCServo bit end(STS/SMS=0, SCS=1)
    motor_data = {}

    for motor_id in motor_ids:
        data: Dict = {"id": motor_id, "error": None}

        try:
            # Read motor ID (verify it matches)
            motor_id_read, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_ID[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["motor_id"] = motor_id_read
            else:
                data["error"] = f"Failed to read motor ID: result={result}, error={error}"
                motor_data[motor_id] = data
                continue

            # Read model number (address 3, 2 bytes)
            # Use read2ByteTxRx to read both bytes at once
            model_number, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_MODEL_NUMBER[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["model_number"] = model_number

            # Read present position (address 56, 2 bytes)
            # Position is absolute (0-4095 for STS3215), not sign-magnitude
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            position_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_POSITION[0]
            )
            if result == scservo.COMM_SUCCESS:
                # store raw register value for debugging
                data["present_position_raw"] = position_raw
                # Position is absolute, not sign-magnitude encoded
                # Clamp to 12-bit encoder range (0-4095) using mask and bounds
                position_raw = position_raw & 0x0FFF  # Mask to 12 bits
                data["present_position"] = max(0, min(4095, position_raw))

            # Read goal position (address 42, 2 bytes)
            # Position is absolute (0-4095 for STS3215), not sign-magnitude
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            goal_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_GOAL_POSITION[0]
            )
            if result == scservo.COMM_SUCCESS:
                # Position is absolute, not sign-magnitude encoded
                # Clamp to 12-bit encoder range (0-4095) using mask and bounds
                goal_raw = goal_raw & 0x0FFF  # Mask to 12 bits
                data["goal_position"] = max(0, min(4095, goal_raw))

            # Read present velocity (address 58, 2 bytes, bit 15 for sign)
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            speed_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_VELOCITY[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["present_speed_raw"] = speed_raw
                data["present_speed"] = decode_sign_magnitude(
                    speed_raw, sign_bit=ENCODING_BIT_VELOCITY
                )

            # Read present load (address 60, 2 bytes)
            # read2ByteTxRx reads 2 bytes starting from the given address and returns the combined value
            load_raw, result, error = packet_handler.read2ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_LOAD[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["present_load_raw"] = load_raw
                # Load might use sign-magnitude, but check documentation
                # For now, try standard decoding
                data["present_load"] = decode_sign_magnitude(load_raw)

            # Read voltage (address 62, 1 byte)
            voltage, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_VOLTAGE[0]
            )
            if result == scservo.COMM_SUCCESS:
                # Store raw value for debugging
                data["voltage_raw"] = voltage
                # Voltage is in 0.1V units (e.g., 50 = 5.0V)
                # According to Feetech docs, voltage register is at address 62
                data["voltage"] = voltage / 10.0  # Voltage is in 0.1V units

            # Read voltage limits (for detecting voltage rating)
            # Addresses: Max=14, Min=15
            if read_voltage_limits:
                max_voltage, result, error = packet_handler.read1ByteTxRx(
                    port_handler, motor_id, ADDR_MAX_VOLTAGE_LIMIT[0]
                )
                if result == scservo.COMM_SUCCESS:
                    data["max_limit_voltage"] = max_voltage / 10.0  # In 0.1V units

                min_voltage, result, error = packet_handler.read1ByteTxRx(
                    port_handler, motor_id, ADDR_MIN_VOLTAGE_LIMIT[0]
                )
                if result == scservo.COMM_SUCCESS:
                    data["min_limit_voltage"] = min_voltage / 10.0  # In 0.1V units

            # Read temperature
            temperature, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_PRESENT_TEMPERATURE[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["temperature"] = temperature  # Temperature in Celsius

            # Read torque enable
            torque_enable, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_TORQUE_ENABLE[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["torque_enabled"] = bool(torque_enable)

            # Read moving status
            moving, result, error = packet_handler.read1ByteTxRx(
                port_handler, motor_id, ADDR_MOVING[0]
            )
            if result == scservo.COMM_SUCCESS:
                data["moving"] = bool(moving)

        except Exception as e:
            data["error"] = str(e)

        motor_data[motor_id] = data

    port_handler.closePort()
    return motor_data


def format_motor_data(
    motor_data: Dict,
    motor_names: Optional[Dict[int, str]] = None,
    voltage_rating: Optional[int] = None,
    show_raw: bool = False,
) -> str:
    """
    Format motor data for display.

    Args:
        motor_data: Dictionary of motor data from read_motor_data
        motor_names: Optional mapping of motor ID to name

    Returns:
        Formatted string
    """
    lines = []
    lines.append("=" * 80)
    lines.append("SO101 Motor Data")
    if voltage_rating:
        lines.append(f"Voltage Rating: {voltage_rating}V (detected)")
    lines.append("=" * 80)

    for motor_id, data in sorted(motor_data.items()):
        motor_name = (
            motor_names.get(motor_id, f"Motor {motor_id}") if motor_names else f"Motor {motor_id}"
        )
        lines.append(f"\n{motor_name} (ID: {motor_id})")
        lines.append("-" * 80)

        if data.get("error"):
            lines.append(f"  ERROR: {data['error']}")
            continue

        if "model_number" in data:
            lines.append(f"  Model Number: {data['model_number']}")

        if "present_position" in data:
            pos_line = f"  Present Position: {data['present_position']}"
            if show_raw and "present_position_raw" in data:
                pos_line += f" (raw: {data['present_position_raw']})"
            lines.append(pos_line)

        if "goal_position" in data:
            lines.append(f"  Goal Position: {data['goal_position']}")

        if "present_speed" in data:
            speed_str = f"  Present Speed: {data['present_speed']}"
            if show_raw and "present_speed_raw" in data:
                speed_str += f" (raw: {data['present_speed_raw']})"
            lines.append(speed_str)

        if "present_load" in data:
            load_str = f"  Present Load: {data['present_load']}"
            if show_raw and "present_load_raw" in data:
                load_str += f" (raw: {data['present_load_raw']})"
            lines.append(load_str)

        if "voltage" in data:
            voltage_str = f"  Voltage: {data['voltage']:.1f} V"
            if show_raw and "voltage_raw" in data:
                voltage_str += f" (raw: {data['voltage_raw']})"
            lines.append(voltage_str)
            # Warn if voltage seems incorrect
            if data["voltage"] > 15.0:
                lines.append("    ⚠️  WARNING: Voltage reading seems unusually high!")
                lines.append("    This might indicate a measurement error or wrong register.")
                if "voltage_raw" in data:
                    lines.append(
                        f"    Raw value: {data['voltage_raw']} (if > 255, register might be wrong)"
                    )

        if "max_limit_voltage" in data or "min_limit_voltage" in data:
            voltage_range = []
            if "min_limit_voltage" in data:
                voltage_range.append(f"{data['min_limit_voltage']:.1f}")
            else:
                voltage_range.append("?")
            voltage_range.append("-")
            if "max_limit_voltage" in data:
                voltage_range.append(f"{data['max_limit_voltage']:.1f}")
            else:
                voltage_range.append("?")
            lines.append(f"  Voltage Limits: {' '.join(voltage_range)} V")

        if "temperature" in data:
            lines.append(f"  Temperature: {data['temperature']} °C")

        if "torque_enabled" in data:
            lines.append(f"  Torque Enabled: {data['torque_enabled']}")

        if "moving" in data:
            lines.append(f"  Moving: {data['moving']}")

    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


def print_usage(parser: argparse.ArgumentParser) -> None:
    """Print a Rich-styled usage/help panel for the provided argparse parser.

    This renders a compact Usage line and a table of options using Rich. If
    `rich` is not installed we fall back to the standard argparse help text.
    """
    # If Rich was not importable at module scope, or key pieces are missing,
    # fall back to argparse help.
    if any(x is None for x in (Console, Text, Table, Panel, Group)):
        print(parser.format_help())
        return

    from rich.rule import Rule

    # Create local aliases so static analyzers understand these are the
    # Rich types we verified above.
    RichConsole = Console  # type: ignore
    RichText = Text  # type: ignore
    RichTable = Table  # type: ignore
    RichPanel = Panel  # type: ignore
    RichGroup = Group  # type: ignore

    console = RichConsole()  # type: ignore

    # Usage line
    usage = parser.format_usage().strip()
    usage_text = RichText.from_markup(f"[bold cyan]{usage}[/]")  # type: ignore

    # Build options table from parser actions
    table = RichTable.grid(padding=(0, 1))  # type: ignore
    table.add_column(ratio=3)
    table.add_column(ratio=7)
    table.add_column(ratio=2, justify="right")

    for action in parser._actions:
        # Skip the help action (we'll provide our own presentation)
        if isinstance(action, argparse._HelpAction):
            continue

        # Compose flag names (e.g., "-p, --port") or positional name
        if action.option_strings:
            flags = ", ".join(action.option_strings)
        else:
            flags = action.dest

        help_text = action.help or ""
        # Show default if it's not None and not argparse.SUPPRESS
        default = ""
        if getattr(action, "default", None) not in (None, argparse.SUPPRESS):
            default_val = action.default
            # Hide trivial defaults
            if default_val not in (None, False, True):
                default = str(default_val)

        table.add_row(
            RichText(flags, style="bold"),
            RichText(help_text),
            RichText(default, style="dim"),  # type: ignore
        )

    panel = RichPanel(RichGroup(usage_text, table), title="so101 read_device — Usage", expand=True)  # type: ignore

    console.print(Rule(style="grey66"))
    console.print(panel)
    console.print(Rule(style="grey66"))


def _make_bar(value: Optional[float], max_value: float = 1.0, width: int = 20) -> str:
    """Render a simple ASCII progress bar for a value in [0, max_value]."""
    if value is None:
        return "[" + " " * width + "]"
    try:
        ratio = max(0.0, min(1.0, float(value) / float(max_value)))
    except Exception:
        ratio = 0.0
    filled = int(round(ratio * width))
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def run_tui(
    port: str,
    motor_ids: List[int],
    motor_names: Dict[int, str],
    baudrate: int = 1000000,
    interval: float = 0.25,
    voltage_rating: Optional[int] = None,
    show_raw: bool = False,
    interactive_control: bool = False,
):
    """Run a simple terminal UI using rich that updates motor status live.

    Falls back with an instructive message if `rich` is not installed.
    """
    # Ensure Rich was importable at module scope and required pieces are present.
    if any(x is None for x in (Live, Table, Panel, box, Console, Group, Text, Align, Padding)):
        print("TUI requires the 'rich' package. Install it with: pip install rich", file=sys.stderr)
        sys.exit(1)

    console = Console()  # type: ignore

    # If interactive control is active, prepare port/packet handler and shared state
    input_thread = None
    stop_input = None
    goals: Dict[int, int] = {}
    # cache of the most recently read motor data (populated in the update loop)
    latest_motor_data: Dict[int, Dict] = {}
    last_status: str = ""
    selected_idx = 0
    port_handler = None
    packet_handler = None

    if interactive_control:
        import threading

        try:
            from scservo_sdk import PacketHandler, PortHandler
        except ImportError as err:
            print(
                "scservo_sdk is required for interactive control. Install it with: pip install scservo_sdk",
                file=sys.stderr,
            )
            raise

        stop_input = threading.Event()

        # Open port and packet handler
        port_handler = PortHandler(port)
        port_handler.setBaudRate(baudrate)
        if not port_handler.openPort():
            raise ConnectionError(f"Failed to open port: {port}")

        packet_handler = PacketHandler(0)
        # Try to load calibration data once so we can clamp goal updates to
        # a joint-specific range when available. If no calibration file is
        # present we fall back to the full encoder range 0..4095.
        calibration_data = None
        try:
            from pathlib import Path
            from utils import load_calibration

            calib_dir = Path.home() / ".so101_lib" / "calibrations"
            if calib_dir.exists():
                candidate = calib_dir / "leader1.json"
                if candidate.exists():
                    calibration_data = load_calibration(candidate)
                else:
                    files = list(calib_dir.glob("*.json"))
                    if files:
                        calibration_data = load_calibration(files[0])
        except Exception:
            calibration_data = None

        # Small helper to clamp a goal value to the calibrated per-joint range.
        # Defined here so it can close over `calibration_data` and `motor_names`.
        def clamp_goal_to_calibration(mid: int, goal_val: int) -> int:
            """Return goal_val clamped into the calibration range for joint `mid`.

            Falls back to the full encoder range 0..4095 if no calibration exists
            or if the calibration entry is malformed.
            """
            name = motor_names.get(mid, f"Motor {mid}")
            if calibration_data and name in calibration_data:
                try:
                    rmin = int(calibration_data[name].get("range_min", 0))
                    rmax = int(calibration_data[name].get("range_max", 4095))
                except Exception:
                    rmin, rmax = 0, 4095
            else:
                rmin, rmax = 0, 4095
            try:
                return max(rmin, min(rmax, int(goal_val)))
            except Exception:
                # If conversion fails, return a safe default
                return max(rmin, min(rmax, 0))

        # Initialize goal cache with present positions
        for mid in motor_ids:
            try:
                # Should be shifted to initialization
                accel_val = 10
                res, err = packet_handler.write2ByteTxRx(
                    port_handler, mid, ADDR_ACCELERATION[0], accel_val
                )     
                speed_val = 800  # example fixed speed; could be made adjustable
                speed_encoded = encode_sign_magnitude(int(speed_val), sign_bit=15)                    
                res, err = packet_handler.write2ByteTxRx(
                    port_handler, mid, ADDR_GOAL_VELOCITY[0], speed_encoded
                )                      
                val, result, error = packet_handler.read2ByteTxRx(
                    port_handler, mid, ADDR_PRESENT_POSITION[0]
                )
                if result == 0:
                    goals[mid] = int(val) & 0x0FFF
                else:
                    goals[mid] = 0
            except Exception:
                goals[mid] = 0

        selected_idx = 0
        step = 100

        def input_loop() -> None:
            """Background input loop for TUI interactive control using prompt_toolkit.

            This launches a prompt_toolkit Application in a background thread and
            returns immediately. Key handlers update shared state (`goals`,
            `selected_idx`, `last_status`) and call `stop_input.set()` when quitting.
            """
            nonlocal selected_idx, last_status, step

            # Require prompt_toolkit for TUI interactive mode
            from prompt_toolkit.key_binding import KeyBindings
            from prompt_toolkit.application import Application

            bindings = KeyBindings()

            # numeric selection bindings (1-6)
            for i in range(1, 7):

                def _make_sel(n):
                    def _sel(event=None):
                        nonlocal selected_idx
                        idx = n - 1
                        if idx < len(motor_ids):
                            selected_idx = idx

                    return _sel

                bindings.add(str(i))(_make_sel(i))

            # increase
            @bindings.add("+")
            @bindings.add("=")
            def _inc(event=None):
                mid = motor_ids[selected_idx]
                # Simple check against the last-known motor state: if the motor
                # was reported moving in the most recent read, skip increasing.
                m = latest_motor_data.get(mid, {})
                if m.get("moving"):
                    return
                # Increase goal
                new_goal = goals.get(mid, 0) + step
                # Clamp to per-joint calibration range (or 0..4095 fallback)
                new_goal = clamp_goal_to_calibration(mid, new_goal)

                try:
                    res, err = packet_handler.write2ByteTxRx(
                        port_handler, mid, ADDR_GOAL_POSITION[0], int(new_goal)
                    )
                except Exception:
                    lo = new_goal & 0xFF
                    hi = (new_goal >> 8) & 0xFF
                    data = [lo, hi]
                    try:
                        res, err = packet_handler.writeTxRx(
                            port_handler, mid, ADDR_GOAL_POSITION[0], 2, data
                        )
                    except Exception:
                        return
                try:
                    if res == scservo.COMM_SUCCESS:
                        goals[mid] = new_goal
                except Exception:
                    goals[mid] = new_goal

            # decrease
            @bindings.add("-")
            @bindings.add("_")
            def _dec(event=None):
                mid = motor_ids[selected_idx]
                # Simple check against the last-known motor state: if the motor
                # was reported moving in the most recent read, skip increasing.
                m = latest_motor_data.get(mid, {})
                if m.get("moving"):
                    return
                new_goal = goals.get(mid, 0) - step
                # Clamp to per-joint calibration range (or 0..4095 fallback)
                new_goal = clamp_goal_to_calibration(mid, new_goal)

                try:
                    res, err = packet_handler.write2ByteTxRx(
                        port_handler, mid, ADDR_GOAL_POSITION[0], int(new_goal)
                    )
                except Exception:
                    lo = new_goal & 0xFF
                    hi = (new_goal >> 8) & 0xFF
                    data = [lo, hi]
                    try:
                        res, err = packet_handler.writeTxRx(
                            port_handler, mid, ADDR_GOAL_POSITION[0], 2, data
                        )
                    except Exception:
                        return
                try:
                    if res == scservo.COMM_SUCCESS:
                        goals[mid] = new_goal
                except Exception:
                    goals[mid] = new_goal

            # show status
            @bindings.add("s")
            @bindings.add("S")
            def _show(event=None):
                nonlocal last_status
                mid = motor_ids[selected_idx]
                try:
                    pos_raw, result, error = packet_handler.read2ByteTxRx(
                        port_handler, mid, ADDR_PRESENT_POSITION[0]
                    )
                    if result == 0:
                        pos_raw = int(pos_raw) & 0x0FFF
                        last_status = f"Joint {selected_idx + 1} (ID={mid}) present={pos_raw}, goal={goals.get(mid)}"
                    else:
                        last_status = (
                            f"Read present position failed: result={result}, error={error}"
                        )
                except Exception as e:
                    last_status = f"Read failed: {e}"

            # quit
            @bindings.add("q")
            @bindings.add("Q")
            def _quit(event=None):
                if stop_input:
                    stop_input.set()
                # Guard access to event.app (some linters warn it can be None).
                try:
                    app = getattr(event, "app", None)
                    if app is not None:
                        app.exit()
                except Exception:
                    pass

            # Provide a minimal layout to avoid prompt_toolkit's "No layout specified" message.
            from prompt_toolkit.layout import Layout
            from prompt_toolkit.layout.containers import Window

            app = Application(layout=Layout(Window()), key_bindings=bindings, full_screen=False)

            # run the prompt_toolkit application in the background
            t = threading.Thread(target=app.run, daemon=True)
            t.start()

            # return: the background app will handle key events until stop_input is set

        input_thread = threading.Thread(target=input_loop, daemon=True)
        input_thread.start()

        # When interactive_control is enabled we must not open the serial port
        # again from read_motor_data (that would conflict). Create a small
        # shared-reader that uses the already-open packet_handler/port_handler.
        def read_motor_data_shared(motor_ids_local: List[int]) -> Dict:
            """Read motor data using the already-open packet_handler/port_handler."""
            motor_data_local: Dict[int, Dict] = {}
            for mid in motor_ids_local:
                data: Dict = {"id": mid, "error": None}
                try:
                    motor_id_read, result, error = packet_handler.read1ByteTxRx(
                        port_handler, mid, ADDR_ID[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["motor_id"] = motor_id_read

                    model_number, result, error = packet_handler.read2ByteTxRx(
                        port_handler, mid, ADDR_MODEL_NUMBER[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["model_number"] = model_number

                    position_raw, result, error = packet_handler.read2ByteTxRx(
                        port_handler, mid, ADDR_PRESENT_POSITION[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["present_position_raw"] = position_raw
                        position_raw = int(position_raw) & 0x0FFF
                        data["present_position"] = max(0, min(4095, position_raw))

                    goal_raw, result, error = packet_handler.read2ByteTxRx(
                        port_handler, mid, ADDR_GOAL_POSITION[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        goal_raw = int(goal_raw) & 0x0FFF
                        data["goal_position"] = max(0, min(4095, goal_raw))

                    speed_raw, result, error = packet_handler.read2ByteTxRx(
                        port_handler, mid, ADDR_PRESENT_VELOCITY[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["present_speed_raw"] = speed_raw
                        data["present_speed"] = float(
                            decode_sign_magnitude(speed_raw, sign_bit=ENCODING_BIT_VELOCITY)
                        )

                    load_raw, result, error = packet_handler.read2ByteTxRx(
                        port_handler, mid, ADDR_PRESENT_LOAD[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["present_load_raw"] = load_raw
                        data["present_load"] = float(decode_sign_magnitude(load_raw))

                    voltage, result, error = packet_handler.read1ByteTxRx(
                        port_handler, mid, ADDR_PRESENT_VOLTAGE[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["voltage_raw"] = voltage
                        data["voltage"] = voltage / 10.0

                    # temperature, torque, moving
                    temperature, result, error = packet_handler.read1ByteTxRx(
                        port_handler, mid, ADDR_PRESENT_TEMPERATURE[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["temperature"] = temperature

                    torque_enable, result, error = packet_handler.read1ByteTxRx(
                        port_handler, mid, ADDR_TORQUE_ENABLE[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["torque_enabled"] = bool(torque_enable)

                    moving, result, error = packet_handler.read1ByteTxRx(
                        port_handler, mid, ADDR_MOVING[0]
                    )
                    if result == scservo.COMM_SUCCESS:
                        data["moving"] = bool(moving)

                except Exception as e:
                    data["error"] = str(e)

                motor_data_local[mid] = data

            return motor_data_local

    def build_table(motor_data: Dict) -> Any:
        # Do not show a per-table title; the surrounding Panel displays status.
        table = Table(box=box.SIMPLE_HEAVY)  # type: ignore
        table.add_column("Joint", no_wrap=True)
        table.add_column("Id", no_wrap=True)
        table.add_column("Position", no_wrap=True)
        table.add_column("Pos (norm/raw)", no_wrap=True)
        table.add_column("Goal", no_wrap=True)
        table.add_column("Speed", no_wrap=True, width=16)
        table.add_column("Load", no_wrap=True, width=20)
        table.add_column("Voltage", no_wrap=True)
        table.add_column("Temp", no_wrap=True)
        table.add_column("Moving", no_wrap=True)

        for motor_id, data in sorted(motor_data.items()):
            name = motor_names.get(motor_id, f"Motor {motor_id}")

            # Highlight selected joint when interactive control is active
            is_selected = False
            try:
                if interactive_control and motor_ids.index(motor_id) == selected_idx:
                    is_selected = True
            except Exception:
                is_selected = False

            id = data.get("motor_id")

            pos = data.get("present_position")
            pos_bar = (
                _make_bar(pos, max_value=4095.0, width=20) if pos is not None else _make_bar(None)
            )
            pos_text = Text(f"{pos_bar} {pos if pos is not None else '-'}", style="white")  # type: ignore

            # Build a separate cell that shows raw encoder counts and normalized value side-by-side.
            pos_raw = data.get("present_position_raw", data.get("present_position"))
            pos_norm: Optional[float] = None
            calibration_data = None
            try:
                from pathlib import Path
                from utils import load_calibration, convert_position_with_calibration

                calib_dir = Path.home() / ".so101_lib" / "calibrations"
                if calib_dir.exists():
                    # Prefer leader1.json if present, otherwise pick the first JSON file
                    candidate = calib_dir / "leader1.json"
                    if candidate.exists():
                        calibration_data = load_calibration(candidate)
                    else:
                        files = list(calib_dir.glob("*.json"))
                        if files:
                            calibration_data = load_calibration(files[0])
            except Exception:
                calibration_data = None

            # Try to compute a normalized value using calibration when available, otherwise fall back
            # to a simple linear mapping based on expected norm_mode for the motor.
            try:
                # Use the masked/clamped 12-bit present_position value for normalization when available.
                base_position = pos if pos is not None else pos_raw
                if base_position is not None:
                    motor_def = SO101_LEADER_MOTORS.get(name)
                    # Prepare calibration mapping for the call. If a calibration file exists and contains
                    # this joint, use it. Otherwise synthesize a sensible default so the conversion
                    # function still returns a valid normalized value.
                    if calibration_data and name in calibration_data:
                        calib_for_call = calibration_data
                    else:
                        # Synthesize minimal calibration entry for the joint: full 0..4095 range
                        calib_for_call = {
                            name: {"range_min": 0.0, "range_max": 4095.0, "drive_mode": 0}
                        }

                    # Determine norm_mode; default to RANGE_M100_100 if unknown
                    from motors.models import MotorNormMode

                    norm_mode = MotorNormMode.RANGE_M100_100
                    if motor_def is not None:
                        norm_mode = motor_def.norm_mode

                    # Call centralized conversion routine which implements calibration-aware mapping
                    pos_norm = convert_position_with_calibration(
                        float(base_position), name, calib_for_call, norm_mode
                    )  # type: ignore
            except Exception:
                # If anything goes wrong, leave pos_norm as None and display the raw value only
                pos_norm = None

            if pos_raw is None:
                pos_raw_norm_text = Text("-", style="dim")  # type: ignore
            else:
                try:
                    raw_int = int(pos_raw)
                except Exception:
                    raw_int = pos_raw
                if pos_norm is None:
                    pos_raw_norm_text = Text(f"{raw_int}", style="dim")  # type: ignore
                else:
                    pos_raw_norm_text = Text(f"{pos_norm:.1f} / {raw_int}", style="white")  # type: ignore

            # Append calibrated range_min..range_max for this joint if available.
            # Fall back to the full encoder range [0-4095] when no calibration exists.
            try:
                if calibration_data and name in calibration_data:
                    range_min_val = calibration_data[name].get("range_min", 0)
                    range_max_val = calibration_data[name].get("range_max", 4095)
                else:
                    range_min_val = 0
                    range_max_val = 4095
                # Append as dim text to avoid overpowering the primary numbers
                pos_raw_norm_text.append(
                    f" [{int(range_min_val)}-{int(range_max_val)}]", style="dim"
                )  # type: ignore
            except Exception:
                # Non-fatal: if something goes wrong, don't break the UI
                pass

            goal = data.get("goal_position")
            goal_text = Text(str(goal) if goal is not None else "-", style="cyan")  # type: ignore

            speed = data.get("present_speed")
            if speed is None:
                speed_text = Text("-", style="dim")  # type: ignore
            else:
                speed_text = Text(f"{speed}", style="magenta")  # type: ignore
                if show_raw and "present_speed_raw" in data:
                    # append raw register value in dim style for debugging
                    speed_text.append(f" (raw: {data['present_speed_raw']})", style="dim")  # type: ignore

            load = data.get("present_load")
            load_bar = _make_bar(
                abs(load) if load is not None else None, max_value=1023.0, width=10
            )
            # color load
            if load is None:
                load_cell = Text(f"{load_bar} -", style="dim")  # type: ignore
            else:
                if abs(load) > 800:
                    load_style = "bold red"
                elif abs(load) > 400:
                    load_style = "yellow"
                else:
                    load_style = "green"
                load_cell = Text(f"{load_bar} {load}", style=load_style)  # type: ignore
                # Do not show raw load here; dedicated Pos (raw/norm) column shows raw encoder counts

            voltage = data.get("voltage")
            # color voltage based on rating when possible
            if voltage is None:
                voltage_cell = Text("-", style="dim")  # type: ignore
            else:
                if voltage_rating in (5, 12):
                    if voltage < (voltage_rating - 1.0):
                        v_style = "red"
                    elif voltage < voltage_rating:
                        v_style = "yellow"
                    else:
                        v_style = "green"
                else:
                    # generic thresholds
                    if voltage < 3.8:
                        v_style = "red"
                    elif voltage < 4.8:
                        v_style = "yellow"
                    else:
                        v_style = "green"
                voltage_cell = Text(f"{voltage:.1f}V", style=v_style)  # type: ignore

            temp = data.get("temperature")
            if temp is None:
                temp_cell = Text("-", style="dim")  # type: ignore
            else:
                if temp > 70:
                    t_style = "bold red"
                elif temp > 50:
                    t_style = "yellow"
                else:
                    t_style = "green"
                temp_cell = Text(f"{temp}°C", style=t_style)  # type: ignore

            moving = data.get("moving")
            if moving is None:
                moving_cell = Text("-", style="dim")  # type: ignore
            else:
                moving_cell = Text("Yes" if moving else "No", style=("green" if moving else "red"))  # type: ignore

            name_style = "bold yellow" if is_selected else "bold white"

            table.add_row(
                Text(name, style=name_style),  # type: ignore
                Text(str(id) if id is not None else "-", style="cyan"),  # type: ignore
                pos_text,
                pos_raw_norm_text,
                goal_text,
                speed_text,
                load_cell,
                voltage_cell,
                temp_cell,
                moving_cell,
            )  # type: ignore

        title_extra = f" Port: {port} | Baud: {baudrate} | Interval: {interval}s"
        if voltage_rating:
            title_extra += f" | Voltage rating: {voltage_rating}V"
        # Show a short interactive status message if available
        try:
            if interactive_control and last_status:
                # trim status to avoid overflowing the title
                short = last_status if len(last_status) <= 60 else last_status[:57] + "..."
                title_extra += f" | {short}"
        except Exception:
            pass

        # add a small top padding inside the panel so the table title is spaced down
        padded_table = Padding(table, (1, 0, 0, 0))  # type: ignore
        # center the table within the panel
        content = Align.center(Group(padded_table), vertical="middle")  # type: ignore
        panel = Panel(content, title="SO101 Status" + title_extra, expand=True)  # type: ignore

        # add a small top padding so the panel title isn't flush with the terminal top
        # use Rich's Padding for consistent layout
        return Padding(panel, (1, 0, 0, 0))  # type: ignore

    try:
        # Build an empty motor_data mapping so we can render a table immediately
        # (this is fast) while the first, potentially slow, serial reads occur
        # inside the update loop. Each motor id gets an empty dict; build_table
        # will render placeholders for missing fields.
        motor_data_empty: Dict[int, Dict] = {mid: {} for mid in motor_ids}

        # Clear the terminal so the Live panel appears on a clean screen and
        # doesn't get mixed with prior stdout output. Use ANSI escape which
        # works on POSIX terminals (Linux/macOS). We avoid os.system('clear') to
        # keep this lightweight and not spawn a subprocess.
        print("\033[2J\033[H", end="", flush=True)

        # Render the table built from the empty motor data immediately so the
        # UI is visible while the first read cycle completes.
        with Live(build_table(motor_data_empty), screen=True, refresh_per_second=4) as live:  # type: ignore
            while True:
                # If interactive_control is active and the input thread signalled
                # a stop (via 'q'), break the loop so the Live context exits and
                # we perform cleanup. This makes a single 'q' sufficient to quit.
                if interactive_control and stop_input is not None and stop_input.is_set():
                    break

                if interactive_control:
                    motor_data = read_motor_data_shared(motor_ids)
                    # Update the cache used by input handlers so they can consult
                    # the most recent 'moving' state without issuing extra reads.
                    latest_motor_data = motor_data
                    # Add goals to motor_data for display
                    for mid, g in goals.items():
                        if mid in motor_data:
                            motor_data[mid]["goal_position"] = g
                else:
                    motor_data = read_motor_data(port, motor_ids, baudrate)

                live.update(build_table(motor_data))
                time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\nStopped TUI by user.")
    finally:
        # Cleanup interactive resources
        if interactive_control:
            try:
                if stop_input:
                    stop_input.set()
            except Exception:
                pass
            if input_thread is not None:
                input_thread.join(timeout=1.0)
            try:
                if port_handler:
                    port_handler.closePort()
            except Exception:
                pass


def main():
    parser = argparse.ArgumentParser(description="Read and display data from SO101 device motors")

    parser.add_argument(
        "--port",
        type=str,
        help="Serial port path (e.g., /dev/tty.usbmodem123)",
    )
    parser.add_argument(
        "--find-port",
        action="store_true",
        help="Interactively find the port",
    )
    parser.add_argument(
        "--motor-ids",
        type=str,
        default="1,2,3,4,5,6",
        help="Comma-separated list of motor IDs to read (default: 1,2,3,4,5,6)",
    )
    parser.add_argument(
        "--baudrate",
        type=int,
        default=1000000,
        help="Serial communication baudrate (default: 1000000)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.1,
        help="Update interval in seconds for continuous mode (default: 0.1)",
    )
    parser.add_argument(
        "--voltage-rating",
        type=int,
        choices=[5, 12],
        help="Voltage rating (5V or 12V). If not specified, will attempt to detect.",
    )
    parser.add_argument(
        "--show-raw",
        action="store_true",
        help="Show raw register values for debugging",
    )
    parser.add_argument(
        "--tui",
        action="store_true",
        help="Show a live terminal UI (requires 'rich')",
    )

    # If the user asked for help, show our Rich-styled usage and exit
    if "-h" in sys.argv or "--help" in sys.argv:
        print_usage(parser)
        sys.exit(0)

    args = parser.parse_args()

    # Determine port
    port = args.port
    if args.find_port:
        try:
            port = find_port()
        except Exception as e:
            print(f"Error finding port: {e}", file=sys.stderr)
            sys.exit(1)
    elif not port:
        # Try to find available ports
        available_ports = find_available_ports()
        if len(available_ports) == 1:
            port = available_ports[0]
            print(f"Using auto-detected port: {port}")
        elif len(available_ports) > 1:
            print("Multiple ports available. Please specify --port or use --find-port:")
            for p in available_ports:
                print(f"  {p}")
            sys.exit(1)
        else:
            print("No ports found. Please specify --port or use --find-port.", file=sys.stderr)
            sys.exit(1)

    # Parse motor IDs
    try:
        motor_ids = [int(id.strip()) for id in args.motor_ids.split(",")]
    except ValueError:
        print(f"Invalid motor IDs: {args.motor_ids}", file=sys.stderr)
        sys.exit(1)

    # Create motor name mapping from SO101_LEADER_MOTORS
    motor_names = {motor.id: name for name, motor in SO101_LEADER_MOTORS.items()}

    # Detect or use specified voltage rating
    voltage_rating = args.voltage_rating
    if voltage_rating is None:
        print("Attempting to detect voltage rating...")
        voltage_rating = detect_voltage_rating(
            port, motor_ids[0] if motor_ids else 1, args.baudrate
        )
        if voltage_rating:
            print(f"Detected voltage rating: {voltage_rating}V")
        else:
            print("Could not detect voltage rating. Use --voltage-rating to specify (5 or 12).")

    # If TUI requested, run it
    if args.tui:
        run_tui(
            port,
            motor_ids,
            motor_names,
            baudrate=args.baudrate,
            interval=args.interval,
            voltage_rating=voltage_rating,
            show_raw=args.show_raw,
            interactive_control=True,
        )
        return

    try:
        # Single-shot read (continuous mode removed). Perform one read and print.
        motor_data = read_motor_data(port, motor_ids, args.baudrate)
        output = format_motor_data(motor_data, motor_names, voltage_rating, args.show_raw)
        print(output)

    except Exception as e:
        print(f"Error reading device: {e}", file=sys.stderr)
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
