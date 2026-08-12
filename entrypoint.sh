#!/bin/sh
set -e

# If CYBERWAVE_TWIN_JSON_FILE is set and the file exists,
# read the JSON and export each top-level key-value pair as a CYBERWAVE_* env var.
if [ -n "$CYBERWAVE_TWIN_JSON_FILE" ] && [ -f "$CYBERWAVE_TWIN_JSON_FILE" ]; then
    eval "$(python3 -c "
import json, os, re, shlex

with open(os.environ['CYBERWAVE_TWIN_JSON_FILE']) as f:
    data = json.load(f)

_VALID_ENV_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
_INVALID_ENV_CHARS = re.compile(r'[^A-Za-z0-9_]')

def sanitize_key(key):
    return _INVALID_ENV_CHARS.sub('_', str(key)).upper()

def export_vars(data, prefix='CYBERWAVE'):
    for key, value in data.items():
        if prefix == 'CYBERWAVE' and key == 'uuid':
            env_name = 'CYBERWAVE_TWIN_UUID'
        else:
            sanitized_key = sanitize_key(key)
            env_name = prefix + '_' + sanitized_key
        if not _VALID_ENV_NAME.match(env_name):
            continue
        # Don't override env vars that were explicitly passed to the container
        if env_name in os.environ:
            continue
        if isinstance(value, dict):
            export_vars(value, env_name)
        elif isinstance(value, list):
            print(f'export {env_name}={shlex.quote(json.dumps(value))}')
        else:
            print(f'export {env_name}={shlex.quote(str(value))}')

export_vars(data)
")"
fi

# Ensure CYBERWAVE_EDGE_CONFIG_DIR is set (edge-core passes this)
if [ -z "$CYBERWAVE_EDGE_CONFIG_DIR" ]; then
    export CYBERWAVE_EDGE_CONFIG_DIR="/app/.cyberwave"
fi

# --- USB/IP device passthrough (macOS Docker Desktop) ---
# When CYBERWAVE_USBIP_ENABLED=1 (set by edge-core on macOS), use nsenter
# to attach USB devices from the host via USB/IP. Requires --pid=host and
# --privileged on the docker run command, plus a USB/IP server on the host.
if [ "$CYBERWAVE_USBIP_ENABLED" = "1" ] || [ "$CYBERWAVE_USBIP_ENABLED" = "true" ]; then
    _usbip_attach() {
        if ! command -v nsenter >/dev/null 2>&1; then
            echo "[usbip] nsenter not found, skipping USB/IP attachment"
            return 0
        fi

        # Verify --pid=host is in effect by checking that PID 1 is a known VM
        # init process.  Without --pid=host, nsenter -t 1 targets the
        # container's own PID 1, which would attach devices to the wrong
        # namespace.  We use an allowlist of known VM init names rather than
        # a blocklist so that an unrecognised name fails safe (warn + skip)
        # instead of proceeding blindly into the wrong namespace.
        _pid1_comm=""
        if [ -r /proc/1/comm ]; then
            _pid1_comm=$(cat /proc/1/comm 2>/dev/null || true)
        fi
        case "$_pid1_comm" in
            init|initd|systemd|openrc-init)
                ;;
            "")
                ;;
            *)
                echo "[usbip] WARNING: --pid=host does not appear to be set (PID 1 is '$_pid1_comm')."
                echo "[usbip] nsenter requires --pid=host; skipping USB/IP attachment."
                return 0
                ;;
        esac

        USBIP_HOST="${CYBERWAVE_USBIP_HOST:-host.docker.internal}"
        # VID:PID of the SO101 arm serial bus (WCH). Used below to wait on the
        # exact number of arms rather than "any serial device". Override with
        # CYBERWAVE_USBIP_SERIAL_VIDPID if a different adapter is in use.
        USBIP_SERIAL_VIDPID="${CYBERWAVE_USBIP_SERIAL_VIDPID:-1a86:55d3}"
        _attached=0
        _expected_serial=0

        # Detach any stale USB/IP imports left over from previous container
        # runs before attaching fresh. On macOS every driver container shares
        # Docker Desktop's single Linux VM, so imports survive `docker rm` and
        # pile up (ttyACM0..N) across restarts; once autodiscovery probes one
        # of those zombie nodes the container can wedge (docker stop / kill -9
        # / rm -f all hang, requiring a full Docker Desktop restart). Starting
        # each run from a clean slate avoids the pileup. Disable with
        # CYBERWAVE_USBIP_DETACH_STALE=0.
        if [ "${CYBERWAVE_USBIP_DETACH_STALE:-1}" != "0" ]; then
            _stale_ports=$(nsenter -t 1 -m -- usbip port 2>/dev/null \
                | sed -n 's/^Port \([0-9][0-9]*\):.*/\1/p' \
                || true)
            for _p in $_stale_ports; do
                echo "[usbip] Detaching stale import on port $_p"
                nsenter -t 1 -m -- usbip detach -p "$_p" 2>&1 || true
            done
        fi

        if [ -n "$CYBERWAVE_USBIP_BUSID" ]; then
            echo "[usbip] Attaching device $CYBERWAVE_USBIP_BUSID from $USBIP_HOST"
            _expected_serial=1
            if nsenter -t 1 -m -- usbip attach -r "$USBIP_HOST" -d "$CYBERWAVE_USBIP_BUSID" 2>&1; then
                _attached=1
            else
                echo "[usbip] Attach failed for $CYBERWAVE_USBIP_BUSID (device may already be attached)"
            fi
        else
            echo "[usbip] Auto-discovering USB devices from $USBIP_HOST..."
            LIST_OUTPUT=$(nsenter -t 1 -m -- usbip list -r "$USBIP_HOST" 2>/dev/null || true)
            # usbip list output groups each device as:
            #   "  1-1-1: Vendor : Product (idVendor:idProduct)"
            #   "       : /sys/bus/1/1/1"
            #   "       : Class Description (bDeviceClass/bDeviceSubClass/bDeviceProtocol)"
            #   "        0 - Interface description ..."
            # Extract bus IDs (digits-and-dashes before the first colon), skipping
            # Hub-class (bDeviceClass 09) devices: attaching a root/internal hub
            # (every Mac exports at least one) makes the host USB/IP server's
            # control-transfer handling panic — confirmed by attaching a hub bus ID
            # in isolation and observing "not implemented: control out" in the
            # host's log, which then empties its exportable-device list until the
            # launchd service is restarted. We never need the hub itself via
            # USB/IP anyway; the devices behind it are exported individually.
            BUSIDS=$(printf '%s\n' "$LIST_OUTPUT" | awk '
                /^[[:space:]]*[0-9][0-9]*-[0-9][0-9-]*[[:space:]]*:/ {
                    if (busid != "" && !is_hub) print busid
                    match($0, /[0-9][0-9]*-[0-9][0-9-]*/)
                    busid = substr($0, RSTART, RLENGTH)
                    line = 0
                    is_hub = 0
                    next
                }
                {
                    line++
                    if (line == 2 && $0 ~ /Hub \//) is_hub = 1
                }
                END {
                    if (busid != "" && !is_hub) print busid
                }
            ' || true)

            if [ -z "$BUSIDS" ]; then
                echo "[usbip] No exportable USB devices found on $USBIP_HOST"
                return 0
            fi

            # Expected serial count = number of exported devices whose VID:PID
            # is the SO101 arm bus. The readiness wait below blocks until this
            # many /dev/ttyACM* nodes exist, so we don't proceed with only the
            # leader bound while the follower is still enumerating.
            _expected_serial=$(printf '%s\n' "$LIST_OUTPUT" \
                | grep -c "($USBIP_SERIAL_VIDPID)" \
                || true)
            echo "[usbip] Exported arm serial device(s) matching $USBIP_SERIAL_VIDPID: ${_expected_serial}"

            for busid in $BUSIDS; do
                echo "[usbip] Attaching device $busid"
                if nsenter -t 1 -m -- usbip attach -r "$USBIP_HOST" -d "$busid" 2>&1; then
                    _attached=$((_attached + 1))
                else
                    echo "[usbip] Attach failed for $busid (device may already be attached)"
                fi
            done
        fi

        if [ "$_attached" -eq 0 ] 2>/dev/null; then
            echo "[usbip] No new devices attached (may already be attached from a previous run)"
        fi

        # Poll for serial devices (ttyACM* / ttyUSB*) instead of a fixed sleep.
        # Default 30s; override with CYBERWAVE_USBIP_WAIT_SECS.
        #
        # Wait for the EXPECTED number of arm serial nodes (one per exported
        # $USBIP_SERIAL_VIDPID device) rather than breaking out as soon as the
        # FIRST /dev/ttyACM* appears. Auto-discovery attaches several devices
        # (both arms + camera + hub) one at a time, so the leader arm often
        # enumerates seconds before the follower; the old "any serial device"
        # check raced and returned with only one arm bound, leaving the other
        # unusable — same code, same hardware, different outcome per run. When
        # the expected count is unknown (explicit-busid or no VID/PID match) we
        # fall back to "at least one".
        #
        # Count each glob element via `[ -e ]`: an unmatched glob stays literal
        # in POSIX sh, and `[ -e /dev/ttyUSB* ]` on the literal is simply false,
        # so this is robust even though the SO101's WCH chips only ever create
        # /dev/ttyACM* (never /dev/ttyUSB*).
        _serial_count() {
            _c=0
            for _d in /dev/ttyACM* /dev/ttyUSB*; do
                [ -e "$_d" ] && _c=$((_c + 1))
            done
            echo "$_c"
        }
        _have_expected_serial() {
            _n=$(_serial_count)
            if [ "$_expected_serial" -gt 0 ] 2>/dev/null; then
                [ "$_n" -ge "$_expected_serial" ]
            else
                [ "$_n" -ge 1 ]
            fi
        }
        USBIP_WAIT="${CYBERWAVE_USBIP_WAIT_SECS:-30}"
        if _have_expected_serial; then
            echo "[usbip] Serial devices found immediately:"
            ls -la /dev/ttyACM* /dev/ttyUSB* 2>/dev/null
        else
            if [ "$_expected_serial" -gt 0 ] 2>/dev/null; then
                echo "[usbip] Waiting up to ${USBIP_WAIT}s for ${_expected_serial} arm serial device(s)..."
            else
                echo "[usbip] Waiting up to ${USBIP_WAIT}s for serial devices..."
            fi
            _elapsed=0
            while [ "$_elapsed" -lt "$USBIP_WAIT" ]; do
                sleep 1
                _elapsed=$((_elapsed + 1))
                if _have_expected_serial; then
                    echo "[usbip] Serial devices found after ${_elapsed}s:"
                    ls -la /dev/ttyACM* /dev/ttyUSB* 2>/dev/null
                    break
                fi
            done
            if ! _have_expected_serial; then
                echo "[usbip] Expected ${_expected_serial} arm serial device(s); found $(_serial_count) after ${USBIP_WAIT}s"
            fi
        fi

        # Video devices (UVC cameras) take longer to enumerate than serial.
        # Only poll when explicitly requested via CYBERWAVE_USBIP_VIDEO_TIMEOUT_SECS
        # (edge-core sets this when the twin has camera attachments).
        USBIP_VIDEO_TIMEOUT="${CYBERWAVE_USBIP_VIDEO_TIMEOUT_SECS:-0}"
        if [ "$USBIP_VIDEO_TIMEOUT" -gt 0 ] 2>/dev/null; then
            if ls /dev/video* >/dev/null 2>&1; then
                echo "[usbip] Video devices found immediately:"
                ls -la /dev/video* 2>/dev/null
            else
                echo "[usbip] Waiting up to ${USBIP_VIDEO_TIMEOUT}s for video devices..."
                _elapsed=0
                while [ "$_elapsed" -lt "$USBIP_VIDEO_TIMEOUT" ]; do
                    sleep 1
                _elapsed=$((_elapsed + 1))
                if ls /dev/video* >/dev/null 2>&1; then
                        echo "[usbip] Video devices found after ${_elapsed}s:"
                        ls -la /dev/video* 2>/dev/null
                        break
                    fi
                done
                if ! ls /dev/video* >/dev/null 2>&1; then
                    echo "[usbip] No video devices detected after ${USBIP_VIDEO_TIMEOUT}s"
                fi
            fi
        fi

        if command -v lsusb >/dev/null 2>&1; then
            _rs_devices=$(lsusb 2>/dev/null | grep -i "8086:" || true)
            if [ -n "$_rs_devices" ]; then
                echo "[usbip] RealSense USB device(s) detected:"
                echo "$_rs_devices"
            fi
        fi
    }
    _usbip_attach || true
fi

# Use python3 explicitly: the Dockerfile installs python3 from apt and does
# not create a python -> python3 symlink (no python-is-python3).
exec python3 main.py "$@"
