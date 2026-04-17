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
        _attached=0

        if [ -n "$CYBERWAVE_USBIP_BUSID" ]; then
            echo "[usbip] Attaching device $CYBERWAVE_USBIP_BUSID from $USBIP_HOST"
            if nsenter -t 1 -m -- usbip attach -r "$USBIP_HOST" -d "$CYBERWAVE_USBIP_BUSID" 2>&1; then
                _attached=1
            else
                echo "[usbip] Attach failed for $CYBERWAVE_USBIP_BUSID (device may already be attached)"
            fi
        else
            echo "[usbip] Auto-discovering USB devices from $USBIP_HOST..."
            # usbip list output: "  1-1-1: Vendor : Product (idVendor:idProduct)"
            # Extract bus IDs (digits-and-dashes before the first colon).
            BUSIDS=$(nsenter -t 1 -m -- usbip list -r "$USBIP_HOST" 2>/dev/null \
                | sed -n 's/^[[:space:]]*\([0-9][0-9]*-[0-9][0-9-]*\)[[:space:]]*:.*/\1/p' \
                || true)

            if [ -z "$BUSIDS" ]; then
                echo "[usbip] No exportable USB devices found on $USBIP_HOST"
                return 0
            fi

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
        # Default 10s; override with CYBERWAVE_USBIP_WAIT_SECS.
        USBIP_WAIT="${CYBERWAVE_USBIP_WAIT_SECS:-10}"
        if ls /dev/ttyACM* /dev/ttyUSB* >/dev/null 2>&1; then
            echo "[usbip] Serial devices found immediately:"
            ls -la /dev/ttyACM* /dev/ttyUSB* 2>/dev/null
        else
            echo "[usbip] Waiting up to ${USBIP_WAIT}s for serial devices..."
            _elapsed=0
            while [ "$_elapsed" -lt "$USBIP_WAIT" ]; do
                sleep 1
                _elapsed=$((_elapsed + 1))
                if ls /dev/ttyACM* /dev/ttyUSB* >/dev/null 2>&1; then
                    echo "[usbip] Serial devices found after ${_elapsed}s:"
                    ls -la /dev/ttyACM* /dev/ttyUSB* 2>/dev/null
                    break
                fi
            done
            if ! ls /dev/ttyACM* /dev/ttyUSB* >/dev/null 2>&1; then
                echo "[usbip] No serial devices detected after ${USBIP_WAIT}s"
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

exec python main.py "$@"
