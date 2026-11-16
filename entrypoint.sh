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

# --- macOS serial bridge ---
# macOS keeps AppleUSBACM attached to the SO101's CDC interface, so a USB/IP
# import enumerates the device but its bulk endpoints stay inert. When the host
# publishes each arm as a TCP listener (a pyserial launchd service, same shape as
# the camera MJPEG and audio bridges), turn those back into local PTYs instead.
# Both paths create /dev/ttyACM*, so this one wins when configured.
_serial_bridge_active=0
if [ -n "$CYBERWAVE_SERIAL_BRIDGE_PORTS" ]; then
    _CW_SCRIPT_DIR=$(dirname "$0")
    if [ -f "$_CW_SCRIPT_DIR/serial_bridge_lib.sh" ]; then
        . "$_CW_SCRIPT_DIR/serial_bridge_lib.sh"
        _serial_bridge_active=1
        _start_serial_bridges "$CYBERWAVE_SERIAL_BRIDGE_PORTS" \
            "${CYBERWAVE_SERIAL_BRIDGE_HOST:-host.docker.internal}"
    else
        echo "[serial-bridge] WARNING: serial_bridge_lib.sh missing from image"
    fi
fi

# --- USB/IP device passthrough (macOS Docker Desktop) ---
# When CYBERWAVE_USBIP_ENABLED=1 (set by edge-core on macOS), use nsenter
# to attach USB devices from the host via USB/IP. Requires --pid=host and
# --privileged on the docker run command, plus a USB/IP server on the host.
#
# ``--usbip-attach-only`` runs the attach half alone and prints the resulting
# VM device paths. edge-core invokes that mode in a throwaway container before
# creating the driver container, so the nodes already exist when Docker
# snapshots the VM's device list (a container cannot see devices it attaches
# itself — the snapshot is taken at creation time).
_usbip_attach_only=0
if [ "$1" = "--usbip-attach-only" ]; then
    _usbip_attach_only=1
    shift
fi

if [ "$_serial_bridge_active" = "1" ] && [ "$_usbip_attach_only" != "1" ]; then
    # Skips the whole block, video included: on macOS the kernel keeps its class
    # drivers attached, so a USB/IP import enumerates a device without ever
    # carrying its data. Cameras go through the MJPEG stream URL instead, and
    # edge-core drops CYBERWAVE_USBIP_ENABLED / --pid=host in bridge mode so the
    # two sides agree that USB/IP is not in play.
    echo "[usbip] Serial bridge in use; skipping USB/IP passthrough (serial + video)"
elif [ "$_usbip_attach_only" = "1" ] || [ "$CYBERWAVE_USBIP_ENABLED" = "1" ] || [ "$CYBERWAVE_USBIP_ENABLED" = "true" ]; then
    # Device-node helpers, sourced only on this macOS-only path. The image is
    # published for linux/amd64+arm64 and macOS Docker Desktop runs that same
    # Linux image, so the file cannot be excluded at build time — a Linux edge
    # host simply never reads it.
    _CW_SCRIPT_DIR=$(dirname "$0")
    if [ -f "$_CW_SCRIPT_DIR/usbip_lib.sh" ]; then
        . "$_CW_SCRIPT_DIR/usbip_lib.sh"
    fi

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
        # Resolve here, in the container's namespace: the attach below crosses
        # into the VM's netns where Docker's embedded DNS cannot be reached.
        USBIP_HOST_ADDR=$(_usbip_resolve_host "$USBIP_HOST")
        if [ "$USBIP_HOST_ADDR" != "$USBIP_HOST" ]; then
            echo "[usbip] Resolved $USBIP_HOST to $USBIP_HOST_ADDR"
        fi
        # VID:PID of the SO101 arm serial bus (WCH). Used below to wait on the
        # exact number of arms rather than "any serial device". Override with
        # CYBERWAVE_USBIP_SERIAL_VIDPID if a different adapter is in use.
        USBIP_SERIAL_VIDPID="${CYBERWAVE_USBIP_SERIAL_VIDPID:-1a86:55d3}"
        _attached=0
        _expected_serial=0
        _usbip_ready_serial_nodes=""

        # Count each glob element via `[ -e ]`: an unmatched glob stays literal
        # in POSIX sh, and `[ -e /dev/ttyUSB* ]` on the literal is simply false,
        # so this is robust even though the SO101's WCH chips only ever create
        # /dev/ttyACM* (never /dev/ttyUSB*). Counts this *container's* /dev,
        # which is what the driver will actually open.
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

        # Fast path: edge-core already attached these buses and mapped them in
        # with --device before creating this container. Re-running discovery
        # would detach the very imports we depend on and duplicate the rest, so
        # only top up any node that did not make it through, then stop.
        if [ "${CYBERWAVE_USBIP_PREATTACHED:-0}" = "1" ] && [ "$_usbip_attach_only" != "1" ]; then
            _usbip_mirror_nodes
            if [ "$(_serial_count)" -gt 0 ]; then
                echo "[usbip] Using device(s) pre-attached by edge-core:"
                ls -la /dev/ttyACM* /dev/ttyUSB* 2>/dev/null
                return 0
            fi
            echo "[usbip] Pre-attached devices are missing; running in-container attach"
        fi

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
            if _usbip_attach_device "$USBIP_HOST_ADDR" "$CYBERWAVE_USBIP_BUSID" 2>&1; then
                _attached=1
            else
                echo "[usbip] Attach failed for $CYBERWAVE_USBIP_BUSID (device may already be attached)"
            fi
        else
            echo "[usbip] Auto-discovering USB devices from $USBIP_HOST..."
            LIST_OUTPUT=$(nsenter -t 1 -m -- usbip list -r "$USBIP_HOST_ADDR" 2>/dev/null || true)
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
                if _usbip_attach_device "$USBIP_HOST_ADDR" "$busid" 2>&1; then
                    _attached=$((_attached + 1))
                else
                    echo "[usbip] Attach failed for $busid (device may already be attached)"
                fi
            done
        fi

        # Attach-only mode runs in a throwaway container with no --device
        # mappings, so the nodes will never appear in *its* /dev. Wait on the
        # VM's namespace instead and report what the next container can map.
        if [ "$_usbip_attach_only" = "1" ]; then
            _elapsed=0
            while [ "$_elapsed" -lt "${CYBERWAVE_USBIP_WAIT_SECS:-30}" ]; do
                _vm_nodes=$(_usbip_vm_serial_nodes_for_vidpid "$USBIP_SERIAL_VIDPID")
                if [ -n "$_vm_nodes" ]; then
                    if [ "$_expected_serial" -le 0 ] 2>/dev/null; then
                        break
                    fi
                    if [ "$(printf '%s\n' "$_vm_nodes" | wc -l)" -ge "$_expected_serial" ]; then
                        break
                    fi
                fi
                sleep 1
                _elapsed=$((_elapsed + 1))
            done
            _vm_nodes=$(_usbip_vm_serial_nodes_for_vidpid "$USBIP_SERIAL_VIDPID")
            _vm_serial_count=0
            if [ -n "$_vm_nodes" ]; then
                _vm_serial_count=$(printf '%s\n' "$_vm_nodes" | wc -l)
            fi
            if [ "$_expected_serial" -gt 0 ] 2>/dev/null \
                && [ "$_vm_serial_count" -lt "$_expected_serial" ]; then
                echo "[usbip] Expected ${_expected_serial} arm serial device(s); found ${_vm_serial_count} after ${_elapsed}s"
                return 1
            fi
            _usbip_ready_serial_nodes=$_vm_nodes
            echo "[usbip] VM serial node(s) ready after ${_elapsed}s"
            return 0
        fi

        # Recover any node that USB/IP created in the VM after Docker had
        # already snapshotted this container's /dev.
        _usbip_mirror_nodes

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
        # Each poll re-runs the mirror: this container's /dev is a private
        # tmpfs that Docker never updates, so a node the VM enumerates while we
        # wait only becomes visible here once we mknod it.
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
                _usbip_mirror_nodes >/dev/null
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
    _usbip_attach_status=0
    _usbip_attach || _usbip_attach_status=$?

    # Attach-only mode is a pre-flight step for edge-core, not a driver run.
    if [ "$_usbip_attach_only" = "1" ]; then
        if [ "$_usbip_attach_status" -ne 0 ]; then
            exit "$_usbip_attach_status"
        fi
        _usbip_print_device_lines "$_usbip_ready_serial_nodes"
        exit 0
    fi
fi

# Use python3 explicitly: the Dockerfile installs python3 from apt and does
# not create a python -> python3 symlink (no python-is-python3).
exec python3 main.py "$@"
