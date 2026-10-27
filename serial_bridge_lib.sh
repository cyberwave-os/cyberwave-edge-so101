# shellcheck shell=sh
# Container half of the macOS serial bridge.
#
# Why this exists: macOS keeps its own AppleUSBACM driver attached to the
# SO101's CDC interface and Darwin has no libusb_detach_kernel_driver, so a
# USB/IP import enumerates /dev/ttyACM* but its bulk endpoints are inert — the
# kernel driver, not the USB/IP server, owns the data path. The host therefore
# publishes each arm as a TCP listener (a pyserial launchd service, the same shape
# as the camera MJPEG and audio bridges) and this turns each endpoint back into
# an ordinary PTY the driver can open with pyserial.
#
# Roles are still resolved by the usual voltage autodiscovery: the bridge
# carries real motor traffic, so Present_Voltage reads work normally.
#
# Sourced, not executed — no side effects until a function is called.

# Where the PTY nodes are created. Overridable so tests never touch a real /dev.
_serial_bridge_dev_prefix() {
    echo "${CYBERWAVE_SERIAL_BRIDGE_DEV_PREFIX:-/dev/ttyACM}"
}

# _start_serial_bridges "<port>[,<port>...]" <host>
#
# Starts one backgrounded socat per port, mapping tcp://<host>:<port> to
# <prefix><index>, and returns once every node exists (or the wait expires).
_start_serial_bridges() {
    _ports_raw="$1"
    _bridge_host="${2:-host.docker.internal}"

    # Split on commas using IFS rather than tr/printf: this runs before the
    # socat check, and a parse that depends on external binaries would fail
    # silently in a stripped environment instead of reporting what is missing.
    # Blank and non-numeric entries are dropped so a trailing comma in config
    # cannot spawn a bridge to an empty port.
    _old_ifs=$IFS
    IFS=','
    # shellcheck disable=SC2086
    set -- $_ports_raw
    IFS=$_old_ifs

    _clean_ports=""
    for _p in "$@"; do
        # Strip surrounding whitespace without calling out to tr/sed.
        while :; do
            case "$_p" in
                ' '*) _p=${_p# } ;;
                *' ') _p=${_p% } ;;
                *) break ;;
            esac
        done
        case "$_p" in
            '' | *[!0-9]*) continue ;;
        esac
        _clean_ports="$_clean_ports $_p"
    done

    if [ -z "${_clean_ports# }" ]; then
        return 0
    fi

    if ! command -v socat >/dev/null 2>&1; then
        echo "[serial-bridge] ERROR: socat not found in image; arms unreachable"
        return 0
    fi

    _prefix=$(_serial_bridge_dev_prefix)
    _idx=0
    _expected=""
    for _p in $_clean_ports; do
        _node="${_prefix}${_idx}"
        # Remove a stale node from a previous run so the readiness wait below
        # cannot pass on a leftover that no socat is backing.
        rm -f "$_node" 2>/dev/null || true
        echo "[serial-bridge] tcp://${_bridge_host}:${_p} -> ${_node}"
        # Supervised, like the host wrapper. socat exits whenever the TCP peer
        # goes away, and the host side restarts by design on device replug —
        # without this loop the PTY symlink would vanish for the rest of the
        # container's life the first time that happened.
        (
            # entrypoint.sh enables `set -e`, which this subshell inherits.
            # A routine connection refusal while the host bridge restarts must
            # not terminate the supervision loop.
            set +e
            while :; do
                socat "PTY,link=${_node},raw,echo=0,mode=660" \
                    "TCP:${_bridge_host}:${_p},nodelay,keepalive"
                echo "[serial-bridge] ${_node} peer closed; reconnecting"
                sleep 1
            done
        ) &
        _expected="$_expected $_node"
        _idx=$((_idx + 1))
    done

    # socat creates the symlink a moment after starting; the driver opens these
    # as soon as we return, so wait for them rather than racing.
    _wait="${CYBERWAVE_SERIAL_BRIDGE_WAIT_SECS:-10}"
    _elapsed=0
    while [ "$_elapsed" -lt "$_wait" ]; do
        _missing=0
        for _node in $_expected; do
            [ -e "$_node" ] || _missing=1
        done
        [ "$_missing" -eq 0 ] && break
        sleep 1
        _elapsed=$((_elapsed + 1))
    done

    for _node in $_expected; do
        if [ -e "$_node" ]; then
            echo "[serial-bridge] ready: $_node"
        else
            echo "[serial-bridge] WARNING: $_node did not appear after ${_wait}s"
        fi
    done
    return 0
}
