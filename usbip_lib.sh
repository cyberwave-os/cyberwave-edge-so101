# shellcheck shell=sh
# USB/IP device-node helpers shared by entrypoint.sh (normal + attach-only mode).
#
# Background: Docker populates a privileged container's private tmpfs /dev by
# snapshotting the VM's device list at container-*creation* time. USB/IP
# attaches performed from inside the container therefore create nodes in the
# VM's devtmpfs that can never appear in this container's /dev. edge-core
# pre-attaches before creating the container so the nodes are mapped normally;
# these helpers are the second layer that recovers the cases it misses (a
# device plugged in after start, a failed pre-attach, a hot re-attach).
#
# Sourced, not executed — every function must be side-effect free until called.

# Resolve a USB/IP host name to an IPv4 address, echoing the input unchanged if
# it is already an address or cannot be resolved.
#
# The attach below runs in the VM's network namespace, where Docker's embedded
# DNS is not reachable and "host.docker.internal" does not resolve. Resolution
# therefore has to happen here, in the container's namespace, before we cross
# over. Never fails: a bad lookup returns the original string so the caller
# reports a normal connection error instead of aborting under `set -e`.
_usbip_resolve_host() {
    _host="$1"
    case "$_host" in
        '' | *[!0-9.]*) ;;
        *) echo "$_host"; return 0 ;;
    esac
    _ip=$(getent ahostsv4 "$_host" 2>/dev/null | awk '{print $1; exit}')
    if [ -z "$_ip" ]; then
        _ip=$(python3 -c 'import socket,sys; print(socket.gethostbyname(sys.argv[1]))' \
            "$_host" 2>/dev/null || true)
    fi
    if [ -n "$_ip" ]; then echo "$_ip"; else echo "$_host"; fi
    return 0
}

# Attach one exported device, entering the VM's mount *and network* namespaces.
#
# The -n is load-bearing. `usbip attach` hands its TCP socket to the VM kernel,
# which keeps it open for the entire device session. With -m alone that socket
# is created in *this container's* network namespace, so the session dies the
# moment the container exits — taking the import with it and leaving a dangling
# /dev/ttyACM* node that opens with EIO or hangs. Creating it in the VM's netns
# ties the session to the VM instead, so imports survive container restarts and
# the short-lived pre-attach helper.
_usbip_attach_device() {
    nsenter -t 1 -m -n -- usbip attach -r "$1" -d "$2"
}

# Print the serial device paths that exist in the Docker VM's /dev, one per
# line. Globbing happens *inside* the VM mount namespace: an unmatched glob
# stays literal in POSIX sh, so expanding it in the container would ask the VM
# about a path named "/dev/ttyACM*".
_usbip_vm_serial_nodes() {
    nsenter -t 1 -m -- sh -c 'ls -d /dev/ttyACM* /dev/ttyUSB* 2>/dev/null' 2>/dev/null || true
    return 0
}

# Print only VM serial nodes whose USB parent matches a VID:PID. This prevents
# an unrelated adapter already present in Docker Desktop's VM from satisfying
# the SO101 arm count or being passed through to the driver container.
_usbip_vm_serial_nodes_for_vidpid() {
    _vidpid="$1"
    _vid=${_vidpid%%:*}
    _pid=${_vidpid#*:}
    case "$_vid" in '' | *[!0-9a-fA-F]*) return 0 ;; esac
    case "$_pid" in '' | *[!0-9a-fA-F]*) return 0 ;; esac

    nsenter -t 1 -m -- sh -c '
        _wanted_vid="$1"
        _wanted_pid="$2"
        for _dev in /dev/ttyACM* /dev/ttyUSB*; do
            [ -e "$_dev" ] || continue
            _tty=${_dev##*/}
            _sys=$(readlink -f "/sys/class/tty/$_tty/device" 2>/dev/null || true)
            while [ -n "$_sys" ] && [ "$_sys" != "/" ]; do
                if [ -r "$_sys/idVendor" ] && [ -r "$_sys/idProduct" ]; then
                    _actual_vid=$(cat "$_sys/idVendor" 2>/dev/null || true)
                    _actual_pid=$(cat "$_sys/idProduct" 2>/dev/null || true)
                    if [ "$_actual_vid" = "$_wanted_vid" ] \
                        && [ "$_actual_pid" = "$_wanted_pid" ]; then
                        echo "$_dev"
                    fi
                    break
                fi
                _parent=${_sys%/*}
                [ "$_parent" != "$_sys" ] || break
                _sys=$_parent
            done
        done
    ' sh "$_vid" "$_pid" 2>/dev/null || true
    return 0
}

# Recreate VM serial nodes inside this container with matching major/minor.
# Requires --privileged (mknod is otherwise blocked by CAP_MKNOD).
_usbip_mirror_nodes() {
    _mirrored=0
    for _dev in $(_usbip_vm_serial_nodes); do
        # Already visible here (Docker snapshotted it, or a previous mirror).
        [ -e "$_dev" ] && continue

        # stat -c '%t %T' reports major/minor in hex.
        _mm=$(nsenter -t 1 -m -- stat -c '%t %T' "$_dev" 2>/dev/null || true)
        [ -n "$_mm" ] || continue
        _hexmaj=${_mm%% *}
        _hexmin=${_mm##* }
        # Guard the arithmetic expansion below: a malformed value would abort
        # the whole entrypoint under `set -e`.
        case "$_hexmaj" in '' | *[!0-9a-fA-F]*) continue ;; esac
        case "$_hexmin" in '' | *[!0-9a-fA-F]*) continue ;; esac

        if mknod "$_dev" c "$((0x$_hexmaj))" "$((0x$_hexmin))" 2>/dev/null; then
            chmod 660 "$_dev" 2>/dev/null || true
            echo "[usbip] Mirrored VM device node into container: $_dev"
            _mirrored=$((_mirrored + 1))
        else
            echo "[usbip] WARNING: could not mknod $_dev (needs --privileged)"
        fi
    done
    [ "$_mirrored" -gt 0 ] && echo "[usbip] Mirrored ${_mirrored} device node(s)"
    return 0
}

# Emit the machine-readable contract consumed by edge-core's pre-attach helper.
_usbip_print_device_lines() {
    if [ "$#" -gt 0 ]; then
        _device_lines_nodes="$1"
    else
        _device_lines_nodes=$(_usbip_vm_serial_nodes)
    fi
    for _dev in $_device_lines_nodes; do
        echo "USBIP_DEVICE=$_dev"
    done
    return 0
}
