"""Command-line script to find the port of a MotorsBus device.

Example:
    python -m so101_lib.find_port
    or
    so101-find-port
"""

import sys

from utils import find_port


def main():
    """Main entry point for find_port script."""
    try:
        port = find_port()
        print(f"\n✓ Found port: {port}")
        sys.exit(0)
    except OSError as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nCancelled by user.", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
