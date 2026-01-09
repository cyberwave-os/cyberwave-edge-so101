# Cyberwave SO101 Robot Library

[![License: MIT](https://img.shields.io/github/license/cyberwave-os/cyberwave-edge-python-so101)](https://github.com/cyberwave-os/cyberwave-edge-python-so101/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![GitHub stars](https://img.shields.io/github/stars/cyberwave-os/cyberwave-edge-python-so101)](https://github.com/cyberwave-os/cyberwave-edge-python-so101/stargazers)
[![GitHub contributors](https://img.shields.io/github/contributors/cyberwave-os/cyberwave-edge-python-so101)](https://github.com/cyberwave-os/cyberwave-edge-python-so101/graphs/contributors)
[![GitHub issues](https://img.shields.io/github/issues/cyberwave-os/cyberwave-edge-python-so101)](https://github.com/cyberwave-os/cyberwave-edge-python-so101/issues)

A standalone Python library for operating SO101 leader and follower robots using `scservo_sdk` (Feetech) directly.

## Features

- **Motor Bus Layer**: Abstraction for Feetech STS3215 motor communication
- **Leader/Follower Support**: Separate classes for leader (passive) and follower (active) devices
- **Calibration System**: Interactive calibration with range-of-motion recording
- **Teleoperation**: Real-time teleoperation loop with Cyberwave integration
- **Normalization Modes**: Support for `RANGE_M100_100`, `RANGE_0_100`, and `DEGREES` normalization
- **Command-line Tools**: Easy-to-use scripts for common tasks

## Installation

### Prerequisites

- Python 3.8 or higher
- Serial port access to SO101 devices

### Install from Source

```bash
git clone https://github.com/cyberwave/cyberwave-edge-python-so101.git
cd cyberwave-edge-python-so101
pip install -e .
```

### Dependencies

- `pyserial>=3.5` - Serial communication
- `feetech-servo-sdk` - Feetech motor SDK
- `cyberwave` - Cyberwave platform integration (for teleoperation)

### Environment Variables

For Cyberwave integration, set the following environment variables:

```bash
export CYBERWAVE_TOKEN=your_token_here
export CYBERWAVE_ENVIRONMENT_ID=your_environment_uuid  # Optional
```

You can add these to your `~/.bashrc` or `~/.zshrc` to make them persistent:

```bash
echo 'export CYBERWAVE_TOKEN=your_token_here' >> ~/.bashrc
echo 'export CYBERWAVE_ENVIRONMENT_ID=your_environment_uuid' >> ~/.bashrc
source ~/.bashrc
```

## Quick Start

### 0. Set Up Environment Variables

Before using Cyberwave integration, set your API token and optionally environment UUID:

```bash
export CYBERWAVE_TOKEN=your_token_here
export CYBERWAVE_ENVIRONMENT_ID=your_environment_uuid  # Optional
```

### 1. Find Device Port

```bash
so101-find-port
```

This will interactively help you identify the serial port of your SO101 device.

### 2. Read Device Data

```bash
so101-read-device --port /dev/tty.usbmodem123
```

Or in continuous mode:

```bash
so101-read-device --port /dev/tty.usbmodem123 --continuous
```

### 3. Calibrate Device

Calibrate a leader device:

```bash
so101-calibrate --type leader --port /dev/tty.usbmodem123 --id leader1
```

Calibrate a follower device:

```bash
so101-calibrate --type follower --port /dev/tty.usbmodem456 --id follower1
```

### 4. Teleoperate with Cyberwave

```bash
so101-teleoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --leader-port /dev/tty.usbmodem123 \
    --follower-port /dev/tty.usbmodem456 \
    --fps 30
```

## Command-Line Tools

### `so101-find-port`

Interactively find the serial port of your SO101 device.

```bash
so101-find-port
```

### `so101-read-device`

Read and display comprehensive data from SO101 motors.

```bash
# Single read
so101-read-device --port /dev/tty.usbmodem123

# Continuous mode (updates every second)
so101-read-device --port /dev/tty.usbmodem123 --continuous

# Read specific motors
so101-read-device --port /dev/tty.usbmodem123 --motor-ids 1 2 3

# Show raw register values
so101-read-device --port /dev/tty.usbmodem123 --show-raw
```

### `so101-calibrate`

Calibrate SO101 leader or follower devices.

```bash
# Basic calibration
so101-calibrate --type leader --port /dev/tty.usbmodem123 --id leader1

# With interactive port finding
so101-calibrate --type leader --find-port --id leader1

# Custom calibration directory
so101-calibrate --type leader --port /dev/tty.usbmodem123 --id leader1 \
    --calibration-dir ~/my_calibrations

# Specify voltage rating (5V or 12V)
so101-calibrate --type leader --port /dev/tty.usbmodem123 --id leader1 --voltage-rating 5
```

**Calibration Process:**

1. Move the device to the middle of its range of motion
2. Press ENTER to set homing offsets
3. Move all joints through their full ranges of motion
4. Press ENTER to stop recording
5. Calibration is saved to `~/.so101_lib/calibrations/{id}.json`

### `so101-teleoperate`

Run teleoperation loop with Cyberwave integration.

**Note:** Requires `CYBERWAVE_TOKEN` environment variable to be set.

```bash
# Set environment variable first
export CYBERWAVE_TOKEN=your_token_here

# Run teleoperation
so101-teleoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --leader-port /dev/tty.usbmodem123 \
    --fps 30 \
    --camera-fps 30

# Stream depth camera instead of RGB
so101-teleoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --leader-port /dev/tty.usbmodem123 \
    --fps 30 \
    --camera-fps 30 \
    --camera-type depth
```

**Options:**

- `--twin-uuid`: UUID of the Cyberwave twin to update (required)
- `--leader-port`: Serial port for leader device (required, unless using --camera-only)
- `--follower-port`: Optional serial port for follower device
- `--fps`: Target frames per second for teleoperation loop (default: 30)
- `--camera-fps`: Frames per second for camera streaming (default: 30)
- `--camera-uuid`: UUID of the twin to stream camera to (default: same as --twin-uuid)
- `--camera-type`: Camera sensor type: 'rgb' or 'depth' (default: 'rgb')
- `--camera-only`: Only stream camera, skip teleoperation loop (requires --follower-port)

**Note:** Positions are always converted to radians for Cyberwave integration.

**Note:** Positions are always converted to radians for Cyberwave integration.

## Python API

### Basic Usage

```python
from leader import SO101Leader
from config import LeaderConfig

# Create leader configuration
config = LeaderConfig(port="/dev/tty.usbmodem123", id="leader1")

# Initialize and connect
leader = SO101Leader(config=config)
leader.connect()

# Get current positions, velocities, and loads
action = leader.get_action()
# Returns: {
#     "shoulder_pan": {"position": 2048.0, "velocity": 0.0, "load": 0.0},
#     "shoulder_lift": {"position": 2048.0, "velocity": 0.0, "load": 0.0},
#     ...
# }

# Disconnect
leader.disconnect()
```

### Calibration

```python
from leader import SO101Leader
from config import LeaderConfig

config = LeaderConfig(port="/dev/tty.usbmodem123", id="leader1")
leader = SO101Leader(config=config)
leader.connect()

# Run calibration (interactive)
leader.calibrate()

# Calibration is automatically saved to ~/.so101_lib/calibrations/leader1.json
```

### Teleoperation

```python
from leader import SO101Leader
from follower import SO101Follower
from config import LeaderConfig, FollowerConfig
from teleoperate import teleoperate
from cyberwave import Cyberwave

# Initialize Cyberwave client (reads token from CYBERWAVE_TOKEN env var)
cyberwave_client = Cyberwave()
robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id="YOUR_TWIN_UUID", name="robot")
camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id="YOUR_TWIN_UUID", name="camera")

# Initialize leader
leader_config = LeaderConfig(port="/dev/tty.usbmodem123", id="leader1")
leader = SO101Leader(config=leader_config)
leader.connect()

# Optionally initialize follower
follower_config = FollowerConfig(port="/dev/tty.usbmodem456", id="follower1")
follower = SO101Follower(config=follower_config)
follower.connect()

# Run teleoperation
try:
    teleoperate(
        leader=leader,
        cyberwave_client=cyberwave_client,
        follower=follower,  # Optional
        fps=30,
        camera_fps=30,  # Separate FPS for camera streaming
        robot=robot,  # Robot twin instance
        camera=camera,  # Camera twin instance (optional)
        camera_type="rgb",  # Camera sensor type: "rgb" or "depth" (default: "rgb")
    )
finally:
    leader.disconnect()
    if follower:
        follower.disconnect()
    cyberwave_client.disconnect()
```

## Calibration Format

Calibration files are saved as JSON in `~/.so101_lib/calibrations/{id}.json`:

```json
{
  "shoulder_pan": {
    "id": 1,
    "drive_mode": 0,
    "homing_offset": 116,
    "range_min": 741,
    "range_max": 3441
  },
  "shoulder_lift": {
    "id": 2,
    "drive_mode": 0,
    "homing_offset": 778,
    "range_min": 907,
    "range_max": 3284
  },
  ...
}
```

**Fields:**

- `id`: Motor ID (1-6)
- `drive_mode`: Drive mode (0 = normal, 1 = reversed)
- `homing_offset`: Homing offset value (raw encoder units)
- `range_min`: Minimum position recorded during calibration (raw encoder units)
- `range_max`: Maximum position recorded during calibration (raw encoder units)

## Normalization Modes

The library supports three normalization modes:

### `RANGE_M100_100`

Normalizes positions to the range [-100, 100] based on calibrated min/max.

**Formula:** `norm = (((bounded_val - min_) / (max_ - min_)) * 200) - 100`

### `RANGE_0_100`

Normalizes positions to the range [0, 100] based on calibrated min/max.

**Formula:** `norm = ((bounded_val - min_) / (max_ - min_)) * 100`

### `DEGREES`

Converts positions to degrees relative to the calibrated center.

**Formula:** `degrees = (raw_encoder_value - mid) * 360 / 4095`

Where:

- `mid = (range_min + range_max) / 2` (center of calibrated range)
- `max_res = 4095` (12-bit encoder resolution)

**Characteristics:**

- 0 degrees is at the midpoint of the calibrated range
- Values can be negative (below center) or positive (above center)
- Theoretical maximum: ±180 degrees if using full encoder range (0-4095)
- Practical maximum: depends on calibration (how far joints were moved)

## Architecture

### Core Components

- **`motors/`**: Motor bus layer and data models

  - `bus.py`: Abstract base class for motor buses
  - `feetech_bus.py`: Feetech STS3215 implementation
  - `models.py`: Motor and calibration data models
  - `tables.py`: Control table addresses and constants
  - `encoding.py`: Sign-magnitude encoding/decoding utilities

- **`leader.py`**: SO101Leader class for passive teleoperation
- **`follower.py`**: SO101Follower class for active robot control
- **`teleoperate.py`**: Teleoperation loop with Cyberwave integration
- **`config.py`**: Configuration dataclasses
- **`utils.py`**: Utility functions (port detection, calibration I/O, etc.)

### Motor Configuration

The SO101 has 6 motors:

- `shoulder_pan` (ID: 1)
- `shoulder_lift` (ID: 2)
- `elbow_flex` (ID: 3)
- `wrist_flex` (ID: 4)
- `wrist_roll` (ID: 5)
- `gripper` (ID: 6)

By default, all motors use `RANGE_M100_100` normalization except `gripper` which uses `RANGE_0_100`.

## Configuration

### LeaderConfig

```python
from config import LeaderConfig

config = LeaderConfig(
    port="/dev/tty.usbmodem123",  # Serial port
    use_degrees=True,              # Use degrees (deprecated, use norm_mode instead)
    id="leader1",                  # Device identifier
    calibration_dir=None,          # Custom calibration directory (default: ~/.so101_lib/calibrations)
    voltage_rating=None,           # 5 or 12 (auto-detected if None)
)
```

### FollowerConfig

```python
from config import FollowerConfig

config = FollowerConfig(
    port="/dev/tty.usbmodem456",
    use_degrees=True,
    id="follower1",
    calibration_dir=None,
    voltage_rating=None,
    max_relative_target=0.1,       # Maximum relative target change (safety limit)
    cameras=None,                   # Optional camera configuration
)
```

## Troubleshooting

### Port Not Found

If you can't find your device port:

```bash
so101-find-port
```

This will list available ports and help you identify the correct one.

### Calibration Required

Teleoperation requires a valid calibration file. If you see:

```
RuntimeError: No calibration file found at ...
```

Run calibration first:

```bash
so101-calibrate --type leader --port /dev/tty.usbmodem123 --id leader1
```

### Environment Variables Not Set

If you see errors about missing Cyberwave token:

```
ValueError: No CYBERWAVE_API_KEY found! Get yours at https://cyberwave.com/profile
```

Set the required environment variable:

```bash
export CYBERWAVE_API_KEY=your_token_here
```

Get your token from [https://cyberwave.com/profile](https://cyberwave.com/profile)

### Voltage Detection

The library can auto-detect voltage rating (5V or 12V) from motor registers. If detection fails, you can specify it manually:

```bash
so101-calibrate --type leader --port /dev/tty.usbmodem123 --id leader1 --voltage-rating 5
```

### Connection Issues

- Ensure the device is powered on
- Check USB cable connection
- Verify port permissions (on Linux, you may need to add your user to the `dialout` group)
- Try a different USB port

## Development

### Install Development Dependencies

```bash
pip install -e ".[dev]"
```

### Code Formatting

```bash
black .
ruff check .
```

### Type Checking

```bash
mypy .
```

## License

Apache-2.0

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## References

- [Feetech STS3215 Documentation](https://www.feetechrc.com/)
- [Cyberwave Platform](https://cyberwave.com/)
