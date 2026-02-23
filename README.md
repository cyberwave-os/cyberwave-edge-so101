---
# Cyberwave Edge SO-101 node configuration

cyberwave-edge-node:
  name: so101
  description: "Cyberwave Edge node for SO-101"
  commands:
    - find_port
    - local_teleoperate:
        parameters:
          - type: string
            name: leader-port
          - type: string
            name: follower-port
          - type: string
            name: camera-type
            choices:
              - cv2
              - realsense
    - remote_teleoperate:
      parameters:
        - type: string
          name: leader-port
    - calibrate:
        parameters:
          - type: string
            name: port
            description: The port of the SO101 device
            required: true
          - type: string
            name: type
            description: The type of the SO101 device
            required: true
            choices:
              - leader
              - follower
---

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
- **Camera Streaming**: Support for CV2 (USB/webcam/IP) and Intel RealSense cameras with WebRTC streaming
- **Camera Configuration**: JSON-based camera configuration files for easy setup sharing
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
- `cyberwave[camera]` - Camera streaming support (CV2 cameras)
- `cyberwave[realsense]` - Intel RealSense camera support (optional, install with `pip install cyberwave[realsense]`)

### Environment Variables

For Cyberwave integration, set the following environment variables:

```bash
export CYBERWAVE_API_KEY=your_token_here
export CYBERWAVE_ENVIRONMENT_ID=your_environment_uuid  # Optional
```

You can add these to your `~/.bashrc` or `~/.zshrc` to make them persistent:

```bash
echo 'export CYBERWAVE_API_KEY=your_token_here' >> ~/.bashrc
echo 'export CYBERWAVE_ENVIRONMENT_ID=your_environment_uuid' >> ~/.bashrc
source ~/.bashrc
```

## Quick Start

### 0. Set Up Environment Variables

Before using Cyberwave integration, set your API token and optionally environment UUID:

```bash
export CYBERWAVE_API_KEY=your_token_here
export CYBERWAVE_ENVIRONMENT_ID=your_environment_uuid  # Optional
```

### 1. Find Device Port

```bash
so101-find-port
```

This will interactively help you identify the serial port of your SO101 device.

### 2. Read Device Data

```bash
so101-read-device --port /dev/ttyACM0
```

Or in continuous mode:

```bash
so101-read-device --port /dev/ttyACM0 --continuous
```

### 3. Calibrate Device

Calibrate a leader device (ID defaults to `leader1` if not specified):

```bash
so101-calibrate --type leader --port /dev/ttyACM0
```

Calibrate a follower device (ID defaults to `follower1` if not specified):

```bash
so101-calibrate --type follower --port /dev/ttyACM1
```

Or specify a custom ID:

```bash
so101-calibrate --type leader --port /dev/ttyACM0 --id blue
so101-calibrate --type follower --port /dev/ttyACM1 --id red
```

### 4. Teleoperate with Cyberwave

```bash
# Basic teleoperation (twin will be created automatically if --twin-uuid is not provided)
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --follower-port /dev/ttyACM1 \
    --fps 30

# With existing twin UUID
so101-teleoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --leader-port /dev/ttyACM0 \
    --follower-port /dev/ttyACM1 \
    --fps 30

# With camera configuration file
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --camera-config camera_config.json
```

**Optional:** Generate a camera configuration file:

```bash
# Generate default CV2 camera config
so101-teleoperate --generate-camera-config

# Or with RealSense auto-detection
so101-teleoperate --generate-camera-config --camera-type realsense --auto-detect
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
so101-read-device --port /dev/ttyACM0

# Continuous mode (updates every second)
so101-read-device --port /dev/ttyACM0 --continuous

# Read specific motors
so101-read-device --port /dev/ttyACM0 --motor-ids 1 2 3

# Show raw register values
so101-read-device --port /dev/ttyACM0 --show-raw
```

### `so101-calibrate`

Calibrate SO101 leader or follower devices.

**Note:** The `--id` argument is optional. If not provided, it defaults to `{type}1` (e.g., `leader1` for `--type leader`, `follower1` for `--type follower`).

```bash
# Basic calibration (ID defaults to leader1)
so101-calibrate --type leader --port /dev/ttyACM0

# With interactive port finding (ID defaults to leader1)
so101-calibrate --type leader --find-port

# Custom ID
so101-calibrate --type leader --port /dev/ttyACM0 --id blue

# Custom calibration directory
so101-calibrate --type leader --port /dev/ttyACM0 \
    --calibration-dir ~/my_calibrations

# Specify voltage rating (5V or 12V)
so101-calibrate --type leader --port /dev/ttyACM0 --voltage-rating 5

# Follower calibration (ID defaults to follower1)
so101-calibrate --type follower --port /dev/ttyACM1
```

**Calibration Process:**

1. Move the device to the zero pose (starting position) as shown in the image below:

   ![SO101 Zero Pose](so101-zero-pose.png)

   This is the recommended starting position for calibration, with all joints in a well-defined configuration.

2. Press ENTER to set homing offsets
3. Move all joints through their full ranges of motion
4. Press ENTER to stop recording
5. Calibration is saved to `~/.cyberwave/so101_lib/calibrations/{id}.json`

### `so101-teleoperate`

Run teleoperation loop with Cyberwave integration and camera streaming.

**Note:** Requires `CYBERWAVE_API_KEY` environment variable to be set.

```bash
# Set environment variable first
export CYBERWAVE_API_KEY=your_token_here

# Basic teleoperation with CV2 USB camera (twin will be created automatically)
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --fps 30 \
    --camera-fps 30

# With existing twin UUID
so101-teleoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --leader-port /dev/ttyACM0 \
    --fps 30 \
    --camera-fps 30

# Use RealSense camera with depth streaming
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --follower-port /dev/ttyACM1 \
    --camera-type realsense \
    --camera-resolution HD \
    --enable-depth \
    --depth-fps 15

# Use IP camera / RTSP stream
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --camera-id "rtsp://192.168.1.100:554/stream" \
    --camera-resolution VGA

# Use camera configuration file
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --camera-config camera_config.json

# Camera-only mode (no teleoperation, just streaming)
so101-teleoperate \
    --follower-port /dev/ttyACM1 \
    --camera-only \
    --camera-config camera_config.json
```

**Options:**

- `--twin-uuid`: UUID of the Cyberwave twin to update (optional - if not provided, a new twin will be created automatically)
- `--leader-port`: Serial port for leader device (required, unless using --camera-only)
- `--follower-port`: Optional serial port for follower device
- `--fps`: Target frames per second for teleoperation loop (default: 30)
- `--camera-fps`: Frames per second for camera streaming (default: 30)
- `--camera-uuid`: UUID of the twin to stream camera to (default: same as --twin-uuid)
- `--camera-type`: Camera type: 'cv2' for USB/webcam/IP, 'realsense' for Intel RealSense (default: 'cv2')
- `--camera-id`: Camera device ID (int) or stream URL (str) for CV2 cameras (default: '0')
- `--camera-resolution`: Camera resolution: QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT (default: VGA)
- `--enable-depth`: Enable depth streaming for RealSense cameras
- `--depth-fps`: Depth stream FPS for RealSense cameras (default: 30)
- `--depth-resolution`: Depth stream resolution for RealSense (default: same as --camera-resolution)
- `--depth-publish-interval`: Publish depth frame every N frames for RealSense (default: 30)
- `--camera-config`: Path to camera configuration JSON file (overrides CLI arguments)
- `--generate-camera-config`: Generate a camera configuration file and exit
- `--auto-detect`: Auto-detect RealSense device capabilities when generating config
- `--list-realsense`: List available RealSense devices and exit
- `--camera-only`: Only stream camera, skip teleoperation loop (requires --follower-port)

**Note:** Positions are always converted to radians for Cyberwave integration.

### Camera Configuration

Camera settings can be managed via JSON configuration files for easy sharing and reuse.

#### Generate Camera Configuration

```bash
# Generate default CV2 camera config
so101-teleoperate --generate-camera-config

# Generate RealSense config with auto-detection
so101-teleoperate --generate-camera-config --camera-type realsense --auto-detect

# Generate to custom path
so101-teleoperate --generate-camera-config my_camera.json --camera-type cv2
```

#### List RealSense Devices

```bash
# List all connected RealSense devices
so101-teleoperate --list-realsense
```

#### Camera Configuration File Format

**CV2 USB Camera (`camera_config.json`):**

```json
{
  "camera_type": "cv2",
  "camera_id": 0,
  "fps": 30,
  "resolution": [640, 480]
}
```

**RealSense Camera with Depth:**

```json
{
  "camera_type": "realsense",
  "fps": 30,
  "resolution": [1280, 720],
  "enable_depth": true,
  "depth_fps": 15,
  "depth_resolution": [640, 480],
  "depth_publish_interval": 30
}
```

**IP Camera / RTSP Stream:**

```json
{
  "camera_type": "cv2",
  "camera_id": "rtsp://192.168.1.100:554/stream",
  "fps": 15,
  "resolution": [640, 480]
}
```

#### Using Camera Configuration

```bash
# Use config file instead of CLI arguments (twin will be created automatically)
so101-teleoperate \
    --leader-port /dev/ttyACM0 \
    --camera-config camera_config.json

# Or with existing twin UUID
so101-teleoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --leader-port /dev/ttyACM0 \
    --camera-config camera_config.json
```

The same camera configuration file can be used with both `so101-teleoperate` and `so101-remoteoperate`.

### `so101-remoteoperate`

Run remote operation loop: receive joint states via MQTT and write to follower motors.

**Note:** Requires `CYBERWAVE_API_KEY` environment variable to be set.

```bash
# Basic remote operation with CV2 camera (twin will be created automatically)
so101-remoteoperate \
    --follower-port /dev/ttyACM1 \
    --camera-fps 30

# With existing twin UUID
so101-remoteoperate \
    --twin-uuid YOUR_TWIN_UUID \
    --follower-port /dev/ttyACM1 \
    --camera-fps 30

# Use RealSense camera with depth
so101-remoteoperate \
    --follower-port /dev/ttyACM1 \
    --camera-type realsense \
    --enable-depth \
    --camera-resolution HD

# Use camera configuration file
so101-remoteoperate \
    --follower-port /dev/ttyACM1 \
    --camera-config camera_config.json
```

**Options:**

- `--twin-uuid`: UUID of the Cyberwave twin to subscribe to (optional - if not provided, a new twin will be created automatically)
- `--follower-port`: Serial port for follower device (required)
- `--max-relative-target`: Maximum change per update for follower (safety limit)
- `--follower-id`: Device identifier for calibration file (default: 'follower1')
- `--camera-uuid`: UUID of the twin to stream camera to (default: same as --twin-uuid)
- `--camera-fps`: Frames per second for camera streaming (default: 30)
- `--camera-type`: Camera type: 'cv2' or 'realsense' (default: 'cv2')
- `--camera-id`: Camera device ID or stream URL for CV2 cameras (default: '0')
- `--camera-resolution`: Camera resolution (default: VGA)
- `--enable-depth`: Enable depth streaming for RealSense
- `--depth-fps`: Depth stream FPS for RealSense (default: 30)
- `--depth-resolution`: Depth stream resolution for RealSense
- `--depth-publish-interval`: Publish depth every N frames (default: 30)
- `--camera-config`: Path to camera configuration JSON file
- `--list-realsense`: List available RealSense devices and exit

## Python API

### Basic Usage

```python
from leader import SO101Leader
from config import LeaderConfig

# Create leader configuration
config = LeaderConfig(port="/dev/ttyACM0", id="leader1")

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

config = LeaderConfig(port="/dev/ttyACM0", id="leader1")
leader = SO101Leader(config=config)
leader.connect()

# Run calibration (interactive)
leader.calibrate()

# Calibration is automatically saved to ~/.cyberwave/so101_lib/calibrations/leader1.json
```

### Teleoperation

```python
from leader import SO101Leader
from follower import SO101Follower
from config import LeaderConfig, FollowerConfig
from teleoperate import teleoperate
from cyberwave import Cyberwave
from cyberwave.sensor import Resolution

# Initialize Cyberwave client (reads token from CYBERWAVE_API_KEY env var)
cyberwave_client = Cyberwave()
robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id="YOUR_TWIN_UUID", name="robot")
camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id="YOUR_TWIN_UUID", name="camera")

# Initialize leader
leader_config = LeaderConfig(port="/dev/ttyACM0", id="leader1")
leader = SO101Leader(config=leader_config)
leader.connect()

# Optionally initialize follower
follower_config = FollowerConfig(port="/dev/ttyACM1", id="follower1")
follower = SO101Follower(config=follower_config)
follower.connect()

# Run teleoperation with CV2 camera
try:
    teleoperate(
        leader=leader,
        cyberwave_client=cyberwave_client,
        follower=follower,  # Optional
        fps=30,
        camera_fps=30,
        robot=robot,
        camera=camera,
        camera_type="cv2",  # "cv2" or "realsense"
        camera_id=0,  # Camera device ID or URL
        camera_resolution=Resolution.HD,  # Resolution enum
        enable_depth=False,
    )
finally:
    leader.disconnect()
    if follower:
        follower.disconnect()
    cyberwave_client.disconnect()

# Run teleoperation with RealSense camera and depth
try:
    teleoperate(
        leader=leader,
        cyberwave_client=cyberwave_client,
        follower=follower,
        fps=30,
        camera_fps=30,
        robot=robot,
        camera=camera,
        camera_type="realsense",
        camera_resolution=Resolution.HD,
        enable_depth=True,
        depth_fps=15,
        depth_resolution=Resolution.VGA,
        depth_publish_interval=30,
    )
finally:
    leader.disconnect()
    if follower:
        follower.disconnect()
    cyberwave_client.disconnect()
```

### Remote Operation

```python
from follower import SO101Follower
from config import FollowerConfig
from remoteoperate import remoteoperate
from cyberwave import Cyberwave
from cyberwave.sensor import Resolution

# Initialize Cyberwave client
cyberwave_client = Cyberwave()
robot = cyberwave_client.twin(asset_key="the-robot-studio/so101", twin_id="YOUR_TWIN_UUID", name="robot")
camera = cyberwave_client.twin(asset_key="cyberwave/standard-cam", twin_id="YOUR_TWIN_UUID", name="camera")

# Initialize follower
follower_config = FollowerConfig(port="/dev/ttyACM1", id="follower1")
follower = SO101Follower(config=follower_config)
follower.connect()

# Run remote operation
try:
    remoteoperate(
        client=cyberwave_client,
        follower=follower,
        robot=robot,
        camera=camera,
        camera_fps=30,
        camera_type="cv2",
        camera_id=0,
        camera_resolution=Resolution.VGA,
    )
finally:
    follower.disconnect()
    cyberwave_client.disconnect()
```

## Calibration Format

Calibration files are saved as JSON in `~/.cyberwave/so101_lib/calibrations/{id}.json`:

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
- **`teleoperate.py`**: Teleoperation loop with Cyberwave integration and camera streaming
- **`remoteoperate.py`**: Remote operation loop: receive joint states via MQTT and control follower
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
    port="/dev/ttyACM0",  # Serial port
    use_degrees=True,              # Use degrees (deprecated, use norm_mode instead)
    id="leader1",                  # Device identifier
    calibration_dir=None,          # Custom calibration directory (default: ~/.cyberwave/so101_lib/calibrations)
    voltage_rating=None,           # 5 or 12 (auto-detected if None)
)
```

### FollowerConfig

```python
from config import FollowerConfig

config = FollowerConfig(
    port="/dev/ttyACM1",
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
so101-calibrate --type leader --port /dev/ttyACM0
```

(Note: `--id` is optional and defaults to `leader1` for `--type leader`)

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
so101-calibrate --type leader --port /dev/ttyACM0 --voltage-rating 5
```

(Note: `--id` is optional and defaults to `leader1` for `--type leader`)

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
