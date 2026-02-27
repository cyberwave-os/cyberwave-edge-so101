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

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

A standalone Python library for operating SO101 leader and follower robots using `scservo_sdk` (Feetech) directly. Part of the [Cyberwave](https://cyberwave.com/) edge nodes.

## Features

- **Local teleoperation** — Control the follower with a physical leader arm (both connected locally)
- **Remote operation** — Control the follower from anywhere via the Cyberwave web app
- **Calibration** — Interactive calibration with range-of-motion recording
- **Camera streaming** — CV2 (USB/webcam/IP) and Intel RealSense with WebRTC
- **Edge Core integration** — Auto-discovers leader/follower ports (5V=leader, 12V=follower), cameras from twin JSONs, and starts the right mode from the controller policy
- **CLI tools** — `so101-find-port`, `so101-calibrate`, `so101-teleoperate`, `so101-remoteoperate`

## Installation

### Prerequisites

- Python 3.8 or higher
- Serial port access to SO101 devices (e.g. `/dev/ttyACM0`, `/dev/tty.usbmodem*` on macOS)

### Install from Source

```bash
git clone https://github.com/cyberwave/cyberwave.git
cd cyberwave/cyberwave-edge-nodes/cyberwave-edge-so101
pip install -e .
```

### Dependencies

- `pyserial>=3.5` - Serial communication
- `feetech-servo-sdk` - Feetech motor SDK
- `cyberwave[camera]>=0.3.24` - Cyberwave platform integration and camera streaming (CV2 cameras)
- `python-dotenv>=1.0.0` - Environment variable loading

For Intel RealSense support, install `cyberwave[realsense]` separately.

### Environment Variables

```bash
export CYBERWAVE_API_KEY=your_token_here
```

Required for teleoperation and remote operation. Add to `~/.bashrc` or `~/.zshrc` to make it persistent.

## Quick Start

### 1. Set Up Environment

```bash
export CYBERWAVE_API_KEY=your_token_here
```

### 2. Find Device Port

```bash
so101-find-port
```

This will interactively help you identify the serial port of your SO101 device.

### 3. Read Device Data

```bash
so101-read-device --port /dev/ttyACM0
```

### 4. Calibrate Device

```bash
so101-calibrate --type leader --port /dev/ttyACM0
so101-calibrate --type follower --port /dev/ttyACM1
```

### 5. Teleoperate with Cyberwave

```bash
so101-teleoperate --leader-port /dev/ttyACM0 --follower-port /dev/ttyACM1
```

A twin is created automatically if `--twin-uuid` is not provided. Camera streaming uses default settings (USB camera 0). See [Advanced Settings](#advanced-settings) for camera options.

## Command-Line Tools

### `so101-setup`

Generate `setup.json` for wrist camera and additional cameras. Used by the edge node; typically not needed when using edge core (it auto-configures).

### `so101-find-port`

Interactively find the serial port of your SO101 device.

```bash
so101-find-port
```

### `so101-read-device`

Read and display data from SO101 motors.

```bash
so101-read-device --port /dev/ttyACM0
```

Add `--continuous` for live updates every second.

### `so101-calibrate`

Calibrate SO101 leader or follower devices.

```bash
so101-calibrate --type leader --port /dev/ttyACM0
so101-calibrate --type follower --port /dev/ttyACM1
```

Use `--find-port` to discover the port interactively. The `--id` defaults to `leader1` or `follower1`. See [Advanced Settings](#advanced-settings) for custom calibration dir, voltage rating, etc.

**Calibration Process:**

1. Move the device to the zero pose (starting position) as shown in the image below:

   ![SO101 Zero Pose](so101-zero-pose.png)

   This is the recommended starting position for calibration, with all joints in a well-defined configuration.

2. Press ENTER to set homing offsets
3. Move all joints through their full ranges of motion
4. Press ENTER to stop recording
5. Calibration is saved to `~/.cyberwave/so101_lib/calibrations/{id}.json`

### Teleoperate vs Remote Operate

| | **Teleoperate** (local controller) | **Remote Operate** (Cyberwave controller) |
|---|---|---|
| **Controller** | Physical leader arm connected locally (same machine as follower) | Cyberwave web app or API (user controls from browser/remote) |
| **Hardware** | Leader + follower (both on serial ports) | Follower only |
| **Data flow** | Leader → local process → follower → Cyberwave | Cyberwave (MQTT) → local process → follower |
| **Use case** | Hands-on teleoperation: move the leader arm, follower mirrors in real time | Remote control: operate the robot from anywhere via the Cyberwave application |

**Teleoperate** reads joint positions from the leader arm, sends them to the follower, and publishes the follower state to Cyberwave. The operator is physically at the edge device.

**Remote Operate** subscribes to joint states from Cyberwave (sent when a user controls the twin in the app), then writes those targets to the follower. The operator can be anywhere.

### `so101-teleoperate`

Run teleoperation with a **local leader arm**: read from leader, mirror to follower, stream to Cyberwave. Requires `CYBERWAVE_API_KEY`.

```bash
so101-teleoperate --leader-port /dev/ttyACM0 --follower-port /dev/ttyACM1
```

Uses default USB camera (device 0) and 30 FPS. See [Advanced Settings](#advanced-settings) for camera options, RealSense, IP streams, and config files.

### `so101-remoteoperate`

Run remote operation: receive joint states from the **Cyberwave application** via MQTT and write to the follower. Requires `CYBERWAVE_API_KEY`.

```bash
so101-remoteoperate --follower-port /dev/ttyACM1
```

See [Advanced Settings](#advanced-settings) for camera options.

## Advanced Settings

### `so101-teleoperate` and `so101-remoteoperate` — Camera Options

| Option | Description | Default |
|--------|-------------|---------|
| `--twin-uuid` | Cyberwave twin UUID | Auto-created if omitted |
| `--fps` | Teleoperation loop FPS | 30 |
| `--camera-fps` | Camera streaming FPS | 30 |
| `--camera-type` | `cv2` (USB/webcam/IP) or `realsense` | cv2 |
| `--camera-id` | Camera device ID or RTSP URL | 0 |
| `--camera-resolution` | QVGA, VGA, SVGA, HD, FULL_HD, or WIDTHxHEIGHT | VGA |
| `--camera-config` | Path to JSON config file | — |
| `--enable-depth` | RealSense depth streaming | false |
| `--depth-fps`, `--depth-resolution`, `--depth-publish-interval` | RealSense depth options | — |
| `--camera-only` | Stream camera only, no teleoperation | — |
| `--max-relative-target` | Follower safety limit (remoteoperate) | — |
| `--follower-id` | Calibration file ID (remoteoperate) | follower1 |

**Examples:**

```bash
# RealSense with depth
so101-teleoperate --leader-port /dev/ttyACM0 --follower-port /dev/ttyACM1 \
    --camera-type realsense --enable-depth --camera-resolution HD

# IP camera / RTSP
so101-teleoperate --leader-port /dev/ttyACM0 --camera-id "rtsp://192.168.1.100:554/stream"

# Use config file
so101-teleoperate --leader-port /dev/ttyACM0 --camera-config camera_config.json
```

### Camera Configuration File

Generate a config: `so101-teleoperate --generate-camera-config` (add `--camera-type realsense --auto-detect` for RealSense). List RealSense devices: `so101-teleoperate --list-realsense`.

**CV2 USB:**
```json
{"camera_type": "cv2", "camera_id": 0, "fps": 30, "resolution": [640, 480]}
```

**RealSense with depth:**
```json
{"camera_type": "realsense", "fps": 30, "resolution": [1280, 720], "enable_depth": true, "depth_fps": 15}
```

**IP/RTSP:**
```json
{"camera_type": "cv2", "camera_id": "rtsp://192.168.1.100:554/stream", "fps": 15, "resolution": [640, 480]}
```

### `so101-calibrate` — Advanced Options

- `--id` — Device ID for calibration file (default: leader1 / follower1)
- `--calibration-dir` — Custom calibration directory
- `--voltage-rating` — 5 or 12 (auto-detected if omitted)

### `so101-read-device` — Advanced Options

- `--motor-ids` — Read specific motors (e.g. `--motor-ids 1 2 3`)
- `--show-raw` — Show raw register values

## Python API

### Basic Usage

```python
from so101.leader import SO101Leader
from utils.config import LeaderConfig

# Create leader configuration
config = LeaderConfig(port="/dev/ttyACM0", id="leader1")

# Initialize and connect
leader = SO101Leader(config=config)
leader.connect()

# Get current joint positions (normalized)
positions = leader.get_observation()
# Returns: {"shoulder_pan.pos": 0.0, "shoulder_lift.pos": 0.0, ...}

# Disconnect
leader.disconnect()
```

### Calibration

```python
from so101.leader import SO101Leader
from utils.config import LeaderConfig

config = LeaderConfig(port="/dev/ttyACM0", id="leader1")
leader = SO101Leader(config=config)
leader.connect()

# Run calibration (interactive)
leader.calibrate()

# Calibration is automatically saved to ~/.cyberwave/so101_lib/calibrations/leader1.json
```

### Teleoperation

```python
from so101.leader import SO101Leader
from so101.follower import SO101Follower
from utils.config import LeaderConfig, FollowerConfig
from scripts.cw_teleoperate import teleoperate
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

# Build cameras list (each dict: twin, camera_id, camera_type, camera_resolution, fps)
cameras = [{
    "twin": camera,
    "camera_id": 0,
    "camera_type": "cv2",
    "camera_resolution": Resolution.HD,
    "fps": 30,
}]

try:
    teleoperate(
        leader=leader,
        cyberwave_client=cyberwave_client,
        follower=follower,
        robot=robot,
        cameras=cameras,
    )
finally:
    leader.disconnect()
    if follower:
        follower.disconnect()
    cyberwave_client.disconnect()
```

### Remote Operation

```python
from so101.follower import SO101Follower
from utils.config import FollowerConfig
from scripts.cw_remoteoperate import remoteoperate
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

# Build cameras list
cameras = [{
    "twin": camera,
    "camera_id": 0,
    "camera_type": "cv2",
    "camera_resolution": Resolution.VGA,
    "fps": 30,
}]

try:
    remoteoperate(
        client=cyberwave_client,
        follower=follower,
        robot=robot,
        cameras=cameras,
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

## Configuration

### LeaderConfig

```python
from utils.config import LeaderConfig

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
from utils.config import FollowerConfig

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

## Edge Core Integration

When deployed with [Cyberwave Edge Core](https://github.com/cyberwave/cyberwave-edge-core), the SO101 node runs as a Docker container and handles configuration **automatically**. No manual port or camera setup is required.

### How Edge Core Runs the SO101 Node

- Edge core mounts the config directory (`/etc/cyberwave` or `CYBERWAVE_EDGE_CONFIG_DIR`) into the container at `/app/.cyberwave`
- Twin JSON files (from the edge install flow) are synced into this directory
- Environment variables: `CYBERWAVE_TWIN_UUID`, `CYBERWAVE_API_KEY`, `CYBERWAVE_TWIN_JSON_FILE`, `CYBERWAVE_EDGE_CONFIG_DIR`
- The entrypoint (`entrypoint.sh`) loads twin metadata from the JSON file into env vars, then runs `main.py`

### Automatic Setup on Startup

On startup, `main.py` runs `_ensure_setup(twin_uuid)`, which bootstraps `setup.json` without user input:

1. **Leader/follower port detection**  
   Scans serial ports (`/dev/ttyACM*`, `/dev/ttyUSB*` on Linux; `/dev/tty.usbmodem*` on macOS), runs voltage detection on each SO101 device, and assigns:
   - **5V** → leader port  
   - **12V** → follower port  
   If one port fails voltage detection but exactly two ports are found, the missing role is inferred from the other.

2. **Camera discovery**  
   Reads twin JSONs from the edge config dir:
   - **Wrist camera**: From the primary robot twin’s `sensors_devices` (e.g. `/dev/video0`)
   - **Additional cameras**: Other twin JSONs with `attach_to_twin_uuid` matching the primary robot (e.g. RealSense attached as a separate twin)

3. **Setup merge**  
   Writes `setup.json` to `~/.cyberwave/so101_lib/` (or `CYBERWAVE_EDGE_CONFIG_DIR/so101_lib/` in the container), merging discovered ports and cameras with any existing config (e.g. from a previous `so101-calibrate` run).

### Controller Policy and Operations

The node subscribes to MQTT command messages. When the backend sends `controller-changed` (e.g. after a user switches the twin’s controller policy):

- **`localop`** → starts `so101-teleoperate` (leader + follower, local teleoperation)
- **Other** → starts `so101-remoteoperate` (follower only, receives joint states from the frontend via MQTT)

Both operations read hardware config from `setup.json` (ports, cameras, `max_relative_target`). No CLI arguments are needed.

### Calibration-Required Flow

If teleoperation is requested but no calibration file exists for the follower (or leader, for local teleop):

1. An alert is created on the twin (“Calibration Needed”)
2. The node switches to calibration mode
3. In interactive mode (TTY), it runs `so101-calibrate` as a subprocess
4. After calibration completes, the alert is resolved and the original operation can be started again

### Manual Docker Run (without Edge Core)

```bash
# Build the image
docker build -t cyberwave-so101 .

# Run (requires CYBERWAVE_API_KEY, CYBERWAVE_TWIN_UUID; config from edge-core)
docker run --rm -e CYBERWAVE_API_KEY=... -e CYBERWAVE_TWIN_UUID=... cyberwave-so101
```

For full automatic setup, run via edge core so it can mount the config directory and twin JSONs.

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
- [Cyberwave Edge Core](https://github.com/cyberwave/cyberwave-edge-core) – orchestrates edge nodes and mounts config
