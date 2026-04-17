FROM python:3.12-slim as base

ARG ENABLE_REALSENSE=false

WORKDIR /app



# Install system dependencies for serial communication, OpenCV, USB access,
# tini (proper init for signal handling), util-linux (nsenter) for USB/IP
# passthrough on macOS Docker Desktop, usbutils (lsusb) for RealSense, and
# socat for serial port bridging in E2E tests (test-so101-e2e.sh).
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgl1 \
    libglib2.0-0 \
    libusb-1.0-0 \
    socat \
    tini \
    udev \
    usbutils \
    util-linux \
    v4l-utils \
    && rm -rf /var/lib/apt/lists/*

# Optional: install RealSense support (pyrealsense2)
# On amd64 this uses pre-built pip wheels; on arm64 it builds from source.
COPY install_realsense_docker.sh .
RUN if [ "${ENABLE_REALSENSE}" = "true" ]; then \
    chmod +x install_realsense_docker.sh && ./install_realsense_docker.sh; \
    fi

FROM base as build

# Copy project files
COPY pyproject.toml .
COPY *.py ./
COPY motors/ ./motors/
COPY scripts/ ./scripts/
COPY so101/ ./so101/
COPY utils/ ./utils/
COPY assets/ ./assets/
COPY README.md .
COPY LICENSE .
COPY MANIFEST.in .
# Optional: local SDK source for pre-release CI builds (populated by test script before docker build).
# An empty sdk-local/ directory is used in production builds so this step is a no-op.
COPY sdk-local /tmp/sdk-local
RUN if [ -f "/tmp/sdk-local/pyproject.toml" ]; then \
      pip install --no-cache-dir "/tmp/sdk-local"; \
    fi

# Install the package and its dependencies
RUN pip install --no-cache-dir -e .

# Pre-create the directory that edge-core bind-mounts with the edge config
RUN mkdir -p /app/.cyberwave

# Copy entrypoint script that loads CYBERWAVE_TWIN_JSON_FILE into env vars
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# tini as PID 1 ensures proper signal forwarding and child reaping.
# Without it, Python becomes PID 1 and can get stuck in uninterruptible
# serial I/O, making the container impossible to stop.
ENTRYPOINT ["tini", "-s", "--", "./entrypoint.sh"]