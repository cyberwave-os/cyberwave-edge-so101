# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Stage 1: Builder — compiles Python wheels.
#
# build-essential + python3-dev are kept here only; they're not copied to
# the runtime image, saving ~230 MB.
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-dev \
    python3-pip \
    build-essential \
    libgl1 \
    libglib2.0-0 \
    libusb-1.0-0 \
    && rm -rf /var/lib/apt/lists/*

RUN rm -f /usr/lib/python*/EXTERNALLY-MANAGED

WORKDIR /app

COPY pyproject.toml .
COPY *.py ./
COPY motors/ ./motors/
COPY scripts/ ./scripts/
COPY so101/ ./so101/
COPY utils/ ./utils/
COPY assets/ ./assets/
COPY README.md LICENSE MANIFEST.in ./

# Pin NumPy 1.x and install opencv-python-headless before other packages so
# nothing can pull in the full opencv-python wheel.
RUN pip install "numpy>=1.26,<2" \
    && pip install "opencv-python-headless" \
    && pip install "cyberwave-video-sync>=0.1.0" \
    && python3 -c "import cyberwave_video_sync; cyberwave_video_sync.install(); print('video-sync OK')" \
    && pip uninstall -y opencv-python 2>/dev/null || true \
    && pip install . \
    && pip install "numpy>=1.26,<2" \
    && rm -rf /app/build

RUN python3 -c "import numpy; v = numpy.__version__; \
    assert v.startswith('1.'), f'Expected NumPy 1.x, got {v}'"

RUN python3 -c "import cv2; print('cv2 import OK at', cv2.__file__)"

# -----------------------------------------------------------------------------
# Stage 2: Runtime base — no build tools, just runtime apt deps.
# opencv-python-headless is copied from the builder stage via pip packages.
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS so101-base

ARG ENABLE_REALSENSE=false

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CYBERWAVE_CAMERA_STRICT_GEOMETRY=1 \
    CYBERWAVE_VIDEO_SYNC_REQUIRED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    libgl1 \
    libglib2.0-0 \
    libusb-1.0-0 \
    libv4l-0 \
    v4l-utils \
    socat \
    tini \
    udev \
    usbutils \
    util-linux \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Debian 12 marks the system Python as externally-managed (PEP 668). This is a
# single-purpose container, so allow pip to install alongside apt rather than
# passing --break-system-packages on every invocation (matches the builder).
RUN rm -f /usr/lib/python*/EXTERNALLY-MANAGED

# Copy compiled pip packages from builder (no build tools needed at runtime).
# This must happen BEFORE the RealSense install so the amd64 pyrealsense2 wheel
# (installed into /usr/local/lib/python3.11) is not clobbered by this copy.
COPY --from=builder /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=builder /usr/local/bin /usr/local/bin

COPY install_realsense_docker.sh .
RUN chmod +x install_realsense_docker.sh && \
    if [ "${ENABLE_REALSENSE}" = "true" ]; then ./install_realsense_docker.sh; \
    else echo "Skipping RealSense (build with --build-arg ENABLE_REALSENSE=true to enable)"; fi

# -----------------------------------------------------------------------------
# Stage 3: Application
# -----------------------------------------------------------------------------
FROM so101-base AS runtime

WORKDIR /app

COPY --from=builder /app /app

RUN python3 -c "import numpy; v = numpy.__version__; \
    assert v.startswith('1.'), f'Expected NumPy 1.x, got {v}'"

RUN python3 -c "import cv2; print('Runtime cv2 OK at', cv2.__file__)"

RUN python3 -c "import cyberwave_video_sync; print('video-sync import OK')"

RUN mkdir -p /app/.cyberwave

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["tini", "-s", "--", "./entrypoint.sh"]
