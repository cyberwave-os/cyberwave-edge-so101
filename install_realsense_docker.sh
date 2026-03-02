#!/bin/bash
# Install pyrealsense2 inside a Docker image.
#   - amd64: pre-built pip wheel + runtime system libs
#   - arm64: build librealsense + Python bindings from source
set -e

ARCH=$(dpkg --print-architecture)
echo "Installing RealSense support for architecture: $ARCH"

if [ "$ARCH" = "amd64" ]; then
    apt-get update && apt-get install -y --no-install-recommends \
        libusb-1.0-0 \
        udev
    pip install --no-cache-dir 'pyrealsense2>=2.50'
    rm -rf /var/lib/apt/lists/*

elif [ "$ARCH" = "arm64" ]; then
    PYTHON_VERSION=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")

    apt-get update
    # Build-time + runtime deps (-dev packages also install runtime libs)
    apt-get install -y --no-install-recommends \
        git wget cmake build-essential pkg-config \
        libssl-dev libusb-1.0-0-dev libudev-dev \
        libglfw3-dev libgl1-mesa-dev libglu1-mesa-dev \
        python3-dev \
        udev

    git clone --depth 1 https://github.com/IntelRealSense/librealsense.git /tmp/librealsense
    mkdir -p /tmp/librealsense/build && cd /tmp/librealsense/build

    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_EXAMPLES=false \
        -DBUILD_GRAPHICAL_EXAMPLES=false \
        -DBUILD_PYTHON_BINDINGS:bool=true \
        -DPYTHON_EXECUTABLE="$(which python3)"
    make -j"$(nproc)"
    make install
    ldconfig

    mkdir -p "/usr/local/lib/python${PYTHON_VERSION}/dist-packages"
    echo "/usr/local/lib/python${PYTHON_VERSION}/site-packages" \
        > "/usr/local/lib/python${PYTHON_VERSION}/dist-packages/pyrealsense2.pth"

    # Clean up build artifacts and source, keep installed libs
    rm -rf /tmp/librealsense
    apt-get purge -y --auto-remove git wget cmake build-essential pkg-config python3-dev
    rm -rf /var/lib/apt/lists/*
else
    echo "Unsupported architecture: $ARCH" >&2
    exit 1
fi

python3 -c "import pyrealsense2; print('pyrealsense2 installed successfully')"
