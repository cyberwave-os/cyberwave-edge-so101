FROM python:3.12-slim

WORKDIR /app

# Install system dependencies for serial communication, OpenCV, and USB access
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libgl1 \
    libglib2.0-0 \
    libusb-1.0-0 \
    udev \
    && rm -rf /var/lib/apt/lists/*

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

# Install the package and its dependencies
RUN pip install --no-cache-dir -e .

# Pre-create the directory that edge-core bind-mounts with the edge config
RUN mkdir -p /app/.cyberwave

# Copy entrypoint script that loads CYBERWAVE_TWIN_JSON_FILE into env vars
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

# Default entrypoint — override with specific so101-* commands at runtime
ENTRYPOINT ["./entrypoint.sh"]