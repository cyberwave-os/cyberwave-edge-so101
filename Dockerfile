# syntax=docker/dockerfile:1
#
# Build context = repo ROOT, so a build can bundle the local Cyberwave SDK from
# cyberwave-sdks/ instead of the published wheel. Build with:
#   DOCKER_BUILDKIT=1 docker build -f cyberwave-edge-nodes/cyberwave-edge-so101/Dockerfile .
#
# ── Cyberwave SDK source (CW_SDK_SOURCE build-arg) ───────────────────────────
#   CW_SDK_SOURCE=wheel  (DEFAULT) — install a PUBLISHED SDK. `pip install .`
#       resolves it from PyPI within the range pyproject allows; CW_SDK_SPEC
#       plus the `buildkite_token` secret then optionally swap in a pre-release
#       from the internal index (see Stage 1b). This is the publish path.
#
#   CW_SDK_SOURCE=local — install the local checkout at
#       cyberwave-sdks/cyberwave-python/ over the top, so the image runs THIS
#       commit's SDK. Needed to test SDK-side changes (e.g. anything under
#       cyberwave/calibration/) on real hardware without publishing a release
#       first. Requires a generated cyberwave/rest/ — it is gitignored and
#       produced by python-sdk-gen.sh against a live backend; a fresh checkout
#       has an empty rest/ and the build fails with `cannot import name
#       'DefaultApi' from 'cyberwave.rest'`. Usage:
#         DOCKER_BUILDKIT=1 docker build \
#           --build-arg CW_SDK_SOURCE=local \
#           -f cyberwave-edge-nodes/cyberwave-edge-so101/Dockerfile \
#           -t cyberwaveos/so101-driver:local .
#
# Stage selection (FROM sdk-${CW_SDK_SOURCE}) means the unused stage never runs,
# so the local bind mounts are never evaluated in a wheel build and an
# ungenerated rest/ cannot break the publish path.

# Global ARG, usable in the FROM below. Default = wheel (CI / publish).
ARG CW_SDK_SOURCE=wheel

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

# Paths are relative to the repo root (see the build-context note at the top).
ARG NODE_DIR=cyberwave-edge-nodes/cyberwave-edge-so101
COPY ${NODE_DIR}/pyproject.toml .
COPY ${NODE_DIR}/*.py ./
COPY ${NODE_DIR}/motors/ ./motors/
COPY ${NODE_DIR}/scripts/ ./scripts/
COPY ${NODE_DIR}/so101/ ./so101/
COPY ${NODE_DIR}/utils/ ./utils/
COPY ${NODE_DIR}/assets/ ./assets/
COPY ${NODE_DIR}/README.md ${NODE_DIR}/LICENSE ${NODE_DIR}/MANIFEST.in ./

# Pin NumPy 1.x and install opencv-python-headless before other packages so
# nothing can pull in the full opencv-python wheel.
RUN pip install "numpy>=1.26,<2" \
    && pip install "opencv-python-headless" \
    && pip install "cyberwave-video-sync>=0.1.0" \
    && python3 -c "import cyberwave_video_sync; cyberwave_video_sync.install(); print('video-sync OK')" \
    && (pip uninstall -y opencv-python 2>/dev/null || true) \
    && pip install . \
    && pip install "numpy>=1.26,<2" \
    && rm -rf /app/build

RUN python3 -c "import numpy; v = numpy.__version__; \
    assert v.startswith('1.'), f'Expected NumPy 1.x, got {v}'"

RUN python3 -c "import cv2; print('cv2 import OK at', cv2.__file__)"

# -----------------------------------------------------------------------------
# Stage 1b: Cyberwave SDK — published wheel (default) vs local checkout.
#
# `pip install .` in the builder already resolved the SDK from PyPI at the range
# pyproject allows. The wheel stage then optionally swaps in a PRE-RELEASE from
# the Buildkite internal index (CW_SDK_SPEC + the buildkite_token secret), and
# asserts the import either way. The local stage reinstalls from the checkout.
#
# WHY THE PRE-RELEASE PATH. PyPI only ever carries released versions, so an SDK
# change cannot reach this image until a release ships — which makes every
# SDK-side fix a release ceremony before it can be tested in a built image. The
# SDK release workflow already publishes a `X.Y.Z.devN` wheel per dev merge to
# the internal index; CI points dev/staging builds at it. Production takes no
# spec and stays on PyPI, so a shipped image is always built from a released,
# publicly reproducible wheel. Mirrors the piper driver (piper/Dockerfile:155).
#
# The wheel stage ASSERTS `cyberwave.calibration`, it does not merely report it.
# This is the publish path, so a wheel that lacks the package ships an image
# whose hand-eye button raises a `handeye_unavailable` alert -- a runtime
# failure on real hardware, for a build that passed CI. Failing here instead
# means the pin above and the released SDK can never drift apart silently.
# If this fails: the version pinned in pyproject.toml was published from a tree
# without cyberwave/calibration/. Cut a release that contains it and re-pin.
# -----------------------------------------------------------------------------
FROM builder AS sdk-wheel

# Full pip specifier, computed by the CI workflow (e.g. "cyberwave[camera]==0.7.2.*"
# on dev/staging). Empty by default: a bare `docker build` and every production
# build keep exactly what `pip install .` resolved from PyPI above.
ARG CW_SDK_SPEC=

# --no-deps: the builder already installed the full dependency tree, and this
# only swaps the cyberwave package itself. Without it pip re-resolves and can
# pull the NumPy 2.x wheel the builder deliberately pins away from.
#
# Deliberately NOT followed by `pip check`. This image is permanently in a state
# pip calls inconsistent, on purpose: it pins NumPy 1.x so apt's python3-opencv
# (NumPy 1.x ABI) keeps working, while opencv-python-headless and cmeel-boost
# both declare numpy>=2. `pip check` therefore fails here whatever the SDK is,
# so it can only ever be noise. The import assert below is the real gate.
#
# required=false so builds without the secret (local, forks — GitHub withholds
# secrets from fork `pull_request` runs) fall through to the PyPI install rather
# than failing.
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=secret,id=buildkite_token,required=false \
    set -eu; \
    TOKEN="$(cat /run/secrets/buildkite_token 2>/dev/null || true)"; \
    if [ -n "$TOKEN" ] && [ -n "$CW_SDK_SPEC" ]; then \
      BK_INDEX="https://buildkite:${TOKEN}@packages.buildkite.com/cyberwave/cyberwave-internal-python/pypi/simple"; \
      echo "Installing SDK pre-release from Buildkite: ${CW_SDK_SPEC}"; \
      pip install --no-deps --force-reinstall --pre \
          --extra-index-url="$BK_INDEX" "${CW_SDK_SPEC}"; \
    else \
      echo "No CW_SDK_SPEC/buildkite token -- keeping the PyPI SDK resolved by 'pip install .'"; \
    fi

RUN python3 -c "from cyberwave import Cyberwave; print('cyberwave SDK (wheel) importable')" \
    && python3 -c "import importlib.util, cyberwave; \
        assert importlib.util.find_spec('cyberwave.calibration'), ( \
            'The installed cyberwave wheel (%s) ships no cyberwave.calibration -- ' \
            'hand-eye would be dead in this image. Either build with CW_SDK_SPEC ' \
            'plus the buildkite_token secret to pull a pre-release that has it, ' \
            'or publish a release built from a tree containing it and widen the ' \
            'floor in pyproject.toml.' % cyberwave.__version__); \
        print('hand-eye support: yes (cyberwave %s)' % cyberwave.__version__)"

# Installed via bind mounts so the SDK *source* never becomes an image layer —
# only the built package lands in site-packages. A COPY + later `rm` would NOT
# reclaim anything (the COPY layer persists under the rm whiteout). Targeted
# mounts (packaging metadata + the cyberwave/ package) keep the 491MB .venv,
# tests/ and examples/ out of the poetry-core build.
#
# --no-deps: the builder already installed the full dependency tree at the
# pinned version, and this only swaps the cyberwave package itself. Without it,
# pip re-resolves and can pull the NumPy 2.x wheel the builder deliberately
# pins away from.
FROM builder AS sdk-local
RUN --mount=type=cache,target=/root/.cache/pip \
    --mount=type=bind,source=cyberwave-sdks/cyberwave-python/pyproject.toml,target=/src/pyproject.toml \
    --mount=type=bind,source=cyberwave-sdks/cyberwave-python/README.md,target=/src/README.md \
    --mount=type=bind,source=cyberwave-sdks/cyberwave-python/cyberwave,target=/src/cyberwave \
    pip install --no-deps --force-reinstall /src \
    && python3 -c "from cyberwave import Cyberwave; from cyberwave.calibration import HandEyeSession; \
        print('cyberwave SDK (local checkout) importable')"

# Select the SDK stage. Only the selected one is built, so the local bind mounts
# are never evaluated in a wheel build.
FROM sdk-${CW_SDK_SOURCE} AS sdk

# -----------------------------------------------------------------------------
# Stage 2: Runtime base — no build tools, just runtime apt deps.
# opencv-python-headless is copied from the builder stage via pip packages.
# -----------------------------------------------------------------------------
FROM debian:bookworm-slim AS so101-base

ARG ENABLE_REALSENSE=false
# Re-declared: an ARG is scoped to the stage that declares it.
ARG NODE_DIR=cyberwave-edge-nodes/cyberwave-edge-so101

ENV DEBIAN_FRONTEND=noninteractive \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    CYBERWAVE_CAMERA_STRICT_GEOMETRY=1 \
    CYBERWAVE_VIDEO_SYNC_REQUIRED=1 \
    OPENCV_VIDEOIO_PRIORITY_V4L2=1000

# ``OPENCV_VIDEOIO_PRIORITY_V4L2`` (above) ranks the V4L2 backend ahead of
# FFMPEG. Without it, ``cv2.VideoCapture("/dev/videoN")`` — a *string* source —
# resolves to FFmpeg's libavdevice V4L2 demuxer, which logs
# ``ioctl(VIDIOC_QBUF): Bad file descriptor`` on nodes it cannot grab and makes
# ``cap.set(CAP_PROP_FOURCC)`` a no-op, so ``CYBERWAVE_CAMERA_STRICT_GEOMETRY``
# has nothing to enforce. FFMPEG stays available (non-zero priority) because
# ``CYBERWAVE_METADATA_VIDEO_DEVICE`` may be an HTTP MJPEG URL.
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
COPY --from=sdk /usr/local/lib/python3.11 /usr/local/lib/python3.11
COPY --from=sdk /usr/local/bin /usr/local/bin

COPY ${NODE_DIR}/install_realsense_docker.sh .
RUN chmod +x install_realsense_docker.sh && \
    if [ "${ENABLE_REALSENSE}" = "true" ]; then ./install_realsense_docker.sh; \
    else echo "Skipping RealSense (build with --build-arg ENABLE_REALSENSE=true to enable)"; fi

# -----------------------------------------------------------------------------
# Stage 3: Application
# -----------------------------------------------------------------------------
FROM so101-base AS runtime

ARG ENABLE_REALSENSE=false
ARG NODE_DIR=cyberwave-edge-nodes/cyberwave-edge-so101

WORKDIR /app

COPY --from=sdk /app /app

RUN python3 -c "import numpy; v = numpy.__version__; \
    assert v.startswith('1.'), f'Expected NumPy 1.x, got {v}'"

RUN python3 -c "import cv2; print('Runtime cv2 OK at', cv2.__file__)"

# Fail the build if the OpenCV wheel ever ships without V4L2. Local cameras
# would then fall back to FFmpeg's libavformat V4L2 demuxer and silently take
# the camera's native default format (often YUYV 1920x1080 @ 5 fps on USB 2.0)
# instead of the negotiated one. Casing differs across builds — the manylinux
# wheels emit lowercase ``v4l/v4l2:`` — so match with re.IGNORECASE.
RUN python3 -c "import cv2, re; info = cv2.getBuildInformation(); \
    assert re.search(r'V4L/V4L2:\s+YES', info, re.IGNORECASE), \
        'cv2 has no V4L2 backend:\n' + info; \
    print('OpenCV V4L2 backend confirmed at', cv2.__file__)"

# pyrealsense2 is installed in ``so101-base`` *after* the builder's
# site-packages are copied in. Assert it still imports here so a future reorder
# of those two steps fails the build instead of silently shipping an image where
# every RealSense twin degrades to cv2 at runtime.
RUN if [ "${ENABLE_REALSENSE}" = "true" ]; then \
        python3 -c "import pyrealsense2; print('pyrealsense2 import OK')"; \
    fi

RUN python3 -c "import cyberwave_video_sync; print('video-sync import OK')"

RUN mkdir -p /app/.cyberwave

# Declares that this image understands ``--usbip-attach-only``; edge-core
# refuses to send that flag to images without the label, since any other
# entrypoint would forward it to its own driver process.
LABEL com.cyberwave.usbip.attach-only="1"
# Declares that this image can turn host TCP serial bridges back into PTYs.
# edge-core keeps USB/IP enabled for images without this capability so a host
# SO101 bridge cannot silently change transport for unrelated drivers.
LABEL com.cyberwave.serial-bridge="1"

COPY ${NODE_DIR}/entrypoint.sh ${NODE_DIR}/usbip_lib.sh ${NODE_DIR}/serial_bridge_lib.sh ./
RUN chmod +x entrypoint.sh

ENTRYPOINT ["tini", "-s", "--", "./entrypoint.sh"]
