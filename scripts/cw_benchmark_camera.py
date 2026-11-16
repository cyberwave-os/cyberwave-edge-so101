"""Benchmark camera capture over USB/IP or local device.

Measures FPS, latency, and frame drop rate at VGA, 720p, and 1080p.
Run inside a Docker container with USB/IP-attached camera or locally.

Usage:
    python -m scripts.cw_benchmark_camera [--device 0] [--seconds 10]
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from typing import Union


@dataclass
class BenchmarkResult:
    resolution: str
    requested_width: int
    requested_height: int
    actual_width: int
    actual_height: int
    frames_captured: int
    frames_failed: int
    duration_s: float
    avg_fps: float
    avg_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    p95_latency_ms: float

    def summary(self) -> str:
        total = self.frames_captured + self.frames_failed
        drop_pct = (self.frames_failed / total * 100) if total else 0
        return (
            f"  {self.resolution:>8s}: "
            f"{self.actual_width}x{self.actual_height} | "
            f"{self.avg_fps:5.1f} fps | "
            f"latency avg={self.avg_latency_ms:.1f}ms "
            f"p95={self.p95_latency_ms:.1f}ms "
            f"max={self.max_latency_ms:.1f}ms | "
            f"drops={self.frames_failed}/{total} ({drop_pct:.1f}%)"
        )


RESOLUTIONS = [
    ("VGA", 640, 480),
    ("720p", 1280, 720),
    ("1080p", 1920, 1080),
]


def _open_camera(device: Union[int, str]) -> "cv2.VideoCapture":
    import cv2

    cap = cv2.VideoCapture(device)
    if not cap.isOpened():
        print(f"ERROR: Cannot open camera device {device}", file=sys.stderr)
        sys.exit(1)
    return cap


def benchmark_resolution(
    device: Union[int, str],
    width: int,
    height: int,
    duration_s: float,
    label: str,
) -> BenchmarkResult:
    import cv2

    cap = _open_camera(device)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, width)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, height)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    actual_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    actual_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    print(f"  [{label}] Requested {width}x{height}, got {actual_w}x{actual_h}")

    # Warm up
    for _ in range(5):
        cap.read()

    latencies: list[float] = []
    failures = 0
    start = time.monotonic()

    while (time.monotonic() - start) < duration_s:
        t0 = time.monotonic()
        ret, frame = cap.read()
        elapsed_ms = (time.monotonic() - t0) * 1000

        if ret and frame is not None:
            latencies.append(elapsed_ms)
        else:
            failures += 1

    cap.release()

    if not latencies:
        return BenchmarkResult(
            resolution=label,
            requested_width=width,
            requested_height=height,
            actual_width=actual_w,
            actual_height=actual_h,
            frames_captured=0,
            frames_failed=failures,
            duration_s=duration_s,
            avg_fps=0,
            avg_latency_ms=0,
            min_latency_ms=0,
            max_latency_ms=0,
            p95_latency_ms=0,
        )

    sorted_lat = sorted(latencies)
    p95_idx = int(len(sorted_lat) * 0.95)
    wall = time.monotonic() - start

    return BenchmarkResult(
        resolution=label,
        requested_width=width,
        requested_height=height,
        actual_width=actual_w,
        actual_height=actual_h,
        frames_captured=len(latencies),
        frames_failed=failures,
        duration_s=wall,
        avg_fps=len(latencies) / wall,
        avg_latency_ms=sum(latencies) / len(latencies),
        min_latency_ms=sorted_lat[0],
        max_latency_ms=sorted_lat[-1],
        p95_latency_ms=sorted_lat[p95_idx],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark camera over USB/IP")
    parser.add_argument(
        "--device",
        default="0",
        help="Camera device index or path (default: 0)",
    )
    parser.add_argument(
        "--seconds",
        type=float,
        default=10,
        help="Seconds to capture per resolution (default: 10)",
    )
    parser.add_argument(
        "--resolutions",
        nargs="*",
        choices=["VGA", "720p", "1080p"],
        default=["VGA", "720p", "1080p"],
        help="Resolutions to test",
    )
    args = parser.parse_args()

    try:
        device: Union[int, str] = int(args.device)
    except ValueError:
        device = args.device

    try:
        import cv2  # noqa: F401
    except ImportError:
        print("ERROR: opencv-python is required. Install with: pip install opencv-python", file=sys.stderr)
        sys.exit(1)

    print(f"\nCamera benchmark: device={device}, duration={args.seconds}s per resolution\n")

    results: list[BenchmarkResult] = []
    for label, w, h in RESOLUTIONS:
        if label not in args.resolutions:
            continue
        print(f"  Testing {label}...")
        result = benchmark_resolution(device, w, h, args.seconds, label)
        results.append(result)
        print(result.summary())

    print("\n--- Summary ---")
    for r in results:
        print(r.summary())

    viable = all(r.avg_fps >= 15 for r in results if r.frames_captured > 0)
    print(f"\nUSB/IP viable for video: {'YES' if viable else 'NO (fps < 15 at some resolution)'}")


if __name__ == "__main__":
    main()
