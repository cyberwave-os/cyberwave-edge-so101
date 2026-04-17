"""Benchmark RealSense camera over USB/IP or local USB.

Tests color, depth, and combined streams. Measures FPS and latency.
Run inside a Docker container with USB/IP-attached RealSense or locally.

Usage:
    python -m scripts.cw_benchmark_realsense [--seconds 10]
"""

from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass


@dataclass
class StreamResult:
    stream_name: str
    width: int
    height: int
    fps_requested: int
    frames_captured: int
    frames_failed: int
    duration_s: float
    avg_fps: float
    avg_latency_ms: float

    def summary(self) -> str:
        total = self.frames_captured + self.frames_failed
        drop_pct = (self.frames_failed / total * 100) if total else 0
        return (
            f"  {self.stream_name:>12s}: "
            f"{self.width}x{self.height}@{self.fps_requested} | "
            f"{self.avg_fps:5.1f} fps | "
            f"latency avg={self.avg_latency_ms:.1f}ms | "
            f"drops={self.frames_failed}/{total} ({drop_pct:.1f}%)"
        )


def _test_stream(
    pipeline: "rs.pipeline",
    config: "rs.config",
    stream_name: str,
    width: int,
    height: int,
    fps: int,
    duration_s: float,
    wait_for_depth: bool = False,
) -> StreamResult:
    import pyrealsense2 as rs  # type: ignore[import-untyped]

    pipeline.start(config)
    time.sleep(1)

    latencies: list[float] = []
    failures = 0
    start = time.monotonic()

    while (time.monotonic() - start) < duration_s:
        t0 = time.monotonic()
        try:
            frames = pipeline.wait_for_frames(timeout_ms=5000)
            elapsed_ms = (time.monotonic() - t0) * 1000

            if wait_for_depth:
                color = frames.get_color_frame()
                depth = frames.get_depth_frame()
                if color and depth:
                    latencies.append(elapsed_ms)
                else:
                    failures += 1
            else:
                if frames.size() > 0:
                    latencies.append(elapsed_ms)
                else:
                    failures += 1
        except RuntimeError:
            failures += 1

    pipeline.stop()

    wall = time.monotonic() - start
    if not latencies:
        return StreamResult(
            stream_name=stream_name,
            width=width, height=height, fps_requested=fps,
            frames_captured=0, frames_failed=failures,
            duration_s=wall, avg_fps=0, avg_latency_ms=0,
        )

    return StreamResult(
        stream_name=stream_name,
        width=width, height=height, fps_requested=fps,
        frames_captured=len(latencies),
        frames_failed=failures,
        duration_s=wall,
        avg_fps=len(latencies) / wall,
        avg_latency_ms=sum(latencies) / len(latencies),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark RealSense over USB/IP")
    parser.add_argument("--seconds", type=float, default=10, help="Seconds per test (default: 10)")
    args = parser.parse_args()

    try:
        import pyrealsense2 as rs  # type: ignore[import-untyped]
    except ImportError:
        print("ERROR: pyrealsense2 not installed", file=sys.stderr)
        sys.exit(1)

    ctx = rs.context()
    devices = ctx.query_devices()
    if len(devices) == 0:
        print("ERROR: No RealSense devices found (is USB/IP attached?)", file=sys.stderr)
        sys.exit(1)

    dev = devices[0]
    serial = dev.get_info(rs.camera_info.serial_number)
    name = dev.get_info(rs.camera_info.name)
    print(f"\nRealSense device: {name} (S/N: {serial})")
    print(f"USB type: {dev.get_info(rs.camera_info.usb_type_descriptor)}")
    print(f"Firmware: {dev.get_info(rs.camera_info.firmware_version)}\n")

    results: list[StreamResult] = []
    pipe = rs.pipeline()

    print("Test 1: Color only (640x480@30)")
    cfg = rs.config()
    cfg.enable_stream(rs.stream.color, 640, 480, rs.format.bgr8, 30)
    result = _test_stream(pipe, cfg, "color-VGA", 640, 480, 30, args.seconds)
    results.append(result)
    print(result.summary())

    print("Test 2: Depth only (640x480@30)")
    cfg = rs.config()
    cfg.enable_stream(rs.stream.depth, 640, 480, rs.format.z16, 30)
    result = _test_stream(pipe, cfg, "depth-VGA", 640, 480, 30, args.seconds)
    results.append(result)
    print(result.summary())

    print("Test 3: Color + Depth (640x480@30)")
    cfg = rs.config()
    cfg.enable_stream(rs.stream.color, 640, 480, rs.format.bgr8, 30)
    cfg.enable_stream(rs.stream.depth, 640, 480, rs.format.z16, 30)
    result = _test_stream(pipe, cfg, "color+depth", 640, 480, 30, args.seconds, wait_for_depth=True)
    results.append(result)
    print(result.summary())

    print("\n--- Summary ---")
    for r in results:
        print(r.summary())

    viable = all(r.avg_fps >= 10 for r in results if r.frames_captured > 0)
    print(f"\nRealSense over USB/IP viable: {'YES' if viable else 'NO (fps < 10 at some test)'}")


if __name__ == "__main__":
    main()
