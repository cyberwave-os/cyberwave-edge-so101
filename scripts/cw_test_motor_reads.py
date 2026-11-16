#!/usr/bin/env python3
"""Diagnostic script to test batch motor readings and identify communication issues."""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from motors import FeetechMotorsBus, Motor, MotorNormMode
from so101.robot import SO101_MOTORS

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def test_sync_read(
    bus: FeetechMotorsBus,
    register: str,
    num_reads: int = 10,
    delay_ms: float = 10,
    num_retry: int = 0,
    use_sequential: bool = False,
) -> Dict[str, any]:
    """
    Test sync read for a register multiple times.
    
    Returns stats: success_count, fail_count, timing, values
    """
    motor_ids = [m.id for m in bus.motors.values()]
    results = {
        "register": register,
        "num_reads": num_reads,
        "success_count": 0,
        "fail_count": 0,
        "empty_count": 0,
        "timings_ms": [],
        "values": [],
        "errors": [],
    }
    
    for i in range(num_reads):
        start = time.perf_counter()
        try:
            values = bus.sync_read_register(
                register,
                motor_ids,
                num_retry=num_retry,
                use_sequential=use_sequential,
                decode=True,
            )
            elapsed_ms = (time.perf_counter() - start) * 1000
            results["timings_ms"].append(elapsed_ms)
            
            if values and len(values) == len(motor_ids):
                results["success_count"] += 1
                results["values"].append(values)
            elif values:
                results["fail_count"] += 1
                results["errors"].append(f"Partial read: got {len(values)}/{len(motor_ids)} motors")
            else:
                results["empty_count"] += 1
                results["errors"].append("Empty result")
                
        except Exception as e:
            elapsed_ms = (time.perf_counter() - start) * 1000
            results["timings_ms"].append(elapsed_ms)
            results["fail_count"] += 1
            results["errors"].append(str(e))
        
        if delay_ms > 0:
            time.sleep(delay_ms / 1000)
    
    return results


def test_individual_reads(
    bus: FeetechMotorsBus,
    register: str,
    num_reads: int = 5,
) -> Dict[int, Dict[str, any]]:
    """Test reading each motor individually to isolate issues."""
    results = {}
    
    for name, motor in bus.motors.items():
        motor_results = {
            "name": name,
            "success_count": 0,
            "fail_count": 0,
            "values": [],
            "errors": [],
        }
        
        for _ in range(num_reads):
            try:
                value = bus.read_register_by_id(register, motor.id, decode=True)
                motor_results["success_count"] += 1
                motor_results["values"].append(value)
            except Exception as e:
                motor_results["fail_count"] += 1
                motor_results["errors"].append(str(e))
        
        results[motor.id] = motor_results
    
    return results


def print_results(results: Dict[str, any], title: str) -> None:
    """Print test results in a readable format."""
    print(f"\n{'='*60}")
    print(f" {title}")
    print(f"{'='*60}")
    print(f"Register: {results['register']}")
    print(f"Total reads: {results['num_reads']}")
    print(f"  ✓ Success: {results['success_count']}")
    print(f"  ✗ Failed:  {results['fail_count']}")
    print(f"  ∅ Empty:   {results['empty_count']}")
    
    if results["timings_ms"]:
        avg_ms = sum(results["timings_ms"]) / len(results["timings_ms"])
        min_ms = min(results["timings_ms"])
        max_ms = max(results["timings_ms"])
        print(f"Timing (ms): avg={avg_ms:.2f}, min={min_ms:.2f}, max={max_ms:.2f}")
    
    if results["values"]:
        last_values = results["values"][-1]
        print(f"Last values: {last_values}")
    
    if results["errors"]:
        unique_errors = list(set(results["errors"][:5]))
        print(f"Sample errors: {unique_errors}")


def print_individual_results(results: Dict[int, Dict[str, any]]) -> None:
    """Print individual motor test results."""
    print(f"\n{'='*60}")
    print(" Individual Motor Reads")
    print(f"{'='*60}")
    
    for motor_id, data in sorted(results.items()):
        name = data["name"]
        success = data["success_count"]
        fail = data["fail_count"]
        total = success + fail
        status = "✓" if fail == 0 else "✗"
        
        values_str = ""
        if data["values"]:
            values_str = f" values={data['values'][-3:]}"
        
        errors_str = ""
        if data["errors"]:
            errors_str = f" errors={data['errors'][:2]}"
        
        print(f"  {status} Motor {motor_id} ({name:14s}): {success}/{total} OK{values_str}{errors_str}")


def main():
    parser = argparse.ArgumentParser(
        description="Test batch motor readings to diagnose communication issues"
    )
    parser.add_argument(
        "--port",
        "-p",
        required=True,
        help="Serial port (e.g., /dev/tty.usbmodem5B141129631)",
    )
    parser.add_argument(
        "--num-reads",
        "-n",
        type=int,
        default=20,
        help="Number of read attempts per test (default: 20)",
    )
    parser.add_argument(
        "--delay-ms",
        "-d",
        type=float,
        default=10,
        help="Delay between reads in ms (default: 10)",
    )
    parser.add_argument(
        "--retry",
        "-r",
        type=int,
        default=0,
        help="Number of retries per read (default: 0)",
    )
    parser.add_argument(
        "--sequential",
        "-s",
        action="store_true",
        help="Use sequential reads instead of sync read",
    )
    parser.add_argument(
        "--skip-preflight",
        action="store_true",
        help="Skip preflight check during connect",
    )
    parser.add_argument(
        "--registers",
        nargs="+",
        default=["Present_Position", "Present_Speed", "Present_Load"],
        help="Registers to test (default: Present_Position Present_Speed Present_Load)",
    )
    
    args = parser.parse_args()
    
    print(f"\n{'#'*60}")
    print(f" Motor Read Diagnostic Test")
    print(f"{'#'*60}")
    print(f"Port: {args.port}")
    print(f"Reads per test: {args.num_reads}")
    print(f"Delay between reads: {args.delay_ms}ms")
    print(f"Retries per read: {args.retry}")
    print(f"Mode: {'Sequential' if args.sequential else 'Sync (batch)'}")
    print(f"Registers: {args.registers}")
    
    # Initialize motor bus
    print(f"\nConnecting to motor bus...")
    bus = FeetechMotorsBus(port=args.port, motors=SO101_MOTORS, calibration=None)
    
    try:
        bus.connect(preflight_check=not args.skip_preflight)
        print(f"✓ Connected to {args.port}")
        
        # Test 1: Individual motor reads (to isolate bad motors)
        print("\n" + "-"*60)
        print("Test 1: Individual motor reads (isolate issues)")
        print("-"*60)
        individual_results = test_individual_reads(bus, "Present_Position", num_reads=5)
        print_individual_results(individual_results)
        
        # Test 2: Sync reads for each register
        for register in args.registers:
            print(f"\n" + "-"*60)
            print(f"Test 2: Sync read - {register}")
            print("-"*60)
            
            results = test_sync_read(
                bus,
                register,
                num_reads=args.num_reads,
                delay_ms=args.delay_ms,
                num_retry=args.retry,
                use_sequential=args.sequential,
            )
            print_results(results, f"Sync Read: {register}")
        
        # Test 3: Rapid fire reads (stress test)
        print(f"\n" + "-"*60)
        print("Test 3: Rapid fire reads (0ms delay, stress test)")
        print("-"*60)
        rapid_results = test_sync_read(
            bus,
            "Present_Position",
            num_reads=50,
            delay_ms=0,
            num_retry=0,
            use_sequential=args.sequential,
        )
        print_results(rapid_results, "Rapid Fire: Present_Position")
        
        # Test 4: With retries
        print(f"\n" + "-"*60)
        print("Test 4: With retries (num_retry=2)")
        print("-"*60)
        retry_results = test_sync_read(
            bus,
            "Present_Position",
            num_reads=args.num_reads,
            delay_ms=args.delay_ms,
            num_retry=2,
            use_sequential=args.sequential,
        )
        print_results(retry_results, "With Retries: Present_Position")
        
        # Test 5: Sequential vs Sync comparison
        if not args.sequential:
            print(f"\n" + "-"*60)
            print("Test 5: Sequential reads (for comparison)")
            print("-"*60)
            seq_results = test_sync_read(
                bus,
                "Present_Position",
                num_reads=args.num_reads,
                delay_ms=args.delay_ms,
                num_retry=0,
                use_sequential=True,
            )
            print_results(seq_results, "Sequential: Present_Position")
        
        # Summary
        print(f"\n{'#'*60}")
        print(" Summary")
        print(f"{'#'*60}")
        
        total_success = sum(1 for r in individual_results.values() if r["fail_count"] == 0)
        total_motors = len(individual_results)
        print(f"Individual motor health: {total_success}/{total_motors} motors OK")
        
        if rapid_results["fail_count"] > 0 or rapid_results["empty_count"] > 0:
            fail_rate = (rapid_results["fail_count"] + rapid_results["empty_count"]) / rapid_results["num_reads"] * 100
            print(f"⚠ Rapid fire failure rate: {fail_rate:.1f}%")
            if fail_rate > 10:
                print("  → Consider adding delays between reads or using retries")
        else:
            print("✓ Rapid fire reads: all successful")
        
        if retry_results["success_count"] > rapid_results["success_count"]:
            print("✓ Retries help - consider setting num_retry=1 or 2 in production")
        
    except Exception as e:
        print(f"\n✗ Error: {e}")
        logger.exception("Test failed")
        return 1
    finally:
        if bus.connected:
            bus.disconnect()
            print(f"\nDisconnected from {args.port}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
