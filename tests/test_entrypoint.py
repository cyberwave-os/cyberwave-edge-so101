"""Unit tests for entrypoint env var export behavior."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path


ENTRYPOINT_PATH = Path(__file__).resolve().parent.parent / "entrypoint.sh"


def _extract_embedded_python() -> str:
    """Extract the Python snippet used by entrypoint.sh for env exports."""
    lines = ENTRYPOINT_PATH.read_text().splitlines()
    start_marker = 'eval "$(python3 -c "'
    end_marker = '")"'

    start_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if line.strip() == start_marker:
            start_idx = i + 1
            break
    if start_idx is None:
        raise AssertionError("Could not find embedded python start marker in entrypoint.sh")

    for i in range(start_idx, len(lines)):
        if lines[i].strip() == end_marker:
            end_idx = i
            break
    if end_idx is None:
        raise AssertionError("Could not find embedded python end marker in entrypoint.sh")

    return "\n".join(lines[start_idx:end_idx]) + "\n"


def _run_export_script(twin_payload: dict, extra_env: dict[str, str] | None = None) -> list[str]:
    """Run entrypoint embedded exporter and return emitted export lines."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        twin_file = Path(tmp_dir) / "entrypoint_test_twin.json"
        twin_file.write_text(json.dumps(twin_payload))

        env = os.environ.copy()
        env["CYBERWAVE_TWIN_JSON_FILE"] = str(twin_file)
        if extra_env:
            env.update(extra_env)

        code = _extract_embedded_python()
        result = subprocess.run(
            [sys.executable, "-c", code],
            check=True,
            capture_output=True,
            text=True,
            env=env,
        )
        return [line for line in result.stdout.splitlines() if line.strip()]


def test_entrypoint_sanitizes_special_chars_in_nested_keys():
    """Special chars in nested keys are sanitized before env var construction."""
    payload = {
        "uuid": "abc-123",
        "metadata": {
            "drivers": {
                "intel/realsensed455": {
                    "docker-image": "cyberwaveos/so101-driver:latest-realsense",
                    "driver.version": "1.0.0",
                }
            }
        },
    }

    exports = _run_export_script(payload)

    assert "export CYBERWAVE_TWIN_UUID=abc-123" in exports
    assert any(
        line.startswith(
            "export CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DOCKER_IMAGE="
        )
        for line in exports
    )
    assert any(
        line.startswith(
            "export CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DRIVER_VERSION="
        )
        for line in exports
    )
    assert all("/" not in line.split("=", 1)[0] for line in exports)


def test_entrypoint_respects_existing_sanitized_env_var():
    """Exporter should not override explicitly provided env vars."""
    payload = {
        "metadata": {
            "drivers": {
                "intel/realsensed455": {
                    "docker-image": "new-image:tag",
                }
            }
        }
    }
    existing = {
        "CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DOCKER_IMAGE": "pre-set-image:tag",
    }

    exports = _run_export_script(payload, extra_env=existing)

    assert not any(
        line.startswith("export CYBERWAVE_METADATA_DRIVERS_INTEL_REALSENSED455_DOCKER_IMAGE=")
        for line in exports
    )
