"""Integration-test session management.

Automatically starts SABnzbd via docker compose when the integration test
suite runs and tears it down afterwards.  If the container is already
running (e.g. started manually) it is left untouched after the tests.
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path

import pytest

_COMPOSE_FILE = Path(__file__).parent.parent.parent / "docker-compose.dev.yml"
_SERVICE = "sabnzbd"
_HEALTH_RETRIES = 40
_HEALTH_INTERVAL = 3  # seconds


def _container_status() -> str:
    """Return the Docker health/running status of the SABnzbd container."""
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Health.Status}}", _SERVICE],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _is_running() -> bool:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", _SERVICE],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "true"


def _start_sabnzbd() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), "up", _SERVICE, "-d"],
        check=True,
    )


def _stop_sabnzbd() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), "stop", _SERVICE],
        check=True,
    )


def _wait_for_healthy() -> None:
    for attempt in range(1, _HEALTH_RETRIES + 1):
        status = _container_status()
        if status == "healthy":
            return
        if attempt % 5 == 0:
            print(
                f"\n  Waiting for SABnzbd to be healthy... ({attempt}/{_HEALTH_RETRIES}, status={status!r})"
            )
        time.sleep(_HEALTH_INTERVAL)
    raise RuntimeError(
        f"SABnzbd did not become healthy after {_HEALTH_RETRIES * _HEALTH_INTERVAL}s"
    )


@pytest.fixture(scope="session", autouse=True)
def sabnzbd_service() -> None:  # type: ignore[misc]
    """Ensure SABnzbd is running for the duration of the integration test session."""
    already_running = _is_running()

    if not already_running:
        print(f"\n  Starting SABnzbd via docker compose ({_COMPOSE_FILE.name})...")
        _start_sabnzbd()

    _wait_for_healthy()

    yield

    if not already_running:
        print("\n  Stopping SABnzbd (was started by test session)...")
        _stop_sabnzbd()
