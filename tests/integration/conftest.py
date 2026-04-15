"""Integration-test session management.

Automatically starts the mock services via docker compose when the integration
test suite runs and tears them down afterwards.  If the containers are already
running (e.g. started manually) they are left untouched after the tests.
"""

from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

import pytest
import pytest_asyncio

from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
    SonarrControlClient,
)

# Ensure the session-scoped sabnzbd_api_key fixture (defined in tests/conftest.py)
# always returns the mock key when running the integration suite.  We set this
# at module-import time (before any fixture is instantiated) so the env-var
# branch in that fixture is hit first, regardless of session fixture scoping.
os.environ.setdefault("SABNZBD_API_KEY", "sabnzbd-test-key")

_COMPOSE_FILE = Path(__file__).parent.parent.parent / "docker-compose.dev.yml"
_SERVICES = ["mock-sabnzbd", "mock-radarr", "mock-sonarr"]
_HEALTH_RETRIES = 40
_HEALTH_INTERVAL = 3  # seconds


def _container_health(name: str) -> str:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Health.Status}}", name],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _is_running(name: str) -> bool:
    result = subprocess.run(
        ["docker", "inspect", "--format", "{{.State.Running}}", name],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() == "true"


def _all_running() -> bool:
    return all(_is_running(s) for s in _SERVICES)


def _start_mocks() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), "up", *_SERVICES, "-d"],
        check=True,
    )


def _stop_mocks() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(_COMPOSE_FILE), "stop", *_SERVICES],
        check=True,
    )


def _wait_for_healthy() -> None:
    for attempt in range(1, _HEALTH_RETRIES + 1):
        statuses = {s: _container_health(s) for s in _SERVICES}
        if all(v == "healthy" for v in statuses.values()):
            return
        if attempt % 5 == 0:
            print(f"\n  Waiting for mocks… ({attempt}/{_HEALTH_RETRIES}, {statuses})")
        time.sleep(_HEALTH_INTERVAL)
    raise RuntimeError(
        f"Mock services not healthy after {_HEALTH_RETRIES * _HEALTH_INTERVAL}s"
    )


@pytest.fixture(scope="session", autouse=True)
def mock_services() -> None:  # type: ignore[misc]
    """Ensure mock services are running for integration tests."""
    already_running = _all_running()

    if not already_running:
        print(f"\n  Starting mock services via docker compose ({_COMPOSE_FILE.name})…")
        _start_mocks()

    _wait_for_healthy()

    yield

    if not already_running:
        print("\n  Stopping mock services (were started by test session)…")
        _stop_mocks()


@pytest_asyncio.fixture(autouse=True)
async def reset_mocks(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    sonarr_control: SonarrControlClient,
) -> None:
    """Reset all mock services before each integration test."""
    await sabnzbd_control.reset()
    await radarr_control.reset()
    await sonarr_control.reset()
