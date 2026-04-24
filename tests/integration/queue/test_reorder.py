"""Integration tests for queue reordering and active-job enforcement.

Requires the mock services to be running (started automatically by the
session-scoped ``mock_services`` fixture in tests/integration/conftest.py).
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
import pytest_asyncio

from conductarr.config import (
    ConductarrConfig,
    Config,
    GeneralConfig,
    LoggingConfig,
    MatcherConfig,
    MemoryDatabaseConfig,
    RadarrConfig,
    SabnzbdConfig,
    SonarrConfig,
    VirtualQueueConfig,
)
from conductarr.db.repository import QueueRepository
from conductarr.orchestrator import Orchestrator
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
)


def _make_infra_config() -> Config:
    return Config(
        config_dir=Path("."),
        config_file="conductarr.yml",
        general=GeneralConfig(),
        logging=LoggingConfig(),
        database=MemoryDatabaseConfig(),
    )


def _make_config() -> ConductarrConfig:
    return ConductarrConfig(
        poll_interval=2.0,
        sabnzbd=SabnzbdConfig(
            url=os.getenv("SABNZBD_URL", "http://localhost:8080"),
            api_key="sabnzbd-test-key",
        ),
        radarr=RadarrConfig(
            url=os.getenv("RADARR_URL", "http://localhost:7878"),
            api_key="radarr-test-key",
        ),
        sonarr=SonarrConfig(
            url=os.getenv("SONARR_URL", "http://localhost:8989"),
            api_key="sonarr-test-key",
        ),
        queues=[
            VirtualQueueConfig(
                name="user_requests",
                priority=100,
                matchers=[
                    MatcherConfig(type="tags", tags=["request", "overseerr"]),
                ],
            ),
            VirtualQueueConfig(
                name="german_upgrade",
                priority=50,
                matchers=[
                    MatcherConfig(type="tags", tags=["upgrade-de"]),
                ],
            ),
        ],
    )


@pytest_asyncio.fixture
async def orchestrator() -> AsyncGenerator[Orchestrator, None]:
    eng = Orchestrator(_make_infra_config(), _make_config())
    await eng.connect()
    yield eng
    await eng.stop()


@pytest.fixture
def repo(orchestrator: Orchestrator) -> QueueRepository:
    return orchestrator.repo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _jobs_by_index(state: dict[str, object]) -> dict[int, dict[str, object]]:
    """Return {index: job_dict} from a mock SABnzbd state snapshot."""
    jobs = state["jobs"]
    assert isinstance(jobs, dict)
    return {int(job["index"]): job for job in jobs.values()}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_higher_priority_job_moves_to_front(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """A job assigned to a higher-priority virtual queue is moved to slot 0."""
    # Add two movies: low-priority one is released first (index 0).
    await radarr_control.add_movie(
        title="Low Priority Film",
        tmdb_id=1001,
        tags=["upgrade-de"],  # german_upgrade, priority=50
    )
    await radarr_control.add_movie(
        title="High Priority Film",
        tmdb_id=1002,
        tags=["request"],  # user_requests, priority=100
    )

    nzo_low = await sabnzbd_control.start_job(
        filename="Low.Priority.Film.nzb", cat="radarr"
    )
    nzo_high = await sabnzbd_control.start_job(
        filename="High.Priority.Film.nzb", cat="radarr"
    )

    await radarr_control.release_movie(tmdb_id=1001, nzo_id=nzo_low)
    await radarr_control.release_movie(tmdb_id=1002, nzo_id=nzo_high)

    await orchestrator.poll_once()

    state = await sabnzbd_control.get_state()
    by_index = _jobs_by_index(state)

    assert by_index[0]["nzo_id"] == nzo_high, "Higher-priority job must be at slot 0"
    assert by_index[1]["nzo_id"] == nzo_low, "Lower-priority job must be at slot 1"


async def test_same_priority_order_stays_stable(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """Jobs in the same virtual queue keep their original relative order."""
    await radarr_control.add_movie(
        title="Request Film A", tmdb_id=2001, tags=["request"]
    )
    await radarr_control.add_movie(
        title="Request Film B", tmdb_id=2002, tags=["request"]
    )

    nzo_a = await sabnzbd_control.start_job(filename="Film.A.nzb", cat="radarr")
    nzo_b = await sabnzbd_control.start_job(filename="Film.B.nzb", cat="radarr")

    await radarr_control.release_movie(tmdb_id=2001, nzo_id=nzo_a)
    await radarr_control.release_movie(tmdb_id=2002, nzo_id=nzo_b)

    await orchestrator.poll_once()

    state = await sabnzbd_control.get_state()
    by_index = _jobs_by_index(state)

    # Original insertion order must be preserved (A before B).
    assert by_index[0]["nzo_id"] == nzo_a
    assert by_index[1]["nzo_id"] == nzo_b


async def test_unknown_jobs_go_to_back(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """A SABnzbd job with no Radarr/Sonarr match is pushed behind mapped jobs."""
    await radarr_control.add_movie(title="Known Film", tmdb_id=3001, tags=["request"])

    # Unknown job added first → starts at index 0.
    nzo_unknown = await sabnzbd_control.start_job(
        filename="Mystery.File.nzb", cat="misc"
    )
    nzo_known = await sabnzbd_control.start_job(filename="Known.Film.nzb", cat="radarr")

    await radarr_control.release_movie(tmdb_id=3001, nzo_id=nzo_known)

    await orchestrator.poll_once()

    state = await sabnzbd_control.get_state()
    by_index = _jobs_by_index(state)

    assert by_index[0]["nzo_id"] == nzo_known, "Mapped job must be at slot 0"
    assert by_index[1]["nzo_id"] == nzo_unknown, "Unknown job must be at slot 1"


async def test_reorder_is_idempotent(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """switch() is not called again when the queue is already in the correct order."""
    await radarr_control.add_movie(
        title="Idempotent Low", tmdb_id=4001, tags=["upgrade-de"]
    )
    await radarr_control.add_movie(
        title="Idempotent High", tmdb_id=4002, tags=["request"]
    )

    nzo_low = await sabnzbd_control.start_job(
        filename="Idempotent.Low.nzb", cat="radarr"
    )
    nzo_high = await sabnzbd_control.start_job(
        filename="Idempotent.High.nzb", cat="radarr"
    )

    await radarr_control.release_movie(tmdb_id=4001, nzo_id=nzo_low)
    await radarr_control.release_movie(tmdb_id=4002, nzo_id=nzo_high)

    # First poll: queue is out of order → switch() must be called at least once.
    await orchestrator.poll_once()
    state_after_first = await sabnzbd_control.get_state()
    assert state_after_first["switch_call_count"] >= 1, (
        "Expected switch() on first poll"
    )

    # Second poll: queue is already in the correct order → no additional switch() calls.
    await orchestrator.poll_once()
    state_after_second = await sabnzbd_control.get_state()
    assert (
        state_after_second["switch_call_count"]
        == state_after_first["switch_call_count"]
    ), "switch() must not be called when order is already correct"


async def test_three_queues_correct_order(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """Three jobs spanning two queues plus one unknown sort into the right order."""
    await radarr_control.add_movie(
        title="Three High",
        tmdb_id=5001,
        tags=["request"],  # user_requests prio=100
    )
    await radarr_control.add_movie(
        title="Three Mid",
        tmdb_id=5002,
        tags=["upgrade-de"],  # german_upgrade prio=50
    )

    # Intentionally add in reverse-priority order so reordering is required.
    nzo_unknown = await sabnzbd_control.start_job(filename="Unknown.nzb", cat="misc")
    nzo_mid = await sabnzbd_control.start_job(filename="Three.Mid.nzb", cat="radarr")
    nzo_high = await sabnzbd_control.start_job(filename="Three.High.nzb", cat="radarr")

    await radarr_control.release_movie(tmdb_id=5001, nzo_id=nzo_high)
    await radarr_control.release_movie(tmdb_id=5002, nzo_id=nzo_mid)

    await orchestrator.poll_once()

    state = await sabnzbd_control.get_state()
    by_index = _jobs_by_index(state)

    assert by_index[0]["nzo_id"] == nzo_high, "user_requests job must be first"
    assert by_index[1]["nzo_id"] == nzo_mid, "german_upgrade job must be second"
    assert by_index[2]["nzo_id"] == nzo_unknown, "Unknown job must be last"


async def test_only_slot0_downloading_after_reorder(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """After reconcile only slot 0 is active; all other slots are individually paused."""
    await radarr_control.add_movie(title="Active A", tmdb_id=6001, tags=["request"])
    await radarr_control.add_movie(title="Paused B", tmdb_id=6002, tags=["upgrade-de"])
    await radarr_control.add_movie(title="Paused C", tmdb_id=6003, tags=["upgrade-de"])

    nzo_a = await sabnzbd_control.start_job(filename="Active.A.nzb", cat="radarr")
    nzo_b = await sabnzbd_control.start_job(filename="Paused.B.nzb", cat="radarr")
    nzo_c = await sabnzbd_control.start_job(filename="Paused.C.nzb", cat="radarr")

    await radarr_control.release_movie(tmdb_id=6001, nzo_id=nzo_a)
    await radarr_control.release_movie(tmdb_id=6002, nzo_id=nzo_b)
    await radarr_control.release_movie(tmdb_id=6003, nzo_id=nzo_c)

    await orchestrator.poll_once()

    state = await sabnzbd_control.get_state()
    by_index = _jobs_by_index(state)

    assert by_index[0]["paused"] is False, "Slot 0 must be active (not paused)"
    assert by_index[1]["paused"] is True, "Slot 1 must be paused"
    assert by_index[2]["paused"] is True, "Slot 2 must be paused"


async def test_next_job_starts_when_top_finishes(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
) -> None:
    """When the top job finishes, the new slot 0 is automatically resumed."""
    await radarr_control.add_movie(
        title="Finishing High", tmdb_id=7001, tags=["request"]
    )
    await radarr_control.add_movie(
        title="Waiting Low", tmdb_id=7002, tags=["upgrade-de"]
    )

    nzo_high = await sabnzbd_control.start_job(
        filename="Finishing.High.nzb", cat="radarr"
    )
    nzo_low = await sabnzbd_control.start_job(filename="Waiting.Low.nzb", cat="radarr")

    await radarr_control.release_movie(tmdb_id=7001, nzo_id=nzo_high)
    await radarr_control.release_movie(tmdb_id=7002, nzo_id=nzo_low)

    # First poll: nzo_high → slot 0 (active), nzo_low → slot 1 (paused).
    await orchestrator.poll_once()

    state_mid = await sabnzbd_control.get_state()
    by_index = _jobs_by_index(state_mid)
    assert by_index[0]["nzo_id"] == nzo_high
    assert by_index[0]["paused"] is False
    assert by_index[1]["nzo_id"] == nzo_low
    assert by_index[1]["paused"] is True

    # Top job completes.
    await sabnzbd_control.finish_job(nzo_high)
    await radarr_control.finish_movie(tmdb_id=7001)

    # Second poll: nzo_low is now the only slot → must be resumed automatically.
    await orchestrator.poll_once()

    state_final = await sabnzbd_control.get_state()
    assert nzo_high not in state_final["jobs"], "Finished job must be gone"
    assert state_final["jobs"][nzo_low]["paused"] is False, (
        "New top job must be resumed after previous top job finishes"
    )
