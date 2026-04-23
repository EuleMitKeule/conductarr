"""Integration tests: orchestrator robustness under SABnzbd failures.

Verifies the key guarantees added by the stateless poll-loop redesign:

1. When SABnzbd is unreachable the entire cycle is skipped — no completions
   are falsely triggered and the job-map remains intact.
2. Completion detection is history-based: a job that disappears from the
   active queue (e.g. mid-Extracting) is NOT marked done until it appears in
   the SABnzbd history API.
3. A job that is cancelled (removed without going through history) is never
   marked as completed.
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from conductarr.config import (
    ConductarrConfig,
    MatcherConfig,
    MemoryDatabaseConfig,
    RadarrConfig,
    SabnzbdConfig,
    SonarrConfig,
    VirtualQueueConfig,
)
from conductarr.db.repository import QueueRepository
from conductarr.engine import ConductarrEngine
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
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
        ],
    )


@pytest_asyncio.fixture
async def engine() -> AsyncGenerator[ConductarrEngine, None]:
    eng = ConductarrEngine(_make_config(), database_config=MemoryDatabaseConfig())
    await eng.connect()
    yield eng
    await eng.stop()


@pytest.fixture
def repo(engine: ConductarrEngine) -> QueueRepository:
    return engine.repo


@pytest_asyncio.fixture(autouse=True)
async def ensure_sabnzbd_online(
    sabnzbd_control: SABnzbdControlClient,
) -> AsyncGenerator[None, None]:
    """Guarantee the mock SABnzbd is online before and after every test."""
    await sabnzbd_control.go_online()
    yield
    await sabnzbd_control.go_online()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_cycle_is_fully_skipped_when_sabnzbd_offline(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    engine: ConductarrEngine,
    repo: QueueRepository,
) -> None:
    """When SABnzbd is unreachable the poll cycle does nothing at all.

    Specifically: the job-map must remain intact (no false completions) and
    the reorder / pause logic must not be called.
    """
    movie = await radarr_control.add_movie(
        title="Resilience Test Film",
        tmdb_id=90001,
        tags=["request"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Resilience.Test.Film.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=90001, nzo_id=nzo_id)

    # First cycle: normal — job should be mapped.
    await engine.poll_once()
    job_map = await repo.get_job_map(nzo_id)
    assert job_map is not None, "Job must be mapped after the first normal cycle"

    # Take SABnzbd offline and run another cycle.
    await sabnzbd_control.go_offline()
    await engine.poll_once()

    # Job-map must still be intact — the cycle was skipped, not erroneously completed.
    job_map_after = await repo.get_job_map(nzo_id)
    assert job_map_after is not None, (
        "Job-map must not be deleted when SABnzbd was unreachable"
    )

    item = await repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.status != "completed", (
        "Queue item must not be marked completed when SABnzbd was unreachable"
    )


async def test_completion_requires_history_not_just_disappearance(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    engine: ConductarrEngine,
    repo: QueueRepository,
) -> None:
    """A job that disappears from the SABnzbd queue is NOT marked complete until
    it appears in the history API (i.e. post-processing is truly done).

    This prevents false completions during the Extracting / Moving window.
    """
    movie = await radarr_control.add_movie(
        title="History Detection Film",
        tmdb_id=90002,
        tags=["request"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="History.Detection.Film.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=90002, nzo_id=nzo_id)

    await engine.poll_once()
    assert await repo.get_job_map(nzo_id) is not None

    # Simulate post-processing transition: job vanishes from queue but has NOT
    # yet reached history.  Use cancel_job (removes without adding to history).
    await sabnzbd_control.cancel_job(nzo_id)
    await radarr_control.finish_movie(tmdb_id=90002)

    await engine.poll_once()

    # Job map must still exist and item must NOT be completed yet.
    job_map_after_disappear = await repo.get_job_map(nzo_id)
    assert job_map_after_disappear is not None, (
        "Job-map must NOT be deleted on mere queue disappearance"
    )
    item_after_disappear = await repo.get_item("radarr", str(movie["id"]))
    assert item_after_disappear is not None
    assert item_after_disappear.status != "completed", (
        "Item must NOT be completed until confirmed in SABnzbd history"
    )

    # Now simulate post-processing finishing: add a matching history entry by
    # re-adding the job and finishing it through the proper path.
    await sabnzbd_control.start_job(
        filename="History.Detection.Film.nzb",
        cat="radarr",
        nzo_id=nzo_id,
    )
    await sabnzbd_control.finish_job(nzo_id)

    await engine.poll_once()

    job_map_after_history = await repo.get_job_map(nzo_id)
    assert job_map_after_history is None, (
        "Job-map must be deleted once the job appears in SABnzbd history"
    )
    item_after_history = await repo.get_item("radarr", str(movie["id"]))
    assert item_after_history is not None
    assert item_after_history.status == "completed", (
        "Item must be completed after SABnzbd history confirms it"
    )


async def test_cancelled_job_never_marked_completed(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    engine: ConductarrEngine,
    repo: QueueRepository,
) -> None:
    """A job removed via cancel (no history entry) is never marked as completed."""
    movie = await radarr_control.add_movie(
        title="Cancelled Film",
        tmdb_id=90003,
        tags=["request"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Cancelled.Film.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=90003, nzo_id=nzo_id)

    await engine.poll_once()
    assert await repo.get_job_map(nzo_id) is not None

    # Cancel the download — no history entry created.
    await sabnzbd_control.cancel_job(nzo_id)

    # Multiple cycles: item should never flip to completed.
    for _ in range(3):
        await engine.poll_once()

    item = await repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.status != "completed", (
        "A cancelled job (not in SABnzbd history) must never be marked completed"
    )
