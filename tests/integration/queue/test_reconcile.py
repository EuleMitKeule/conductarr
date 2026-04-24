"""Integration tests for the reconcile feature.

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
    SonarrControlClient,
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
# Tests
# ---------------------------------------------------------------------------


async def test_reconcile_radarr_job(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator: Orchestrator,
    repo: QueueRepository,
) -> None:
    """A SABnzbd job whose nzo_id matches a Radarr downloadId is mapped to the
    correct virtual queue based on the movie's tags."""
    movie = await radarr_control.add_movie(
        title="Inception",
        tmdb_id=27205,
        tags=["upgrade-de"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Inception.2010.German.BluRay.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=27205, nzo_id=nzo_id)

    # Single poll cycle: SABnzbd slot + Radarr queue entry are correlated
    await orchestrator.poll_once()

    job_map = await repo.get_job_map(nzo_id)
    assert job_map is not None, "Job should be mapped after reconcile"
    assert job_map["virtual_queue"] == "german_upgrade"

    item = await repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.status == "pending"

    # Simulate job completion: remove from SABnzbd + Radarr queues
    await sabnzbd_control.finish_job(nzo_id)
    await radarr_control.finish_movie(tmdb_id=27205)

    await orchestrator.poll_once()

    # Job map should be removed and queue item marked completed
    job_map_after = await repo.get_job_map(nzo_id)
    assert job_map_after is None, "Job map should be deleted after job completes"

    item_after = await repo.get_item("radarr", str(movie["id"]))
    assert item_after is not None
    assert item_after.status == "completed"


async def test_reconcile_sonarr_job(
    sabnzbd_control: SABnzbdControlClient,
    sonarr_control: SonarrControlClient,
    orchestrator: Orchestrator,
    repo: QueueRepository,
) -> None:
    """A SABnzbd job whose nzo_id matches a Sonarr downloadId is mapped to the
    correct virtual queue based on the parent series tags."""
    series = await sonarr_control.add_series(
        title="Dark",
        tvdb_id=318408,
        tags=["request"],
        episodes=[{"episode_number": 1, "season_number": 1, "title": "Secrets"}],
    )
    episode_id: int = series["episodes"][0]["id"]

    nzo_id = await sabnzbd_control.start_job(
        filename="Dark.S01E01.German.BluRay.nzb",
        cat="sonarr",
    )
    await sonarr_control.release_episode(episode_id=episode_id, nzo_id=nzo_id)

    await orchestrator.poll_once()

    job_map = await repo.get_job_map(nzo_id)
    assert job_map is not None, "Job should be mapped after reconcile"
    assert job_map["virtual_queue"] == "user_requests"

    item = await repo.get_item("sonarr", str(episode_id))
    assert item is not None
    assert item.status == "pending"

    # Simulate job completion
    await sabnzbd_control.finish_job(nzo_id)
    await sonarr_control.finish_episode(episode_id=episode_id)

    await orchestrator.poll_once()

    job_map_after = await repo.get_job_map(nzo_id)
    assert job_map_after is None

    item_after = await repo.get_item("sonarr", str(episode_id))
    assert item_after is not None
    assert item_after.status == "completed"


async def test_reconcile_unmatched_job_is_ignored(
    sabnzbd_control: SABnzbdControlClient,
    orchestrator: Orchestrator,
    repo: QueueRepository,
) -> None:
    """A SABnzbd job with no corresponding Radarr/Sonarr entry is ignored."""
    nzo_id = await sabnzbd_control.start_job(
        filename="Some.Random.File.nzb",
        cat="misc",
    )

    await orchestrator.poll_once()

    job_map = await repo.get_job_map(nzo_id)
    assert job_map is None, "Unmatched job should not be mapped"
