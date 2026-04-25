"""Integration tests for score-based upgrade filtering.

Verifies that only releases with a custom_format_score strictly greater than
the current media's score are eligible for a grab.

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
    AcceptConditionConfig,
    ConductarrConfig,
    Config,
    GeneralConfig,
    LoggingConfig,
    MatcherConfig,
    MemoryDatabaseConfig,
    RadarrConfig,
    SabnzbdConfig,
    SonarrConfig,
    UpgradeConfig,
    VirtualQueueConfig,
)
from conductarr.orchestrator import Orchestrator
from conductarr.queue.models import QueueItem
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Config factories
# ---------------------------------------------------------------------------


def _make_infra_config() -> Config:
    return Config(
        config_dir=Path("."),
        config_file="conductarr.yml",
        general=GeneralConfig(),
        logging=LoggingConfig(),
        database=MemoryDatabaseConfig(),
    )


def _make_config() -> ConductarrConfig:
    sab = os.getenv("SABNZBD_URL", "http://localhost:8080")
    radarr = os.getenv("RADARR_URL", "http://localhost:7878")
    sonarr = os.getenv("SONARR_URL", "http://localhost:8989")
    return ConductarrConfig(
        poll_interval=2.0,
        sabnzbd=SabnzbdConfig(url=sab, api_key="sabnzbd-test-key"),
        radarr=RadarrConfig(url=radarr, api_key="radarr-test-key"),
        sonarr=SonarrConfig(url=sonarr, api_key="sonarr-test-key"),
        queues=[
            VirtualQueueConfig(
                name="hdr_upgrade",
                priority=50,
                matchers=[MatcherConfig(type="tags", tags=["upgrade"])],
                upgrade=UpgradeConfig(
                    enabled=True,
                    sources=["radarr"],
                    max_active=2,
                    accept_conditions=[
                        AcceptConditionConfig(type="custom_format", name="HDR")
                    ],
                ),
            ),
            VirtualQueueConfig(
                name="fallback",
                priority=0,
                fallback=True,
                matchers=[],
            ),
        ],
    )


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def engine_score(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
) -> AsyncGenerator[Orchestrator, None]:
    await sabnzbd_control.reset()
    await radarr_control.reset()
    eng = Orchestrator(_make_infra_config(), _make_config())
    await eng.connect()
    yield eng
    await eng.stop()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_release_with_higher_score_is_grabbed(
    radarr_control: RadarrControlClient,
    engine_score: Orchestrator,
) -> None:
    """A release whose score exceeds the current file's score is grabbed.

    Setup:
    - Movie has_file=True, current score=50
    - One HDR release available with score=150 (strictly greater)

    Expected: the release is grabbed.
    """
    movie = await radarr_control.add_movie(
        title="Dune",
        tmdb_id=438631,
        has_file=True,
        custom_format_score=50,
        custom_formats=[],
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    await radarr_control.add_release(
        tmdb_id=438631,
        guid="guid-dune-hdr-1080p",
        title="Dune.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=150,
    )

    await engine_score.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_score.poll_once()

    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == ["guid-dune-hdr-1080p"], (
        f"Expected ['guid-dune-hdr-1080p'] to be grabbed, got {mock_state['grabbed']}"
    )


async def test_release_with_equal_score_is_not_grabbed(
    radarr_control: RadarrControlClient,
    engine_score: Orchestrator,
) -> None:
    """A release whose score equals the current file's score is NOT grabbed.

    Setup:
    - Movie has_file=True, current score=100
    - One HDR release available with score=100 (equal — must not be grabbed)

    Expected: no grab occurs.
    """
    movie = await radarr_control.add_movie(
        title="Oppenheimer",
        tmdb_id=872585,
        has_file=True,
        custom_format_score=100,
        custom_formats=[],
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    await radarr_control.add_release(
        tmdb_id=872585,
        guid="guid-oppenheimer-hdr",
        title="Oppenheimer.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=100,
    )

    await engine_score.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_score.poll_once()

    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == [], f"Expected no grab, got {mock_state['grabbed']}"


async def test_release_with_lower_score_is_not_grabbed(
    radarr_control: RadarrControlClient,
    engine_score: Orchestrator,
) -> None:
    """A release whose score is lower than the current file's score is NOT grabbed.

    Setup:
    - Movie has_file=True, current score=200
    - One HDR release available with score=100 (lower — must not be grabbed)

    Expected: no grab occurs.
    """
    movie = await radarr_control.add_movie(
        title="Tenet",
        tmdb_id=577922,
        has_file=True,
        custom_format_score=200,
        custom_formats=["HDR"],
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    await radarr_control.add_release(
        tmdb_id=577922,
        guid="guid-tenet-hdr",
        title="Tenet.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=100,
    )

    await engine_score.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_score.poll_once()

    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == [], f"Expected no grab, got {mock_state['grabbed']}"


async def test_best_release_above_score_is_grabbed_when_mixed(
    radarr_control: RadarrControlClient,
    engine_score: Orchestrator,
) -> None:
    """With a mix of releases, only the one exceeding the current score is grabbed.

    Setup:
    - Movie has_file=True, current score=100
    - Release A: score=80 (lower — must be filtered)
    - Release B: score=150 (higher — eligible)

    Expected: release B is grabbed.
    """
    movie = await radarr_control.add_movie(
        title="Inception",
        tmdb_id=27205,
        has_file=True,
        custom_format_score=100,
        custom_formats=[],
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    await radarr_control.add_release(
        tmdb_id=27205,
        guid="guid-inception-hdr-720p",
        title="Inception.BluRay.HDR.720p",
        custom_formats=["HDR"],
        custom_format_score=80,
    )
    await radarr_control.add_release(
        tmdb_id=27205,
        guid="guid-inception-hdr-1080p",
        title="Inception.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=150,
    )

    await engine_score.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_score.poll_once()

    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == ["guid-inception-hdr-1080p"], (
        f"Expected ['guid-inception-hdr-1080p'] to be grabbed, "
        f"got {mock_state['grabbed']}"
    )
