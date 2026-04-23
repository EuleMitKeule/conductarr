"""Integration tests for blocklist filtering during upgrade grabs.

Verifies that releases whose GUID appears in Radarr's or Sonarr's blocklist
are skipped before a grab is attempted.

Requires the mock services to be running (started automatically by the
session-scoped ``mock_services`` fixture in tests/integration/conftest.py).
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from conductarr.config import (
    AcceptConditionConfig,
    ConductarrConfig,
    MatcherConfig,
    MemoryDatabaseConfig,
    RadarrConfig,
    SabnzbdConfig,
    SonarrConfig,
    UpgradeConfig,
    VirtualQueueConfig,
)
from conductarr.engine import ConductarrEngine
from conductarr.queue.models import QueueItem
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
)

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Config factory
# ---------------------------------------------------------------------------


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
async def engine_blocklist() -> AsyncGenerator[ConductarrEngine, None]:
    eng = ConductarrEngine(_make_config(), database_config=MemoryDatabaseConfig())
    await eng.connect()
    yield eng
    await eng.stop()


@pytest_asyncio.fixture(autouse=True)
async def reset_mocks(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
) -> AsyncGenerator[None, None]:
    await sabnzbd_control.reset()
    await radarr_control.reset()
    yield


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_blocklisted_release_is_skipped_clean_release_grabbed(
    radarr_control: RadarrControlClient,
    engine_blocklist: ConductarrEngine,
) -> None:
    """The blocklisted release is filtered out; the clean release is grabbed.

    Setup:
    - Movie has_file=True, no HDR → eligible for upgrade
    - Two HDR releases available: one blocklisted (guid-bad), one clean (guid-good)
    - Blocklist contains guid-bad

    Expected: guid-good is grabbed; guid-bad is never grabbed.
    """
    movie = await radarr_control.add_movie(
        title="Blade Runner 2049",
        tmdb_id=335984,
        has_file=True,
        custom_format_score=10,
        custom_formats=[],  # no HDR yet
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    # Seed two HDR releases
    await radarr_control.add_release(
        tmdb_id=335984,
        guid="guid-bad",
        title="Blade.Runner.2049.BluRay.HDR.720p",
        custom_formats=["HDR"],
        custom_format_score=100,
    )
    await radarr_control.add_release(
        tmdb_id=335984,
        guid="guid-good",
        title="Blade.Runner.2049.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=200,  # better score → would normally be preferred
    )

    # Blocklist the better release
    await radarr_control.add_to_blocklist("guid-good")

    # Seed upgrade candidate in DB (simulates prior queue assignment)
    await engine_blocklist.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_blocklist.poll_once()

    # Verify guid-bad was grabbed (the only remaining match)
    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == ["guid-bad"], (
        f"Expected ['guid-bad'] to be grabbed, got {mock_state['grabbed']}"
    )

    # Verify DB marks the candidate as grabbed
    db_item = await engine_blocklist.repo.get_item("radarr", str(movie_id))
    assert db_item is not None
    assert db_item.metadata.get("upgrade_grabbed") is True


async def test_all_releases_blocklisted_no_grab_occurs(
    radarr_control: RadarrControlClient,
    engine_blocklist: ConductarrEngine,
) -> None:
    """When all matching releases are blocklisted, no grab occurs.

    The candidate cursor is still advanced so the next cycle moves on.
    """
    movie = await radarr_control.add_movie(
        title="Interstellar",
        tmdb_id=157336,
        has_file=True,
        custom_format_score=5,
        custom_formats=[],
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    await radarr_control.add_release(
        tmdb_id=157336,
        guid="guid-blocklisted-only",
        title="Interstellar.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=100,
    )
    await radarr_control.add_to_blocklist("guid-blocklisted-only")

    await engine_blocklist.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_blocklist.poll_once()

    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == [], (
        f"Expected no grabs, got {mock_state['grabbed']}"
    )

    # upgrade_grabbed must NOT be set since no grab happened
    db_item = await engine_blocklist.repo.get_item("radarr", str(movie_id))
    assert db_item is not None
    assert not db_item.metadata.get("upgrade_grabbed")


async def test_empty_blocklist_does_not_filter_releases(
    radarr_control: RadarrControlClient,
    engine_blocklist: ConductarrEngine,
) -> None:
    """With an empty blocklist, the best release is grabbed normally."""
    movie = await radarr_control.add_movie(
        title="The Dark Knight",
        tmdb_id=155,
        has_file=True,
        custom_format_score=5,
        custom_formats=[],
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    await radarr_control.add_release(
        tmdb_id=155,
        guid="guid-tdk-hdr",
        title="The.Dark.Knight.BluRay.HDR.1080p",
        custom_formats=["HDR"],
        custom_format_score=100,
    )
    # No blocklist entries added

    await engine_blocklist.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
        )
    )

    await engine_blocklist.poll_once()

    mock_state = await radarr_control.get_state()
    assert mock_state["grabbed"] == ["guid-tdk-hdr"]

    db_item = await engine_blocklist.repo.get_item("radarr", str(movie_id))
    assert db_item is not None
    assert db_item.metadata.get("upgrade_grabbed") is True
