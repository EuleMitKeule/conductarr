"""Integration tests for queue assignment features.

Covers:
- has_no_file matcher
- Automatic upgrade-condition skip when accept_conditions are already satisfied
- External download queue resolution (no DB persist for ordering)
"""

from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from pathlib import Path

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
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
)

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


def _base_urls() -> tuple[str, str, str]:
    return (
        os.getenv("SABNZBD_URL", "http://localhost:8080"),
        os.getenv("RADARR_URL", "http://localhost:7878"),
        os.getenv("SONARR_URL", "http://localhost:8989"),
    )


def _make_config_with_has_no_file() -> ConductarrConfig:
    sab, radarr, sonarr = _base_urls()
    return ConductarrConfig(
        poll_interval=2.0,
        sabnzbd=SabnzbdConfig(url=sab, api_key="sabnzbd-test-key"),
        radarr=RadarrConfig(url=radarr, api_key="radarr-test-key"),
        sonarr=SonarrConfig(url=sonarr, api_key="sonarr-test-key"),
        queues=[
            VirtualQueueConfig(
                name="first_download",
                priority=100,
                matchers=[MatcherConfig(type="has_no_file")],
            ),
            VirtualQueueConfig(
                name="fallback",
                priority=0,
                fallback=True,
                matchers=[],
            ),
        ],
    )


def _make_config_with_upgrade_auto_skip() -> ConductarrConfig:
    sab, radarr, sonarr = _base_urls()
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
                    max_active=1,
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


def _make_config_external_download() -> ConductarrConfig:
    sab, radarr, sonarr = _base_urls()
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
                    max_active=1,
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
async def orchestrator_has_no_file() -> AsyncGenerator[Orchestrator, None]:
    eng = Orchestrator(_make_infra_config(), _make_config_with_has_no_file())
    await eng.connect()
    yield eng
    await eng.stop()


@pytest_asyncio.fixture
async def orchestrator_upgrade_skip() -> AsyncGenerator[Orchestrator, None]:
    eng = Orchestrator(_make_infra_config(), _make_config_with_upgrade_auto_skip())
    await eng.connect()
    yield eng
    await eng.stop()


@pytest_asyncio.fixture
async def orchestrator_external() -> AsyncGenerator[Orchestrator, None]:
    eng = Orchestrator(_make_infra_config(), _make_config_external_download())
    await eng.connect()
    yield eng
    await eng.stop()


# ---------------------------------------------------------------------------
# Tests: has_no_file matcher
# ---------------------------------------------------------------------------


async def test_has_no_file_matcher_routes_movie_without_file(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_has_no_file: Orchestrator,
) -> None:
    """Movie with no existing file is assigned to the 'first_download' queue."""
    movie = await radarr_control.add_movie(
        title="Interstellar",
        tmdb_id=157336,
        has_file=False,  # no existing file
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Interstellar.2014.BluRay.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=157336, nzo_id=nzo_id)

    await orchestrator_has_no_file.poll_once()

    job_map = await orchestrator_has_no_file.repo.get_job_map(nzo_id)
    assert job_map is not None
    assert job_map["virtual_queue"] == "first_download"

    item = await orchestrator_has_no_file.repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.virtual_queue == "first_download"


async def test_has_no_file_matcher_does_not_match_when_file_exists(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_has_no_file: Orchestrator,
) -> None:
    """Movie that already has a file does not match has_no_file → goes to fallback."""
    movie = await radarr_control.add_movie(
        title="Gravity",
        tmdb_id=49047,
        has_file=True,  # file already present
        custom_format_score=50,
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Gravity.2013.BluRay.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=49047, nzo_id=nzo_id)

    await orchestrator_has_no_file.poll_once()

    job_map = await orchestrator_has_no_file.repo.get_job_map(nzo_id)
    assert job_map is not None
    assert job_map["virtual_queue"] == "fallback"

    item = await orchestrator_has_no_file.repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.virtual_queue == "fallback"


# ---------------------------------------------------------------------------
# Tests: upgrade auto-condition skip
# ---------------------------------------------------------------------------


async def test_upgrade_queue_skipped_when_conditions_already_satisfied(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_upgrade_skip: Orchestrator,
) -> None:
    """Movie already having HDR is NOT routed to the upgrade queue.

    This is the "The Acolyte bug": a movie that already satisfies the upgrade
    accept_conditions should fall through to the fallback queue, not be
    assigned to the upgrade queue.
    """
    movie = await radarr_control.add_movie(
        title="Dune",
        tmdb_id=438631,
        has_file=True,
        custom_format_score=100,
        custom_formats=["HDR"],  # already satisfies accept_conditions
        tags=["upgrade"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Dune.2021.BluRay.HDR.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=438631, nzo_id=nzo_id)

    await orchestrator_upgrade_skip.poll_once()

    job_map = await orchestrator_upgrade_skip.repo.get_job_map(nzo_id)
    assert job_map is not None
    # Must NOT be in hdr_upgrade — conditions already satisfied
    assert job_map["virtual_queue"] == "fallback"

    item = await orchestrator_upgrade_skip.repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.virtual_queue == "fallback"


async def test_upgrade_queue_skipped_when_no_file(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_upgrade_skip: Orchestrator,
) -> None:
    """Movie with no file is NOT an upgrade candidate → goes to fallback."""
    movie = await radarr_control.add_movie(
        title="Tenet",
        tmdb_id=577922,
        has_file=False,  # no file → can't be an upgrade
        tags=["upgrade"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Tenet.2020.BluRay.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=577922, nzo_id=nzo_id)

    await orchestrator_upgrade_skip.poll_once()

    job_map = await orchestrator_upgrade_skip.repo.get_job_map(nzo_id)
    assert job_map is not None
    assert job_map["virtual_queue"] == "fallback"

    item = await orchestrator_upgrade_skip.repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.virtual_queue == "fallback"


async def test_upgrade_queue_assigned_when_conditions_not_satisfied(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_upgrade_skip: Orchestrator,
) -> None:
    """Movie with a file but HDR not yet satisfied IS assigned to the upgrade queue."""
    movie = await radarr_control.add_movie(
        title="Oppenheimer",
        tmdb_id=872585,
        has_file=True,
        custom_format_score=10,
        custom_formats=[],  # HDR not yet present
        tags=["upgrade"],
    )
    nzo_id = await sabnzbd_control.start_job(
        filename="Oppenheimer.2023.BluRay.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=872585, nzo_id=nzo_id)

    await orchestrator_upgrade_skip.poll_once()

    job_map = await orchestrator_upgrade_skip.repo.get_job_map(nzo_id)
    assert job_map is not None
    assert job_map["virtual_queue"] == "hdr_upgrade"

    item = await orchestrator_upgrade_skip.repo.get_item("radarr", str(movie["id"]))
    assert item is not None
    assert item.virtual_queue == "hdr_upgrade"


# ---------------------------------------------------------------------------
# Tests: external download queue resolution
# ---------------------------------------------------------------------------


async def test_external_grab_uses_fresh_context_not_db_queue(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_external: Orchestrator,
) -> None:
    """An external (non-conductarr) grab is assigned a fresh queue for ordering.

    The DB virtual_queue record is not overwritten so upgrade cursor tracking
    is unaffected.  The NZO cache entry receives the freshly resolved queue.
    External grabs are identified by the absence of upgrade_grabbed in metadata.
    """
    # Seed: create movie with no HDR → ends up in hdr_upgrade queue in DB
    movie = await radarr_control.add_movie(
        title="Avatar",
        tmdb_id=19995,
        has_file=True,
        custom_format_score=5,
        custom_formats=[],  # HDR missing → upgrade queue
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    # Simulate that conductarr previously seeded this movie → DB has hdr_upgrade
    # but no upgrade_grabbed flag (item was seeded, not yet grabbed)
    from conductarr.queue.models import QueueItem

    item = await orchestrator_external.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
            # metadata has no upgrade_grabbed → external grab path
        )
    )
    assert item.virtual_queue == "hdr_upgrade"

    # NOW the movie is finished (has HDR) by an external downloader
    await radarr_control.finish_movie(
        tmdb_id=19995,
        custom_format_score=100,
        custom_formats=["HDR"],
    )

    # External download appears in SABnzbd + Radarr queue (NOT via conductarr grab)
    nzo_id = await sabnzbd_control.start_job(
        filename="Avatar.2009.BluRay.HDR.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=19995, nzo_id=nzo_id)

    await orchestrator_external.poll_once()

    # DB virtual_queue must remain unchanged
    db_item = await orchestrator_external.repo.get_item("radarr", str(movie_id))
    assert db_item is not None
    assert db_item.virtual_queue == "hdr_upgrade"  # DB unchanged

    # The NZO cache entry for ordering should use the freshly resolved queue
    # (fallback, since HDR is now satisfied → upgrade queue skipped)
    cache_entry = orchestrator_external._nzo_cache_entries.get(nzo_id)
    assert cache_entry is not None
    assert cache_entry.virtual_queue == "fallback"


async def test_conductarr_grab_uses_persisted_virtual_queue(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    orchestrator_external: Orchestrator,
) -> None:
    """A conductarr-initiated grab is detected via upgrade_grabbed metadata.

    When upgrade_grabbed=True is set (by the grab loop before the nzo_id is
    known), _resolve_one must use the persisted virtual_queue for the job-map
    entry rather than re-deriving it.  This prevents runaway grabbing caused
    by sab_active counting going to zero because all job-map entries have the
    wrong queue name.
    """
    movie = await radarr_control.add_movie(
        title="Mad Max: Fury Road",
        tmdb_id=76341,
        has_file=True,
        custom_format_score=5,
        custom_formats=[],  # HDR not satisfied
        tags=["upgrade"],
    )
    movie_id: int = movie["id"]

    from conductarr.queue.models import QueueItem

    # Simulate state after conductarr's grab loop ran: item is in DB with
    # virtual_queue=hdr_upgrade and upgrade_grabbed=True in metadata.
    item = await orchestrator_external.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            virtual_queue="hdr_upgrade",
            tags=["upgrade"],
            metadata={"upgrade_grabbed": True},
        )
    )
    assert item.virtual_queue == "hdr_upgrade"

    # The nzo_id now appears in SABnzbd (the download conductarr grabbed)
    nzo_id = await sabnzbd_control.start_job(
        filename="Mad.Max.Fury.Road.2015.BluRay.HDR.1080p.nzb",
        cat="radarr",
    )
    await radarr_control.release_movie(tmdb_id=76341, nzo_id=nzo_id)

    await orchestrator_external.poll_once()

    # Job map must record hdr_upgrade, not a freshly-derived queue
    job_map = await orchestrator_external.repo.get_job_map(nzo_id)
    assert job_map is not None
    assert job_map["virtual_queue"] == "hdr_upgrade"

    # NZO cache entry must also reflect hdr_upgrade for correct slot counting
    cache_entry = orchestrator_external._nzo_cache_entries.get(nzo_id)
    assert cache_entry is not None
    assert cache_entry.virtual_queue == "hdr_upgrade"
