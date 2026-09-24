"""End-to-end safety tests against the HTTP mock services.

These exercise the real HTTP clients, JSON parsing, the queue manager and
the upgrade scheduler together.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

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
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
    SonarrControlClient,
)

pytestmark = pytest.mark.asyncio


def _config(**upgrade: Any) -> ConductarrConfig:
    upgrade_cfg: dict[str, Any] = {
        "sources": ["radarr", "sonarr"],
        "max_active": 1,
        "search_interval": 0,
        "max_searches_per_day": 0,
        "accept_conditions": [
            AcceptConditionConfig(type="custom_format", name="German DL")
        ],
        **upgrade,
    }
    return ConductarrConfig(
        poll_interval=1.0,
        min_free_space_gb=20,
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
                name="requests",
                priority=100,
                matchers=[MatcherConfig(type="tags", tags=["request"])],
            ),
            VirtualQueueConfig(
                name="german_upgrade",
                priority=50,
                upgrade=UpgradeConfig(**upgrade_cfg),
            ),
            VirtualQueueConfig(name="other", priority=0, fallback=True),
        ],
    )


def _infra(tmp_path: Path) -> Config:
    return Config(
        config_dir=tmp_path,
        config_file="conductarr.yml",
        general=GeneralConfig(),
        logging=LoggingConfig(),
        database=MemoryDatabaseConfig(),
    )


@pytest_asyncio.fixture
async def make_engine(
    tmp_path: Path,
) -> AsyncGenerator[Any, None]:
    engines: list[Orchestrator] = []

    async def _make(**upgrade: Any) -> Orchestrator:
        engine = Orchestrator(_infra(tmp_path), _config(**upgrade))
        await engine.connect()
        await engine.scan_library()
        engines.append(engine)
        return engine

    yield _make
    for engine in engines:
        await engine.stop()


async def _movie(radarr: RadarrControlClient, tmdb_id: int, **kwargs: Any) -> int:
    kwargs.setdefault("has_file", True)
    kwargs.setdefault("custom_format_score", 10)
    movie = await radarr.add_movie(title=f"Movie {tmdb_id}", tmdb_id=tmdb_id, **kwargs)
    return int(movie["id"])


# ---------------------------------------------------------------------------
# Release safety
# ---------------------------------------------------------------------------


async def test_only_safe_usenet_release_is_grabbed(
    radarr_control: RadarrControlClient, make_engine: Any
) -> None:
    await _movie(radarr_control, 100)
    await _movie(radarr_control, 999)
    common: dict[str, Any] = {"tmdb_id": 100, "custom_formats": ["German DL"]}
    await radarr_control.add_release(
        guid="torrent",
        title="Torrent",
        protocol="torrent",
        custom_format_score=9000,
        **common,
    )
    await radarr_control.add_release(
        guid="wrong",
        title="Wrong",
        mapped_tmdb_id=999,
        custom_format_score=8000,
        **common,
    )
    await radarr_control.add_release(
        guid="rejected",
        title="Rejected",
        rejections=["Existing file meets cutoff: Bluray-1080p", "Wrong movie"],
        custom_format_score=7000,
        **common,
    )
    await radarr_control.add_release(
        guid="lowres",
        title="LowRes",
        resolution=720,
        custom_format_score=6000,
        **common,
    )
    await radarr_control.add_release(
        guid="safe",
        title="Safe",
        rejections=["Existing file meets cutoff: Bluray-1080p"],
        custom_format_score=500,
        **common,
    )
    engine = await make_engine(sources=["radarr"])

    await engine.poll_once()

    state = await radarr_control.get_state()
    assert state["grabbed"] == ["safe"]


async def test_no_grab_when_disk_space_low(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    make_engine: Any,
) -> None:
    await _movie(radarr_control, 100)
    await radarr_control.add_release(
        tmdb_id=100,
        guid="g",
        title="G",
        custom_formats=["German DL"],
        custom_format_score=500,
    )
    await sabnzbd_control.set_diskspace(5)
    engine = await make_engine(sources=["radarr"])

    await engine.poll_once()

    assert (await radarr_control.get_state())["grabbed"] == []


async def test_season_pack_is_used_but_only_for_the_right_season(
    sonarr_control: SonarrControlClient, make_engine: Any
) -> None:
    series = await sonarr_control.add_series(
        title="Dark",
        tvdb_id=1,
        episodes=[{"season_number": 1, "episode_number": 1, "title": "Secrets"}],
    )
    episode_id = int(series["episodes"][0]["id"])
    await sonarr_control.finish_episode(episode_id, custom_format_score=10)
    await sonarr_control.add_release(
        episode_id=episode_id,
        guid="pack",
        title="Dark.S01.German.DL",
        custom_formats=["German DL"],
        custom_format_score=900,
        full_season=True,
        mapped_episode_ids=[98, 99],  # Sonarr mapped it to another season
    )
    await sonarr_control.add_release(
        episode_id=episode_id,
        guid="pack-s01",
        title="Dark.S01.German.DL.1080p",
        custom_formats=["German DL"],
        custom_format_score=800,
        full_season=True,
        mapped_episode_ids=[episode_id, 99],
    )
    engine = await make_engine(sources=["sonarr"])

    await engine.poll_once()

    assert (await sonarr_control.get_state())["grabbed"] == ["pack-s01"]


# ---------------------------------------------------------------------------
# Full upgrade lifecycle
# ---------------------------------------------------------------------------


async def test_upgrade_lifecycle_with_user_request_priority(
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
    make_engine: Any,
) -> None:
    movie_id = await _movie(radarr_control, 100)
    await radarr_control.add_release(
        tmdb_id=100,
        guid="g-up",
        title="Movie.German.DL",
        custom_formats=["German DL"],
        custom_format_score=500,
    )
    engine = await make_engine(sources=["radarr"])

    # 1. conductarr grabs; the Arr hands the NZB to SABnzbd
    await engine.poll_once()
    assert (await radarr_control.get_state())["grabbed"] == ["g-up"]
    upgrade_nzo = await sabnzbd_control.start_job("Movie.German.DL", cat="movies")
    await radarr_control.release_movie(100, upgrade_nzo)

    # 2. a user request arrives afterwards
    await _movie(radarr_control, 200, has_file=False, tags=["request"])
    request_nzo = await sabnzbd_control.start_job("Request", cat="movies")
    await radarr_control.release_movie(200, request_nzo)

    await engine.poll_once()
    jobs = (await sabnzbd_control.get_state())["jobs"]
    order = sorted(jobs, key=lambda n: jobs[n]["index"])
    assert order == [request_nzo, upgrade_nzo]
    assert jobs[upgrade_nzo]["paused"] is True
    assert jobs[request_nzo]["paused"] is False

    # 3. the request finishes → the upgrade resumes
    await sabnzbd_control.finish_job(request_nzo)
    await radarr_control.finish_movie(200)
    await engine.poll_once()
    jobs = (await sabnzbd_control.get_state())["jobs"]
    assert jobs[upgrade_nzo]["paused"] is False

    # 4. the upgrade finishes → slot is free again, item marked upgraded
    await sabnzbd_control.finish_job(upgrade_nzo)
    await radarr_control.finish_movie(
        100, custom_format_score=500, custom_formats=["German DL"]
    )
    await engine.poll_once()
    item = await engine.repo.get_item("radarr", str(movie_id))
    assert item is not None
    assert "upgrade_grabbed" not in item.metadata
    assert "upgrade_completed_at" in item.metadata


async def test_user_paused_job_stays_paused_and_stop_releases_ours(
    tmp_path: Path,
    sabnzbd_control: SABnzbdControlClient,
    radarr_control: RadarrControlClient,
) -> None:
    await _movie(radarr_control, 1, has_file=False)
    await _movie(radarr_control, 2, has_file=False)
    await _movie(radarr_control, 3, has_file=False)
    user_paused = await sabnzbd_control.start_job("A", cat="movies")
    second = await sabnzbd_control.start_job("B", cat="movies")
    third = await sabnzbd_control.start_job("C", cat="movies")
    for tmdb_id, nzo in ((1, user_paused), (2, second), (3, third)):
        await radarr_control.release_movie(tmdb_id, nzo)
    await sabnzbd_control.pause_job(user_paused)

    engine = Orchestrator(_infra(tmp_path), _config())
    await engine.start()
    try:
        for _ in range(50):
            jobs = (await sabnzbd_control.get_state())["jobs"]
            if jobs[third]["paused"]:
                break
            await asyncio.sleep(0.1)
        assert jobs[user_paused]["paused"] is True  # never resumed
        assert jobs[second]["paused"] is False  # becomes the active download
        assert jobs[third]["paused"] is True  # paused by conductarr
        assert (tmp_path / "heartbeat").exists()
    finally:
        await engine.stop()

    jobs = (await sabnzbd_control.get_state())["jobs"]
    assert jobs[third]["paused"] is False  # released on shutdown
    assert jobs[user_paused]["paused"] is True  # still the user's decision
