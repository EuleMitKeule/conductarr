"""Unit tests for the UpgradeScheduler."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio

from conductarr.clients.release import ReleaseResult
from conductarr.config import AcceptConditionConfig, UpgradeConfig, VirtualQueueConfig
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue.models import QueueItem
from conductarr.upgrade.scheduler import (
    UpgradeScheduler,
    _filter_releases,
    _media_satisfies_conditions,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def db() -> AsyncGenerator[Database, None]:
    from conductarr.config import MemoryDatabaseConfig

    config = MemoryDatabaseConfig()
    database = Database(config)
    await database.connect()
    yield database
    await database.disconnect()


@pytest_asyncio.fixture
async def repo(db: Database) -> AsyncGenerator[QueueRepository, None]:
    yield QueueRepository(db)


def _make_queue_config(
    name: str = "german_upgrade",
    sources: list[str] | None = None,
    max_active: int = 2,
    retry_after_days: int = 7,
    accept_conditions: list[AcceptConditionConfig] | None = None,
    search_interval: float = 0.0,
) -> VirtualQueueConfig:
    return VirtualQueueConfig(
        name=name,
        priority=30,
        upgrade=UpgradeConfig(
            enabled=True,
            sources=sources or ["radarr", "sonarr"],
            max_active=max_active,
            daily_scan_interval=86400,
            retry_after_days=retry_after_days,
            accept_conditions=accept_conditions or [],
            search_interval=search_interval,
        ),
    )


def _make_release(
    guid: str = "abc",
    title: str = "Test Release",
    custom_formats: list[str] | None = None,
    custom_format_score: int = 100,
    download_allowed: bool = True,
) -> ReleaseResult:
    return ReleaseResult(
        guid=guid,
        title=title,
        indexer_id=1,
        custom_formats=custom_formats or [],
        custom_format_score=custom_format_score,
        download_allowed=download_allowed,
    )


async def _seed_item(
    repo: QueueRepository,
    source: str,
    source_id: str,
    virtual_queue: str,
    metadata: dict[str, Any] | None = None,
) -> QueueItem:
    item = QueueItem(
        source=source,
        source_id=source_id,
        tags=[],
        virtual_queue=virtual_queue,
        metadata=metadata or {},
    )
    return await repo.upsert_item(item)


# ---------------------------------------------------------------------------
# _filter_releases
# ---------------------------------------------------------------------------


class TestFilterReleases:
    def test_no_conditions_returns_all(self) -> None:
        releases = [
            _make_release(guid="a"),
            _make_release(guid="b"),
        ]
        assert _filter_releases(releases, []) == releases

    def test_custom_format_condition(self) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        releases = [
            _make_release(guid="a", custom_formats=["German DL", "HI"]),
            _make_release(guid="b", custom_formats=["HI"]),
        ]
        result = _filter_releases(releases, [cond])
        assert len(result) == 1
        assert result[0].guid == "a"

    def test_custom_format_min_score_condition(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=80)
        releases = [
            _make_release(guid="a", custom_format_score=90),
            _make_release(guid="b", custom_format_score=50),
        ]
        result = _filter_releases(releases, [cond])
        assert len(result) == 1
        assert result[0].guid == "a"

    def test_multiple_conditions_and_combined(self) -> None:
        conds = [
            AcceptConditionConfig(type="custom_format", name="German DL"),
            AcceptConditionConfig(type="custom_format_min_score", value=80),
        ]
        releases = [
            _make_release(
                guid="a", custom_formats=["German DL"], custom_format_score=90
            ),
            _make_release(
                guid="b", custom_formats=["German DL"], custom_format_score=50
            ),
            _make_release(guid="c", custom_formats=[], custom_format_score=90),
        ]
        result = _filter_releases(releases, conds)
        assert len(result) == 1
        assert result[0].guid == "a"

    def test_unknown_condition_type_passes_through(self) -> None:
        cond = AcceptConditionConfig(type="unknown_type")
        release = _make_release(guid="a")
        # Unknown conditions are logged but don't filter out the release
        result = _filter_releases([release], [cond])
        assert len(result) == 1


# ---------------------------------------------------------------------------
# UpgradeScheduler._fill_slots
# ---------------------------------------------------------------------------


class TestFillSlots:
    async def test_fills_one_slot_radarr(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release(guid="r1")])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        radarr.search_releases.assert_awaited_once_with(1)
        radarr.grab_release.assert_awaited_once()

        # Item should be marked as grabbed
        item = await repo.get_item("radarr", "1")
        assert item is not None
        assert item.metadata.get("upgrade_grabbed") is True

    async def test_no_candidates_does_nothing(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        # No items seeded

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[])

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        radarr.search_releases.assert_not_awaited()

    async def test_does_not_exceed_max_active(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "radarr", "2", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        # Simulate 1 already active job map
        item1 = await repo.get_item("radarr", "1")
        assert item1 is not None and item1.id is not None
        await repo.upsert_job_map("nzo-active", item1.id, "german_upgrade")

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        # max_active=1 and 1 already active → 0 slots to fill → no grab
        radarr.grab_release.assert_not_awaited()

    async def test_skips_already_grabbed(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(
            repo, "radarr", "1", "german_upgrade", metadata={"upgrade_grabbed": True}
        )

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        radarr.grab_release.assert_not_awaited()

    async def test_no_matching_release_marks_no_match(
        self, repo: QueueRepository
    ) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(
            sources=["radarr"], max_active=1, accept_conditions=[cond]
        )
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        # Release exists but doesn't have the required custom format
        radarr.search_releases = AsyncMock(
            return_value=[_make_release(custom_formats=["English"])]
        )
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        radarr.grab_release.assert_not_awaited()

        item = await repo.get_item("radarr", "1")
        assert item is not None
        assert "upgrade_no_match_at" in item.metadata
        assert "upgrade_last_searched_at" in item.metadata

    async def test_alternates_sources(self, repo: QueueRepository) -> None:
        """After grabbing from radarr, next fill should try sonarr first."""
        qc = _make_queue_config(sources=["radarr", "sonarr"], max_active=2)
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "sonarr", "10", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release(guid="r1")])
        radarr.grab_release = AsyncMock()

        sonarr = MagicMock()
        sonarr.search_releases = AsyncMock(return_value=[_make_release(guid="s1")])
        sonarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(
            repo, [qc], radarr_client=radarr, sonarr_client=sonarr
        )
        await scheduler._fill_slots(qc)

        radarr.grab_release.assert_awaited_once()
        sonarr.grab_release.assert_awaited_once()

    async def test_search_error_continues_to_next_candidate(
        self, repo: QueueRepository
    ) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "radarr", "2", "german_upgrade")

        radarr = MagicMock()
        # First call raises, second call succeeds
        radarr.search_releases = AsyncMock(
            side_effect=[Exception("network error"), [_make_release(guid="r2")]]
        )
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(
            repo, [qc], radarr_client=radarr, sonarr_client=None
        )
        await scheduler._fill_slots(qc)

        assert radarr.search_releases.call_count == 2
        radarr.grab_release.assert_awaited_once()

    async def test_grabs_highest_score_release(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(
            return_value=[
                _make_release(guid="low", custom_format_score=50),
                _make_release(guid="high", custom_format_score=200),
                _make_release(guid="mid", custom_format_score=100),
            ]
        )
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        grabbed_release = radarr.grab_release.call_args[0][0]
        assert grabbed_release.guid == "high"

    async def test_disabled_upgrade_config_does_nothing(
        self, repo: QueueRepository
    ) -> None:
        qc = VirtualQueueConfig(
            name="disabled_queue",
            priority=10,
            upgrade=UpgradeConfig(enabled=False, sources=["radarr"]),
        )
        await _seed_item(repo, "radarr", "1", "disabled_queue")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._fill_slots(qc)

        radarr.grab_release.assert_not_awaited()


# ---------------------------------------------------------------------------
# UpgradeScheduler.on_job_completed
# ---------------------------------------------------------------------------


class TestOnJobCompleted:
    async def test_triggers_fill_for_matching_queue(
        self, repo: QueueRepository
    ) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.on_job_completed("german_upgrade")

        radarr.grab_release.assert_awaited_once()

    async def test_ignores_unknown_queue(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.on_job_completed("nonexistent_queue")

        radarr.grab_release.assert_not_awaited()


# ---------------------------------------------------------------------------
# UpgradeScheduler._daily_scan
# ---------------------------------------------------------------------------


class TestDailyScan:
    async def test_scans_all_candidates_up_to_max_active(
        self, repo: QueueRepository
    ) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=2)
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "radarr", "2", "german_upgrade")
        await _seed_item(repo, "radarr", "3", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        # Simulate active count growing as grabs are committed to SABnzbd:
        # outer-loop check → 0, candidate-1 check → 0, candidate-2 check → 1,
        # candidate-3 check → 2 (at capacity, stop)
        scheduler._count_active = AsyncMock(side_effect=[0, 0, 1, 2])  # type: ignore[method-assign]
        await scheduler._daily_scan(qc)

        # max_active=2 → should grab exactly 2 even though there are 3 candidates
        assert radarr.grab_release.await_count == 2

    async def test_skips_already_grabbed(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=2)
        await _seed_item(
            repo, "radarr", "1", "german_upgrade", metadata={"upgrade_grabbed": True}
        )
        await _seed_item(repo, "radarr", "2", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._daily_scan(qc)

        # Only item "2" should be searched (item "1" already grabbed)
        radarr.search_releases.assert_awaited_once_with(2)
        radarr.grab_release.assert_awaited_once()

    async def test_marks_no_match_when_no_releases(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler._daily_scan(qc)

        radarr.grab_release.assert_not_awaited()
        item = await repo.get_item("radarr", "1")
        assert item is not None
        assert "upgrade_no_match_at" in item.metadata


# ---------------------------------------------------------------------------
# get_upgrade_candidates (repository)
# ---------------------------------------------------------------------------


class TestGetUpgradeCandidates:
    async def test_returns_unsearched_items(self, repo: QueueRepository) -> None:
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "radarr", "2", "german_upgrade")

        result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        assert len(result) == 2

    async def test_excludes_grabbed_items(self, repo: QueueRepository) -> None:
        await _seed_item(
            repo, "radarr", "1", "german_upgrade", metadata={"upgrade_grabbed": True}
        )
        await _seed_item(repo, "radarr", "2", "german_upgrade")

        result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        assert len(result) == 1
        assert result[0].source_id == "2"

    async def test_excludes_recently_searched(self, repo: QueueRepository) -> None:
        from datetime import UTC, datetime

        now_iso = datetime.now(UTC).isoformat()
        await _seed_item(
            repo,
            "radarr",
            "1",
            "german_upgrade",
            metadata={"upgrade_last_searched_at": now_iso},
        )
        await _seed_item(repo, "radarr", "2", "german_upgrade")

        result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        # Item "1" was searched just now → excluded; item "2" → included
        assert len(result) == 1
        assert result[0].source_id == "2"

    async def test_includes_old_searched_items(self, repo: QueueRepository) -> None:
        old_iso = "2020-01-01T00:00:00+00:00"
        await _seed_item(
            repo,
            "radarr",
            "1",
            "german_upgrade",
            metadata={"upgrade_last_searched_at": old_iso},
        )

        result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        assert len(result) == 1

    async def test_ordered_by_source_id_numerically(
        self, repo: QueueRepository
    ) -> None:
        await _seed_item(repo, "radarr", "10", "german_upgrade")
        await _seed_item(repo, "radarr", "2", "german_upgrade")
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        assert [r.source_id for r in result] == ["1", "2", "10"]

    async def test_filtered_by_source(self, repo: QueueRepository) -> None:
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "sonarr", "100", "german_upgrade")

        radarr_result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        sonarr_result = await repo.get_upgrade_candidates("german_upgrade", "sonarr", 7)

        assert len(radarr_result) == 1
        assert radarr_result[0].source == "radarr"
        assert len(sonarr_result) == 1
        assert sonarr_result[0].source == "sonarr"

    async def test_filtered_by_virtual_queue(self, repo: QueueRepository) -> None:
        await _seed_item(repo, "radarr", "1", "german_upgrade")
        await _seed_item(repo, "radarr", "2", "other_queue")

        result = await repo.get_upgrade_candidates("german_upgrade", "radarr", 7)
        assert len(result) == 1
        assert result[0].source_id == "1"


# ---------------------------------------------------------------------------
# UpgradeScheduler.seed_upgrade_queues
# ---------------------------------------------------------------------------


def _make_movie(
    movie_id: int,
    *,
    has_file: bool = True,
    custom_formats: list[str] | None = None,
    custom_format_score: int = 0,
    tag_ids: list[int] | None = None,
) -> Any:
    """Return a minimal RadarrMovie-like mock."""
    from conductarr.clients.radarr import RadarrMovie

    return RadarrMovie(
        id=movie_id,
        title=f"Movie {movie_id}",
        tmdb_id=movie_id * 100,
        has_file=has_file,
        monitored=True,
        custom_format_score=custom_format_score,
        quality_profile_id=1,
        tag_ids=tag_ids or [],
        custom_formats=custom_formats or [],
    )


def _make_series(
    series_id: int,
    *,
    tag_ids: list[int] | None = None,
) -> Any:
    """Return a minimal SonarrSeries-like mock."""
    from conductarr.clients.sonarr import SonarrSeries

    return SonarrSeries(
        id=series_id,
        title=f"Series {series_id}",
        tvdb_id=series_id * 100,
        monitored=True,
        status="continuing",
        tag_ids=tag_ids or [],
    )


def _make_episode(
    episode_id: int,
    series_id: int,
    *,
    has_file: bool = True,
    custom_formats: list[str] | None = None,
    custom_format_score: int = 0,
) -> Any:
    """Return a minimal SonarrEpisode-like mock."""
    from conductarr.clients.sonarr import SonarrEpisode

    return SonarrEpisode(
        id=episode_id,
        series_id=series_id,
        episode_number=1,
        season_number=1,
        title=f"Episode {episode_id}",
        monitored=True,
        has_file=has_file,
        custom_format_score=custom_format_score,
        custom_formats=custom_formats or [],
    )


# ---------------------------------------------------------------------------
# _media_satisfies_conditions
# ---------------------------------------------------------------------------


class TestMediaSatisfiesConditions:
    def test_empty_conditions_always_satisfied(self) -> None:
        assert _media_satisfies_conditions([], 0, []) is True
        assert _media_satisfies_conditions(["German DL"], 200, []) is True

    def test_custom_format_present(self) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        assert _media_satisfies_conditions(["German DL"], 0, [cond]) is True

    def test_custom_format_absent(self) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        assert _media_satisfies_conditions(["HI"], 0, [cond]) is False

    def test_score_at_threshold(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert _media_satisfies_conditions([], 100, [cond]) is True

    def test_score_above_threshold(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert _media_satisfies_conditions([], 150, [cond]) is True

    def test_score_below_threshold(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert _media_satisfies_conditions([], 50, [cond]) is False

    def test_multiple_conditions_all_must_match(self) -> None:
        conds = [
            AcceptConditionConfig(type="custom_format", name="German DL"),
            AcceptConditionConfig(type="custom_format_min_score", value=80),
        ]
        # Both satisfied
        assert _media_satisfies_conditions(["German DL"], 90, conds) is True
        # Format ok, score too low
        assert _media_satisfies_conditions(["German DL"], 50, conds) is False
        # Score ok, format missing
        assert _media_satisfies_conditions([], 90, conds) is False

    def test_unknown_condition_type_passes(self) -> None:
        cond = AcceptConditionConfig(type="unknown_type")
        assert _media_satisfies_conditions([], 0, [cond]) is True


class TestSeedUpgradeQueues:
    async def test_seed_radarr_creates_items_for_unqualified_movies(
        self, repo: QueueRepository
    ) -> None:
        """Movies that do not satisfy accept_conditions are seeded."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["radarr"], accept_conditions=[cond])

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(
            return_value=[
                _make_movie(1, custom_formats=[]),  # missing CF → seed
                _make_movie(2, custom_formats=["German DL"]),  # satisfies → skip
            ]
        )

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        item1 = await repo.get_item("radarr", "1")
        assert item1 is not None
        assert item1.virtual_queue == "german_upgrade"

        item2 = await repo.get_item("radarr", "2")
        assert item2 is None

    async def test_seed_radarr_skips_existing_items(
        self, repo: QueueRepository
    ) -> None:
        """Pre-existing items are never overwritten."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["radarr"], accept_conditions=[cond])
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(return_value=[_make_movie(1, custom_formats=[])])

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        items = await repo.get_items_by_queue("german_upgrade")
        assert len(items) == 1

    async def test_seed_radarr_score_condition(self, repo: QueueRepository) -> None:
        """Movies above the score threshold are skipped; those below are seeded."""
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        qc = _make_queue_config(sources=["radarr"], accept_conditions=[cond])

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(
            return_value=[
                _make_movie(1, custom_format_score=50),  # below → seed
                _make_movie(2, custom_format_score=150),  # above → skip
                _make_movie(3, custom_format_score=100),  # at threshold → skip
            ]
        )

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        assert await repo.get_item("radarr", "1") is not None
        assert await repo.get_item("radarr", "2") is None
        assert await repo.get_item("radarr", "3") is None

    async def test_seed_radarr_no_conditions_skips_all(
        self, repo: QueueRepository
    ) -> None:
        """With no accept_conditions every movie vacuously satisfies them → nothing seeded."""
        qc = _make_queue_config(sources=["radarr"], accept_conditions=[])

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(return_value=[_make_movie(1)])

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        assert await repo.get_item("radarr", "1") is None

    async def test_seed_sonarr_creates_episode_items(
        self, repo: QueueRepository
    ) -> None:
        """Episodes that do not satisfy conditions are seeded."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["sonarr"], accept_conditions=[cond])

        sonarr = MagicMock()
        sonarr.get_series = AsyncMock(return_value=[_make_series(1)])
        sonarr.get_episodes = AsyncMock(
            return_value=[
                _make_episode(101, series_id=1, custom_formats=[]),  # seed
                _make_episode(102, series_id=1, custom_formats=["German DL"]),  # skip
            ]
        )

        scheduler = UpgradeScheduler(repo, [qc], sonarr_client=sonarr)
        await scheduler.seed_upgrade_queues()

        assert await repo.get_item("sonarr", "101") is not None
        assert await repo.get_item("sonarr", "102") is None

    async def test_seed_sonarr_episode_fetch_error_continues(
        self, repo: QueueRepository
    ) -> None:
        """An error fetching episodes for one series does not stop seeding others."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["sonarr"], accept_conditions=[cond])

        sonarr = MagicMock()
        sonarr.get_series = AsyncMock(return_value=[_make_series(1), _make_series(2)])
        sonarr.get_episodes = AsyncMock(
            side_effect=[
                Exception("fetch error"),
                [_make_episode(201, series_id=2, custom_formats=[])],
            ]
        )

        scheduler = UpgradeScheduler(repo, [qc], sonarr_client=sonarr)
        await scheduler.seed_upgrade_queues()

        assert await repo.get_item("sonarr", "201") is not None

    async def test_seed_radarr_error_does_not_crash(
        self, repo: QueueRepository
    ) -> None:
        """A network error fetching movies is logged and seeding exits gracefully."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["radarr"], accept_conditions=[cond])

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(side_effect=Exception("network error"))

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()  # must not raise

    async def test_seed_sonarr_series_fetch_error_does_not_crash(
        self, repo: QueueRepository
    ) -> None:
        """A network error fetching series is logged and seeding exits gracefully."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["sonarr"], accept_conditions=[cond])

        sonarr = MagicMock()
        sonarr.get_series = AsyncMock(side_effect=Exception("network error"))

        scheduler = UpgradeScheduler(repo, [qc], sonarr_client=sonarr)
        await scheduler.seed_upgrade_queues()  # must not raise

    async def test_seed_multi_source_seeds_both(self, repo: QueueRepository) -> None:
        """When both radarr and sonarr are sources, each is seeded independently."""
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        qc = _make_queue_config(sources=["radarr", "sonarr"], accept_conditions=[cond])

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(return_value=[_make_movie(1, custom_formats=[])])
        sonarr = MagicMock()
        sonarr.get_series = AsyncMock(return_value=[_make_series(1)])
        sonarr.get_episodes = AsyncMock(
            return_value=[_make_episode(101, series_id=1, custom_formats=[])]
        )

        scheduler = UpgradeScheduler(
            repo, [qc], radarr_client=radarr, sonarr_client=sonarr
        )
        await scheduler.seed_upgrade_queues()

        assert await repo.get_item("radarr", "1") is not None
        assert await repo.get_item("sonarr", "101") is not None


# ---------------------------------------------------------------------------
# UpgradeScheduler.on_reconcile_complete
# ---------------------------------------------------------------------------


class TestOnReconcileComplete:
    async def test_fills_queues_with_open_slots(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.on_reconcile_complete()

        radarr.grab_release.assert_awaited_once()

    async def test_skips_queues_at_capacity(self, repo: QueueRepository) -> None:
        qc = _make_queue_config(sources=["radarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        item = await repo.get_item("radarr", "1")
        assert item is not None and item.id is not None
        await repo.upsert_job_map("nzo-active", item.id, "german_upgrade")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release()])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.on_reconcile_complete()

        radarr.grab_release.assert_not_awaited()

    async def test_skips_disabled_queues(self, repo: QueueRepository) -> None:
        qc = VirtualQueueConfig(
            name="disabled",
            priority=10,
            upgrade=UpgradeConfig(enabled=False, sources=["radarr"]),
        )
        radarr = MagicMock()
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.on_reconcile_complete()

        radarr.grab_release.assert_not_awaited()

    async def test_handles_multiple_queues(self, repo: QueueRepository) -> None:
        qc1 = _make_queue_config(name="queue_a", sources=["radarr"], max_active=1)
        qc2 = _make_queue_config(name="queue_b", sources=["sonarr"], max_active=1)
        await _seed_item(repo, "radarr", "1", "queue_a")
        await _seed_item(repo, "sonarr", "10", "queue_b")

        radarr = MagicMock()
        radarr.search_releases = AsyncMock(return_value=[_make_release(guid="r1")])
        radarr.grab_release = AsyncMock()

        sonarr = MagicMock()
        sonarr.search_releases = AsyncMock(return_value=[_make_release(guid="s1")])
        sonarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(
            repo, [qc1, qc2], radarr_client=radarr, sonarr_client=sonarr
        )
        await scheduler.on_reconcile_complete()

        radarr.grab_release.assert_awaited_once()
        sonarr.grab_release.assert_awaited_once()
