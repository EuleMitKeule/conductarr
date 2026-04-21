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
from conductarr.upgrade.scheduler import UpgradeScheduler, _filter_releases

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
    def test_no_conditions_returns_all_allowed(self) -> None:
        releases = [
            _make_release(guid="a", download_allowed=True),
            _make_release(guid="b", download_allowed=True),
        ]
        assert _filter_releases(releases, []) == releases

    def test_download_not_allowed_excluded(self) -> None:
        releases = [
            _make_release(guid="a", download_allowed=True),
            _make_release(guid="b", download_allowed=False),
        ]
        result = _filter_releases(releases, [])
        assert len(result) == 1
        assert result[0].guid == "a"

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
        await scheduler._daily_scan(qc)

        # max_active=2 → should grab at most 2 even though there are 3 candidates
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
    tag_ids: list[int] | None = None,
) -> Any:
    """Return a minimal RadarrMovie-like mock."""
    from conductarr.clients.radarr import RadarrMovie

    return RadarrMovie(
        id=movie_id,
        title=f"Movie {movie_id}",
        tmdb_id=movie_id * 100,
        has_file=False,
        monitored=True,
        custom_format_score=0,
        quality_profile_id=1,
        tag_ids=tag_ids or [],
    )


def _make_series(
    series_id: int,
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


def _make_episode(episode_id: int, series_id: int) -> Any:
    """Return a minimal SonarrEpisode-like mock."""
    from conductarr.clients.sonarr import SonarrEpisode

    return SonarrEpisode(
        id=episode_id,
        series_id=series_id,
        episode_number=1,
        season_number=1,
        title=f"Episode {episode_id}",
        monitored=True,
        has_file=False,
        custom_format_score=0,
    )


class TestSeedUpgradeQueues:
    async def test_seed_radarr_creates_missing_items(
        self, repo: QueueRepository
    ) -> None:
        from conductarr.config import MatcherConfig

        qc = _make_queue_config(sources=["radarr"])
        # Give the queue a tag matcher so _get_tag_filter returns ["upgrade-de"]
        qc.matchers.append(MatcherConfig(type="tags", tags=["upgrade-de"]))

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(
            return_value=[
                _make_movie(1, tag_ids=[10]),
                _make_movie(2, tag_ids=[99]),  # wrong tag → skipped
            ]
        )
        radarr.get_tags = AsyncMock(return_value={10: "upgrade-de"})
        radarr.search_releases = AsyncMock(return_value=[])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        item = await repo.get_item("radarr", "1")
        assert item is not None
        assert item.virtual_queue == "german_upgrade"
        assert "upgrade-de" in item.tags

        missing = await repo.get_item("radarr", "2")
        assert missing is None

    async def test_seed_radarr_skips_existing_items(
        self, repo: QueueRepository
    ) -> None:
        from conductarr.config import MatcherConfig

        qc = _make_queue_config(sources=["radarr"])
        qc.matchers.append(MatcherConfig(type="tags", tags=["upgrade-de"]))
        # Pre-seed the item so it already exists
        await _seed_item(repo, "radarr", "1", "german_upgrade")

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(return_value=[_make_movie(1, tag_ids=[10])])
        radarr.get_tags = AsyncMock(return_value={10: "upgrade-de"})
        radarr.search_releases = AsyncMock(return_value=[])
        radarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        # Item should still exist but not be duplicated
        items = await repo.get_items_by_queue("german_upgrade")
        assert len(items) == 1

    async def test_seed_sonarr_creates_episode_items(
        self, repo: QueueRepository
    ) -> None:
        from conductarr.config import MatcherConfig

        qc = _make_queue_config(sources=["sonarr"])
        qc.matchers.append(MatcherConfig(type="tags", tags=["upgrade-de"]))

        sonarr = MagicMock()
        sonarr.get_series = AsyncMock(
            return_value=[
                _make_series(1, tag_ids=[10]),
                _make_series(2, tag_ids=[99]),  # wrong tag → skipped
            ]
        )
        sonarr.get_tags = AsyncMock(return_value={10: "upgrade-de"})
        sonarr.get_episodes = AsyncMock(return_value=[_make_episode(101, series_id=1)])
        sonarr.search_releases = AsyncMock(return_value=[])
        sonarr.grab_release = AsyncMock()

        scheduler = UpgradeScheduler(repo, [qc], sonarr_client=sonarr)
        await scheduler.seed_upgrade_queues()

        item = await repo.get_item("sonarr", "101")
        assert item is not None
        assert item.virtual_queue == "german_upgrade"

        # Series 2 had a wrong tag → no episode item
        sonarr.get_episodes.assert_awaited_once()

    async def test_seed_no_tag_matchers_does_nothing(
        self, repo: QueueRepository
    ) -> None:
        """Queue with no 'tags' matchers should skip seeding entirely."""
        qc = VirtualQueueConfig(
            name="no_tags_queue",
            priority=10,
            matchers=[],  # no matchers at all
            upgrade=UpgradeConfig(enabled=True, sources=["radarr"]),
        )

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(return_value=[])
        radarr.get_tags = AsyncMock(return_value={})

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        await scheduler.seed_upgrade_queues()

        radarr.get_movies.assert_not_awaited()

    async def test_seed_radarr_error_does_not_crash(
        self, repo: QueueRepository
    ) -> None:
        from conductarr.config import MatcherConfig

        qc = _make_queue_config(sources=["radarr"])
        qc.matchers.append(MatcherConfig(type="tags", tags=["upgrade-de"]))

        radarr = MagicMock()
        radarr.get_movies = AsyncMock(side_effect=Exception("network error"))
        radarr.get_tags = AsyncMock(return_value={})

        scheduler = UpgradeScheduler(repo, [qc], radarr_client=radarr)
        # Should not raise
        await scheduler.seed_upgrade_queues()

    async def test_get_tag_filter_deduplicates(self) -> None:
        from conductarr.config import MatcherConfig

        qc = VirtualQueueConfig(
            name="q",
            priority=10,
            matchers=[
                MatcherConfig(type="tags", tags=["upgrade-de", "other"]),
                MatcherConfig(type="tags", tags=["upgrade-de"]),  # duplicate
                MatcherConfig(type="unknown", tags=["ignored"]),
            ],
        )
        result = UpgradeScheduler._get_tag_filter(qc)
        assert result == ["upgrade-de", "other"]


# ---------------------------------------------------------------------------
# _filter_releases download_allowed logging
# ---------------------------------------------------------------------------


class TestFilterReleasesDownloadAllowedLog:
    def test_download_not_allowed_is_logged(self, caplog: Any) -> None:
        import logging

        release = _make_release(
            guid="a", title="Blocked.Release", download_allowed=False
        )
        with caplog.at_level(logging.DEBUG, logger="conductarr.upgrade.scheduler"):
            _filter_releases([release], [])
        assert any("Blocked.Release" in r.message for r in caplog.records)
        assert any("download_allowed=False" in r.message for r in caplog.records)
