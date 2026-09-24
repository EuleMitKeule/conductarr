"""Unit tests for the upgrade scheduler using in-memory fakes."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import pytest_asyncio

from conductarr.clients.arr import ArrClient
from conductarr.clients.release import ArrQueueItem, ReleaseResult
from conductarr.config import ConductarrConfig
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue.manager import QueueManager
from conductarr.queue.models import QueueItem
from conductarr.upgrade.scheduler import UpgradeScheduler
from tests.unit.fakes import (
    FakeArr,
    FakeRadarr,
    FakeSab,
    FakeSonarr,
    make_config,
    make_db,
)

pytestmark = pytest.mark.asyncio

QUEUE = "german_upgrade"


@dataclass
class Harness:
    config: ConductarrConfig
    db: Database
    repo: QueueRepository
    sab: FakeSab
    radarr: FakeArr
    sonarr: FakeArr
    manager: QueueManager
    scheduler: UpgradeScheduler

    async def cycle(self) -> None:
        snapshot = await self.manager.run_cycle()
        await self.scheduler.run_cycle(snapshot)

    async def seed(self) -> None:
        await self.scheduler.seed_if_due(force=True)

    async def item(self, source: str, source_id: int) -> QueueItem:
        item = await self.repo.get_item(source, str(source_id))
        assert item is not None
        return item


async def _build(**config_overrides: Any) -> Harness:
    config = make_config(**config_overrides)
    db = await make_db()
    repo = QueueRepository(db)
    sab = FakeSab()
    radarr = FakeRadarr()
    sonarr = FakeSonarr()
    arr: dict[str, ArrClient] = {"radarr": radarr, "sonarr": sonarr}
    manager = QueueManager(config, repo, sab, arr)
    scheduler = UpgradeScheduler(config, repo, arr, manager)
    return Harness(config, db, repo, sab, radarr, sonarr, manager, scheduler)


@pytest_asyncio.fixture
async def h() -> AsyncGenerator[Harness, None]:
    harness = await _build()
    yield harness
    await harness.db.disconnect()


def _german(arr: FakeArr, media_id: int, **kwargs: Any) -> ReleaseResult:
    kwargs.setdefault("custom_formats", ["German DL"])
    kwargs.setdefault("custom_format_score", 1000)
    return arr.add_release(media_id, **kwargs)


# ---------------------------------------------------------------------------
# Rate limiting / indexer protection
# ---------------------------------------------------------------------------


async def test_at_most_one_search_per_cycle(h: Harness) -> None:
    """Regression: no-match candidates used to be searched back-to-back."""
    for movie_id in range(1, 21):
        h.radarr.add_media(movie_id)  # no releases → every search is a no-match
    await h.seed()

    await h.cycle()

    assert len(h.radarr.searches) + len(h.sonarr.searches) == 1


async def test_search_interval_is_respected() -> None:
    h = await _build(upgrade={"search_interval": 3600})
    for movie_id in range(1, 5):
        h.radarr.add_media(movie_id)
    await h.seed()

    for _ in range(5):
        await h.cycle()

    assert len(h.radarr.searches) == 1
    await h.db.disconnect()


async def test_daily_search_budget() -> None:
    h = await _build(upgrade={"max_searches_per_day": 3})
    for movie_id in range(1, 10):
        h.radarr.add_media(movie_id)
    await h.seed()

    for _ in range(8):
        await h.cycle()

    assert len(h.radarr.searches) + len(h.sonarr.searches) == 3
    await h.db.disconnect()


async def test_satisfied_items_are_skipped_without_search(h: Harness) -> None:
    h.radarr.add_media(1, formats=["German DL"])
    h.radarr.add_media(2, formats=["German DL"])
    h.radarr.add_media(3)
    await h.seed()

    await h.cycle()

    assert h.radarr.searches == [3]
    assert (await h.item("radarr", 1)).metadata.get("upgrade_satisfied_at")


async def test_unmonitored_skipped_when_configured() -> None:
    h = await _build(upgrade={"include_unmonitored": False})
    h.radarr.add_media(1, monitored=False)
    await h.seed()

    await h.cycle()

    assert h.radarr.searches == []
    await h.db.disconnect()


# ---------------------------------------------------------------------------
# Grabbing
# ---------------------------------------------------------------------------


async def test_grabs_best_usenet_release(h: Harness) -> None:
    h.radarr.add_media(1, score=10)
    _german(h.radarr, 1, protocol="torrent", custom_format_score=5000, title="torrent")
    best = _german(h.radarr, 1, title="usenet")
    await h.seed()

    await h.cycle()

    assert h.radarr.grabbed == [best]
    item = await h.item("radarr", 1)
    assert item.metadata["upgrade_grabbed"] is True
    assert item.metadata["upgrade_grabbed_titles"] == ["usenet"]


async def test_torrent_only_results_never_grab(h: Harness) -> None:
    h.radarr.add_media(1)
    _german(h.radarr, 1, protocol="torrent")
    await h.seed()

    await h.cycle()

    assert h.radarr.grabbed == []
    assert (await h.item("radarr", 1)).metadata["upgrade_last_outcome"] == "no_usenet"


async def test_grab_is_marked_before_arr_is_called(h: Harness) -> None:
    """The SAB job can appear before grab_release returns → must already be tracked."""
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()
    seen: list[Any] = []

    async def on_grab(release: ReleaseResult) -> None:
        seen.append((await h.item("radarr", 1)).metadata.get("upgrade_grabbed"))

    h.radarr.on_grab = on_grab
    await h.cycle()

    assert seen == [True]


async def test_max_active_is_respected(h: Harness) -> None:
    for movie_id in (1, 2, 3):
        h.radarr.add_media(movie_id)
        _german(h.radarr, movie_id)
    await h.seed()

    for _ in range(5):
        await h.cycle()

    assert len(h.radarr.grabbed) == 1


async def test_fresh_grab_is_not_released_as_stale(h: Harness) -> None:
    """Regression: string comparison made every same-day grab 'stale' at once."""
    for movie_id in (1, 2):
        h.radarr.add_media(movie_id)
        _german(h.radarr, movie_id)
    await h.seed()

    await h.cycle()  # grabs movie 1 - SAB job not visible yet
    await h.cycle()
    await h.cycle()

    assert len(h.radarr.grabbed) == 1
    assert (await h.item("radarr", 1)).metadata.get("upgrade_grabbed") is True


async def test_stale_grab_released_after_grab_timeout(h: Harness) -> None:
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()
    await h.cycle()

    item = await h.item("radarr", 1)
    assert item.id is not None
    old = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    item.metadata["upgrade_grabbed_at"] = old
    await h.repo.update_metadata(item.id, item.metadata)
    await h.cycle()

    item = await h.item("radarr", 1)
    assert "upgrade_grabbed" not in item.metadata
    assert item.metadata["upgrade_last_failure"] == "never appeared in SABnzbd"


async def test_grab_failure_reverts_and_gives_up_after_three(h: Harness) -> None:
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()
    h.radarr.fail_grab = True

    for expected_failures in (1, 2):
        await h.cycle()
        item = await h.item("radarr", 1)
        assert "upgrade_grabbed" not in item.metadata
        assert item.metadata["upgrade_grab_failures"] == expected_failures

    await h.cycle()
    item = await h.item("radarr", 1)
    assert item.metadata["upgrade_grab_failures"] == 0
    assert "upgrade_no_release_at" in item.metadata
    assert len(h.radarr.searches) == 3


async def test_search_error_puts_item_on_short_cooldown(h: Harness) -> None:
    h.radarr.add_media(1)
    await h.seed()
    h.radarr.fail_search = True

    await h.cycle()

    item = await h.item("radarr", 1)
    assert item.metadata["upgrade_last_outcome"] == "error"
    assert "upgrade_no_release_at" in item.metadata


async def test_already_downloading_item_is_skipped(h: Harness) -> None:
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()
    h.sab.add("nzo_user")
    h.radarr.queue.append(ArrQueueItem(download_id="nzo_user", media_id=1, title="x"))

    await h.cycle()

    assert h.radarr.searches == []


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


async def test_defers_to_user_downloads() -> None:
    h = await _build(upgrade={"defer_to_other_downloads": True})
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    h.radarr.add_media(50, has_file=False, tags=["request"])
    await h.seed()
    h.sab.add("nzo_request")
    h.radarr.queue.append(
        ArrQueueItem(download_id="nzo_request", media_id=50, title="r")
    )

    await h.cycle()
    assert h.radarr.searches == []

    h.sab.finish("nzo_request")
    h.radarr.queue.clear()
    await h.cycle()
    assert h.radarr.grabbed and h.radarr.grabbed[0].guid.startswith("guid-1-")
    await h.db.disconnect()


async def test_no_upgrades_when_sab_paused(h: Harness) -> None:
    h.radarr.add_media(1)
    await h.seed()
    h.sab.paused = True

    await h.cycle()

    assert h.radarr.searches == []


async def test_no_upgrades_when_disk_space_low(h: Harness) -> None:
    h.radarr.add_media(1)
    await h.seed()
    h.sab.diskspace = 5.0

    await h.cycle()

    assert h.radarr.searches == []


async def test_no_upgrades_when_sab_unreachable(h: Harness) -> None:
    h.radarr.add_media(1)
    await h.seed()
    h.sab.offline = True

    await h.cycle()

    assert h.radarr.searches == []


async def test_dry_run_never_grabs_or_writes_results() -> None:
    h = await _build(dry_run=True)
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()

    await h.cycle()

    assert h.radarr.searches == [1]
    assert h.radarr.grabbed == []
    item = await h.item("radarr", 1)
    assert "upgrade_grabbed" not in item.metadata
    assert "upgrade_last_searched_at" not in item.metadata
    await h.db.disconnect()


# ---------------------------------------------------------------------------
# Library scan
# ---------------------------------------------------------------------------


async def test_seed_adds_new_items_and_adopts_requested_ones(h: Harness) -> None:
    h.radarr.add_media(1)
    await h.repo.upsert_item(
        QueueItem(source="radarr", source_id="2", tags=[], virtual_queue="requests")
    )
    h.radarr.add_media(2)
    h.radarr.add_media(3, has_file=False)

    await h.seed()

    assert (await h.item("radarr", 1)).virtual_queue == QUEUE
    assert (await h.item("radarr", 2)).virtual_queue == QUEUE
    assert await h.repo.get_item("radarr", "3") is None


async def test_seed_respects_rescan_interval(h: Harness) -> None:
    await h.seed()
    h.radarr.add_media(9)

    await h.scheduler.seed_if_due()

    assert await h.repo.get_item("radarr", "9") is None


async def test_cursor_round_robins_through_candidates(h: Harness) -> None:
    for movie_id in (1, 2, 3):
        h.radarr.add_media(movie_id)
    await h.seed()
    upgrade = h.config.upgrade_queues[0].upgrade
    assert upgrade is not None
    upgrade.sources = ["radarr"]

    for _ in range(3):
        await h.cycle()

    assert h.radarr.searches == [1, 2, 3]


async def test_debug_dry_run_has_no_side_effects(h: Harness) -> None:
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()

    results = await h.scheduler.dry_run(source_filter="radarr")

    assert len(results) == 1
    assert results[0].outcome == "would_grab"
    assert h.radarr.grabbed == []
    assert await h.repo.count_searches_last_day(QUEUE) == 0
    assert "upgrade_last_searched_at" not in (await h.item("radarr", 1)).metadata


async def test_user_paused_job_does_not_block_upgrades() -> None:
    h = await _build(upgrade={"defer_to_other_downloads": True})
    h.radarr.add_media(1)
    _german(h.radarr, 1)
    await h.seed()
    h.sab.add("nzo_parked", status="Paused")  # user parked this job in SABnzbd

    await h.cycle()

    assert len(h.radarr.grabbed) == 1
    await h.db.disconnect()


# ---------------------------------------------------------------------------
# Season packs
# ---------------------------------------------------------------------------


async def test_season_pack_grab_covers_other_episodes() -> None:
    h = await _build(upgrade={"sources": ["sonarr"], "max_active": 3})
    for episode_id in (1, 2, 3):
        h.sonarr.add_media(episode_id)
    pack = _german(
        h.sonarr,
        1,
        title="Show.S01.German.DL",
        full_season=True,
        mapped_media_ids=[1, 2, 3],
    )
    for episode_id in (2, 3):  # the same pack is found when searching ep 2/3
        h.sonarr.releases[episode_id] = [pack]
    await h.seed()

    for _ in range(4):
        await h.cycle()

    assert h.sonarr.grabbed == [pack]
    assert h.sonarr.searches == [1]
    ep2 = await h.item("sonarr", 2)
    assert ep2.metadata["upgrade_covered_by"] == "1"
    assert "Show.S01.German.DL" in ep2.metadata["upgrade_grabbed_titles"]
    await h.db.disconnect()


async def test_external_pack_download_blocks_all_its_episodes() -> None:
    h = await _build(upgrade={"sources": ["sonarr"]})
    for episode_id in (1, 2):
        h.sonarr.add_media(episode_id)
        _german(h.sonarr, episode_id)
    await h.seed()
    h.sab.add("nzo_pack", status="Paused")  # user-paused, so it does not defer
    h.sonarr.queue.extend(
        ArrQueueItem(download_id="nzo_pack", media_id=i, title="pack") for i in (1, 2)
    )

    await h.cycle()

    assert h.sonarr.searches == []
    assert h.manager.entries["nzo_pack"].covered_ids == ["1", "2"]
    await h.db.disconnect()
