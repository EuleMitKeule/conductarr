"""Unit tests for the SABnzbd queue manager using in-memory fakes."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio

from conductarr.clients.arr import ArrClient
from conductarr.clients.release import ArrQueueItem
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue import manager as manager_module
from conductarr.queue.manager import QueueManager
from conductarr.queue.models import QueueItem
from tests.unit.fakes import (
    FakeArr,
    FakeRadarr,
    FakeSab,
    FakeSonarr,
    make_config,
    make_db,
)

pytestmark = pytest.mark.asyncio


@dataclass
class Harness:
    db: Database
    repo: QueueRepository
    sab: FakeSab
    radarr: FakeArr
    manager: QueueManager

    def arr_job(
        self, nzo_id: str, movie_id: int, tags: list[str] | None = None, **media: Any
    ) -> None:
        self.sab.add(nzo_id)
        self.radarr.add_media(movie_id, tags=tags, **media)
        self.radarr.queue.append(
            ArrQueueItem(download_id=nzo_id, media_id=movie_id, title=f"m{movie_id}")
        )


async def _build(**overrides: Any) -> Harness:
    config = make_config(**overrides)
    db = await make_db()
    repo = QueueRepository(db)
    sab = FakeSab()
    radarr = FakeRadarr()
    arr: dict[str, ArrClient] = {"radarr": radarr, "sonarr": FakeSonarr()}
    return Harness(db, repo, sab, radarr, QueueManager(config, repo, sab, arr))


@pytest_asyncio.fixture
async def h() -> AsyncGenerator[Harness, None]:
    harness = await _build()
    yield harness
    await harness.db.disconnect()


async def _grabbed_item(h: Harness, movie_id: int) -> QueueItem:
    return await h.repo.upsert_item(
        QueueItem(
            source="radarr",
            source_id=str(movie_id),
            tags=[],
            virtual_queue="german_upgrade",
            metadata={"upgrade_grabbed": True, "upgrade_grabbed_at": "2020-01-01"},
        )
    )


# ---------------------------------------------------------------------------
# Ordering
# ---------------------------------------------------------------------------


async def test_request_moves_above_upgrade_and_unknown(h: Harness) -> None:
    await _grabbed_item(h, 1)
    h.arr_job("nzo_upgrade", 1)
    h.sab.add("nzo_manual")  # added directly to SABnzbd by the user
    h.arr_job("nzo_request", 2, tags=["request"], has_file=False)

    await h.manager.run_cycle()

    assert h.sab.order == ["nzo_request", "nzo_manual", "nzo_upgrade"]
    assert h.sab.status_of("nzo_request") == "Downloading"
    assert h.sab.status_of("nzo_manual") == "Paused"
    assert h.sab.status_of("nzo_upgrade") == "Paused"


async def test_upgrades_last_can_be_disabled() -> None:
    h = await _build(upgrades_last=False)
    await _grabbed_item(h, 1)
    h.sab.add("nzo_manual")
    h.arr_job("nzo_upgrade", 1)

    await h.manager.run_cycle()

    assert h.sab.order == ["nzo_upgrade", "nzo_manual"]
    await h.db.disconnect()


async def test_no_switch_when_order_is_correct(h: Harness) -> None:
    h.arr_job("nzo_a", 1, tags=["request"], has_file=False)
    h.arr_job("nzo_b", 2, has_file=False)

    await h.manager.run_cycle()
    await h.manager.run_cycle()

    assert not [c for c in h.sab.calls if c[0] == "switch"]


# ---------------------------------------------------------------------------
# Pausing
# ---------------------------------------------------------------------------


async def test_user_paused_job_is_never_resumed(h: Harness) -> None:
    h.arr_job("nzo_top", 1, tags=["request"], has_file=False)
    h.arr_job("nzo_next", 2, has_file=False)
    h.sab.jobs[0].status = "Paused"  # user paused the top job in SABnzbd

    await h.manager.run_cycle()

    assert h.sab.status_of("nzo_top") == "Paused"
    assert ("resume", "nzo_top") not in h.sab.calls
    # the next job becomes the active one instead
    assert h.sab.status_of("nzo_next") == "Downloading"


async def test_job_paused_by_conductarr_is_resumed_when_it_becomes_top(
    h: Harness,
) -> None:
    h.arr_job("nzo_request", 1, tags=["request"], has_file=False)
    h.arr_job("nzo_other", 2, has_file=False)
    await h.manager.run_cycle()
    assert h.sab.status_of("nzo_other") == "Paused"

    h.sab.finish("nzo_request")
    await h.manager.run_cycle()

    assert h.sab.status_of("nzo_other") == "Downloading"
    assert await h.repo.get_paused_jobs() == set()


async def test_release_paused_jobs_on_shutdown(h: Harness) -> None:
    h.arr_job("nzo_a", 1, tags=["request"], has_file=False)
    h.arr_job("nzo_b", 2, has_file=False)
    h.arr_job("nzo_c", 3, has_file=False)
    await h.manager.run_cycle()
    assert await h.repo.get_paused_jobs() == {"nzo_b", "nzo_c"}

    await h.manager.release_paused_jobs()

    assert [j.status for j in h.sab.jobs] == ["Downloading"] * 3
    assert await h.repo.get_paused_jobs() == set()


async def test_paused_set_survives_restart(h: Harness) -> None:
    h.arr_job("nzo_a", 1, tags=["request"], has_file=False)
    h.arr_job("nzo_b", 2, has_file=False)
    await h.manager.run_cycle()

    restarted = QueueManager(
        make_config(), h.repo, h.sab, {"radarr": h.radarr, "sonarr": FakeSonarr()}
    )
    await restarted.load_state()
    await restarted.release_paused_jobs()

    assert h.sab.status_of("nzo_b") == "Downloading"


async def test_single_download_mode_disabled_only_reorders() -> None:
    h = await _build(enforce_single_download=False)
    h.arr_job("nzo_a", 1, has_file=False)
    h.arr_job("nzo_b", 2, tags=["request"], has_file=False)

    await h.manager.run_cycle()

    assert h.sab.order == ["nzo_b", "nzo_a"]
    assert not [c for c in h.sab.calls if c[0] == "pause"]
    await h.db.disconnect()


async def test_globally_paused_queue_is_left_alone(h: Harness) -> None:
    h.arr_job("nzo_a", 1, has_file=False)
    h.arr_job("nzo_b", 2, has_file=False)
    h.sab.paused = True

    await h.manager.run_cycle()

    assert not [c for c in h.sab.calls if c[0] in ("pause", "resume")]


async def test_dry_run_makes_no_sabnzbd_writes() -> None:
    h = await _build(dry_run=True)
    h.arr_job("nzo_a", 1, has_file=False)
    h.arr_job("nzo_b", 2, tags=["request"], has_file=False)

    await h.manager.run_cycle()
    await h.manager.release_paused_jobs()

    assert h.sab.write_calls == []
    await h.db.disconnect()


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


async def test_unknown_job_is_resolved_again_later(h: Harness) -> None:
    h.sab.add("nzo_late")
    await h.manager.run_cycle()
    assert h.manager.entries["nzo_late"].unknown

    # Arr lists the job a bit later (e.g. its queue refresh lagged)
    h.radarr.add_media(5, tags=["request"], has_file=False)
    h.radarr.queue.append(ArrQueueItem(download_id="nzo_late", media_id=5, title="x"))
    await h.manager.run_cycle()
    assert h.manager.entries["nzo_late"].unknown  # retry delay not yet reached

    with patch.object(manager_module, "UNKNOWN_RETRY_SECONDS", 0.0):
        await h.manager.run_cycle()
    assert h.manager.entries["nzo_late"].virtual_queue == "requests"


async def test_arr_outage_does_not_poison_cache(h: Harness) -> None:
    h.radarr.fail_queue = True
    h.arr_job("nzo_a", 1, tags=["request"], has_file=False)
    await h.manager.run_cycle()
    assert h.manager.entries["nzo_a"].unknown

    h.radarr.fail_queue = False
    with patch.object(manager_module, "UNKNOWN_RETRY_SECONDS", 0.0):
        await h.manager.run_cycle()
    assert h.manager.entries["nzo_a"].virtual_queue == "requests"


async def test_conductarr_grab_keeps_its_upgrade_queue(h: Harness) -> None:
    await _grabbed_item(h, 1)
    h.arr_job("nzo_up", 1, tags=["request"])  # tags would say "requests"

    await h.manager.run_cycle()

    entry = h.manager.entries["nzo_up"]
    assert entry.virtual_queue == "german_upgrade"
    assert entry.conductarr_grab


# ---------------------------------------------------------------------------
# Completion handling
# ---------------------------------------------------------------------------


async def test_completion_waits_for_post_processing(h: Harness) -> None:
    item = await _grabbed_item(h, 1)
    h.arr_job("nzo_up", 1)
    await h.manager.run_cycle()

    h.sab.finish("nzo_up", status="Extracting")
    await h.manager.run_cycle()
    assert await h.repo.get_job_map("nzo_up") is not None

    h.sab.history[-1]["status"] = "Completed"
    await h.manager.run_cycle()
    assert await h.repo.get_job_map("nzo_up") is None
    assert item.id is not None
    done = await h.repo.get_item_by_id(item.id)
    assert done is not None
    assert "upgrade_grabbed" not in done.metadata
    assert "upgrade_completed_at" in done.metadata
    assert done.status == "completed"


async def test_failed_download_gets_short_cooldown(h: Harness) -> None:
    item = await _grabbed_item(h, 1)
    h.arr_job("nzo_up", 1)
    await h.manager.run_cycle()

    h.sab.finish("nzo_up", status="Failed")
    await h.manager.run_cycle()

    assert item.id is not None
    failed = await h.repo.get_item_by_id(item.id)
    assert failed is not None
    assert failed.status == "failed"
    assert "upgrade_no_release_at" in failed.metadata
    assert "upgrade_grabbed" not in failed.metadata


async def test_job_removed_without_history_is_forgotten_after_grace(
    h: Harness,
) -> None:
    await _grabbed_item(h, 1)
    h.arr_job("nzo_up", 1)
    await h.manager.run_cycle()

    h.sab.remove("nzo_up")  # user deleted it in SABnzbd
    await h.manager.run_cycle()
    assert await h.repo.get_job_map("nzo_up") is not None

    with patch.object(manager_module, "MISSING_JOB_GRACE_SECONDS", 0.0):
        await h.manager.run_cycle()
    assert await h.repo.get_job_map("nzo_up") is None
    assert await h.repo.count_grabbed("german_upgrade") == 0


async def test_history_not_queried_without_departed_jobs(h: Harness) -> None:
    h.arr_job("nzo_a", 1, has_file=False)
    await h.manager.run_cycle()
    await h.manager.run_cycle()

    assert ("history",) not in h.sab.calls


async def test_history_failure_keeps_tracking(h: Harness) -> None:
    await _grabbed_item(h, 1)
    h.arr_job("nzo_up", 1)
    await h.manager.run_cycle()
    h.sab.finish("nzo_up")

    async def broken(*_: Any, **__: Any) -> list[dict[str, Any]]:
        raise ConnectionError("down")

    with patch.object(h.sab, "get_history", broken):
        await h.manager.run_cycle()
    assert await h.repo.get_job_map("nzo_up") is not None
