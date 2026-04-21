"""Unit tests for QueueManager using an in-memory SQLite database."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from unittest.mock import AsyncMock, MagicMock

import pytest_asyncio

from conductarr.clients.sabnzbd import Queue, QueueSlot
from conductarr.config import MemoryDatabaseConfig
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue.manager import QueueManager
from conductarr.queue.models import QueueItem, VirtualQueue


@pytest_asyncio.fixture
async def db() -> AsyncGenerator[Database, None]:
    config = MemoryDatabaseConfig()
    database = Database(config)
    await database.connect()
    yield database
    await database.disconnect()


@pytest_asyncio.fixture
async def repo(db: Database) -> AsyncGenerator[QueueRepository, None]:
    yield QueueRepository(db)


def _make_manager(
    repo: QueueRepository, queues: list[VirtualQueue] | None = None
) -> QueueManager:
    if queues is None:
        queues = [
            VirtualQueue(
                name="user_requests",
                priority=100,
                matchers=[{"type": "tags", "tags": ["request", "overseerr"]}],
            ),
            VirtualQueue(
                name="german_upgrade",
                priority=50,
                matchers=[{"type": "tags", "tags": ["upgrade-de"]}],
            ),
        ]
    return QueueManager(repo, queues)


class TestAssignQueue:
    """Tests for QueueManager.assign_queue."""

    async def test_assign_matching_queue(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)
        item = QueueItem(source="radarr", source_id="movie-1", tags=["request"])
        result = await manager.assign_queue(item)

        assert result.virtual_queue == "user_requests"
        assert result.id is not None

    async def test_assign_no_matching_queue(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)
        item = QueueItem(source="radarr", source_id="movie-2", tags=["kometa"])
        result = await manager.assign_queue(item)

        assert result.virtual_queue is None
        assert result.id is not None

    async def test_assign_second_queue_matches(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)
        item = QueueItem(source="sonarr", source_id="episode-1", tags=["upgrade-de"])
        result = await manager.assign_queue(item)

        assert result.virtual_queue == "german_upgrade"

    async def test_highest_priority_wins(self, repo: QueueRepository) -> None:
        """When item matches multiple queues, highest priority wins."""
        manager = _make_manager(repo)
        item = QueueItem(
            source="radarr",
            source_id="movie-3",
            tags=["request", "upgrade-de"],
        )
        result = await manager.assign_queue(item)

        assert result.virtual_queue == "user_requests"


class TestOnJobRemoved:
    """Tests for QueueManager.on_job_removed."""

    async def test_is_noop(self, repo: QueueRepository) -> None:
        """on_job_removed is now a thin logger; reconcile handles cleanup."""
        manager = _make_manager(repo)

        item = QueueItem(
            source="radarr",
            source_id="movie-10",
            tags=["request"],
            status="downloading",
        )
        saved = await repo.upsert_item(item)
        assert saved.id is not None
        await repo.upsert_job_map("nzo_abc123", saved.id, "user_requests")

        # Should not raise and should not modify state
        await manager.on_job_removed("nzo_abc123")

        # Item and job map remain unchanged (cleanup is done by reconcile)
        updated = await repo.get_item("radarr", "movie-10")
        assert updated is not None
        assert updated.status == "downloading"

        job_map = await repo.get_job_map("nzo_abc123")
        assert job_map is not None

    async def test_no_job_map_is_noop(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)
        # Should not raise
        await manager.on_job_removed("nzo_nonexistent")


class TestGetNextForQueue:
    """Tests for QueueManager.get_next_for_queue."""

    async def test_returns_pending_item(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)

        item = QueueItem(
            source="radarr",
            source_id="movie-20",
            tags=["request"],
            virtual_queue="user_requests",
        )
        await repo.upsert_item(item)

        result = await manager.get_next_for_queue("user_requests")
        assert result is not None
        assert result.source_id == "movie-20"

    async def test_returns_none_for_empty_queue(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)
        result = await manager.get_next_for_queue("user_requests")
        assert result is None

    async def test_skips_non_pending(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)

        item = QueueItem(
            source="radarr",
            source_id="movie-21",
            tags=["request"],
            virtual_queue="user_requests",
            status="downloading",
        )
        await repo.upsert_item(item)

        result = await manager.get_next_for_queue("user_requests")
        assert result is None


class TestMarkFailed:
    """Tests for QueueManager.mark_failed."""

    async def test_increments_and_resets_to_pending(
        self, repo: QueueRepository
    ) -> None:
        manager = _make_manager(repo)

        item = QueueItem(
            source="radarr",
            source_id="movie-30",
            tags=["request"],
            status="downloading",
        )
        saved = await repo.upsert_item(item)
        assert saved.id is not None

        await manager.mark_failed(saved.id)

        updated = await repo.get_item("radarr", "movie-30")
        assert updated is not None
        assert updated.status == "pending"
        assert updated.attempts == 1
        assert updated.last_tried_at is not None


# ---------------------------------------------------------------------------
# Helper: build a minimal QueueSlot
# ---------------------------------------------------------------------------


def _make_slot(
    nzo_id: str,
    index: int,
    status: str = "Downloading",
) -> QueueSlot:
    return QueueSlot(
        nzo_id=nzo_id,
        filename=f"file-{nzo_id}.nzb",
        cat="misc",
        priority="Normal",
        status=status,
        index=index,
        mb="100",
        mbleft="50",
        percentage="50",
        timeleft="0:05:00",
        labels=[],
    )


def _make_queue(*slots: QueueSlot) -> Queue:
    return Queue(
        status="Downloading",
        paused=False,
        noofslots=len(slots),
        slots=list(slots),
    )


# ---------------------------------------------------------------------------
# _reorder_and_enforce_active – pause status guard
# ---------------------------------------------------------------------------


class TestReorderAndEnforceActive:
    """Tests for the pause-status guard in _reorder_and_enforce_active."""

    async def test_does_not_pause_verifying_slot(self, repo: QueueRepository) -> None:
        """Slots in final phases (Verifying, Extracting, …) must NOT be paused."""
        sabnzbd = MagicMock()
        sabnzbd.pause_job = AsyncMock()
        sabnzbd.resume_job = AsyncMock()
        sabnzbd.switch = AsyncMock()

        vq = VirtualQueue(
            name="user_requests",
            priority=100,
            matchers=[{"type": "tags", "tags": ["request"]}],
        )
        manager = QueueManager(repo, [vq], sabnzbd_client=sabnzbd)

        # Slot 0 is the top slot (Downloading) – no resume needed.
        # Slot 1 is in "Verifying" phase – must NOT be paused.
        slot_top = _make_slot("nzo-top", index=0, status="Downloading")
        slot_verifying = _make_slot("nzo-verifying", index=1, status="Verifying")
        sab_queue = _make_queue(slot_top, slot_verifying)

        # Map both slots so the manager can look them up
        item_top = await repo.upsert_item(
            QueueItem(
                source="radarr", source_id="1", tags=[], virtual_queue="user_requests"
            )
        )
        item_ver = await repo.upsert_item(
            QueueItem(
                source="radarr", source_id="2", tags=[], virtual_queue="user_requests"
            )
        )
        assert item_top.id is not None
        assert item_ver.id is not None
        await repo.upsert_job_map("nzo-top", item_top.id, "user_requests")
        await repo.upsert_job_map("nzo-verifying", item_ver.id, "user_requests")

        await manager._reorder_and_enforce_active(sab_queue)

        sabnzbd.pause_job.assert_not_awaited()

    async def test_pauses_queued_slot(self, repo: QueueRepository) -> None:
        """Slots with status 'Queued' must be paused when not at slot 0."""
        sabnzbd = MagicMock()
        sabnzbd.pause_job = AsyncMock()
        sabnzbd.resume_job = AsyncMock()
        sabnzbd.switch = AsyncMock()

        vq = VirtualQueue(
            name="user_requests",
            priority=100,
            matchers=[{"type": "tags", "tags": ["request"]}],
        )
        manager = QueueManager(repo, [vq], sabnzbd_client=sabnzbd)

        slot_top = _make_slot("nzo-top", index=0, status="Downloading")
        slot_queued = _make_slot("nzo-queued", index=1, status="Queued")
        sab_queue = _make_queue(slot_top, slot_queued)

        item_top = await repo.upsert_item(
            QueueItem(
                source="radarr", source_id="10", tags=[], virtual_queue="user_requests"
            )
        )
        item_q = await repo.upsert_item(
            QueueItem(
                source="radarr", source_id="11", tags=[], virtual_queue="user_requests"
            )
        )
        assert item_top.id is not None
        assert item_q.id is not None
        await repo.upsert_job_map("nzo-top", item_top.id, "user_requests")
        await repo.upsert_job_map("nzo-queued", item_q.id, "user_requests")

        await manager._reorder_and_enforce_active(sab_queue)

        sabnzbd.pause_job.assert_awaited_once_with("nzo-queued")

    async def test_does_not_pause_extracting_slot(self, repo: QueueRepository) -> None:
        """Slots in 'Extracting' phase must NOT be paused."""
        sabnzbd = MagicMock()
        sabnzbd.pause_job = AsyncMock()
        sabnzbd.resume_job = AsyncMock()
        sabnzbd.switch = AsyncMock()

        vq = VirtualQueue(
            name="user_requests",
            priority=100,
            matchers=[{"type": "tags", "tags": ["request"]}],
        )
        manager = QueueManager(repo, [vq], sabnzbd_client=sabnzbd)

        slot_top = _make_slot("nzo-top", index=0, status="Downloading")
        slot_extracting = _make_slot("nzo-extract", index=1, status="Extracting")
        sab_queue = _make_queue(slot_top, slot_extracting)

        item_top = await repo.upsert_item(
            QueueItem(
                source="radarr", source_id="20", tags=[], virtual_queue="user_requests"
            )
        )
        item_ext = await repo.upsert_item(
            QueueItem(
                source="radarr", source_id="21", tags=[], virtual_queue="user_requests"
            )
        )
        assert item_top.id is not None
        assert item_ext.id is not None
        await repo.upsert_job_map("nzo-top", item_top.id, "user_requests")
        await repo.upsert_job_map("nzo-extract", item_ext.id, "user_requests")

        await manager._reorder_and_enforce_active(sab_queue)

        sabnzbd.pause_job.assert_not_awaited()
