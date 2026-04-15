"""Unit tests for QueueManager using an in-memory SQLite database."""

from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest_asyncio

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

    async def test_marks_item_completed(self, repo: QueueRepository) -> None:
        manager = _make_manager(repo)

        # Create and persist an item
        item = QueueItem(
            source="radarr",
            source_id="movie-10",
            tags=["request"],
            status="downloading",
        )
        saved = await repo.upsert_item(item)
        assert saved.id is not None

        # Create a job map linking nzo_id to the queue item
        await repo.upsert_job_map("nzo_abc123", saved.id, "user_requests")

        # Simulate job removal
        await manager.on_job_removed("nzo_abc123")

        # Verify the item is now completed
        updated = await repo.get_item("radarr", "movie-10")
        assert updated is not None
        assert updated.status == "completed"

        # Verify the job map was deleted
        job_map = await repo.get_job_map("nzo_abc123")
        assert job_map is None

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
