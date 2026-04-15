"""CRUD queries for queue items and SABnzbd job mappings."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from conductarr.db.database import Database
from conductarr.queue.models import QueueItem


class QueueRepository:
    """Data-access layer for queue_items and sabnzbd_job_map tables."""

    def __init__(self, db: Database) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # queue_items
    # ------------------------------------------------------------------

    async def upsert_item(self, item: QueueItem) -> QueueItem:
        """Insert or replace a queue item based on (source, source_id)."""
        await self._db.execute(
            """
            INSERT INTO queue_items
                (source, source_id, virtual_queue, tags, status,
                 attempts, last_tried_at, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(source, source_id) DO UPDATE SET
                virtual_queue = excluded.virtual_queue,
                tags          = excluded.tags,
                status        = excluded.status,
                attempts      = excluded.attempts,
                last_tried_at = excluded.last_tried_at,
                metadata      = excluded.metadata,
                updated_at    = datetime('now')
            """,
            (
                item.source,
                item.source_id,
                item.virtual_queue,
                json.dumps(item.tags),
                item.status,
                item.attempts,
                item.last_tried_at.isoformat() if item.last_tried_at else None,
                json.dumps(item.metadata),
            ),
        )
        return await self._get_item_by_unique(item.source, item.source_id)  # type: ignore[return-value]

    async def get_item(self, source: str, source_id: str) -> QueueItem | None:
        """Fetch a single queue item by source and source_id."""
        return await self._get_item_by_unique(source, source_id)

    async def get_items_by_queue(self, virtual_queue: str) -> list[QueueItem]:
        """Return items for a virtual queue in rotation order."""
        rows = await self._db.fetchall(
            """
            SELECT * FROM queue_items
            WHERE virtual_queue = ?
            ORDER BY attempts ASC, last_tried_at ASC NULLS FIRST
            """,
            (virtual_queue,),
        )
        return [self._row_to_item(r) for r in rows]

    async def get_items_by_status(self, status: str) -> list[QueueItem]:
        """Return all items with the given status."""
        rows = await self._db.fetchall(
            "SELECT * FROM queue_items WHERE status = ?",
            (status,),
        )
        return [self._row_to_item(r) for r in rows]

    async def update_status(self, item_id: int, status: str) -> None:
        """Update the status of a queue item."""
        await self._db.execute(
            "UPDATE queue_items SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, item_id),
        )

    async def increment_attempts(self, item_id: int) -> None:
        """Increment attempts and set last_tried_at for rotation."""
        await self._db.execute(
            """
            UPDATE queue_items
            SET attempts = attempts + 1,
                last_tried_at = datetime('now'),
                updated_at = datetime('now')
            WHERE id = ?
            """,
            (item_id,),
        )

    # ------------------------------------------------------------------
    # sabnzbd_job_map
    # ------------------------------------------------------------------

    async def upsert_job_map(
        self, nzo_id: str, queue_item_id: int, virtual_queue: str
    ) -> None:
        """Create or update a SABnzbd job mapping."""
        await self._db.execute(
            """
            INSERT INTO sabnzbd_job_map (nzo_id, queue_item_id, virtual_queue)
            VALUES (?, ?, ?)
            ON CONFLICT(nzo_id) DO UPDATE SET
                queue_item_id = excluded.queue_item_id,
                virtual_queue = excluded.virtual_queue
            """,
            (nzo_id, queue_item_id, virtual_queue),
        )

    async def get_job_map(self, nzo_id: str) -> dict[str, Any] | None:
        """Look up a SABnzbd job mapping by nzo_id."""
        row = await self._db.fetchone(
            "SELECT * FROM sabnzbd_job_map WHERE nzo_id = ?", (nzo_id,)
        )
        if row is None:
            return None
        return {
            "nzo_id": row[0],
            "queue_item_id": row[1],
            "virtual_queue": row[2],
            "detected_at": row[3],
        }

    async def delete_job_map(self, nzo_id: str) -> None:
        """Delete a SABnzbd job mapping."""
        await self._db.execute(
            "DELETE FROM sabnzbd_job_map WHERE nzo_id = ?", (nzo_id,)
        )

    async def get_all_job_maps(self) -> list[dict[str, Any]]:
        """Return all SABnzbd job mappings."""
        rows = await self._db.fetchall("SELECT * FROM sabnzbd_job_map")
        return [
            {
                "nzo_id": row[0],
                "queue_item_id": row[1],
                "virtual_queue": row[2],
                "detected_at": row[3],
            }
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _get_item_by_unique(
        self, source: str, source_id: str
    ) -> QueueItem | None:
        row = await self._db.fetchone(
            "SELECT * FROM queue_items WHERE source = ? AND source_id = ?",
            (source, source_id),
        )
        return self._row_to_item(row) if row else None

    @staticmethod
    def _row_to_item(row: Any) -> QueueItem:
        """Convert a database row to a QueueItem."""
        return QueueItem(
            id=row[0],
            source=row[1],
            source_id=row[2],
            virtual_queue=row[3],
            tags=json.loads(row[4]),
            status=row[5],
            attempts=row[6],
            last_tried_at=(datetime.fromisoformat(row[7]) if row[7] else None),
            created_at=datetime.fromisoformat(row[8]) if row[8] else None,
            updated_at=datetime.fromisoformat(row[9]) if row[9] else None,
            metadata=json.loads(row[10]),
        )
