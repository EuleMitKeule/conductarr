"""CRUD queries for queue items and SABnzbd job mappings."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from conductarr.db.database import Database
from conductarr.queue.models import QueueItem

_LOGGER = logging.getLogger(__name__)


class QueueRepository:
    """Data-access layer for queue_items and sabnzbd_job_map tables."""

    def __init__(self, db: Database) -> None:
        self._db = db

    # ------------------------------------------------------------------
    # queue_items
    # ------------------------------------------------------------------

    async def upsert_item(self, item: QueueItem) -> QueueItem:
        """Insert or replace a queue item based on (source, source_id)."""
        sql = """
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
            """
        params = (
            item.source,
            item.source_id,
            item.virtual_queue,
            json.dumps(item.tags),
            item.status,
            item.attempts,
            item.last_tried_at.isoformat() if item.last_tried_at else None,
            json.dumps(item.metadata),
        )
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        await self._db.execute(sql, params)
        return await self._get_item_by_unique(item.source, item.source_id)  # type: ignore[return-value]

    async def get_item(self, source: str, source_id: str) -> QueueItem | None:
        """Fetch a single queue item by source and source_id."""
        return await self._get_item_by_unique(source, source_id)

    async def get_item_by_id(self, item_id: int) -> QueueItem | None:
        """Fetch a single queue item by its primary key."""
        sql = "SELECT * FROM queue_items WHERE id = ?"
        params = (item_id,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        row = await self._db.fetchone(sql, params)
        found = row is not None
        _LOGGER.debug("DB fetchone: id=%d found=%s", item_id, found)
        return self._row_to_item(row) if row else None

    async def get_items_by_queue(self, virtual_queue: str) -> list[QueueItem]:
        """Return items for a virtual queue in rotation order."""
        sql = """
            SELECT * FROM queue_items
            WHERE virtual_queue = ?
            ORDER BY attempts ASC, last_tried_at ASC NULLS FIRST
            """
        params = (virtual_queue,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        rows = await self._db.fetchall(sql, params)
        _LOGGER.debug(
            "DB fetchall: virtual_queue='%s' row_count=%d", virtual_queue, len(rows)
        )
        return [self._row_to_item(r) for r in rows]

    async def get_items_by_status(self, status: str) -> list[QueueItem]:
        """Return all items with the given status."""
        sql = "SELECT * FROM queue_items WHERE status = ?"
        params = (status,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        rows = await self._db.fetchall(sql, params)
        _LOGGER.debug("DB fetchall: status='%s' row_count=%d", status, len(rows))
        return [self._row_to_item(r) for r in rows]

    async def update_status(self, item_id: int, status: str) -> None:
        """Update the status of a queue item."""
        sql = "UPDATE queue_items SET status = ?, updated_at = datetime('now') WHERE id = ?"
        params = (status, item_id)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        await self._db.execute(sql, params)

    async def update_metadata(self, item_id: int, metadata: dict[str, Any]) -> None:
        """Overwrite the metadata JSON for a queue item."""
        sql = """
            UPDATE queue_items
            SET metadata = ?, updated_at = datetime('now')
            WHERE id = ?
            """
        params = (json.dumps(metadata), item_id)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        await self._db.execute(sql, params)

    async def get_upgrade_candidates(
        self,
        virtual_queue: str,
        source: str,
        retry_after_days: int,
    ) -> list[QueueItem]:
        """Return items eligible for an upgrade search.

        An item is a candidate when:
        - It belongs to *virtual_queue* and *source*.
        - ``upgrade_grabbed`` is not ``true`` (or absent).
        - ``upgrade_last_searched_at`` is NULL **or** older than
          ``now - retry_after_days``.
        Items are ordered numerically by *source_id* (ascending).
        """
        sql = """
            SELECT * FROM queue_items
            WHERE virtual_queue = ?
              AND source = ?
              AND (
                json_extract(metadata, '$.upgrade_grabbed') IS NOT 1
              )
              AND (
                json_extract(metadata, '$.upgrade_last_searched_at') IS NULL
                OR datetime(json_extract(metadata, '$.upgrade_last_searched_at'))
                   < datetime('now', '-' || ? || ' days')
              )
            ORDER BY CAST(source_id AS INTEGER) ASC
            """
        params = (virtual_queue, source, str(retry_after_days))
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        rows = await self._db.fetchall(sql, params)
        _LOGGER.debug(
            "DB fetchall: upgrade_candidates virtual_queue='%s' source='%s' row_count=%d",
            virtual_queue,
            source,
            len(rows),
        )
        return [self._row_to_item(r) for r in rows]

    async def increment_attempts(self, item_id: int) -> None:
        """Increment attempts and set last_tried_at for rotation."""
        sql = """
            UPDATE queue_items
            SET attempts = attempts + 1,
                last_tried_at = datetime('now'),
                updated_at = datetime('now')
            WHERE id = ?
            """
        params = (item_id,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        await self._db.execute(sql, params)

    # ------------------------------------------------------------------
    # sabnzbd_job_map
    # ------------------------------------------------------------------

    async def count_grabbed_not_in_jobmap(self, virtual_queue: str) -> int:
        """Count items grabbed but not yet in the job map (in-flight grabs)."""
        sql = """
            SELECT COUNT(*) FROM queue_items qi
            WHERE qi.virtual_queue = ?
              AND json_extract(qi.metadata, '$.upgrade_grabbed') IS 1
              AND qi.id NOT IN (SELECT queue_item_id FROM sabnzbd_job_map WHERE queue_item_id IS NOT NULL)
            """
        params = (virtual_queue,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        row = await self._db.fetchone(sql, params)
        count = row[0] if row else 0
        _LOGGER.debug(
            "DB fetchone: count_grabbed_not_in_jobmap virtual_queue='%s' count=%d",
            virtual_queue,
            count,
        )
        return count

    async def upsert_job_map(
        self, nzo_id: str, queue_item_id: int, virtual_queue: str
    ) -> None:
        """Create or update a SABnzbd job mapping."""
        sql = """
            INSERT INTO sabnzbd_job_map (nzo_id, queue_item_id, virtual_queue)
            VALUES (?, ?, ?)
            ON CONFLICT(nzo_id) DO UPDATE SET
                queue_item_id = excluded.queue_item_id,
                virtual_queue = excluded.virtual_queue
            """
        params = (nzo_id, queue_item_id, virtual_queue)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        await self._db.execute(sql, params)

    async def get_job_map(self, nzo_id: str) -> dict[str, Any] | None:
        """Look up a SABnzbd job mapping by nzo_id."""
        sql = "SELECT * FROM sabnzbd_job_map WHERE nzo_id = ?"
        params = (nzo_id,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        row = await self._db.fetchone(sql, params)
        found = row is not None
        _LOGGER.debug("DB fetchone: get_job_map nzo_id=%s found=%s", nzo_id, found)
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
        sql = "DELETE FROM sabnzbd_job_map WHERE nzo_id = ?"
        params = (nzo_id,)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        await self._db.execute(sql, params)

    async def get_all_job_maps(self) -> list[dict[str, Any]]:
        """Return all SABnzbd job mappings."""
        sql = "SELECT * FROM sabnzbd_job_map"
        _LOGGER.debug("DB query: %s | params: ()", sql.strip())
        rows = await self._db.fetchall(sql)
        _LOGGER.debug("DB fetchall: get_all_job_maps row_count=%d", len(rows))
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
        sql = "SELECT * FROM queue_items WHERE source = ? AND source_id = ?"
        params = (source, source_id)
        _LOGGER.debug("DB query: %s | params: %s", sql.strip(), str(params)[:200])
        row = await self._db.fetchone(sql, params)
        found = row is not None
        _LOGGER.debug(
            "DB fetchone: source=%s source_id=%s found=%s", source, source_id, found
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
