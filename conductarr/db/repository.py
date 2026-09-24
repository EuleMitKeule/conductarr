"""CRUD queries for conductarr's own SQLite state.

Nothing in here touches media files or the Arr/SABnzbd instances; the
database only records what conductarr has observed and done.

All timestamps are stored as ISO-8601 UTC strings.  Comparisons always wrap
*both* sides in ``datetime()`` so that ``2026-01-01T10:00:00+00:00`` and
``2026-01-01 10:00:00`` compare correctly.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any

from conductarr.db.database import Database
from conductarr.queue.models import QueueItem

_LOGGER = logging.getLogger(__name__)

_CANDIDATE_FILTER = """
    virtual_queue = ?
    AND source = ?
    AND json_extract(metadata, '$.upgrade_grabbed') IS NOT 1
    AND (
        json_extract(metadata, '$.upgrade_last_searched_at') IS NULL
        OR datetime(json_extract(metadata, '$.upgrade_last_searched_at'))
           < datetime('now', '-' || ? || ' days')
    )
    AND (
        json_extract(metadata, '$.upgrade_no_release_at') IS NULL
        OR datetime(json_extract(metadata, '$.upgrade_no_release_at'))
           < datetime('now', '-' || ? || ' days')
    )
"""


class QueueRepository:
    """Data-access layer for conductarr's tables."""

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
        await self._db.execute(sql, params)
        stored = await self._get_item_by_unique(item.source, item.source_id)
        assert stored is not None  # just written
        return stored

    async def insert_candidates(
        self, source: str, source_ids: list[str], virtual_queue: str
    ) -> None:
        """Bulk-insert new upgrade candidates (existing rows are left untouched)."""
        await self._db.executemany(
            """
            INSERT INTO queue_items (source, source_id, virtual_queue, tags, status)
            VALUES (?, ?, ?, '[]', 'pending')
            ON CONFLICT(source, source_id) DO NOTHING
            """,
            [(source, sid, virtual_queue) for sid in source_ids],
        )

    async def get_queue_assignments(self, source: str) -> dict[str, str | None]:
        """Return ``source_id → virtual_queue`` for every row of *source*."""
        rows = await self._db.fetchall(
            "SELECT source_id, virtual_queue FROM queue_items WHERE source = ?",
            (source,),
        )
        return {str(r[0]): r[1] for r in rows}

    async def reassign_queue(
        self, source: str, source_ids: list[str], virtual_queue: str
    ) -> None:
        await self._db.executemany(
            """
            UPDATE queue_items SET virtual_queue = ?, updated_at = datetime('now')
            WHERE source = ? AND source_id = ?
              AND json_extract(metadata, '$.upgrade_grabbed') IS NOT 1
            """,
            [(virtual_queue, source, sid) for sid in source_ids],
        )

    async def get_item(self, source: str, source_id: str) -> QueueItem | None:
        """Fetch a single queue item by source and source_id."""
        return await self._get_item_by_unique(source, source_id)

    async def get_item_by_id(self, item_id: int) -> QueueItem | None:
        """Fetch a single queue item by its primary key."""
        row = await self._db.fetchone(
            "SELECT * FROM queue_items WHERE id = ?", (item_id,)
        )
        return self._row_to_item(row) if row else None

    async def update_status(self, item_id: int, status: str) -> None:
        await self._db.execute(
            "UPDATE queue_items SET status = ?, updated_at = datetime('now') WHERE id = ?",
            (status, item_id),
        )

    async def update_metadata(self, item_id: int, metadata: dict[str, Any]) -> None:
        """Overwrite the metadata JSON for a queue item."""
        await self._db.execute(
            """
            UPDATE queue_items
            SET metadata = ?, updated_at = datetime('now')
            WHERE id = ?
            """,
            (json.dumps(metadata), item_id),
        )

    async def get_upgrade_candidates(
        self,
        virtual_queue: str,
        source: str,
        retry_after_days: int,
        no_release_retry_days: int = 1,
        *,
        after_source_id: int | None = None,
        limit: int | None = None,
    ) -> list[QueueItem]:
        """Return items that are due for an upgrade check, ordered by source_id.

        An item is a candidate when it belongs to *virtual_queue*/*source*,
        is not currently grabbed, and neither its last search nor its last
        "no usable release" result is within the respective cool-down.
        """
        sql = f"SELECT * FROM queue_items WHERE {_CANDIDATE_FILTER}"
        params: list[Any] = [
            virtual_queue,
            source,
            str(retry_after_days),
            str(no_release_retry_days),
        ]
        if after_source_id is not None:
            sql += " AND CAST(source_id AS INTEGER) > ?"
            params.append(after_source_id)
        sql += " ORDER BY CAST(source_id AS INTEGER) ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        rows = await self._db.fetchall(sql, tuple(params))
        return [self._row_to_item(r) for r in rows]

    async def count_upgrade_candidates(
        self,
        virtual_queue: str,
        source: str,
        retry_after_days: int,
        no_release_retry_days: int,
    ) -> int:
        row = await self._db.fetchone(
            f"SELECT COUNT(*) FROM queue_items WHERE {_CANDIDATE_FILTER}",
            (virtual_queue, source, str(retry_after_days), str(no_release_retry_days)),
        )
        return int(row[0]) if row else 0

    async def count_grabbed(self, virtual_queue: str) -> int:
        """Number of conductarr grabs of *virtual_queue* that are still in flight."""
        row = await self._db.fetchone(
            """
            SELECT COUNT(*) FROM queue_items
            WHERE virtual_queue = ?
              AND json_extract(metadata, '$.upgrade_grabbed') IS 1
            """,
            (virtual_queue,),
        )
        return int(row[0]) if row else 0

    async def get_grabbed_items_without_jobmap(
        self, virtual_queue: str, grabbed_before: datetime
    ) -> list[QueueItem]:
        """Grabbed items that never showed up in SABnzbd before *grabbed_before*."""
        rows = await self._db.fetchall(
            """
            SELECT qi.* FROM queue_items qi
            WHERE qi.virtual_queue = ?
              AND json_extract(qi.metadata, '$.upgrade_grabbed') IS 1
              AND qi.id NOT IN (
                  SELECT queue_item_id FROM sabnzbd_job_map
                  WHERE queue_item_id IS NOT NULL
              )
              AND (
                  json_extract(qi.metadata, '$.upgrade_grabbed_at') IS NULL
                  OR datetime(json_extract(qi.metadata, '$.upgrade_grabbed_at'))
                     < datetime(?)
              )
            """,
            (virtual_queue, grabbed_before.isoformat()),
        )
        return [self._row_to_item(r) for r in rows]

    async def get_stats(self) -> list[dict[str, Any]]:
        """Per queue/source counts for the ``status`` command."""
        rows = await self._db.fetchall(
            """
            SELECT
                COALESCE(virtual_queue, '-'),
                source,
                COUNT(*),
                SUM(json_extract(metadata, '$.upgrade_grabbed') IS 1),
                SUM(json_extract(metadata, '$.upgrade_satisfied_at') IS NOT NULL),
                SUM(json_extract(metadata, '$.upgrade_no_match_at') IS NOT NULL),
                SUM(json_extract(metadata, '$.upgrade_completed_at') IS NOT NULL)
            FROM queue_items
            GROUP BY virtual_queue, source
            ORDER BY virtual_queue, source
            """
        )
        return [
            {
                "queue": r[0],
                "source": r[1],
                "items": int(r[2] or 0),
                "grabbed": int(r[3] or 0),
                "satisfied": int(r[4] or 0),
                "no_match": int(r[5] or 0),
                "upgraded": int(r[6] or 0),
            }
            for r in rows
        ]

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
        return self._row_to_job_map(row) if row else None

    async def delete_job_map(self, nzo_id: str) -> None:
        await self._db.execute(
            "DELETE FROM sabnzbd_job_map WHERE nzo_id = ?", (nzo_id,)
        )

    async def get_all_job_maps(self) -> list[dict[str, Any]]:
        rows = await self._db.fetchall("SELECT * FROM sabnzbd_job_map")
        return [self._row_to_job_map(row) for row in rows]

    async def has_job_map_for_item(self, queue_item_id: int) -> bool:
        row = await self._db.fetchone(
            "SELECT 1 FROM sabnzbd_job_map WHERE queue_item_id = ? LIMIT 1",
            (queue_item_id,),
        )
        return row is not None

    # ------------------------------------------------------------------
    # paused_jobs
    # ------------------------------------------------------------------

    async def get_paused_jobs(self) -> set[str]:
        rows = await self._db.fetchall("SELECT nzo_id FROM paused_jobs")
        return {str(r[0]) for r in rows}

    async def add_paused_job(self, nzo_id: str) -> None:
        await self._db.execute(
            "INSERT INTO paused_jobs (nzo_id) VALUES (?) ON CONFLICT DO NOTHING",
            (nzo_id,),
        )

    async def remove_paused_job(self, nzo_id: str) -> None:
        await self._db.execute("DELETE FROM paused_jobs WHERE nzo_id = ?", (nzo_id,))

    # ------------------------------------------------------------------
    # search_log
    # ------------------------------------------------------------------

    async def record_search(
        self, virtual_queue: str, source: str, source_id: str, outcome: str = ""
    ) -> None:
        await self._db.execute(
            """
            INSERT INTO search_log (virtual_queue, source, source_id, outcome)
            VALUES (?, ?, ?, ?)
            """,
            (virtual_queue, source, source_id, outcome),
        )

    async def update_last_search_outcome(
        self, virtual_queue: str, outcome: str
    ) -> None:
        await self._db.execute(
            """
            UPDATE search_log SET outcome = ?
            WHERE id = (SELECT MAX(id) FROM search_log WHERE virtual_queue = ?)
            """,
            (outcome, virtual_queue),
        )

    async def seconds_since_last_search(self, virtual_queue: str) -> float | None:
        row = await self._db.fetchone(
            """
            SELECT (julianday('now') - julianday(MAX(searched_at))) * 86400.0
            FROM search_log WHERE virtual_queue = ?
            """,
            (virtual_queue,),
        )
        return float(row[0]) if row and row[0] is not None else None

    async def count_searches_last_day(self, virtual_queue: str) -> int:
        row = await self._db.fetchone(
            """
            SELECT COUNT(*) FROM search_log
            WHERE virtual_queue = ? AND searched_at > datetime('now', '-1 day')
            """,
            (virtual_queue,),
        )
        return int(row[0]) if row else 0

    async def prune_search_log(self, keep_days: int = 14) -> None:
        await self._db.execute(
            "DELETE FROM search_log WHERE searched_at < datetime('now', '-' || ? || ' days')",
            (str(keep_days),),
        )

    # ------------------------------------------------------------------
    # kv_state
    # ------------------------------------------------------------------

    async def get_state(self, key: str) -> str | None:
        row = await self._db.fetchone(
            "SELECT value FROM kv_state WHERE key = ?", (key,)
        )
        return str(row[0]) if row else None

    async def set_state(self, key: str, value: str) -> None:
        await self._db.execute(
            """
            INSERT INTO kv_state (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
            """,
            (key, value),
        )

    async def delete_state(self, key: str) -> None:
        await self._db.execute("DELETE FROM kv_state WHERE key = ?", (key,))

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
    def _row_to_job_map(row: Any) -> dict[str, Any]:
        return {
            "nzo_id": row[0],
            "queue_item_id": row[1],
            "virtual_queue": row[2],
            "detected_at": row[3],
        }

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
