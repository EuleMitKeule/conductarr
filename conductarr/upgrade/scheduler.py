"""Upgrade scheduler: finds library items that need an upgrade and grabs one.

Per upgrade queue and cycle the scheduler performs **at most one indexer
search**.  Before searching it checks, in order:

* the queue is below ``max_active`` in-flight grabs,
* SABnzbd is reachable, not globally paused and has enough free space,
* no other (user-initiated) download is waiting (``defer_to_other_downloads``),
* ``search_interval`` and ``max_searches_per_day`` allow another search.

Candidates are walked in ``source_id`` order from a persisted cursor.
Items that already satisfy the accept_conditions, have no file, are gone,
or are already downloading are skipped cheaply (no indexer search).

A grab is marked in the database *before* the Arr is asked to grab it, so a
grab can never go untracked (which would free a slot and cause over-grabbing).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from conductarr.clients.arr import ArrClient
from conductarr.clients.release import MediaState, ReleaseResult
from conductarr.config import ConductarrConfig, UpgradeConfig, VirtualQueueConfig
from conductarr.db.repository import QueueRepository
from conductarr.queue.manager import QueueManager, QueueSnapshot
from conductarr.queue.models import QueueItem
from conductarr.upgrade.selection import (
    TRANSIENT_OUTCOMES,
    Outcome,
    SelectionResult,
    media_satisfies_conditions,
    select_release,
)

_LOGGER = logging.getLogger(__name__)

# Candidates pre-checked (cheap Arr calls, no indexer search) per cycle.
CHECK_BATCH_SIZE = 25
# How long cached blocklist answers stay valid.
_BLOCKLIST_TTL = 600.0
# Repeated "blocked" log lines are emitted at most this often.
_BLOCK_LOG_INTERVAL = 3600.0
_MAX_GRAB_FAILURES = 3
_MAX_REMEMBERED_TITLES = 25


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class DryRunCandidateResult:
    """Result of a single candidate's dry-run upgrade check."""

    queue: str
    source: str
    source_id: str
    outcome: str
    reason: str
    media_title: str = ""
    releases_total: int = 0
    steps: list[tuple[str, int]] = field(default_factory=list)
    current_score: int | None = None
    current_resolution: int | None = None
    best_release: ReleaseResult | None = None


class UpgradeScheduler:
    def __init__(
        self,
        config: ConductarrConfig,
        repository: QueueRepository,
        arr_clients: dict[str, ArrClient],
        queue_manager: QueueManager,
    ) -> None:
        self._config = config
        self._repo = repository
        self._arr = arr_clients
        self._queue_manager = queue_manager
        self._source_turn: dict[str, int] = {}
        self._blocklist: dict[str, tuple[set[str], float]] = {}
        self._block_logged: dict[str, tuple[str, float]] = {}

    # ------------------------------------------------------------------
    # Library scan (seeding)
    # ------------------------------------------------------------------

    async def seed_if_due(self, force: bool = False) -> None:
        for queue in self._config.upgrade_queues:
            upgrade = queue.upgrade
            assert upgrade is not None
            key = f"last_scan:{queue.name}"
            last = await self._repo.get_state(key)
            if not force and last is not None:
                if upgrade.rescan_interval <= 0:
                    continue
                elapsed = (
                    datetime.now(UTC) - datetime.fromisoformat(last)
                ).total_seconds()
                if elapsed < upgrade.rescan_interval:
                    continue
            ok = True
            for source in upgrade.sources:
                ok = await self._seed_source(queue.name, source) and ok
            if ok:
                await self._repo.set_state(key, _now_iso())

    async def _seed_source(self, queue_name: str, source: str) -> bool:
        try:
            media_ids = await self._arr[source].list_media_ids_with_files()
        except Exception as exc:
            _LOGGER.warning("Library scan of %s failed: %s", source, exc)
            return False
        existing = await self._repo.get_queue_assignments(source)
        upgrade_queues = self._queue_manager.upgrade_queue_names
        new = [str(i) for i in media_ids if str(i) not in existing]
        # Items first seen as a normal download (e.g. a user request) now have
        # a file and become upgrade candidates as well.
        adopt = [
            str(i)
            for i in media_ids
            if str(i) in existing and existing[str(i)] not in upgrade_queues
        ]
        await self._repo.insert_candidates(source, new, queue_name)
        await self._repo.reassign_queue(source, adopt, queue_name)
        _LOGGER.info(
            "Library scan %s → '%s': %d with files, %d new, %d adopted",
            source,
            queue_name,
            len(media_ids),
            len(new),
            len(adopt),
        )
        return True

    # ------------------------------------------------------------------
    # Cycle
    # ------------------------------------------------------------------

    async def run_cycle(self, snapshot: QueueSnapshot | None) -> None:
        for queue in self._config.upgrade_queues:
            try:
                await self._run_queue(queue, snapshot)
            except Exception:
                _LOGGER.exception("Error in upgrade queue '%s'", queue.name)

    async def _run_queue(
        self, queue: VirtualQueueConfig, snapshot: QueueSnapshot | None
    ) -> None:
        upgrade = queue.upgrade
        assert upgrade is not None
        await self._clear_stale_grabs(queue.name, upgrade)

        active = await self._repo.count_grabbed(queue.name)
        if active >= upgrade.max_active:
            return

        if reason := self._blocked_reason(snapshot, upgrade):
            self._log_blocked(queue.name, reason)
            return

        since = await self._repo.seconds_since_last_search(queue.name)
        if since is not None and since < upgrade.search_interval:
            return
        if upgrade.max_searches_per_day:
            searches = await self._repo.count_searches_last_day(queue.name)
            if searches >= upgrade.max_searches_per_day:
                self._log_blocked(
                    queue.name,
                    f"daily search budget used ({searches}/"
                    f"{upgrade.max_searches_per_day})",
                )
                return
        self._block_logged.pop(queue.name, None)

        sources = upgrade.sources
        turn = self._source_turn.get(queue.name, 0)
        for offset in range(len(sources)):
            source = sources[(turn + offset) % len(sources)]
            if await self._process_next_candidate(
                queue.name, upgrade, source, snapshot
            ):
                self._source_turn[queue.name] = turn + offset + 1
                return
        self._source_turn[queue.name] = turn + 1

    def _blocked_reason(
        self, snapshot: QueueSnapshot | None, upgrade: UpgradeConfig
    ) -> str | None:
        if snapshot is None:
            return "SABnzbd state unknown"
        if snapshot.paused:
            return "SABnzbd queue is paused"
        free = snapshot.diskspace_free_gb
        if free is not None and free < self._config.min_free_space_gb:
            return (
                f"low disk space ({free:.1f} GB < {self._config.min_free_space_gb} GB)"
            )
        if upgrade.defer_to_other_downloads:
            upgrade_queues = self._queue_manager.upgrade_queue_names
            others = [
                nzo
                for nzo, entry in snapshot.entries.items()
                if entry.virtual_queue not in upgrade_queues
                and nzo not in snapshot.user_paused
            ]
            if others:
                return f"{len(others)} other download(s) in SABnzbd"
        return None

    def _log_blocked(self, queue_name: str, reason: str) -> None:
        now = time.monotonic()
        previous = self._block_logged.get(queue_name)
        if (
            previous is None
            or previous[0] != reason
            or now - previous[1] > _BLOCK_LOG_INTERVAL
        ):
            _LOGGER.info("Upgrade queue '%s' waiting: %s", queue_name, reason)
            self._block_logged[queue_name] = (reason, now)

    async def _clear_stale_grabs(self, queue_name: str, upgrade: UpgradeConfig) -> None:
        cutoff = datetime.now(UTC) - timedelta(seconds=upgrade.grab_timeout)
        for item in await self._repo.get_grabbed_items_without_jobmap(
            queue_name, cutoff
        ):
            if item.id is None:
                continue
            item.metadata.pop("upgrade_grabbed", None)
            item.metadata.pop("upgrade_grabbed_at", None)
            item.metadata["upgrade_no_release_at"] = _now_iso()
            item.metadata["upgrade_last_failure"] = "never appeared in SABnzbd"
            await self._repo.update_metadata(item.id, item.metadata)
            _LOGGER.warning(
                "Grab for %s/%s never showed up in SABnzbd within %.0fs; releasing slot",
                item.source,
                item.source_id,
                upgrade.grab_timeout,
            )

    # ------------------------------------------------------------------
    # Candidate processing
    # ------------------------------------------------------------------

    async def _next_candidates(
        self, queue_name: str, upgrade: UpgradeConfig, source: str
    ) -> list[QueueItem]:
        cursor_key = f"cursor:{queue_name}:{source}"
        raw_cursor = await self._repo.get_state(cursor_key)
        cursor = int(raw_cursor) if raw_cursor and raw_cursor.isdigit() else None
        candidates = await self._repo.get_upgrade_candidates(
            queue_name,
            source,
            upgrade.retry_after_days,
            upgrade.no_release_retry_days,
            after_source_id=cursor,
            limit=CHECK_BATCH_SIZE,
        )
        if not candidates and cursor is not None:
            await self._repo.delete_state(cursor_key)  # wrap around
            candidates = await self._repo.get_upgrade_candidates(
                queue_name,
                source,
                upgrade.retry_after_days,
                upgrade.no_release_retry_days,
                limit=CHECK_BATCH_SIZE,
            )
        return candidates

    async def _set_cursor(self, queue_name: str, source: str, source_id: str) -> None:
        await self._repo.set_state(f"cursor:{queue_name}:{source}", source_id)

    def _is_downloading(
        self, snapshot: QueueSnapshot | None, source: str, source_id: str
    ) -> bool:
        entries = snapshot.entries.values() if snapshot else ()
        known = self._queue_manager.entries.values()
        return any(
            e.source == source
            and (e.source_id == source_id or source_id in e.covered_ids)
            for e in (*entries, *known)
        )

    async def _skip(self, candidate: QueueItem, key: str) -> None:
        assert candidate.id is not None
        now = _now_iso()
        candidate.metadata[key] = now
        candidate.metadata["upgrade_last_searched_at"] = now
        await self._repo.update_metadata(candidate.id, candidate.metadata)

    async def _process_next_candidate(
        self,
        queue_name: str,
        upgrade: UpgradeConfig,
        source: str,
        snapshot: QueueSnapshot | None,
    ) -> bool:
        """Pre-check candidates and search the first real one.

        Returns ``True`` when an indexer search was performed.
        """
        client = self._arr[source]
        for candidate in await self._next_candidates(queue_name, upgrade, source):
            if candidate.id is None:
                continue
            if self._is_downloading(
                snapshot, source, candidate.source_id
            ) or await self._repo.has_job_map_for_item(candidate.id):
                await self._set_cursor(queue_name, source, candidate.source_id)
                continue
            try:
                media = await client.get_media_state(int(candidate.source_id))
            except Exception as exc:
                _LOGGER.warning(
                    "Could not check %s/%s: %s", source, candidate.source_id, exc
                )
                return False  # retry this candidate next cycle

            skip_key = self._skip_reason(media, upgrade)
            if skip_key is not None:
                if not self._config.dry_run:
                    await self._skip(candidate, skip_key)
                await self._set_cursor(queue_name, source, candidate.source_id)
                continue
            assert media is not None

            result = await self._search_and_select(
                queue_name, source, media, candidate.metadata, upgrade
            )
            advance = await self._apply_result(
                queue_name, source, candidate, media, result, upgrade
            )
            if advance:
                await self._set_cursor(queue_name, source, candidate.source_id)
            return True
        return False

    @staticmethod
    def _skip_reason(media: MediaState | None, upgrade: UpgradeConfig) -> str | None:
        if media is None:
            return "upgrade_missing_at"
        if not media.has_file:
            return "upgrade_no_file_at"
        if not upgrade.include_unmonitored and not media.monitored:
            return "upgrade_unmonitored_at"
        if media_satisfies_conditions(
            media.custom_formats, media.custom_format_score, upgrade.accept_conditions
        ):
            return "upgrade_satisfied_at"
        return None

    async def _get_blocklist(self, source: str) -> set[str]:
        cached = self._blocklist.get(source)
        now = time.monotonic()
        if cached is not None and now - cached[1] < _BLOCKLIST_TTL:
            return cached[0]
        try:
            titles = await self._arr[source].get_blocklist_source_titles()
        except Exception as exc:
            _LOGGER.warning("Failed to fetch %s blocklist: %s", source, exc)
            return cached[0] if cached else set()
        self._blocklist[source] = (titles, now)
        return titles

    async def _search_and_select(
        self,
        queue_name: str,
        source: str,
        media: MediaState,
        metadata: dict[str, Any],
        upgrade: UpgradeConfig,
        *,
        record: bool = True,
    ) -> SelectionResult | Exception:
        if record:
            await self._repo.record_search(queue_name, source, str(media.media_id))
        _LOGGER.info(
            "Searching %s/%d '%s' (score %d, %sp)",
            source,
            media.media_id,
            media.title,
            media.custom_format_score,
            media.resolution or "?",
        )
        try:
            releases = await self._arr[source].search_releases(media.media_id)
        except Exception as exc:
            _LOGGER.warning(
                "Release search for %s/%d failed: %s", source, media.media_id, exc
            )
            if record:
                await self._repo.update_last_search_outcome(queue_name, "error")
            return exc
        result = select_release(
            releases,
            media=media,
            upgrade=upgrade,
            blocklist=await self._get_blocklist(source),
            previously_grabbed=set(metadata.get("upgrade_grabbed_titles", [])),
        )
        if record:
            await self._repo.update_last_search_outcome(queue_name, result.outcome)
        _LOGGER.info(
            "%s/%d: %s - %s [%s]",
            source,
            media.media_id,
            result.outcome,
            result.reason,
            " → ".join(f"{name}:{n}" for name, n in result.steps),
        )
        return result

    async def _apply_result(
        self,
        queue_name: str,
        source: str,
        candidate: QueueItem,
        media: MediaState,
        result: SelectionResult | Exception,
        upgrade: UpgradeConfig,
    ) -> bool:
        """Persist the search outcome; returns whether to advance the cursor."""
        assert candidate.id is not None
        metadata = candidate.metadata
        now = _now_iso()

        if isinstance(result, Exception):
            if not self._config.dry_run:
                metadata["upgrade_no_release_at"] = now
                metadata["upgrade_last_outcome"] = "error"
                await self._repo.update_metadata(candidate.id, metadata)
            return True

        if result.outcome == Outcome.WOULD_GRAB and result.best is not None:
            return await self._grab(queue_name, source, candidate, media, result.best)

        if self._config.dry_run:
            return True
        metadata["upgrade_last_outcome"] = str(result.outcome)
        if result.outcome in TRANSIENT_OUTCOMES:
            metadata["upgrade_no_release_at"] = now
        else:
            metadata["upgrade_last_searched_at"] = now
            metadata["upgrade_no_match_at"] = now
        await self._repo.update_metadata(candidate.id, metadata)
        return True

    async def _grab(
        self,
        queue_name: str,
        source: str,
        candidate: QueueItem,
        media: MediaState,
        release: ReleaseResult,
    ) -> bool:
        assert candidate.id is not None
        if self._config.dry_run:
            _LOGGER.info(
                "[dry-run] would grab %s/%s '%s': %s (score %d → %d)",
                source,
                candidate.source_id,
                media.title,
                release.title,
                media.custom_format_score,
                release.custom_format_score,
            )
            return True

        metadata = candidate.metadata
        now = _now_iso()
        # Mark first: the SABnzbd job may appear before grab_release() returns.
        metadata.update(
            {
                "upgrade_grabbed": True,
                "upgrade_grabbed_at": now,
                "upgrade_grabbed_title": release.title,
                "upgrade_grabbed_score": release.custom_format_score,
                "upgrade_previous_score": media.custom_format_score,
                "upgrade_previous_quality": media.quality,
            }
        )
        await self._repo.update_metadata(candidate.id, metadata)
        try:
            await self._arr[source].grab_release(release)
        except Exception as exc:
            metadata.pop("upgrade_grabbed", None)
            metadata.pop("upgrade_grabbed_at", None)
            failures = int(metadata.get("upgrade_grab_failures", 0)) + 1
            metadata["upgrade_grab_failures"] = failures
            give_up = failures >= _MAX_GRAB_FAILURES
            if give_up:
                metadata["upgrade_grab_failures"] = 0
                metadata["upgrade_no_release_at"] = now
            await self._repo.update_metadata(candidate.id, metadata)
            _LOGGER.warning(
                "Grab of '%s' for %s/%s failed (%d/%d): %s",
                release.title,
                source,
                candidate.source_id,
                failures,
                _MAX_GRAB_FAILURES,
                exc,
            )
            return give_up

        titles = list(metadata.get("upgrade_grabbed_titles", []))
        titles.append(release.title)
        metadata["upgrade_grabbed_titles"] = titles[-_MAX_REMEMBERED_TITLES:]
        metadata["upgrade_grab_failures"] = 0
        metadata["upgrade_last_outcome"] = str(Outcome.WOULD_GRAB)
        await self._repo.update_metadata(candidate.id, metadata)
        await self._mark_covered_by_pack(source, candidate, release)
        _LOGGER.info(
            "Grabbed upgrade for %s/%s '%s': %s (score %d → %d, %s → %s) [queue %s]",
            source,
            candidate.source_id,
            media.title,
            release.title,
            media.custom_format_score,
            release.custom_format_score,
            media.quality or "?",
            release.quality or "?",
            queue_name,
        )
        return True

    async def _mark_covered_by_pack(
        self, source: str, candidate: QueueItem, release: ReleaseResult
    ) -> None:
        """Keep the other episodes of a grabbed pack from being searched.

        They get the pack title (never grabbed again for them) and a short
        cool-down; once the pack is imported the normal satisfied-check
        decides whether they still need an upgrade.
        """
        now = _now_iso()
        for media_id in release.mapped_media_ids:
            if str(media_id) == candidate.source_id:
                continue
            item = await self._repo.get_item(source, str(media_id))
            if item is None or item.id is None:
                continue
            titles = list(item.metadata.get("upgrade_grabbed_titles", []))
            titles.append(release.title)
            item.metadata["upgrade_grabbed_titles"] = titles[-_MAX_REMEMBERED_TITLES:]
            item.metadata["upgrade_no_release_at"] = now
            item.metadata["upgrade_covered_by"] = candidate.source_id
            await self._repo.update_metadata(item.id, item.metadata)

    # ------------------------------------------------------------------
    # debug-upgrades
    # ------------------------------------------------------------------

    async def dry_run(
        self, source_filter: str | None = None, source_id_filter: str | None = None
    ) -> list[DryRunCandidateResult]:
        """Evaluate the next candidate (or one given id) without side effects."""
        for queue in self._config.upgrade_queues:
            upgrade = queue.upgrade
            assert upgrade is not None
            for source in upgrade.sources:
                if source_filter is not None and source != source_filter:
                    continue
                if source_id_filter is not None:
                    return [
                        await self._evaluate(
                            queue.name, upgrade, source, source_id_filter
                        )
                    ]
                for candidate in await self._next_candidates(
                    queue.name, upgrade, source
                ):
                    if candidate.id is None or await self._repo.has_job_map_for_item(
                        candidate.id
                    ):
                        continue
                    try:
                        media = await self._arr[source].get_media_state(
                            int(candidate.source_id)
                        )
                    except Exception:
                        continue
                    if self._skip_reason(media, upgrade) is not None:
                        continue
                    return [
                        await self._evaluate(
                            queue.name, upgrade, source, candidate.source_id
                        )
                    ]
        return []

    async def _evaluate(
        self, queue_name: str, upgrade: UpgradeConfig, source: str, source_id: str
    ) -> DryRunCandidateResult:
        base = DryRunCandidateResult(
            queue=queue_name, source=source, source_id=source_id, outcome="", reason=""
        )
        try:
            media = await self._arr[source].get_media_state(int(source_id))
        except Exception as exc:
            base.outcome, base.reason = "error", f"Could not read media: {exc}"
            return base
        if media is None:
            base.outcome, base.reason = "error", "Item not found"
            return base
        base.media_title = media.title
        base.current_score = media.custom_format_score
        base.current_resolution = media.resolution
        if (skip := self._skip_reason(media, upgrade)) is not None:
            base.outcome = "skipped"
            base.reason = skip.removeprefix("upgrade_").removesuffix("_at")
            return base
        item = await self._repo.get_item(source, source_id)
        result = await self._search_and_select(
            queue_name,
            source,
            media,
            item.metadata if item else {},
            upgrade,
            record=False,
        )
        if isinstance(result, Exception):
            base.outcome, base.reason = "error", f"Release search failed: {result}"
            return base
        base.outcome = str(result.outcome)
        base.reason = result.reason
        base.releases_total = result.releases_total
        base.steps = result.steps
        base.best_release = result.best
        return base
