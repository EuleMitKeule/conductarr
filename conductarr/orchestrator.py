"""Central orchestrator: single SABnzbd poll loop with lazy Arr lookups.

Architecture
------------
The poll cycle runs every ``poll_interval`` seconds and executes in order:

1. Read SABnzbd queue.
2. For each nzo_id: resolve which virtual queue it belongs to via an
   in-memory cache (nzo_id → source/source_id/virtual_queue/tags).
   Cache misses trigger one combined Radarr + Sonarr queue fetch per cycle,
   not per item.  Unknown jobs are also cached to avoid repeated lookups.
3. Handle completed jobs (nzo_ids that disappeared from SABnzbd).
4. Reorder SABnzbd slots by virtual-queue priority; only call switch() when
   the order is actually wrong.
5. Enforce exactly one active download: resume slot[0] if paused, pause all
   others that are in a pausable state.
6. For each upgrade queue: if below max_active, search for the next eligible
   candidate and grab a release.  The cursor position is kept in memory so
   each cycle continues where it left off.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from conductarr.clients.radarr import RadarrClient
from conductarr.clients.release import ReleaseResult
from conductarr.clients.sabnzbd import Queue, QueueSlot, SABnzbdClient
from conductarr.clients.sonarr import SonarrClient
from conductarr.config import (
    AnyDatabaseConfig,
    ConductarrConfig,
    SQLiteDatabaseConfig,
    UpgradeConfig,
    VirtualQueueConfig,
)
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue.matchers import MATCHER_REGISTRY
from conductarr.queue.models import QueueItem, VirtualQueue

_LOGGER = logging.getLogger(__name__)

# SABnzbd slot statuses that are safe to pause (job has not yet started post-processing)
_PAUSABLE_STATUSES = frozenset({"Downloading", "Queued", "Checking"})


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def _media_satisfies_conditions(
    custom_formats: list[str],
    custom_format_score: int,
    conditions: list[Any],
) -> bool:
    """Return ``True`` if existing media already satisfies ALL *conditions*."""
    for cond in conditions:
        if cond.type == "custom_format":
            if cond.name not in custom_formats:
                return False
        elif cond.type == "custom_format_min_score":
            if custom_format_score < cond.value:
                return False
    return True


def _matches_all_conditions(release: ReleaseResult, conditions: list[Any]) -> bool:
    for cond in conditions:
        if cond.type == "custom_format":
            if cond.name not in release.custom_formats:
                return False
        elif cond.type == "custom_format_min_score":
            if release.custom_format_score < cond.value:
                return False
        else:
            _LOGGER.warning("Unknown accept_condition type: %s", cond.type)
    return True


def _filter_releases(
    releases: list[ReleaseResult], conditions: list[Any]
) -> list[ReleaseResult]:
    """Return only releases that satisfy ALL *conditions*."""
    return [r for r in releases if _matches_all_conditions(r, conditions)]


# ---------------------------------------------------------------------------
# Cache entry
# ---------------------------------------------------------------------------


@dataclass
class _NzoCacheEntry:
    """Resolved identity for a single SABnzbd nzo_id."""

    source: str | None  # "radarr" | "sonarr" | None
    source_id: str | None  # str(movie_id) or str(episode_id)
    virtual_queue: str | None  # assigned virtual queue name
    tags: list[str]
    unknown: bool = False  # True → queried Arr APIs and found nothing


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


class Orchestrator:
    """Drives the main poll loop.

    SABnzbd is polled every cycle.  Radarr and Sonarr are consulted lazily:
    only when a new unknown nzo_id appears, and only once per cycle regardless
    of how many new items appeared.
    """

    def __init__(
        self,
        config: ConductarrConfig,
        database_config: AnyDatabaseConfig | None = None,
    ) -> None:
        self._config = config
        self._sab = SABnzbdClient(
            url=config.sabnzbd.url,
            api_key=config.sabnzbd.api_key,
        )
        self._radarr = RadarrClient(
            url=config.radarr.url,
            api_key=config.radarr.api_key,
        )
        self._sonarr = SonarrClient(
            url=config.sonarr.url,
            api_key=config.sonarr.api_key,
        )

        db_cfg = database_config or SQLiteDatabaseConfig()
        self._db = Database(db_cfg)
        self._repo = QueueRepository(self._db)

        self._virtual_queues: list[VirtualQueue] = sorted(
            [
                VirtualQueue(
                    name=q.name,
                    priority=q.priority,
                    enabled=q.enabled,
                    fallback=q.fallback,
                    matchers=[{"type": m.type, "tags": m.tags} for m in q.matchers],
                )
                for q in config.queues
                if q.enabled
            ],
            key=lambda vq: vq.priority,
            reverse=True,
        )

        self._upgrade_queue_configs: list[VirtualQueueConfig] = [
            q for q in config.queues if q.upgrade is not None and q.upgrade.enabled
        ]

        # nzo_id → resolved cache entry (survives restart via DB warm-up)
        self._nzo_cache: dict[str, _NzoCacheEntry] = {}

        # Upgrade cursor: (queue_name, source) → last processed source_id
        # Allows continuing where we left off rather than always starting from item 1.
        self._candidate_cursor: dict[tuple[str, str], str | None] = {}

        # Round-robin source turn per upgrade queue
        self._source_turn: dict[str, int] = {}

        # Rate-limiting for upgrade searches: last time a search was dispatched
        # per (queue_name, source).  Prevents hammering indexers within search_interval.
        self._last_search_at: dict[tuple[str, str], datetime] = {}

        # Change-detection to avoid log spam on idle cycles
        self._prev_sab_slots: int | None = None
        self._prev_sab_paused: bool | None = None

        self._task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open DB and HTTP connections without starting the poll loop.

        Also warms the NZO cache from any existing sabnzbd_job_map rows so
        that items already present in SABnzbd at startup are handled correctly
        on the very first poll cycle without redundant Arr API calls.

        Suitable for integration tests that drive the engine via ``poll_once()``.
        """
        await self._db.connect()
        await self._sab.__aenter__()
        await self._warm_cache_from_db()

    async def start(self) -> None:
        """Connect (includes cache warm-up), seed upgrade queues, then start the loop."""
        await self.connect()
        await self._seed_upgrade_queues()
        self._task = asyncio.create_task(self._run(), name="orchestrator-poll-loop")
        _LOGGER.info(
            "Orchestrator started (poll_interval=%.1fs)", self._config.poll_interval
        )

    async def stop(self) -> None:
        """Cancel the poll loop and close connections."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        await self._sab.__aexit__(None, None, None)
        await self._db.disconnect()
        _LOGGER.info("Orchestrator stopped")

    async def poll_once(self) -> None:
        """Execute exactly one poll cycle (for integration tests)."""
        await self._poll_cycle()

    @property
    def repo(self) -> QueueRepository:
        """Expose the repository for test introspection."""
        return self._repo

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def _run(self) -> None:
        while True:
            try:
                await self._poll_cycle()
            except Exception:
                _LOGGER.exception(
                    "Unhandled error in poll cycle; retrying next interval"
                )
            await asyncio.sleep(self._config.poll_interval)

    async def _poll_cycle(self) -> None:
        """One complete orchestration cycle."""
        # Step 1: Read SABnzbd queue
        try:
            sab_queue = await self._sab.get_queue()
        except Exception:
            _LOGGER.exception("Failed to read SABnzbd queue; skipping cycle")
            return

        slots = sab_queue.slots
        slot_count = len(slots)
        if (
            slot_count != self._prev_sab_slots
            or sab_queue.paused != self._prev_sab_paused
        ):
            _LOGGER.info("SABnzbd: %d slot(s), paused=%s", slot_count, sab_queue.paused)
            self._prev_sab_slots = slot_count
            self._prev_sab_paused = sab_queue.paused

        # Step 2: Resolve each nzo_id to its virtual queue (lazy Arr lookup)
        nzo_to_entry = await self._resolve_all(slots)

        # Step 3: Handle completed jobs (confirmed via SABnzbd history API)
        await self._handle_completions()

        # Build nzo_id → virtual_queue for ordering (only mapped entries)
        nzo_to_vq: dict[str, str] = {
            nzo_id: entry.virtual_queue
            for nzo_id, entry in nzo_to_entry.items()
            if entry.virtual_queue is not None
        }

        # Step 4+5: Reorder by virtual-queue priority + enforce one active download
        await self._reorder_and_enforce(sab_queue, nzo_to_vq)

        # Step 6: Fill open upgrade slots
        await self._fill_upgrade_slots(nzo_to_vq)

    # ------------------------------------------------------------------
    # Step 2: NZO resolution
    # ------------------------------------------------------------------

    async def _resolve_all(self, slots: list[QueueSlot]) -> dict[str, _NzoCacheEntry]:
        """Return the cache entry for every current SABnzbd slot.

        Cache hits are O(1).  On a miss the Radarr and Sonarr queues are fetched
        once for the entire set of misses — not once per unknown nzo_id.
        """
        missing = [s.nzo_id for s in slots if s.nzo_id not in self._nzo_cache]

        if missing:
            # Fetch both Arr queues exactly once and build lookup dicts
            radarr_by_nzo: dict[str, Any] = {}
            sonarr_by_nzo: dict[str, Any] = {}

            try:
                radarr_items = await self._radarr.get_queue()
                radarr_by_nzo = {
                    item.download_id: item for item in radarr_items if item.download_id
                }
            except Exception:
                _LOGGER.warning("Failed to fetch Radarr queue for nzo resolution")

            try:
                sonarr_items = await self._sonarr.get_queue()
                sonarr_by_nzo = {
                    item.download_id: item for item in sonarr_items if item.download_id
                }
            except Exception:
                _LOGGER.warning("Failed to fetch Sonarr queue for nzo resolution")

            for nzo_id in missing:
                entry = await self._resolve_one(nzo_id, radarr_by_nzo, sonarr_by_nzo)
                self._nzo_cache[nzo_id] = entry

        return {
            s.nzo_id: self._nzo_cache[s.nzo_id]
            for s in slots
            if s.nzo_id in self._nzo_cache
        }

    async def _resolve_one(
        self,
        nzo_id: str,
        radarr_by_nzo: dict[str, Any],
        sonarr_by_nzo: dict[str, Any],
    ) -> _NzoCacheEntry:
        """Resolve a single nzo_id to a cache entry, persisting to DB as needed."""
        if nzo_id in radarr_by_nzo:
            radarr_item = radarr_by_nzo[nzo_id]
            source_id = str(radarr_item.movie_id)

            tags: list[str] = []
            try:
                tags = await self._radarr.get_movie_tags(radarr_item.movie_id)
            except Exception:
                _LOGGER.warning("Failed to fetch tags for Radarr movie %s", source_id)

            item = await self._repo.get_item("radarr", source_id)
            if item is None:
                item = QueueItem(source="radarr", source_id=source_id, tags=tags)
                item = await self._assign_queue(item)
            elif item.id is not None:
                # Ensure job map exists (may be missing after restart)
                if await self._repo.get_job_map(nzo_id) is None:
                    await self._repo.upsert_job_map(
                        nzo_id, item.id, item.virtual_queue or ""
                    )

            if item.id is not None and await self._repo.get_job_map(nzo_id) is None:
                await self._repo.upsert_job_map(
                    nzo_id, item.id, item.virtual_queue or ""
                )

            _LOGGER.info(
                "Resolved nzo_id=%s → Radarr '%s' (movie_id=%s) → queue=%s",
                nzo_id,
                radarr_item.title,
                source_id,
                item.virtual_queue,
            )
            return _NzoCacheEntry(
                source="radarr",
                source_id=source_id,
                virtual_queue=item.virtual_queue,
                tags=item.tags,
            )

        if nzo_id in sonarr_by_nzo:
            sonarr_item = sonarr_by_nzo[nzo_id]
            source_id = str(sonarr_item.episode_id)

            tags = []
            try:
                tags = await self._sonarr.get_episode_tags(sonarr_item.episode_id)
            except Exception:
                _LOGGER.warning("Failed to fetch tags for Sonarr episode %s", source_id)

            item = await self._repo.get_item("sonarr", source_id)
            if item is None:
                item = QueueItem(source="sonarr", source_id=source_id, tags=tags)
                item = await self._assign_queue(item)
            elif item.id is not None:
                if await self._repo.get_job_map(nzo_id) is None:
                    await self._repo.upsert_job_map(
                        nzo_id, item.id, item.virtual_queue or ""
                    )

            if item.id is not None and await self._repo.get_job_map(nzo_id) is None:
                await self._repo.upsert_job_map(
                    nzo_id, item.id, item.virtual_queue or ""
                )

            _LOGGER.info(
                "Resolved nzo_id=%s → Sonarr '%s' (episode_id=%s) → queue=%s",
                nzo_id,
                sonarr_item.title,
                source_id,
                item.virtual_queue,
            )
            return _NzoCacheEntry(
                source="sonarr",
                source_id=source_id,
                virtual_queue=item.virtual_queue,
                tags=item.tags,
            )

        _LOGGER.debug(
            "nzo_id=%s not found in Radarr or Sonarr queue (unknown job)", nzo_id
        )
        return _NzoCacheEntry(
            source=None, source_id=None, virtual_queue=None, tags=[], unknown=True
        )

    async def _assign_queue(self, item: QueueItem) -> QueueItem:
        """Assign *item* to the highest-priority matching virtual queue.

        Non-fallback queues are evaluated first.  If none matches, the item
        falls back to the highest-priority fallback queue.  Either way the item
        is persisted via the repository.
        """
        fallback_queue: str | None = None
        for vq in self._virtual_queues:
            if vq.fallback:
                if fallback_queue is None:
                    fallback_queue = vq.name
                continue
            for matcher_config in vq.matchers:
                matcher_type = matcher_config.get("type")
                matcher_cls = MATCHER_REGISTRY.get(matcher_type)  # type: ignore[arg-type]
                if matcher_cls is None:
                    _LOGGER.warning("Unknown matcher type: %s", matcher_type)
                    continue
                if matcher_cls().matches(item, matcher_config):
                    item.virtual_queue = vq.name
                    _LOGGER.info(
                        "Assigned %s/%s → queue '%s'",
                        item.source,
                        item.source_id,
                        vq.name,
                    )
                    return await self._repo.upsert_item(item)

        if fallback_queue is not None:
            item.virtual_queue = fallback_queue
            _LOGGER.info(
                "Assigned %s/%s → fallback queue '%s'",
                item.source,
                item.source_id,
                fallback_queue,
            )

        return await self._repo.upsert_item(item)

    # ------------------------------------------------------------------
    # Step 3: Handle completed jobs
    # ------------------------------------------------------------------

    async def _handle_completions(self) -> None:
        """Mark jobs as completed once they appear in SABnzbd history.

        Uses the SABnzbd history API as the authoritative completion signal
        rather than mere absence from the active queue.  This prevents false
        completions during post-processing transitions (Extracting / Moving)
        where a job temporarily disappears from the queue but has not actually
        finished yet.

        If the history call fails, completion detection is skipped entirely for
        this cycle — it is always safer to leave a job tracked than to
        prematurely mark it done.
        """
        try:
            history_slots = await self._sab.get_history()
        except Exception:
            _LOGGER.warning(
                "Failed to read SABnzbd history; skipping completion detection this cycle"
            )
            return

        completed_nzo_ids = {
            slot["nzo_id"] for slot in history_slots if slot.get("nzo_id")
        }

        all_maps = await self._repo.get_all_job_maps()
        for job_map in all_maps:
            nzo_id = job_map["nzo_id"]
            if nzo_id not in completed_nzo_ids:
                # Not in history yet — could be in-queue, post-processing, or
                # the job finished but history hasn't been fetched yet.
                # Do nothing; wait for the next cycle.
                continue

            queue_item_id = job_map["queue_item_id"]
            if queue_item_id is not None:
                await self._repo.update_status(queue_item_id, "completed")
                # Reset upgrade_grabbed so the item re-enters the candidate pool
                # after retry_after_days.
                queue_item = await self._repo.get_item_by_id(queue_item_id)
                if queue_item is not None:
                    queue_item.metadata.pop("upgrade_grabbed", None)
                    queue_item.metadata.pop("upgrade_grabbed_at", None)
                    queue_item.metadata["upgrade_last_searched_at"] = datetime.now(
                        UTC
                    ).isoformat()
                    await self._repo.update_metadata(queue_item_id, queue_item.metadata)
                _LOGGER.info(
                    "Job %s completed (confirmed in history) → queue item %d reset",
                    nzo_id,
                    queue_item_id,
                )

            await self._repo.delete_job_map(nzo_id)
            self._nzo_cache.pop(nzo_id, None)

    # ------------------------------------------------------------------
    # Steps 4+5: Reorder + enforce one active download
    # ------------------------------------------------------------------

    async def _reorder_and_enforce(
        self, sab_queue: Queue, nzo_to_vq: dict[str, str]
    ) -> None:
        """Reorder slots by virtual-queue priority; ensure exactly one is active.

        Only calls switch() when the current order differs from the desired one.
        Slots in Checking/Verifying/Extracting/Moving are never paused.
        """
        if not sab_queue.slots:
            return

        vq_rank: dict[str, int] = {
            vq.name: i for i, vq in enumerate(self._virtual_queues)
        }
        unknown_rank = len(self._virtual_queues)

        current_slots = sorted(sab_queue.slots, key=lambda s: s.index)
        desired_order = [
            s.nzo_id
            for s in sorted(
                current_slots,
                key=lambda s: (
                    vq_rank.get(nzo_to_vq.get(s.nzo_id, ""), unknown_rank),
                    s.index,
                ),
            )
        ]
        current_order = [s.nzo_id for s in current_slots]

        if desired_order != current_order:
            live_order = list(current_order)
            for i, target_id in enumerate(desired_order):
                if live_order[i] == target_id:
                    continue
                current_at_i = live_order[i]
                try:
                    await self._sab.switch(target_id, current_at_i)
                except Exception:
                    _LOGGER.exception(
                        "Failed to switch %s above %s; aborting reorder",
                        target_id,
                        current_at_i,
                    )
                    return
                j = live_order.index(target_id)
                live_order.pop(j)
                live_order.insert(i, target_id)
                _LOGGER.info(
                    "Reorder: moved %s to position %d (above %s)",
                    target_id,
                    i,
                    current_at_i,
                )

        # Enforce exactly one active download using the post-reorder desired_order.
        slot_by_id = {s.nzo_id: s for s in sab_queue.slots}
        top_id = desired_order[0]
        top_slot = slot_by_id.get(top_id)
        if top_slot is not None and top_slot.status == "Paused":
            _LOGGER.info("Resuming top slot %s", top_id)
            try:
                await self._sab.resume_job(top_id)
            except Exception:
                _LOGGER.exception("Failed to resume top slot %s", top_id)

        for nzo_id in desired_order[1:]:
            slot = slot_by_id.get(nzo_id)
            if slot is not None and slot.status in _PAUSABLE_STATUSES:
                _LOGGER.info("Pausing non-top slot %s (status=%s)", nzo_id, slot.status)
                try:
                    await self._sab.pause_job(nzo_id)
                except Exception:
                    _LOGGER.exception("Failed to pause slot %s", nzo_id)

    # ------------------------------------------------------------------
    # Step 6: Fill upgrade slots
    # ------------------------------------------------------------------

    async def _fill_upgrade_slots(self, nzo_to_vq: dict[str, str]) -> None:
        """Attempt to fill any open slots in every configured upgrade queue."""
        for qc in self._upgrade_queue_configs:
            if qc.upgrade is None or not qc.upgrade.enabled:
                continue
            try:
                await self._fill_one_upgrade_queue(qc, nzo_to_vq)
            except Exception:
                _LOGGER.exception("Error filling upgrade queue '%s'", qc.name)

    async def _fill_one_upgrade_queue(
        self,
        queue_config: VirtualQueueConfig,
        nzo_to_vq: dict[str, str],
    ) -> None:
        upgrade = queue_config.upgrade
        assert upgrade is not None  # guaranteed by caller
        queue_name = queue_config.name

        # Count active slots: currently in SABnzbd + grabbed-but-not-yet-in-SABnzbd
        sab_active = sum(1 for vq in nzo_to_vq.values() if vq == queue_name)
        pending_active = await self._repo.count_grabbed_not_in_jobmap(queue_name)
        active = sab_active + pending_active

        slots_to_fill = upgrade.max_active - active
        if slots_to_fill <= 0:
            return

        _LOGGER.debug(
            "Upgrade queue '%s': %d/%d active, filling %d slot(s)",
            queue_name,
            active,
            upgrade.max_active,
            slots_to_fill,
        )

        sources = upgrade.sources
        if not sources:
            return

        turn = self._source_turn.get(queue_name, 0)
        consecutive_misses = 0

        while slots_to_fill > 0 and consecutive_misses < len(sources):
            source = sources[turn % len(sources)]
            turn += 1

            # Respect search_interval: skip this source if searched too recently
            last_search = self._last_search_at.get((queue_name, source))
            if last_search is not None:
                elapsed = (datetime.now(UTC) - last_search).total_seconds()
                if elapsed < upgrade.search_interval:
                    consecutive_misses += 1
                    continue

            filled = await self._try_fill_from_source(upgrade, queue_name, source)
            if filled:
                slots_to_fill -= 1
                consecutive_misses = 0
            else:
                consecutive_misses += 1

        self._source_turn[queue_name] = turn

    async def _try_fill_from_source(
        self,
        upgrade: UpgradeConfig,
        queue_name: str,
        source: str,
    ) -> bool:
        """Iterate candidates for *source*, continuing from the last cursor position.

        Searches one candidate, grabs a matching release if found, and returns
        ``True`` (slot filled).  Returns ``False`` when the source is exhausted
        without a successful grab.
        """
        try:
            candidates = await self._repo.get_upgrade_candidates(
                queue_name, source, upgrade.retry_after_days
            )
        except Exception:
            _LOGGER.exception(
                "Failed to get upgrade candidates for '%s'/%s", queue_name, source
            )
            return False

        if not candidates:
            _LOGGER.debug(
                "No due candidates for upgrade queue '%s' source '%s'",
                queue_name,
                source,
            )
            return False

        # Apply cursor: start after the last processed source_id, wrap if exhausted
        cursor = self._candidate_cursor.get((queue_name, source))
        if cursor is not None:
            try:
                cursor_int = int(cursor)
                after_cursor = [c for c in candidates if int(c.source_id) > cursor_int]
                if not after_cursor:
                    # Wrapped around: reset cursor and use full list
                    self._candidate_cursor.pop((queue_name, source), None)
                    after_cursor = candidates
                candidates = after_cursor
            except ValueError, TypeError:
                pass  # non-integer source_ids: ignore cursor

        now_str = datetime.now(UTC).isoformat()

        for candidate in candidates:
            if candidate.metadata.get("upgrade_grabbed"):
                continue
            if candidate.id is None:
                continue

            # Check whether the item already satisfies accept_conditions
            try:
                already_satisfied = await self._check_satisfies_conditions(
                    source, candidate.source_id, upgrade.accept_conditions
                )
            except Exception:
                _LOGGER.warning(
                    "Failed to check conditions for %s/%s",
                    source,
                    candidate.source_id,
                    exc_info=True,
                )
                already_satisfied = False

            if already_satisfied:
                # Item is already upgraded — update metadata and skip
                candidate.metadata["upgrade_last_searched_at"] = now_str
                await self._repo.update_metadata(candidate.id, candidate.metadata)
                self._candidate_cursor[(queue_name, source)] = candidate.source_id
                continue

            # Skip if a download of this exact item is already active in SABnzbd
            if await self._is_already_downloading(
                source, candidate.source_id, candidate.id
            ):
                _LOGGER.debug(
                    "Upgrade skip: %s/%s already has an active SABnzbd download",
                    source,
                    candidate.source_id,
                )
                self._candidate_cursor[(queue_name, source)] = candidate.source_id
                continue

            # ------------------------------------------------------------------
            # Phase 1: Search the indexer for matching releases.
            #
            # _last_search_at is NOT set here yet — it is only set once a grab
            # is confirmed successful.  This ensures that a transient SABnzbd
            # failure during Phase 2 never blocks the next cycle from retrying
            # the search+grab immediately.
            # ------------------------------------------------------------------
            _LOGGER.debug("Searching releases for %s/%s", source, candidate.source_id)
            try:
                releases = await self._search_releases(source, int(candidate.source_id))
                matching = _filter_releases(releases, upgrade.accept_conditions)
            except Exception:
                _LOGGER.warning(
                    "Error searching releases for %s/%s",
                    source,
                    candidate.source_id,
                    exc_info=True,
                )
                # Advance cursor so the next cycle tries a different candidate.
                self._candidate_cursor[(queue_name, source)] = candidate.source_id
                return False

            candidate.metadata["upgrade_last_searched_at"] = now_str

            if matching:
                best = max(matching, key=lambda r: r.custom_format_score)

                # ------------------------------------------------------------------
                # Phase 2: Send the release to SABnzbd (via the Arr grab API).
                #
                # On ANY grab failure we do NOT advance the cursor, do NOT set
                # upgrade_grabbed, and do NOT set _last_search_at.  The next cycle
                # will retry the same candidate from scratch.  This prevents
                # double-grabs when SABnzbd was temporarily unreachable and also
                # stops grab failures from poisoning the search rate-limit.
                # ------------------------------------------------------------------
                try:
                    await self._grab_release(source, best)
                except Exception:
                    _LOGGER.warning(
                        "Grab failed for %s/%s ('%s') — will retry next cycle;"
                        " cursor NOT advanced",
                        source,
                        candidate.source_id,
                        best.title,
                        exc_info=True,
                    )
                    # Persist the search timestamp so we know it was checked.
                    await self._repo.update_metadata(candidate.id, candidate.metadata)
                    # Do NOT advance cursor.
                    # Do NOT set _last_search_at.
                    # Do NOT set upgrade_grabbed.
                    return False

                # Grab confirmed — record rate-limit timestamp and grabbed flag.
                self._last_search_at[(queue_name, source)] = datetime.now(UTC)
                candidate.metadata["upgrade_grabbed"] = True
                candidate.metadata["upgrade_grabbed_at"] = now_str
                await self._repo.update_metadata(candidate.id, candidate.metadata)
                self._candidate_cursor[(queue_name, source)] = candidate.source_id
                _LOGGER.info(
                    "Grabbed upgrade for %s/%s: '%s'",
                    source,
                    candidate.source_id,
                    best.title,
                )
                return True
            else:
                candidate.metadata["upgrade_no_match_at"] = now_str
                await self._repo.update_metadata(candidate.id, candidate.metadata)
                self._candidate_cursor[(queue_name, source)] = candidate.source_id

        return False  # source exhausted

    async def _is_already_downloading(
        self,
        source: str,
        source_id: str,
        item_id: int | None,
    ) -> bool:
        """Return True if *source*/*source_id* already has an active SABnzbd download.

        Checks both the in-memory NZO cache (fast, covers the current cycle)
        and the persistent job map in the DB (survives restarts).
        """
        # In-memory cache: O(n) scan but n is tiny (at most a handful of slots)
        for entry in self._nzo_cache.values():
            if entry.source == source and entry.source_id == source_id:
                return True

        # DB job map: check via item_id if we have one
        if item_id is not None:
            all_maps = await self._repo.get_all_job_maps()
            if any(m["queue_item_id"] == item_id for m in all_maps):
                return True

        return False

    async def _check_satisfies_conditions(
        self,
        source: str,
        source_id: str,
        conditions: list[Any],
    ) -> bool:
        """Return True if the current media already satisfies all accept_conditions."""
        if source == "radarr":
            movie = await self._radarr.get_movie(int(source_id))
            if movie is None:
                return False
            return _media_satisfies_conditions(
                movie.custom_formats, movie.custom_format_score, conditions
            )
        if source == "sonarr":
            episode = await self._sonarr.get_episode(int(source_id))
            if episode is None:
                return False
            return _media_satisfies_conditions(
                episode.custom_formats, episode.custom_format_score, conditions
            )
        return False

    async def _search_releases(self, source: str, item_id: int) -> list[ReleaseResult]:
        """Dispatch a release search to the appropriate Arr client."""
        if source == "radarr":
            return await self._radarr.search_releases(item_id)
        if source == "sonarr":
            return await self._sonarr.search_releases(item_id)
        _LOGGER.warning("Unknown upgrade source: %s", source)
        return []

    async def _grab_release(self, source: str, release: ReleaseResult) -> None:
        """Dispatch a release grab to the appropriate Arr client."""
        if source == "radarr":
            await self._radarr.grab_release(release)
        elif source == "sonarr":
            await self._sonarr.grab_release(release)
        else:
            raise ValueError(f"Unknown upgrade source: {source}")

    # ------------------------------------------------------------------
    # Upgrade queue seeding (runs once at startup)
    # ------------------------------------------------------------------

    async def _seed_upgrade_queues(self) -> None:
        """Pre-populate the DB with upgrade candidates from Radarr/Sonarr.

        Items that already satisfy accept_conditions are skipped.  Existing
        DB entries are never overwritten.
        """
        for qc in self._upgrade_queue_configs:
            if qc.upgrade and qc.upgrade.enabled:
                try:
                    await self._seed_one_queue(qc)
                except Exception:
                    _LOGGER.exception("Seed: unexpected error for queue '%s'", qc.name)

    async def _seed_one_queue(self, queue_config: VirtualQueueConfig) -> None:
        upgrade = queue_config.upgrade
        assert upgrade is not None
        for source in upgrade.sources:
            if source == "radarr":
                await self._seed_from_radarr(queue_config.name, upgrade)
            elif source == "sonarr":
                await self._seed_from_sonarr(queue_config.name, upgrade)

    async def _seed_from_radarr(self, queue_name: str, upgrade: UpgradeConfig) -> None:
        try:
            movies = await self._radarr.get_movies(has_file=True)
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Radarr movies")
            return
        seeded = 0
        for movie in movies:
            if _media_satisfies_conditions(
                movie.custom_formats,
                movie.custom_format_score,
                upgrade.accept_conditions,
            ):
                continue
            if await self._repo.get_item("radarr", str(movie.id)) is None:
                await self._repo.upsert_item(
                    QueueItem(
                        source="radarr",
                        source_id=str(movie.id),
                        virtual_queue=queue_name,
                        tags=[],
                    )
                )
                seeded += 1
        _LOGGER.info("Seed: %d new Radarr item(s) → '%s'", seeded, queue_name)

    async def _seed_from_sonarr(self, queue_name: str, upgrade: UpgradeConfig) -> None:
        try:
            all_series = await self._sonarr.get_series()
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Sonarr series")
            return
        seeded = 0
        for series in all_series:
            try:
                episodes = await self._sonarr.get_episodes(series.id, has_file=True)
            except Exception:
                _LOGGER.warning(
                    "Seed: failed to fetch episodes for Sonarr series %d", series.id
                )
                continue
            for episode in episodes:
                if _media_satisfies_conditions(
                    episode.custom_formats,
                    episode.custom_format_score,
                    upgrade.accept_conditions,
                ):
                    continue
                if await self._repo.get_item("sonarr", str(episode.id)) is None:
                    await self._repo.upsert_item(
                        QueueItem(
                            source="sonarr",
                            source_id=str(episode.id),
                            virtual_queue=queue_name,
                            tags=[],
                        )
                    )
                    seeded += 1
        _LOGGER.info("Seed: %d new Sonarr episode(s) → '%s'", seeded, queue_name)

    # ------------------------------------------------------------------
    # Cache warm-up from DB (restart resilience)
    # ------------------------------------------------------------------

    async def _warm_cache_from_db(self) -> None:
        """Pre-populate the NZO cache from the sabnzbd_job_map table.

        Ensures that nzo_ids that were already known before a restart do not
        trigger redundant Arr API calls on the first cycle.
        """
        all_maps = await self._repo.get_all_job_maps()
        for job_map in all_maps:
            nzo_id = job_map["nzo_id"]
            queue_item_id = job_map["queue_item_id"]
            virtual_queue = job_map.get("virtual_queue")
            if queue_item_id is not None:
                item = await self._repo.get_item_by_id(queue_item_id)
                if item is not None:
                    self._nzo_cache[nzo_id] = _NzoCacheEntry(
                        source=item.source,
                        source_id=item.source_id,
                        virtual_queue=virtual_queue or item.virtual_queue,
                        tags=item.tags,
                    )
        _LOGGER.debug(
            "Cache warm-up: loaded %d entry(ies) from DB", len(self._nzo_cache)
        )
