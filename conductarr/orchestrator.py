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

Virtual queue vs. resolved queue
---------------------------------
``virtual_queue`` persisted in the DB is used solely for upgrade candidate
tracking (cursor, slot counting, metadata).  For SABnzbd slot ordering the
orchestrator derives a *resolved queue* at the time of resolution.  For
conductarr-initiated downloads these are the same value.  For externally
initiated downloads the resolved queue is computed fresh from current file
state without overwriting the DB record, so upgrade tracking is unaffected.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from conductarr.clients.radarr import RadarrClient, RadarrQueueItem
from conductarr.clients.release import ReleaseResult
from conductarr.clients.sabnzbd import Queue, QueueSlot, SABnzbdClient
from conductarr.clients.sonarr import SonarrClient, SonarrQueueItem
from conductarr.config import (
    ConductarrConfig,
    Config,
    UpgradeConfig,
    VirtualQueueConfig,
)
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue.matchers import MATCHER_REGISTRY
from conductarr.queue.models import AssignContext, QueueItem, VirtualQueue

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


@dataclass
class DryRunCandidateResult:
    """Result of a single candidate's dry-run upgrade check."""

    queue: str
    source: str
    source_id: str
    outcome: str
    """One of: would_grab | no_releases | all_filtered_conditions |
    all_filtered_transient | no_score_improvement | error"""
    reason: str
    media_title: str = ""
    releases_total: int = 0
    releases_after_conditions: int = 0
    releases_after_availability: int = 0
    releases_after_blocklist: int = 0
    releases_after_score: int = 0
    current_score: int | None = None
    best_release: ReleaseResult | None = None


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
        config: Config,
        conductarr_config: ConductarrConfig,
    ) -> None:
        self._config = config
        self._conductarr_config = conductarr_config
        self._database = Database(config.database)
        self._queue_repository = QueueRepository(self._database)
        self._sab_client = SABnzbdClient(
            url=conductarr_config.sabnzbd.url,
            api_key=conductarr_config.sabnzbd.api_key,
        )
        self._radarr_client = RadarrClient(
            url=conductarr_config.radarr.url,
            api_key=conductarr_config.radarr.api_key,
        )
        self._sonarr_client = SonarrClient(
            url=conductarr_config.sonarr.url,
            api_key=conductarr_config.sonarr.api_key,
        )
        self._virtual_queues: list[VirtualQueue] = sorted(
            [
                VirtualQueue(
                    name=q.name,
                    priority=q.priority,
                    enabled=q.enabled,
                    fallback=q.fallback,
                    matchers=[{"type": m.type, "tags": m.tags} for m in q.matchers],
                )
                for q in conductarr_config.queues
                if q.enabled
            ],
            key=lambda vq: vq.priority,
            reverse=True,
        )
        self._upgrade_queue_configs: list[VirtualQueueConfig] = [
            q
            for q in conductarr_config.queues
            if q.upgrade is not None and q.upgrade.enabled
        ]

        # Fast lookup of upgrade config keyed by queue name (used in _find_queue_for_item)
        self._upgrade_config_by_queue: dict[str, UpgradeConfig] = {
            q.name: q.upgrade for q in conductarr_config.queues if q.upgrade is not None
        }

        # nzo_id → resolved cache entry (survives restart via DB warm-up)
        self._nzo_cache_entries: dict[str, _NzoCacheEntry] = {}

        # Upgrade cursor: (queue_name, source) → last processed source_id
        # Allows continuing where we left off rather than always starting from item 1.
        self._candidate_cursor: dict[tuple[str, str], str | None] = {}

        # Round-robin source turn per upgrade queue
        self._source_turn: dict[str, int] = {}

        # Rate-limiting for upgrade searches: last time a search was dispatched
        # per (queue_name, source).  Prevents hammering indexers within search_interval.
        self._last_search_at: dict[tuple[str, str], datetime] = {}

        # Per-cycle blocklist cache: source → set of blocked identifiers.
        # Cleared at the start of each _fill_upgrade_slots call so each poll
        # cycle fetches a fresh list at most once per source.
        self._blocklist_cache: dict[str, set[str]] = {}

        # Change-detection to avoid log spam on idle cycles
        self._prev_sab_is_paused: bool | None = None
        self._prev_sab_queue_slots: list[QueueSlot] = []

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
        await self._database.connect()
        await self._sab_client.__aenter__()
        await self._warm_cache_from_db()

    async def start(self) -> None:
        """Connect (includes cache warm-up), seed upgrade queues, then start the loop."""
        await self.connect()
        await self._seed_upgrade_queues()
        self._task = asyncio.create_task(self._run(), name="orchestrator-poll-loop")

        _LOGGER.info(
            "Orchestrator started (poll_interval=%.1fs)",
            self._conductarr_config.poll_interval,
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
        await self._sab_client.__aexit__(None, None, None)
        await self._database.disconnect()
        _LOGGER.info("Orchestrator stopped")

    async def poll_once(self) -> None:
        """Execute exactly one poll cycle (for integration tests)."""
        await self._poll_cycle()

    @property
    def repo(self) -> QueueRepository:
        """Expose the repository for test introspection."""
        return self._queue_repository

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
            await asyncio.sleep(self._conductarr_config.poll_interval)

    async def _poll_cycle(self) -> None:
        """One complete orchestration cycle."""
        # Step 1: Read SABnzbd queue
        try:
            sab_queue = await self._sab_client.get_queue()
        except Exception:
            _LOGGER.exception("Failed to read SABnzbd queue; skipping cycle")
            return

        sab_queue_slots = sab_queue.slots
        sab_queue_slot_count = len(sab_queue_slots)
        sab_queue_is_paused = sab_queue.paused

        if {
            sab_queue_slot.nzo_id
            for sab_queue_slot in sab_queue_slots
            if sab_queue_slot.nzo_id
        } != {
            sab_queue_slot.nzo_id
            for sab_queue_slot in self._prev_sab_queue_slots
            if sab_queue_slot.nzo_id
        } or sab_queue_is_paused != self._prev_sab_is_paused:
            _LOGGER.info(
                "SABnzbd: %d slot(s), paused=%s",
                sab_queue_slot_count,
                sab_queue_is_paused,
            )
            self._prev_sab_queue_slots = sab_queue_slots
            self._prev_sab_is_paused = sab_queue_is_paused

        # Step 2: Resolve each nzo_id to its virtual queue (lazy Arr lookup)
        nzo_to_entry = await self._resolve_all(sab_queue_slots)

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

    async def _resolve_all(
        self, sab_queue_slots: list[QueueSlot]
    ) -> dict[str, _NzoCacheEntry]:
        """Return the cache entry for every current SABnzbd slot.

        Cache hits are O(1).  On a miss the Radarr and Sonarr queues are fetched
        once for the entire set of misses — not once per unknown nzo_id.
        """
        missing_nzo_ids = [
            sab_queue_slot.nzo_id
            for sab_queue_slot in sab_queue_slots
            if sab_queue_slot.nzo_id not in self._nzo_cache_entries
        ]

        radarr_queue_items_by_nzo_id: dict[str, RadarrQueueItem] = {}
        sonarr_queue_items_by_nzo_id: dict[str, SonarrQueueItem] = {}

        if missing_nzo_ids:
            try:
                radarr_queue_items = await self._radarr_client.get_queue()
                radarr_queue_items_by_nzo_id = {
                    radarr_queue_item.download_id: radarr_queue_item
                    for radarr_queue_item in radarr_queue_items
                    if radarr_queue_item.download_id
                }
            except Exception:
                _LOGGER.warning("Failed to fetch Radarr queue for nzo resolution")

            try:
                sonarr_queue_items = await self._sonarr_client.get_queue()
                sonarr_queue_items_by_nzo_id = {
                    sonarr_queue_item.download_id: sonarr_queue_item
                    for sonarr_queue_item in sonarr_queue_items
                    if sonarr_queue_item.download_id
                }
            except Exception:
                _LOGGER.warning("Failed to fetch Sonarr queue for nzo resolution")

            self._nzo_cache_entries.update(
                **{
                    nzo_id: await self._resolve_one(
                        nzo_id,
                        radarr_queue_items_by_nzo_id,
                        sonarr_queue_items_by_nzo_id,
                    )
                    for nzo_id in missing_nzo_ids
                }
            )

        return {
            sab_queue_slot.nzo_id: self._nzo_cache_entries[sab_queue_slot.nzo_id]
            for sab_queue_slot in sab_queue_slots
            if sab_queue_slot.nzo_id in self._nzo_cache_entries
        }

    async def _resolve_one(
        self,
        nzo_id: str,
        radarr_queue_items_by_nzo_id: dict[str, RadarrQueueItem],
        sonarr_queue_items_by_nzo_id: dict[str, SonarrQueueItem],
    ) -> _NzoCacheEntry:
        """Resolve a single nzo_id to a cache entry, persisting to DB as needed."""
        if nzo_id in radarr_queue_items_by_nzo_id:
            return await self._resolve_one_radarr(nzo_id, radarr_queue_items_by_nzo_id)

        if nzo_id in sonarr_queue_items_by_nzo_id:
            return await self._resolve_one_sonarr(nzo_id, sonarr_queue_items_by_nzo_id)

        _LOGGER.debug(
            "nzo_id=%s not found in Radarr or Sonarr queue (unknown job)", nzo_id
        )

        return _NzoCacheEntry(
            source=None, source_id=None, virtual_queue=None, tags=[], unknown=True
        )

    async def _resolve_one_radarr(
        self, nzo_id: str, radarr_queue_items_by_nzo_id: dict[str, RadarrQueueItem]
    ) -> _NzoCacheEntry:
        radarr_queue_item = radarr_queue_items_by_nzo_id[nzo_id]
        radarr_movie_id = str(radarr_queue_item.movie_id)

        tags: list[str] = []
        try:
            tags = await self._radarr_client.get_movie_tags(radarr_queue_item.movie_id)
        except Exception:
            _LOGGER.warning("Failed to fetch tags for Radarr movie %s", radarr_movie_id)

        context = await self._build_assign_context("radarr", radarr_queue_item.movie_id)

        sab_queue_item = await self._queue_repository.get_item(
            "radarr", radarr_movie_id
        )
        if sab_queue_item is None:
            sab_queue_item = QueueItem(
                source="radarr", source_id=radarr_movie_id, tags=tags
            )
            sab_queue_item = await self._assign_queue(sab_queue_item, context)
            resolved_queue: str | None = sab_queue_item.virtual_queue
        else:
            is_conductarr_grab = sab_queue_item.metadata.get("upgrade_grabbed") is True
            if is_conductarr_grab:
                # Conductarr-initiated: honour the persisted virtual_queue
                resolved_queue = sab_queue_item.virtual_queue
            else:
                # External grab: re-derive queue for ordering; do NOT persist
                sab_queue_item.tags = tags
                resolved_queue, _ = self._find_queue_for_item(sab_queue_item, context)

            if (
                sab_queue_item.id is not None
                and await self._queue_repository.get_job_map(nzo_id) is None
            ):
                await self._queue_repository.upsert_job_map(
                    nzo_id, sab_queue_item.id, resolved_queue or ""
                )

            _LOGGER.info(
                "Resolved nzo_id=%s → Radarr '%s' (movie_id=%s) → queue=%s%s",
                nzo_id,
                radarr_queue_item.title,
                radarr_movie_id,
                resolved_queue,
                "" if is_conductarr_grab else " (external grab)",
            )
            return _NzoCacheEntry(
                source="radarr",
                source_id=radarr_movie_id,
                virtual_queue=resolved_queue,
                tags=tags,
            )

        if (
            sab_queue_item.id is not None
            and await self._queue_repository.get_job_map(nzo_id) is None
        ):
            await self._queue_repository.upsert_job_map(
                nzo_id, sab_queue_item.id, sab_queue_item.virtual_queue or ""
            )

        _LOGGER.info(
            "Resolved nzo_id=%s → Radarr '%s' (movie_id=%s) → queue=%s",
            nzo_id,
            radarr_queue_item.title,
            radarr_movie_id,
            sab_queue_item.virtual_queue,
        )
        return _NzoCacheEntry(
            source="radarr",
            source_id=radarr_movie_id,
            virtual_queue=sab_queue_item.virtual_queue,
            tags=sab_queue_item.tags,
        )

    async def _resolve_one_sonarr(
        self, nzo_id: str, sonarr_queue_items_by_nzo_id: dict[str, SonarrQueueItem]
    ) -> _NzoCacheEntry:

        sonarr_queue_item = sonarr_queue_items_by_nzo_id[nzo_id]
        sonarr_episode_id = str(sonarr_queue_item.episode_id)

        tags: list[str] = []
        try:
            tags = await self._sonarr_client.get_episode_tags(
                sonarr_queue_item.episode_id
            )
        except Exception:
            _LOGGER.warning(
                "Failed to fetch tags for Sonarr episode %s", sonarr_episode_id
            )

        context = await self._build_assign_context(
            "sonarr", sonarr_queue_item.episode_id
        )

        sab_queue_item = await self._queue_repository.get_item(
            "sonarr", sonarr_episode_id
        )
        if sab_queue_item is None:
            sab_queue_item = QueueItem(
                source="sonarr", source_id=sonarr_episode_id, tags=tags
            )
            sab_queue_item = await self._assign_queue(sab_queue_item, context)
            resolved_queue = sab_queue_item.virtual_queue
        else:
            is_conductarr_grab = sab_queue_item.metadata.get("upgrade_grabbed") is True
            if is_conductarr_grab:
                # Conductarr-initiated: honour the persisted virtual_queue
                resolved_queue = sab_queue_item.virtual_queue
            else:
                # External grab: re-derive queue for ordering; do NOT persist
                sab_queue_item.tags = tags
                resolved_queue, _ = self._find_queue_for_item(sab_queue_item, context)

            if (
                sab_queue_item.id is not None
                and await self._queue_repository.get_job_map(nzo_id) is None
            ):
                await self._queue_repository.upsert_job_map(
                    nzo_id, sab_queue_item.id, resolved_queue or ""
                )

            _LOGGER.info(
                "Resolved nzo_id=%s → Sonarr '%s' (episode_id=%s) → queue=%s%s",
                nzo_id,
                sonarr_queue_item.title,
                sonarr_episode_id,
                resolved_queue,
                "" if is_conductarr_grab else " (external grab)",
            )
            return _NzoCacheEntry(
                source="sonarr",
                source_id=sonarr_episode_id,
                virtual_queue=resolved_queue,
                tags=tags,
            )

        if (
            sab_queue_item.id is not None
            and await self._queue_repository.get_job_map(nzo_id) is None
        ):
            await self._queue_repository.upsert_job_map(
                nzo_id, sab_queue_item.id, sab_queue_item.virtual_queue or ""
            )

        _LOGGER.info(
            "Resolved nzo_id=%s → Sonarr '%s' (episode_id=%s) → queue=%s",
            nzo_id,
            sonarr_queue_item.title,
            sonarr_episode_id,
            sab_queue_item.virtual_queue,
        )
        return _NzoCacheEntry(
            source="sonarr",
            source_id=sonarr_episode_id,
            virtual_queue=sab_queue_item.virtual_queue,
            tags=sab_queue_item.tags,
        )

    def _find_queue_for_item(
        self, item: QueueItem, context: AssignContext
    ) -> tuple[str | None, bool]:
        """Return ``(queue_name, is_fallback)`` without any side-effects.

        Applies the upgrade auto-condition: upgrade-configured queues are
        skipped when the item has no existing file (cannot be an upgrade) or
        when the existing file already satisfies all accept_conditions (no
        upgrade needed).  Both checks are implicit — no extra matcher required.
        """
        fallback_queue: str | None = None
        for vq in self._virtual_queues:
            if vq.fallback:
                if fallback_queue is None:
                    fallback_queue = vq.name
                continue
            # Auto-skip upgrade queues based on current file state
            upgrade_cfg = self._upgrade_config_by_queue.get(vq.name)
            if upgrade_cfg is not None:
                if not context.has_file:
                    # No existing file → cannot be an upgrade candidate
                    continue
                if _media_satisfies_conditions(
                    context.existing_custom_formats,
                    context.existing_custom_format_score,
                    upgrade_cfg.accept_conditions,
                ):
                    # Already satisfies conditions → no upgrade needed
                    continue
            for matcher_config in vq.matchers:
                matcher_type = matcher_config.get("type")
                matcher_cls = MATCHER_REGISTRY.get(matcher_type)  # type: ignore[arg-type]
                if matcher_cls is None:
                    _LOGGER.warning("Unknown matcher type: %s", matcher_type)
                    continue
                if matcher_cls().matches(item, matcher_config, context):
                    return vq.name, False
        return fallback_queue, True

    async def _assign_queue(self, item: QueueItem, context: AssignContext) -> QueueItem:
        """Assign *item* to the highest-priority matching virtual queue.

        Non-fallback queues are evaluated first.  If none matches, the item
        falls back to the highest-priority fallback queue.  Either way the item
        is persisted via the repository.
        """
        queue_name, is_fallback = self._find_queue_for_item(item, context)
        if queue_name is not None:
            item.virtual_queue = queue_name
            if is_fallback:
                _LOGGER.info(
                    "Assigned %s/%s → fallback queue '%s'",
                    item.source,
                    item.source_id,
                    queue_name,
                )
            else:
                _LOGGER.info(
                    "Assigned %s/%s → queue '%s'",
                    item.source,
                    item.source_id,
                    queue_name,
                )
        return await self._queue_repository.upsert_item(item)

    async def _build_assign_context(
        self, source: str, source_item_id: int
    ) -> AssignContext:
        """Fetch current file state and return an :class:`AssignContext`.

        Falls back to ``has_file=True, existing_custom_formats=[]`` on any
        error — this is the safe direction: all matchers can still run and
        no upgrade queue is silently skipped.
        """
        _safe_default = AssignContext(
            has_file=True,
            existing_custom_formats=[],
            existing_custom_format_score=0,
        )
        try:
            if source == "radarr":
                movie = await self._radarr_client.get_movie(source_item_id)
                if movie is None:
                    return _safe_default
                if not movie.has_file:
                    return AssignContext(
                        has_file=False,
                        existing_custom_formats=[],
                        existing_custom_format_score=0,
                    )
                movie_file = await self._radarr_client.get_movie_file(source_item_id)
                if movie_file is not None:
                    return AssignContext(
                        has_file=True,
                        existing_custom_formats=[
                            cf.get("name", "")
                            for cf in movie_file.get("customFormats", [])
                        ],
                        existing_custom_format_score=movie_file.get(
                            "customFormatScore", movie.custom_format_score
                        ),
                    )
                return AssignContext(
                    has_file=True,
                    existing_custom_formats=movie.custom_formats,
                    existing_custom_format_score=movie.custom_format_score,
                )
            if source == "sonarr":
                episode = await self._sonarr_client.get_episode(source_item_id)
                if episode is None:
                    return _safe_default
                if not episode.has_file:
                    return AssignContext(
                        has_file=False,
                        existing_custom_formats=[],
                        existing_custom_format_score=0,
                    )
                if episode.episode_file_id is not None:
                    ep_file = await self._sonarr_client.get_episode_file(
                        episode.episode_file_id
                    )
                    if ep_file is not None:
                        return AssignContext(
                            has_file=True,
                            existing_custom_formats=[
                                cf.get("name", "")
                                for cf in ep_file.get("customFormats", [])
                            ],
                            existing_custom_format_score=ep_file.get(
                                "customFormatScore", episode.custom_format_score
                            ),
                        )
                return AssignContext(
                    has_file=True,
                    existing_custom_formats=episode.custom_formats,
                    existing_custom_format_score=episode.custom_format_score,
                )
        except Exception:
            _LOGGER.debug(
                "Failed to build assign context for %s/%d",
                source,
                source_item_id,
                exc_info=True,
            )
        return _safe_default

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
            history_slots = await self._sab_client.get_history()
        except Exception:
            _LOGGER.warning(
                "Failed to read SABnzbd history; skipping completion detection this cycle"
            )
            return

        completed_nzo_ids = {
            slot["nzo_id"] for slot in history_slots if slot.get("nzo_id")
        }

        all_maps = await self._queue_repository.get_all_job_maps()
        for job_map in all_maps:
            nzo_id = job_map["nzo_id"]
            if nzo_id not in completed_nzo_ids:
                # Not in history yet — could be in-queue, post-processing, or
                # the job finished but history hasn't been fetched yet.
                # Do nothing; wait for the next cycle.
                continue

            queue_item_id = job_map["queue_item_id"]
            if queue_item_id is not None:
                await self._queue_repository.update_status(queue_item_id, "completed")
                # Reset upgrade_grabbed so the item re-enters the candidate pool
                # after retry_after_days.
                queue_item = await self._queue_repository.get_item_by_id(queue_item_id)
                if queue_item is not None:
                    queue_item.metadata.pop("upgrade_grabbed", None)
                    queue_item.metadata.pop("upgrade_grabbed_at", None)
                    queue_item.metadata["upgrade_last_searched_at"] = datetime.now(
                        UTC
                    ).isoformat()
                    await self._queue_repository.update_metadata(
                        queue_item_id, queue_item.metadata
                    )
                _LOGGER.info(
                    "Job %s completed (confirmed in history) → queue item %d reset",
                    nzo_id,
                    queue_item_id,
                )

            await self._queue_repository.delete_job_map(nzo_id)
            self._nzo_cache_entries.pop(nzo_id, None)

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
                    await self._sab_client.switch(target_id, current_at_i)
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
                await self._sab_client.resume_job(top_id)
            except Exception:
                _LOGGER.exception("Failed to resume top slot %s", top_id)

        for nzo_id in desired_order[1:]:
            slot = slot_by_id.get(nzo_id)
            if slot is not None and slot.status in _PAUSABLE_STATUSES:
                _LOGGER.info("Pausing non-top slot %s (status=%s)", nzo_id, slot.status)
                try:
                    await self._sab_client.pause_job(nzo_id)
                except Exception:
                    _LOGGER.exception("Failed to pause slot %s", nzo_id)

    # ------------------------------------------------------------------
    # Step 6: Fill upgrade slots
    # ------------------------------------------------------------------

    async def _fill_upgrade_slots(self, nzo_to_vq: dict[str, str]) -> None:
        """Attempt to fill any open slots in every configured upgrade queue."""
        self._blocklist_cache.clear()  # reset per-cycle; populated lazily in _get_blocklist
        for qc in self._upgrade_queue_configs:
            if qc.upgrade is None or not qc.upgrade.enabled:
                continue
            try:
                await self._fill_one_upgrade_queue(qc, nzo_to_vq)
            except Exception:
                _LOGGER.exception("Error filling upgrade queue '%s'", qc.name)

    async def _clear_stale_grabs(self, queue_config: VirtualQueueConfig) -> None:
        """Clear upgrade_grabbed for items whose SABnzbd job never appeared.

        When a grabbed download immediately fails (before the next poll cycle
        reads the active queue), the job never enters our nzo_id cache and no
        job_map row is created.  The ``upgrade_grabbed`` flag would then persist
        indefinitely, consuming an upgrade slot forever.

        An item is considered stale once it has been in the grabbed-but-unmapped
        state for longer than ``search_interval`` seconds.
        """
        upgrade = queue_config.upgrade
        if upgrade is None:
            return
        cutoff = datetime.now(UTC) - timedelta(seconds=upgrade.search_interval)
        stale = await self._queue_repository.get_grabbed_items_without_jobmap(
            queue_config.name, cutoff
        )
        for item in stale:
            item.metadata.pop("upgrade_grabbed", None)
            item.metadata.pop("upgrade_grabbed_at", None)
            if item.id is not None:
                await self._queue_repository.update_metadata(item.id, item.metadata)
                _LOGGER.info(
                    "Cleared stale upgrade grab for %s/%s"
                    " (job never appeared in SABnzbd active queue)",
                    item.source,
                    item.source_id,
                )

    async def _fill_one_upgrade_queue(
        self,
        queue_config: VirtualQueueConfig,
        nzo_to_vq: dict[str, str],
    ) -> None:
        upgrade = queue_config.upgrade
        assert upgrade is not None  # guaranteed by caller
        queue_name = queue_config.name

        # Clear grabs whose SABnzbd job never appeared (immediate failures)
        await self._clear_stale_grabs(queue_config)

        # Count active slots: currently in SABnzbd + grabbed-but-not-yet-in-SABnzbd
        sab_active = sum(1 for vq in nzo_to_vq.values() if vq == queue_name)
        pending_active = await self._queue_repository.count_grabbed_not_in_jobmap(
            queue_name
        )
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
            candidates = await self._queue_repository.get_upgrade_candidates(
                queue_name,
                source,
                upgrade.retry_after_days,
                upgrade.no_release_retry_days,
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
                await self._queue_repository.update_metadata(
                    candidate.id, candidate.metadata
                )
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

            # Filter out releases that the indexer/Arr has marked as not downloadable
            # (transient — never set no_match_at or advance cursor for this alone)
            available = [r for r in matching if r.download_allowed]

            # Filter out blocklisted releases (per-cycle cached; never blocks upgrade loop)
            # (transient — the blocklist can change, so do NOT set no_match_at)
            blocklist_titles = await self._get_blocklist(source)
            for r in available:
                if r.title in blocklist_titles:
                    _LOGGER.debug(
                        "Skipping blocklisted release for %s/%s: '%s'",
                        source,
                        candidate.source_id,
                        r.title,
                    )
            available = [r for r in available if r.title not in blocklist_titles]

            # If accept_conditions matched releases but ALL were filtered by transient
            # constraints (download_allowed=False or blocklisted), record the attempt
            # and apply the no_release_retry_days cooldown before trying again.
            if matching and not available:
                _LOGGER.debug(
                    "All matching releases for %s/%s were filtered by transient "
                    "constraints (blocklist/not downloadable) — cooling down for %d day(s)",
                    source,
                    candidate.source_id,
                    upgrade.no_release_retry_days,
                )
                candidate.metadata["upgrade_no_release_at"] = now_str
                await self._queue_repository.update_metadata(
                    candidate.id, candidate.metadata
                )
                self._candidate_cursor[(queue_name, source)] = candidate.source_id
                continue

            candidate.metadata["upgrade_last_searched_at"] = now_str

            # Filter out releases that do not improve on the current score.
            # This runs AFTER the transient check so that blocklisted /
            # non-downloadable releases still trigger the correct cooldown.
            if available:
                current_score = await self._get_media_score(source, candidate.source_id)
                if current_score is not None:
                    score_filtered = [
                        r for r in available if r.custom_format_score > current_score
                    ]
                    if available and not score_filtered:
                        _LOGGER.debug(
                            "All releases for %s/%s filtered: none exceed current "
                            "score %d",
                            source,
                            candidate.source_id,
                            current_score,
                        )
                    available = score_filtered

            if available:
                best = max(available, key=lambda r: r.custom_format_score)

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
                    await self._queue_repository.update_metadata(
                        candidate.id, candidate.metadata
                    )
                    # Do NOT advance cursor.
                    # Do NOT set _last_search_at.
                    # Do NOT set upgrade_grabbed.
                    return False

                # Grab confirmed — record rate-limit timestamp and grabbed flag.
                self._last_search_at[(queue_name, source)] = datetime.now(UTC)
                candidate.metadata["upgrade_grabbed"] = True
                candidate.metadata["upgrade_grabbed_at"] = now_str
                await self._queue_repository.update_metadata(
                    candidate.id, candidate.metadata
                )
                self._candidate_cursor[(queue_name, source)] = candidate.source_id
                _LOGGER.info(
                    "Grabbed upgrade for %s/%s: '%s'",
                    source,
                    candidate.source_id,
                    best.title,
                )
                return True
            else:
                # No releases from the indexer, or releases exist but none satisfy
                # accept_conditions — mark as no-match and advance cursor.
                candidate.metadata["upgrade_no_match_at"] = now_str
                await self._queue_repository.update_metadata(
                    candidate.id, candidate.metadata
                )
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
        for entry in self._nzo_cache_entries.values():
            if entry.source == source and entry.source_id == source_id:
                return True

        # DB job map: check via item_id if we have one
        if item_id is not None:
            all_maps = await self._queue_repository.get_all_job_maps()
            if any(m["queue_item_id"] == item_id for m in all_maps):
                return True

        return False

    async def _get_blocklist(self, source: str) -> set[str]:
        """Return cached blocklist identifiers for *source*.

        Results are populated once per cycle and cached in ``_blocklist_cache``
        which is cleared at the start of each ``_fill_upgrade_slots`` call.
        Any fetch failure returns an empty set with a warning — blocklist
        errors must never block the upgrade loop.
        """
        if source in self._blocklist_cache:
            return self._blocklist_cache[source]
        try:
            if source == "radarr":
                titles = await self._radarr_client.get_blocklist_source_titles()
            elif source == "sonarr":
                titles = await self._sonarr_client.get_blocklist_source_titles()
            else:
                titles = set()
        except Exception:
            _LOGGER.warning(
                "Failed to fetch blocklist for '%s' — proceeding without filter",
                source,
                exc_info=True,
            )
            titles = set()
        _LOGGER.debug(
            "Blocklist cache for '%s': %d source title(s)", source, len(titles)
        )
        self._blocklist_cache[source] = titles
        return titles

    async def _check_satisfies_conditions(
        self,
        source: str,
        source_id: str,
        conditions: list[Any],
    ) -> bool:
        """Return True if the current media already satisfies all accept_conditions.

        Fetches file-level custom-format data (more reliable than the bulk
        /movie or /episode endpoint) and falls back to media-level values when
        the file record is unavailable.  Debug-logs the actual score and
        format list used so this class of bug is immediately visible in logs.
        """
        if source == "radarr":
            movie = await self._radarr_client.get_movie(int(source_id))
            if movie is None:
                return False
            # Seed from movie-level score (reliable); update with file-level
            # customFormats which is more accurate for named-format conditions.
            custom_formats: list[str] = movie.custom_formats
            custom_format_score: int = movie.custom_format_score
            try:
                movie_file = await self._radarr_client.get_movie_file(int(source_id))
                if movie_file is not None:
                    custom_formats = [
                        cf.get("name", "") for cf in movie_file.get("customFormats", [])
                    ]
                    custom_format_score = movie_file.get(
                        "customFormatScore", custom_format_score
                    )
            except Exception:
                _LOGGER.debug(
                    "radarr/%s: could not fetch movieFile, using movie-level data",
                    source_id,
                    exc_info=True,
                )
            _LOGGER.debug(
                "radarr/%s: satisfies_conditions check — score=%d formats=%s",
                source_id,
                custom_format_score,
                custom_formats,
            )
            return _media_satisfies_conditions(
                custom_formats, custom_format_score, conditions
            )

        if source == "sonarr":
            episode = await self._sonarr_client.get_episode(int(source_id))
            if episode is None:
                return False
            custom_formats = episode.custom_formats
            custom_format_score = episode.custom_format_score
            if episode.episode_file_id is not None:
                try:
                    ep_file = await self._sonarr_client.get_episode_file(
                        episode.episode_file_id
                    )
                    if ep_file is not None:
                        custom_formats = [
                            cf.get("name", "")
                            for cf in ep_file.get("customFormats", [])
                        ]
                        custom_format_score = ep_file.get(
                            "customFormatScore", custom_format_score
                        )
                except Exception:
                    _LOGGER.debug(
                        "sonarr/%s: could not fetch episodeFile, using episode-level data",
                        source_id,
                        exc_info=True,
                    )
            _LOGGER.debug(
                "sonarr/%s: satisfies_conditions check — score=%d formats=%s",
                source_id,
                custom_format_score,
                custom_formats,
            )
            return _media_satisfies_conditions(
                custom_formats, custom_format_score, conditions
            )

        return False

    async def _get_media_score(self, source: str, source_id: str) -> int | None:
        """Return the current custom-format score for *source*/*source_id*.

        Uses file-level data when available (same logic as
        ``_check_satisfies_conditions``).  Returns ``None`` on any failure so
        callers can skip the score filter rather than blocking upgrades.
        """
        try:
            if source == "radarr":
                movie = await self._radarr_client.get_movie(int(source_id))
                if movie is None:
                    return None
                score: int = movie.custom_format_score
                try:
                    movie_file = await self._radarr_client.get_movie_file(
                        int(source_id)
                    )
                    if movie_file is not None:
                        score = movie_file.get("customFormatScore", score)
                except Exception:
                    pass
                return score

            if source == "sonarr":
                episode = await self._sonarr_client.get_episode(int(source_id))
                if episode is None:
                    return None
                score = episode.custom_format_score
                if episode.episode_file_id is not None:
                    try:
                        ep_file = await self._sonarr_client.get_episode_file(
                            episode.episode_file_id
                        )
                        if ep_file is not None:
                            score = ep_file.get("customFormatScore", score)
                    except Exception:
                        pass
                return score
        except Exception:
            _LOGGER.warning(
                "Failed to get media score for %s/%s",
                source,
                source_id,
                exc_info=True,
            )
        return None

    async def _search_releases(self, source: str, item_id: int) -> list[ReleaseResult]:
        """Dispatch a release search to the appropriate Arr client."""
        if source == "radarr":
            return await self._radarr_client.search_releases(item_id)
        if source == "sonarr":
            return await self._sonarr_client.search_releases(item_id)
        _LOGGER.warning("Unknown upgrade source: %s", source)
        return []

    async def _grab_release(self, source: str, release: ReleaseResult) -> None:
        """Dispatch a release grab to the appropriate Arr client."""
        if source == "radarr":
            await self._radarr_client.grab_release(release)
        elif source == "sonarr":
            await self._sonarr_client.grab_release(release)
        else:
            raise ValueError(f"Unknown upgrade source: {source}")

    # ------------------------------------------------------------------
    # Upgrade queue seeding (runs once at startup)
    # ------------------------------------------------------------------

    async def _seed_upgrade_queues(self) -> None:
        """Pre-populate the DB with upgrade candidates from Radarr/Sonarr."""
        for upgrade_queue_config in self._upgrade_queue_configs:
            if not upgrade_queue_config.enabled:
                continue

            try:
                await self._seed_queue(upgrade_queue_config)
            except Exception:
                _LOGGER.exception(
                    "Seed: unexpected error for queue '%s'", upgrade_queue_config.name
                )

    async def _seed_queue(self, queue_config: VirtualQueueConfig) -> None:
        if not ((upgrade := queue_config.upgrade) and upgrade.enabled):
            return

        for source in upgrade.sources:
            if source == "radarr":
                await self._seed_from_radarr(queue_config.name)
            elif source == "sonarr":
                await self._seed_from_sonarr(queue_config.name)

    async def _seed_from_radarr(self, queue_name: str) -> None:
        try:
            movies = await self._radarr_client.get_movies(has_file=True)
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Radarr movies")
            return

        seeded = 0
        for movie in movies:
            if await self._queue_repository.get_item("radarr", str(movie.id)):
                continue

            await self._queue_repository.upsert_item(
                QueueItem(
                    source="radarr",
                    source_id=str(movie.id),
                    virtual_queue=queue_name,
                    tags=[],
                )
            )
            seeded += 1

        _LOGGER.info("Seed: %d new Radarr item(s) → '%s'", seeded, queue_name)

    async def _seed_from_sonarr(self, queue_name: str) -> None:
        try:
            seriess = await self._sonarr_client.get_series()
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Sonarr series")
            return

        seeded = 0
        for series in seriess:
            try:
                episodes = await self._sonarr_client.get_episodes(
                    series.id, has_file=True
                )
            except Exception:
                _LOGGER.warning(
                    "Seed: failed to fetch episodes for Sonarr series %d", series.id
                )
                continue

            for episode in episodes:
                if await self._queue_repository.get_item("sonarr", str(episode.id)):
                    continue

                await self._queue_repository.upsert_item(
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
        job_maps = await self._queue_repository.get_all_job_maps()

        for job_map in job_maps:
            nzo_id = job_map["nzo_id"]
            queue_item_id = job_map["queue_item_id"]
            virtual_queue = job_map.get("virtual_queue")

            if queue_item_id is None:
                continue

            item = await self._queue_repository.get_item_by_id(queue_item_id)

            if item is None:
                continue

            self._nzo_cache_entries[nzo_id] = _NzoCacheEntry(
                source=item.source,
                source_id=item.source_id,
                virtual_queue=virtual_queue or item.virtual_queue,
                tags=item.tags,
            )

        _LOGGER.debug(
            "Cache warm-up: loaded %d entry(ies) from DB", len(self._nzo_cache_entries)
        )

    # ------------------------------------------------------------------
    # Dry-run (debug) helpers
    # ------------------------------------------------------------------

    async def dry_run_upgrades(
        self,
        source_filter: str | None = None,
        source_id_filter: str | None = None,
    ) -> list[DryRunCandidateResult]:
        """Simulate upgrade selection for the next eligible candidate.

        Iterates candidates exactly as the live scheduler would, silently
        skipping items that already satisfy their accept_conditions or are
        already downloading.  Stops after the first candidate that actually
        reaches the indexer-search step and returns a single result with full
        filter details — without writing to the database or sending a grab.

        Args:
            source_filter: If set, restrict to this source ("radarr"/"sonarr").
            source_id_filter: If set, test this specific media ID directly,
                bypassing cursor and already-satisfied checks.

        Returns:
            A list with at most one :class:`DryRunCandidateResult`.
        """
        self._blocklist_cache.clear()

        for qc in self._upgrade_queue_configs:
            upgrade = qc.upgrade
            if upgrade is None or not upgrade.enabled:
                continue

            sources = upgrade.sources
            if source_filter is not None:
                sources = [s for s in sources if s == source_filter]

            for source in sources:
                try:
                    candidates = await self._queue_repository.get_upgrade_candidates(
                        qc.name,
                        source,
                        upgrade.retry_after_days,
                        upgrade.no_release_retry_days,
                    )
                except Exception as exc:
                    return [
                        DryRunCandidateResult(
                            queue=qc.name,
                            source=source,
                            source_id="?",
                            outcome="error",
                            reason=f"Failed to fetch candidates: {exc}",
                        )
                    ]

                if source_id_filter is not None:
                    # Direct lookup: skip already-satisfied check, test as-is.
                    candidates = [
                        c for c in candidates if c.source_id == source_id_filter
                    ]
                else:
                    # Respect cursor so we test the same candidate the
                    # scheduler would pick next.
                    cursor = self._candidate_cursor.get((qc.name, source))
                    if cursor is not None:
                        try:
                            cursor_int = int(cursor)
                            after = [
                                c for c in candidates if int(c.source_id) > cursor_int
                            ]
                            if after:
                                candidates = after
                        except ValueError, TypeError:
                            pass

                for candidate in candidates:
                    if candidate.metadata.get("upgrade_grabbed"):
                        continue
                    if candidate.id is None:
                        continue

                    if source_id_filter is None:
                        # Skip items that already satisfy conditions — same
                        # as the live scheduler, no output for these.
                        try:
                            if await self._check_satisfies_conditions(
                                source,
                                candidate.source_id,
                                upgrade.accept_conditions,
                            ):
                                continue
                        except Exception:
                            pass  # treat as not satisfied

                        # Skip items already being downloaded.
                        try:
                            all_maps = await self._queue_repository.get_all_job_maps()
                            if any(
                                m["queue_item_id"] == candidate.id for m in all_maps
                            ):
                                continue
                        except Exception:
                            pass

                    # Found the next real candidate — search and report, then stop.
                    result = await self._dry_run_one_candidate(
                        qc.name, source, candidate.source_id, candidate.id, upgrade
                    )
                    return [result]

        return []

    async def _dry_run_one_candidate(
        self,
        queue_name: str,
        source: str,
        source_id: str,
        candidate_id: int,
        upgrade: "UpgradeConfig",
    ) -> DryRunCandidateResult:
        """Run all release-filter steps for one candidate without side effects.

        Assumes the caller has already verified the candidate is not
        already_satisfied and not already_downloading.
        """
        # Fetch media title for display.
        media_title = ""
        try:
            if source == "radarr":
                movie = await self._radarr_client.get_movie(int(source_id))
                if movie is not None:
                    media_title = movie.title
            elif source == "sonarr":
                ep = await self._sonarr_client.get_episode(int(source_id))
                if ep is not None:
                    media_title = (
                        f"S{ep.season_number:02d}E{ep.episode_number:02d} – {ep.title}"
                    )
        except Exception:
            pass

        def _r(**kwargs: Any) -> DryRunCandidateResult:
            return DryRunCandidateResult(
                queue=queue_name,
                source=source,
                source_id=source_id,
                media_title=media_title,
                **kwargs,
            )

        # 1. Search indexer.
        try:
            releases = await self._search_releases(source, int(source_id))
        except Exception as exc:
            return _r(outcome="error", reason=f"Release search failed: {exc}")

        releases_total = len(releases)

        # 2. accept_conditions filter.
        matching = _filter_releases(releases, upgrade.accept_conditions)
        releases_after_conditions = len(matching)

        if not matching:
            if releases_total == 0:
                return _r(
                    outcome="no_releases",
                    reason="Indexer returned no releases",
                    releases_total=0,
                    releases_after_conditions=0,
                )
            return _r(
                outcome="all_filtered_conditions",
                reason=(
                    f"{releases_total} release(s) found but none match accept_conditions"
                ),
                releases_total=releases_total,
                releases_after_conditions=0,
            )

        # 3. download_allowed filter.
        available = [r for r in matching if r.download_allowed]
        releases_after_availability = len(available)

        # 4. Blocklist filter.
        blocklist = await self._get_blocklist(source)
        available = [r for r in available if r.title not in blocklist]
        releases_after_blocklist = len(available)

        if not available:
            not_dl = releases_after_conditions - releases_after_availability
            blocklisted = releases_after_availability - releases_after_blocklist
            parts = []
            if not_dl:
                parts.append(f"{not_dl} not downloadable")
            if blocklisted:
                parts.append(f"{blocklisted} blocklisted")
            return _r(
                outcome="all_filtered_transient",
                reason=(
                    f"{releases_after_conditions} condition-matching release(s) "
                    f"all filtered: {', '.join(parts)}"
                ),
                releases_total=releases_total,
                releases_after_conditions=releases_after_conditions,
                releases_after_availability=releases_after_availability,
                releases_after_blocklist=releases_after_blocklist,
            )

        # 5. Score improvement filter.
        current_score = await self._get_media_score(source, source_id)
        releases_after_score = len(available)
        if current_score is not None:
            score_filtered = [
                r for r in available if r.custom_format_score > current_score
            ]
            releases_after_score = len(score_filtered)
            available = score_filtered

        if not available:
            return _r(
                outcome="no_score_improvement",
                reason=(
                    f"No release exceeds current score {current_score} "
                    f"({releases_after_blocklist} candidate(s) checked)"
                ),
                releases_total=releases_total,
                releases_after_conditions=releases_after_conditions,
                releases_after_availability=releases_after_availability,
                releases_after_blocklist=releases_after_blocklist,
                releases_after_score=0,
                current_score=current_score,
            )

        best = max(available, key=lambda r: r.custom_format_score)
        return _r(
            outcome="would_grab",
            reason=f"Would grab '{best.title}' (score={best.custom_format_score})",
            releases_total=releases_total,
            releases_after_conditions=releases_after_conditions,
            releases_after_availability=releases_after_availability,
            releases_after_blocklist=releases_after_blocklist,
            releases_after_score=releases_after_score,
            current_score=current_score,
            best_release=best,
        )
