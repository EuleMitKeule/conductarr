"""SABnzbd queue management: job resolution, completion tracking, ordering.

One :meth:`QueueManager.run_cycle` call

1. reads the SABnzbd queue,
2. maps every ``nzo_id`` to a virtual queue (lazy Radarr/Sonarr lookups),
3. finalises jobs that left the queue (via SABnzbd history),
4. reorders the queue by virtual-queue rank and - if enabled - keeps exactly
   one job downloading by pausing the others.

Safety rules
------------
* The only SABnzbd write calls are ``switch``, ``pause`` and ``resume`` on
  individual jobs.  Nothing is ever deleted.
* A job that is paused but that conductarr did not pause itself (i.e. the
  user paused it) is never resumed.
* On shutdown every job conductarr paused is resumed again.
* In ``dry_run`` mode no write call is made at all.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime

from conductarr.clients.arr import ArrClient
from conductarr.clients.release import ArrQueueItem
from conductarr.clients.sabnzbd import Queue, QueueSlot, SABnzbdClient
from conductarr.config import ConductarrConfig, UpgradeConfig
from conductarr.db.repository import QueueRepository
from conductarr.queue.matchers import MATCHER_REGISTRY
from conductarr.queue.models import AssignContext, QueueItem, VirtualQueue
from conductarr.upgrade.selection import media_satisfies_conditions

_LOGGER = logging.getLogger(__name__)

# SABnzbd slot statuses that are safe to pause (download phase only).
PAUSABLE_STATUSES = frozenset(
    {"Downloading", "Queued", "Checking", "Fetching", "Propagating", "Grabbing"}
)
# Final SABnzbd history statuses; anything else is still post-processing.
_FINAL_HISTORY_STATUSES = frozenset({"Completed", "Failed"})

# Re-query the Arr queues for jobs that could not be mapped after this long.
UNKNOWN_RETRY_SECONDS = 60.0
# A mapped job that vanished from queue *and* history is forgotten after this.
MISSING_JOB_GRACE_SECONDS = 1800.0


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class NzoEntry:
    """Resolved identity of a single SABnzbd job."""

    source: str | None
    source_id: str | None
    virtual_queue: str | None
    tags: list[str]
    unknown: bool = False
    conductarr_grab: bool = False
    covered_ids: list[str] = field(default_factory=list)
    """All movie/episode ids this job downloads (several for season packs)."""
    resolved_at: float = field(default_factory=time.monotonic)


@dataclass
class QueueSnapshot:
    """What the last queue cycle observed; consumed by the upgrade scheduler."""

    slots: list[QueueSlot]
    entries: dict[str, NzoEntry]
    paused: bool
    diskspace_free_gb: float | None
    user_paused: set[str] = field(default_factory=set)
    """Jobs paused by the user (not by conductarr)."""
    taken_at: float = field(default_factory=time.monotonic)


class QueueManager:
    def __init__(
        self,
        config: ConductarrConfig,
        repository: QueueRepository,
        sab_client: SABnzbdClient,
        arr_clients: dict[str, ArrClient],
    ) -> None:
        self._config = config
        self._repo = repository
        self._sab = sab_client
        self._arr = arr_clients
        self._virtual_queues: list[VirtualQueue] = sorted(
            (
                VirtualQueue(
                    name=q.name,
                    priority=q.priority,
                    enabled=q.enabled,
                    fallback=q.fallback,
                    matchers=[m.model_dump() for m in q.matchers],
                )
                for q in config.queues
                if q.enabled
            ),
            key=lambda vq: vq.priority,
            reverse=True,
        )
        self._upgrade_config_by_queue: dict[str, UpgradeConfig] = {
            q.name: q.upgrade for q in config.upgrade_queues if q.upgrade is not None
        }
        self._rank = {vq.name: i for i, vq in enumerate(self._virtual_queues)}
        self.entries: dict[str, NzoEntry] = {}
        self._paused_by_us: set[str] = set()
        self._missing_since: dict[str, float] = {}
        self._prev_summary: tuple[frozenset[str], bool] | None = None

    @property
    def upgrade_queue_names(self) -> set[str]:
        return set(self._upgrade_config_by_queue)

    # ------------------------------------------------------------------
    # Startup / shutdown
    # ------------------------------------------------------------------

    async def load_state(self) -> None:
        """Warm the nzo cache and the paused-by-us set from the database."""
        for job_map in await self._repo.get_all_job_maps():
            if job_map["queue_item_id"] is None:
                continue
            item = await self._repo.get_item_by_id(job_map["queue_item_id"])
            if item is None:
                continue
            self.entries[job_map["nzo_id"]] = NzoEntry(
                source=item.source,
                source_id=item.source_id,
                virtual_queue=job_map.get("virtual_queue") or item.virtual_queue,
                tags=item.tags,
                conductarr_grab=item.metadata.get("upgrade_grabbed") is True,
            )
        self._paused_by_us = await self._repo.get_paused_jobs()
        _LOGGER.debug(
            "Queue state loaded: %d mapped job(s), %d paused by conductarr",
            len(self.entries),
            len(self._paused_by_us),
        )

    async def release_paused_jobs(self) -> None:
        """Resume every job conductarr paused (called on shutdown)."""
        if self._config.dry_run:
            return
        for nzo_id in sorted(self._paused_by_us):
            try:
                await self._sab.resume_job(nzo_id)
                await self._repo.remove_paused_job(nzo_id)
                _LOGGER.info("Shutdown: resumed %s (paused by conductarr)", nzo_id)
            except Exception:
                _LOGGER.warning("Shutdown: could not resume %s", nzo_id, exc_info=True)
        self._paused_by_us.clear()

    # ------------------------------------------------------------------
    # Cycle
    # ------------------------------------------------------------------

    async def run_cycle(self) -> QueueSnapshot | None:
        try:
            sab_queue = await self._sab.get_queue()
        except Exception as exc:
            _LOGGER.warning("Failed to read SABnzbd queue (%s); skipping cycle", exc)
            return None

        self._log_queue_change(sab_queue)
        current = await self._resolve_all(sab_queue.slots)
        await self._handle_departed_jobs(sab_queue)
        await self._reorder_and_enforce(sab_queue, current)
        await self._forget_departed_state(sab_queue)
        return QueueSnapshot(
            slots=sab_queue.slots,
            entries=current,
            paused=sab_queue.paused,
            diskspace_free_gb=sab_queue.diskspace_free_gb,
            user_paused={
                s.nzo_id
                for s in sab_queue.slots
                if s.status == "Paused" and s.nzo_id not in self._paused_by_us
            },
        )

    def _log_queue_change(self, sab_queue: Queue) -> None:
        summary = (frozenset(s.nzo_id for s in sab_queue.slots), sab_queue.paused)
        if summary != self._prev_summary:
            _LOGGER.info(
                "SABnzbd: %d slot(s), paused=%s", len(sab_queue.slots), sab_queue.paused
            )
            self._prev_summary = summary

    # ------------------------------------------------------------------
    # Resolution
    # ------------------------------------------------------------------

    async def _resolve_all(self, slots: list[QueueSlot]) -> dict[str, NzoEntry]:
        now = time.monotonic()
        todo = [
            s.nzo_id
            for s in slots
            if s.nzo_id not in self.entries
            or (
                self.entries[s.nzo_id].unknown
                and now - self.entries[s.nzo_id].resolved_at >= UNKNOWN_RETRY_SECONDS
            )
        ]
        if todo:
            # One download (e.g. a season pack) can have several queue records.
            arr_queues: dict[str, dict[str, list[ArrQueueItem]]] = {}
            for source, client in self._arr.items():
                try:
                    by_download: dict[str, list[ArrQueueItem]] = {}
                    for item in await client.get_queue():
                        by_download.setdefault(item.download_id, []).append(item)
                    arr_queues[source] = by_download
                except Exception as exc:
                    _LOGGER.warning("Failed to fetch %s queue: %s", source, exc)
            for nzo_id in todo:
                self.entries[nzo_id] = await self._resolve_one(nzo_id, arr_queues)
        return {s.nzo_id: self.entries[s.nzo_id] for s in slots}

    async def _resolve_one(
        self, nzo_id: str, arr_queues: dict[str, dict[str, list[ArrQueueItem]]]
    ) -> NzoEntry:
        for source, items in arr_queues.items():
            if nzo_id in items:
                try:
                    return await self._resolve_arr_job(nzo_id, source, items[nzo_id])
                except Exception:
                    _LOGGER.warning(
                        "Failed to resolve %s via %s", nzo_id, source, exc_info=True
                    )
        previous = self.entries.get(nzo_id)
        if previous is None:
            _LOGGER.info("nzo_id=%s is not in any Arr queue (unknown job)", nzo_id)
        return NzoEntry(
            source=None, source_id=None, virtual_queue=None, tags=[], unknown=True
        )

    async def _resolve_arr_job(
        self, nzo_id: str, source: str, queue_items: list[ArrQueueItem]
    ) -> NzoEntry:
        client = self._arr[source]
        covered_ids = [str(q.media_id) for q in queue_items]
        # Prefer the record conductarr grabbed (for packs: the searched episode).
        queue_item, item = queue_items[0], None
        for candidate in queue_items:
            db_item = await self._repo.get_item(source, str(candidate.media_id))
            if db_item is not None and db_item.metadata.get("upgrade_grabbed") is True:
                queue_item, item = candidate, db_item
                break
        source_id = str(queue_item.media_id)
        if item is None:
            item = await self._repo.get_item(source, source_id)
        conductarr_grab = (
            item is not None and item.metadata.get("upgrade_grabbed") is True
        )

        if item is not None and conductarr_grab:
            # Our own grab: it belongs to the upgrade queue that grabbed it.
            virtual_queue = item.virtual_queue
            tags = item.tags
        else:
            try:
                tags = await client.get_media_tags(queue_item.media_id)
            except Exception:
                _LOGGER.warning("Failed to fetch tags for %s/%s", source, source_id)
                tags = []
            context = await self._build_assign_context(client, queue_item.media_id)
            probe = QueueItem(source=source, source_id=source_id, tags=tags)
            virtual_queue, _ = self.find_queue_for_item(probe, context)
            if item is None:
                probe.virtual_queue = virtual_queue
                item = await self._repo.upsert_item(probe)

        if item.id is not None:
            await self._repo.upsert_job_map(nzo_id, item.id, virtual_queue or "")
        _LOGGER.info(
            "Resolved nzo_id=%s → %s '%s' (id=%s%s) → queue=%s%s",
            nzo_id,
            source,
            queue_item.title,
            source_id,
            f", +{len(covered_ids) - 1} more" if len(covered_ids) > 1 else "",
            virtual_queue,
            " (conductarr upgrade)" if conductarr_grab else "",
        )
        return NzoEntry(
            source=source,
            source_id=source_id,
            virtual_queue=virtual_queue,
            tags=tags,
            conductarr_grab=conductarr_grab,
            covered_ids=covered_ids,
        )

    async def _build_assign_context(
        self, client: ArrClient, media_id: int
    ) -> AssignContext:
        """Current file state; on errors assume "has a file" (safe direction)."""
        try:
            state = await client.get_media_state(media_id)
        except Exception:
            _LOGGER.debug("Could not fetch media state for %d", media_id, exc_info=True)
            state = None
        if state is None:
            return AssignContext(has_file=True, existing_custom_formats=[])
        return AssignContext(
            has_file=state.has_file,
            existing_custom_formats=state.custom_formats,
            existing_custom_format_score=state.custom_format_score,
        )

    def find_queue_for_item(
        self, item: QueueItem, context: AssignContext
    ) -> tuple[str | None, bool]:
        """Return ``(queue_name, is_fallback)`` without side effects.

        Upgrade queues are skipped for items without a file (cannot be an
        upgrade) and for items whose file already satisfies the queue's
        accept_conditions.
        """
        fallback_queue: str | None = None
        for vq in self._virtual_queues:
            if vq.fallback:
                if fallback_queue is None:
                    fallback_queue = vq.name
                continue
            upgrade_cfg = self._upgrade_config_by_queue.get(vq.name)
            if upgrade_cfg is not None and (
                not context.has_file
                or media_satisfies_conditions(
                    context.existing_custom_formats,
                    context.existing_custom_format_score,
                    upgrade_cfg.accept_conditions,
                )
            ):
                continue
            for matcher_config in vq.matchers:
                matcher_cls = MATCHER_REGISTRY.get(str(matcher_config.get("type")))
                if matcher_cls is None:
                    _LOGGER.warning("Unknown matcher type: %s", matcher_config)
                    continue
                if matcher_cls().matches(item, matcher_config, context):
                    return vq.name, False
        return fallback_queue, True

    # ------------------------------------------------------------------
    # Departed jobs (completion / failure / removal)
    # ------------------------------------------------------------------

    async def _handle_departed_jobs(self, sab_queue: Queue) -> None:
        in_queue = {s.nzo_id for s in sab_queue.slots}
        departed = [
            m
            for m in await self._repo.get_all_job_maps()
            if m["nzo_id"] not in in_queue
        ]
        for nzo_id in list(self._missing_since):
            if nzo_id in in_queue:
                del self._missing_since[nzo_id]
        if not departed:
            return

        nzo_ids = [m["nzo_id"] for m in departed]
        try:
            history = await self._sab.get_history(
                nzo_ids=nzo_ids, limit=max(100, 4 * len(nzo_ids))
            )
        except Exception as exc:
            # Leaving a job tracked is always safer than finalising it early.
            _LOGGER.warning("Failed to read SABnzbd history (%s)", exc)
            return
        status_by_id = {str(h.get("nzo_id")): str(h.get("status", "")) for h in history}

        now = time.monotonic()
        for job_map in departed:
            nzo_id = job_map["nzo_id"]
            status = status_by_id.get(nzo_id)
            if status in _FINAL_HISTORY_STATUSES:
                await self._finalise(job_map, status == "Completed", status)
            elif status is not None:
                self._missing_since.pop(nzo_id, None)  # still post-processing
            else:
                since = self._missing_since.setdefault(nzo_id, now)
                if now - since >= MISSING_JOB_GRACE_SECONDS:
                    await self._finalise(job_map, False, "removed from SABnzbd")

    async def _finalise(
        self, job_map: dict[str, object], success: bool, status: str
    ) -> None:
        nzo_id = str(job_map["nzo_id"])
        queue_item_id = job_map["queue_item_id"]
        if isinstance(queue_item_id, int):
            item = await self._repo.get_item_by_id(queue_item_id)
            if item is not None and item.id is not None:
                was_grab = item.metadata.pop("upgrade_grabbed", None) is True
                item.metadata.pop("upgrade_grabbed_at", None)
                now = _now_iso()
                if success:
                    item.metadata["upgrade_last_searched_at"] = now
                    if was_grab:
                        item.metadata["upgrade_completed_at"] = now
                else:
                    # The failed release title stays in upgrade_grabbed_titles,
                    # so a retry never picks the same release again.
                    item.metadata["upgrade_no_release_at"] = now
                    item.metadata["upgrade_last_failure"] = status
                await self._repo.update_metadata(item.id, item.metadata)
                await self._repo.update_status(
                    item.id, "completed" if success else "failed"
                )
                log = _LOGGER.info if success else _LOGGER.warning
                log(
                    "Job %s finished (%s) → %s/%s%s",
                    nzo_id,
                    status,
                    item.source,
                    item.source_id,
                    " [conductarr upgrade]" if was_grab else "",
                )
        await self._repo.delete_job_map(nzo_id)
        self.entries.pop(nzo_id, None)
        self._missing_since.pop(nzo_id, None)

    async def _forget_departed_state(self, sab_queue: Queue) -> None:
        in_queue = {s.nzo_id for s in sab_queue.slots}
        mapped = {m["nzo_id"] for m in await self._repo.get_all_job_maps()}
        for nzo_id in list(self.entries):
            if nzo_id not in in_queue and nzo_id not in mapped:
                del self.entries[nzo_id]
        for nzo_id in list(self._paused_by_us):
            if nzo_id not in in_queue:
                self._paused_by_us.discard(nzo_id)
                await self._repo.remove_paused_job(nzo_id)

    # ------------------------------------------------------------------
    # Ordering + single active download
    # ------------------------------------------------------------------

    def _sort_key(
        self, slot: QueueSlot, entries: dict[str, NzoEntry]
    ) -> tuple[int, int, int]:
        rank = self._rank
        entry = entries.get(slot.nzo_id)
        vq_name = entry.virtual_queue if entry else None
        queue_rank = rank.get(vq_name or "", len(self._virtual_queues))
        is_upgrade = int(
            self._config.upgrades_last and vq_name in self._upgrade_config_by_queue
        )
        return (is_upgrade, queue_rank, slot.index)

    async def _reorder_and_enforce(
        self, sab_queue: Queue, entries: dict[str, NzoEntry]
    ) -> None:
        if not sab_queue.slots:
            return
        dry_run = self._config.dry_run
        current_slots = sorted(sab_queue.slots, key=lambda s: s.index)
        desired = [
            s.nzo_id
            for s in sorted(current_slots, key=lambda s: self._sort_key(s, entries))
        ]
        current = [s.nzo_id for s in current_slots]

        if desired != current:
            live = list(current)
            for i, target in enumerate(desired):
                if live[i] == target:
                    continue
                other = live[i]
                if dry_run:
                    _LOGGER.info("[dry-run] would move %s above %s", target, other)
                else:
                    try:
                        await self._sab.switch(target, other)
                    except Exception:
                        _LOGGER.exception("Failed to move %s above %s", target, other)
                        return
                    _LOGGER.info("Reorder: moved %s to position %d", target, i)
                live.remove(target)
                live.insert(i, target)

        if sab_queue.paused:
            return  # whole queue paused (by the user or SABnzbd itself)
        if not self._config.enforce_single_download:
            await self._resume_all_paused_by_us(sab_queue)
            return

        slot_by_id = {s.nzo_id: s for s in sab_queue.slots}
        active_chosen = False
        for nzo_id in desired:
            slot = slot_by_id[nzo_id]
            paused = slot.status == "Paused"
            if paused and nzo_id not in self._paused_by_us:
                continue  # paused by the user - leave it alone
            if not active_chosen:
                active_chosen = True
                if paused:
                    await self._resume(nzo_id, "top slot")
            elif slot.status in PAUSABLE_STATUSES:
                await self._pause(nzo_id, slot.status)

    async def _resume_all_paused_by_us(self, sab_queue: Queue) -> None:
        for slot in sab_queue.slots:
            if slot.nzo_id in self._paused_by_us and slot.status == "Paused":
                await self._resume(slot.nzo_id, "single-download mode disabled")

    async def _pause(self, nzo_id: str, status: str) -> None:
        if self._config.dry_run:
            _LOGGER.info("[dry-run] would pause %s (status=%s)", nzo_id, status)
            return
        try:
            await self._sab.pause_job(nzo_id)
        except Exception:
            _LOGGER.exception("Failed to pause %s", nzo_id)
            return
        self._paused_by_us.add(nzo_id)
        await self._repo.add_paused_job(nzo_id)
        _LOGGER.info("Paused %s (status was %s)", nzo_id, status)

    async def _resume(self, nzo_id: str, reason: str) -> None:
        if self._config.dry_run:
            _LOGGER.info("[dry-run] would resume %s (%s)", nzo_id, reason)
            return
        try:
            await self._sab.resume_job(nzo_id)
        except Exception:
            _LOGGER.exception("Failed to resume %s", nzo_id)
            return
        self._paused_by_us.discard(nzo_id)
        await self._repo.remove_paused_job(nzo_id)
        _LOGGER.info("Resumed %s (%s)", nzo_id, reason)
