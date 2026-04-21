"""Queue manager: assigns items to virtual queues and handles SABnzbd events."""

from __future__ import annotations

import logging

from conductarr.clients.radarr import RadarrClient, RadarrQueueItem
from conductarr.clients.sabnzbd import Queue, QueueSlot, SABnzbdClient
from conductarr.clients.sonarr import SonarrClient, SonarrQueueItem
from conductarr.db.repository import QueueRepository
from conductarr.queue.matchers import MATCHER_REGISTRY
from conductarr.queue.models import QueueItem, VirtualQueue

_LOGGER = logging.getLogger(__name__)


class QueueManager:
    """Orchestrates virtual queue assignment and SABnzbd job lifecycle."""

    def __init__(
        self,
        repo: QueueRepository,
        virtual_queues: list[VirtualQueue],
        radarr_client: RadarrClient | None = None,
        sonarr_client: SonarrClient | None = None,
        sabnzbd_client: SABnzbdClient | None = None,
    ) -> None:
        self._repo = repo
        self._radarr = radarr_client
        self._sonarr = sonarr_client
        self._sabnzbd = sabnzbd_client
        self._virtual_queues = sorted(
            [vq for vq in virtual_queues if vq.enabled],
            key=lambda vq: vq.priority,
            reverse=True,
        )

    async def assign_queue(self, item: QueueItem) -> QueueItem:
        """Assign an item to the highest-priority matching virtual queue.

        Non-fallback queues are evaluated first in priority order.  If nothing
        matches, the item falls through to the highest-priority fallback queue
        (if one is configured).  Items that match neither are still persisted
        without a virtual queue assignment.
        """
        fallback_queue: str | None = None
        for vq in self._virtual_queues:
            if vq.fallback:
                # Record the first (highest-priority) fallback but keep scanning
                # in case a non-fallback queue matches later.
                if fallback_queue is None:
                    fallback_queue = vq.name
                continue
            for matcher_config in vq.matchers:
                matcher_type = matcher_config.get("type")
                matcher_cls = MATCHER_REGISTRY.get(matcher_type)  # type: ignore[arg-type]
                if matcher_cls is None:
                    _LOGGER.warning("Unknown matcher type: %s", matcher_type)
                    continue
                matcher = matcher_cls()
                if matcher.matches(item, matcher_config):
                    item.virtual_queue = vq.name
                    _LOGGER.info(
                        "Item %s/%s assigned to queue '%s'",
                        item.source,
                        item.source_id,
                        vq.name,
                    )
                    return await self._repo.upsert_item(item)

        if fallback_queue is not None:
            item.virtual_queue = fallback_queue
            _LOGGER.info(
                "Item %s/%s assigned to fallback queue '%s'",
                item.source,
                item.source_id,
                fallback_queue,
            )
            return await self._repo.upsert_item(item)

        # No queue matched at all — still persist the item
        _LOGGER.debug(
            "Item %s/%s did not match any virtual queue",
            item.source,
            item.source_id,
        )
        return await self._repo.upsert_item(item)

    async def on_job_added(self, nzo_id: str, slot: QueueSlot) -> None:
        """Log a SABnzbd JobAddedEvent. Mapping is handled by reconcile."""
        _LOGGER.debug("Job added: nzo_id=%s filename=%s", nzo_id, slot.filename)

    async def on_job_removed(self, nzo_id: str) -> None:
        """Log a SABnzbd JobRemovedEvent. Cleanup is handled by reconcile."""
        _LOGGER.debug("Job removed: nzo_id=%s", nzo_id)

    async def reconcile(
        self,
        sab_queue: Queue,
        radarr_queue: list[RadarrQueueItem],
        sonarr_queue: list[SonarrQueueItem],
    ) -> list[str]:
        """Cross-reference SABnzbd slots with Radarr/Sonarr queue items.

        For each SABnzbd slot whose nzo_id matches a Radarr or Sonarr
        ``downloadId``, creates or updates the corresponding queue item and
        records the mapping in ``sabnzbd_job_map``.

        For slots that have disappeared from SABnzbd (job finished/cancelled),
        marks the linked queue item as completed and removes the mapping.

        Returns the virtual-queue names of all jobs that completed this cycle.
        """
        radarr_by_nzo = {
            item.download_id: item for item in radarr_queue if item.download_id
        }
        sonarr_by_nzo = {
            item.download_id: item for item in sonarr_queue if item.download_id
        }
        current_nzo_ids = {slot.nzo_id for slot in sab_queue.slots}

        # Map new slots to queue items
        for slot in sab_queue.slots:
            nzo_id = slot.nzo_id
            if await self._repo.get_job_map(nzo_id) is not None:
                continue  # already mapped

            if nzo_id in radarr_by_nzo:
                radarr_item = radarr_by_nzo[nzo_id]
                queue_item = await self._repo.get_item(
                    "radarr", str(radarr_item.movie_id)
                )
                if queue_item is None:
                    tags: list[str] = []
                    if self._radarr is not None:
                        tags = await self._radarr.get_movie_tags(radarr_item.movie_id)
                    queue_item = QueueItem(
                        source="radarr",
                        source_id=str(radarr_item.movie_id),
                        tags=tags,
                    )
                queue_item = await self.assign_queue(queue_item)
                if queue_item.id is not None:
                    await self._repo.upsert_job_map(
                        nzo_id, queue_item.id, queue_item.virtual_queue or ""
                    )
                    _LOGGER.info(
                        "Mapped SABnzbd job %s → Radarr movie %s → queue %s",
                        nzo_id,
                        radarr_item.title,
                        queue_item.virtual_queue,
                    )

            elif nzo_id in sonarr_by_nzo:
                sonarr_item = sonarr_by_nzo[nzo_id]
                queue_item = await self._repo.get_item(
                    "sonarr", str(sonarr_item.episode_id)
                )
                if queue_item is None:
                    tags = []
                    if self._sonarr is not None:
                        tags = await self._sonarr.get_episode_tags(
                            sonarr_item.episode_id
                        )
                    queue_item = QueueItem(
                        source="sonarr",
                        source_id=str(sonarr_item.episode_id),
                        tags=tags,
                    )
                queue_item = await self.assign_queue(queue_item)
                if queue_item.id is not None:
                    await self._repo.upsert_job_map(
                        nzo_id, queue_item.id, queue_item.virtual_queue or ""
                    )
                    _LOGGER.info(
                        "Mapped SABnzbd job %s → Sonarr episode %s → queue %s",
                        nzo_id,
                        sonarr_item.title,
                        queue_item.virtual_queue,
                    )

            else:
                _LOGGER.debug(
                    "SABnzbd job %s not found in Radarr or Sonarr queue", nzo_id
                )

        # Mark completed jobs whose nzo_ids are no longer in SABnzbd
        completed_virtual_queues: list[str] = []
        all_maps = await self._repo.get_all_job_maps()
        for job_map in all_maps:
            nzo_id = job_map["nzo_id"]
            if nzo_id not in current_nzo_ids:
                queue_item_id = job_map["queue_item_id"]
                if queue_item_id is not None:
                    await self._repo.update_status(queue_item_id, "completed")
                    _LOGGER.info(
                        "SABnzbd job %s completed → queue item %d marked completed",
                        nzo_id,
                        queue_item_id,
                    )
                vq = job_map.get("virtual_queue", "")
                if vq:
                    completed_virtual_queues.append(vq)
                await self._repo.delete_job_map(nzo_id)

        # Reorder queue by virtual-queue priority and enforce one active download.
        await self._reorder_and_enforce_active(sab_queue)

        return completed_virtual_queues

    async def get_next_for_queue(self, virtual_queue: str) -> QueueItem | None:
        """Return the next pending item for a virtual queue using rotation order."""
        items = await self._repo.get_items_by_queue(virtual_queue)
        for item in items:
            if item.status == "pending":
                return item
        return None

    async def mark_failed(self, item_id: int) -> None:
        """Increment attempts and set status back to pending for retry."""
        await self._repo.increment_attempts(item_id)
        await self._repo.update_status(item_id, "pending")

    async def _reorder_and_enforce_active(self, sab_queue: Queue) -> None:
        """Reorder SABnzbd queue by virtual queue priority and enforce one active download.

        Slots belonging to higher-priority virtual queues are moved to the front.
        Unknown / unmapped jobs are pushed to the back.  The operation is idempotent:
        ``switch()`` is only called when the current order differs from the desired one.
        After reordering, exactly slot[0] is allowed to download; all other slots are
        individually paused.
        """
        if self._sabnzbd is None:
            return

        # Fetch a fresh queue so slot positions and statuses reflect any reordering
        # that may have happened earlier in the same reconcile cycle (e.g. switch calls).
        sab_queue = await self._sabnzbd.get_queue()
        if not sab_queue.slots:
            return

        # Build priority rank: lower number = higher priority (index in sorted list).
        vq_rank: dict[str, int] = {
            vq.name: i for i, vq in enumerate(self._virtual_queues)
        }
        unknown_rank = len(self._virtual_queues)

        # Fetch all job maps for O(1) nzo_id → virtual_queue lookup.
        all_maps = await self._repo.get_all_job_maps()
        nzo_to_vq: dict[str, str] = {m["nzo_id"]: m["virtual_queue"] for m in all_maps}

        # Sort slots: primary by virtual-queue rank, secondary by current index (stable).
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

        # Reorder only when the queue is out of desired order (idempotent).
        if desired_order != current_order:
            live_order = list(current_order)
            for i, target_id in enumerate(desired_order):
                if live_order[i] == target_id:
                    continue
                # Move target_id to directly above the job currently at position i.
                current_at_i = live_order[i]
                try:
                    await self._sabnzbd.switch(target_id, current_at_i)
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
                _LOGGER.debug(
                    "Reorder: moved %s above %s (position %d)",
                    target_id,
                    current_at_i,
                    i,
                )

        # Enforce exactly one active download: slot[0] runs, all others are paused.
        # Only pause slots that are in an interruptible state; skip jobs already in
        # final phases (Fetching, Verifying, Extracting, Moving, …) to avoid data loss.
        _PAUSABLE_STATUSES = frozenset({"Downloading", "Queued"})

        slot_by_id = {s.nzo_id: s for s in sab_queue.slots}
        top_id = desired_order[0]
        top_slot = slot_by_id.get(top_id)
        if top_slot is not None and top_slot.status == "Paused":
            _LOGGER.info("Resuming top slot %s", top_id)
            try:
                await self._sabnzbd.resume_job(top_id)
            except Exception:
                _LOGGER.exception("Failed to resume top slot %s", top_id)

        for nzo_id in desired_order[1:]:
            slot = slot_by_id.get(nzo_id)
            if slot is not None and slot.status in _PAUSABLE_STATUSES:
                _LOGGER.info("Pausing non-top slot %s (status=%s)", nzo_id, slot.status)
                try:
                    await self._sabnzbd.pause_job(nzo_id)
                except Exception:
                    _LOGGER.exception("Failed to pause non-top slot %s", nzo_id)
