"""Queue manager: assigns items to virtual queues and handles SABnzbd events."""

from __future__ import annotations

import logging

from conductarr.clients.radarr import RadarrClient, RadarrQueueItem
from conductarr.clients.sabnzbd import Queue, QueueSlot
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
    ) -> None:
        self._repo = repo
        self._radarr = radarr_client
        self._sonarr = sonarr_client
        self._virtual_queues = sorted(
            [vq for vq in virtual_queues if vq.enabled],
            key=lambda vq: vq.priority,
            reverse=True,
        )

    async def assign_queue(self, item: QueueItem) -> QueueItem:
        """Assign an item to the highest-priority matching virtual queue."""
        for vq in self._virtual_queues:
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

        # No queue matched — still persist the item
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
    ) -> None:
        """Cross-reference SABnzbd slots with Radarr/Sonarr queue items.

        For each SABnzbd slot whose nzo_id matches a Radarr or Sonarr
        ``downloadId``, creates or updates the corresponding queue item and
        records the mapping in ``sabnzbd_job_map``.

        For slots that have disappeared from SABnzbd (job finished/cancelled),
        marks the linked queue item as completed and removes the mapping.
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
                await self._repo.delete_job_map(nzo_id)

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
