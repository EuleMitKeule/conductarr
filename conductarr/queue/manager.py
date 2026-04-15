"""Queue manager: assigns items to virtual queues and handles SABnzbd events."""

from __future__ import annotations

import logging

from conductarr.clients.sabnzbd import QueueSlot
from conductarr.db.repository import QueueRepository
from conductarr.queue.matchers import MATCHER_REGISTRY
from conductarr.queue.models import QueueItem, VirtualQueue

_LOGGER = logging.getLogger(__name__)


class QueueManager:
    """Orchestrates virtual queue assignment and SABnzbd job lifecycle."""

    def __init__(
        self, repo: QueueRepository, virtual_queues: list[VirtualQueue]
    ) -> None:
        self._repo = repo
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
        """Handle a SABnzbd JobAddedEvent.

        If the slot's category indicates a Radarr/Sonarr download_id,
        link the SABnzbd job to the corresponding queue item.
        """
        download_id = slot.cat if slot.cat else None
        if not download_id:
            _LOGGER.debug("Job %s has no category/download_id, skipping map", nzo_id)
            return

        # Try to find a queue item whose source_id matches the download_id
        for source in ("radarr", "sonarr"):
            queue_item = await self._repo.get_item(source, download_id)
            if queue_item is not None and queue_item.id is not None:
                await self._repo.upsert_job_map(
                    nzo_id,
                    queue_item.id,
                    queue_item.virtual_queue or "",
                )
                _LOGGER.info(
                    "Mapped SABnzbd job %s → queue item %s (queue=%s)",
                    nzo_id,
                    queue_item.id,
                    queue_item.virtual_queue,
                )
                return

        _LOGGER.debug(
            "No queue item found for download_id=%s (nzo_id=%s)", download_id, nzo_id
        )

    async def on_job_removed(self, nzo_id: str) -> None:
        """Handle a SABnzbd JobRemovedEvent.

        Marks the linked queue item as completed and removes the job map entry.
        """
        job_map = await self._repo.get_job_map(nzo_id)
        if job_map is None:
            _LOGGER.debug("No job map for removed nzo_id=%s", nzo_id)
            return

        queue_item_id = job_map["queue_item_id"]
        if queue_item_id is not None:
            await self._repo.update_status(queue_item_id, "completed")
            _LOGGER.info(
                "Queue item %d marked completed (nzo_id=%s)", queue_item_id, nzo_id
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
