"""SABnzbd queue monitor: polls periodically and emits diff events."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

from conductarr.clients.sabnzbd import Queue, SABnzbdClient
from conductarr.events import (
    JobAddedEvent,
    JobPriorityChangedEvent,
    JobRemovedEvent,
    JobStatusChangedEvent,
    QueuePausedEvent,
    QueueResumedEvent,
    QueueSnapshotEvent,
    SabnzbdEvent,
)

_LOGGER = logging.getLogger(__name__)


class SabnzbdMonitor:
    """Polls SABnzbd queue periodically and emits events by diffing
    current state against previous state.
    """

    def __init__(
        self,
        client: SABnzbdClient,
        poll_interval: float,
        on_event: Callable[[SabnzbdEvent], Awaitable[None]],
    ) -> None:
        self._client = client
        self._poll_interval = poll_interval
        self._on_event = on_event
        self._previous_queue: Queue | None = None
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Start the poll loop."""
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Gracefully cancel the poll loop."""
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            await self._poll()
            await asyncio.sleep(self._poll_interval)

    async def _poll(self) -> None:
        """Fetch queue, diff against previous state, emit appropriate events."""
        try:
            queue = await self._client.get_queue()
        except Exception:
            _LOGGER.exception("Error fetching SABnzbd queue")
            return

        now = datetime.now(tz=timezone.utc)

        # Always emit snapshot
        snapshot = QueueSnapshotEvent(timestamp=now, queue=queue)
        _LOGGER.debug("Emitting %s", type(snapshot).__name__)
        await self._on_event(snapshot)

        if self._previous_queue is not None:
            prev = self._previous_queue
            prev_by_id = {s.nzo_id: s for s in prev.slots}
            curr_by_id = {s.nzo_id: s for s in queue.slots}

            # Jobs added
            for nzo_id, slot in curr_by_id.items():
                if nzo_id not in prev_by_id:
                    event = JobAddedEvent(timestamp=now, slot=slot)
                    _LOGGER.debug("Emitting %s nzo_id=%s", type(event).__name__, nzo_id)
                    await self._on_event(event)

            # Jobs removed
            for nzo_id, slot in prev_by_id.items():
                if nzo_id not in curr_by_id:
                    removed = JobRemovedEvent(
                        timestamp=now, nzo_id=nzo_id, filename=slot.filename
                    )
                    _LOGGER.debug(
                        "Emitting %s nzo_id=%s", type(removed).__name__, nzo_id
                    )
                    await self._on_event(removed)

            # Status / priority changes
            for nzo_id, prev_slot in prev_by_id.items():
                if nzo_id not in curr_by_id:
                    continue
                curr_slot = curr_by_id[nzo_id]

                if prev_slot.status != curr_slot.status:
                    status_changed = JobStatusChangedEvent(
                        timestamp=now,
                        nzo_id=nzo_id,
                        filename=curr_slot.filename,
                        old_status=prev_slot.status,
                        new_status=curr_slot.status,
                    )
                    _LOGGER.debug(
                        "Emitting %s nzo_id=%s", type(status_changed).__name__, nzo_id
                    )
                    await self._on_event(status_changed)

                if prev_slot.priority != curr_slot.priority:
                    priority_changed = JobPriorityChangedEvent(
                        timestamp=now,
                        nzo_id=nzo_id,
                        filename=curr_slot.filename,
                        old_priority=prev_slot.priority,
                        new_priority=curr_slot.priority,
                    )
                    _LOGGER.debug(
                        "Emitting %s nzo_id=%s", type(priority_changed).__name__, nzo_id
                    )
                    await self._on_event(priority_changed)

            # Queue pause / resume
            if not prev.paused and queue.paused:
                event_p = QueuePausedEvent(timestamp=now)
                _LOGGER.debug("Emitting %s", type(event_p).__name__)
                await self._on_event(event_p)
            elif prev.paused and not queue.paused:
                event_r = QueueResumedEvent(timestamp=now)
                _LOGGER.debug("Emitting %s", type(event_r).__name__)
                await self._on_event(event_r)

        self._previous_queue = queue
