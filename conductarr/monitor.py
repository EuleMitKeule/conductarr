"""Conductarr queue monitor: polls all services and emits diff events."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

from conductarr.clients.radarr import RadarrClient, RadarrQueueItem
from conductarr.clients.sabnzbd import Queue, SABnzbdClient
from conductarr.clients.sonarr import SonarrClient, SonarrQueueItem
from conductarr.events import (
    ConductarrEvent,
    JobAddedEvent,
    JobPriorityChangedEvent,
    JobRemovedEvent,
    JobStatusChangedEvent,
    QueuePausedEvent,
    QueueResumedEvent,
    QueueSnapshotEvent,
    RadarrQueueItemAddedEvent,
    RadarrQueueItemRemovedEvent,
    RadarrQueueSnapshotEvent,
    ServiceUnavailableEvent,
    SonarrQueueItemAddedEvent,
    SonarrQueueItemRemovedEvent,
    SonarrQueueSnapshotEvent,
)

_LOGGER = logging.getLogger(__name__)


class SabnzbdMonitor:
    """Polls SABnzbd, Radarr, and Sonarr queues periodically and emits
    events by diffing current state against previous state.
    """

    def __init__(
        self,
        client: SABnzbdClient,
        radarr_client: RadarrClient,
        sonarr_client: SonarrClient,
        poll_interval: float,
        on_event: Callable[[ConductarrEvent], Awaitable[None]],
        on_cycle_complete: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self._client = client
        self._radarr_client = radarr_client
        self._sonarr_client = sonarr_client
        self._poll_interval = poll_interval
        self._on_event = on_event
        self._on_cycle_complete = on_cycle_complete
        self._previous_queue: Queue | None = None
        self._previous_radarr: list[RadarrQueueItem] | None = None
        self._previous_sonarr: list[SonarrQueueItem] | None = None
        self.last_sab_queue: Queue | None = None
        self.last_radarr_queue: list[RadarrQueueItem] | None = None
        self.last_sonarr_queue: list[SonarrQueueItem] | None = None
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
            try:
                await self._poll()
            except Exception:
                _LOGGER.exception(
                    "Unhandled error in poll cycle; will retry next interval"
                )
            await asyncio.sleep(self._poll_interval)

    async def _poll(self) -> None:
        """Run a single poll cycle: fetch all queues, emit diff events,
        update ``last_*`` snapshot attributes, and invoke the
        ``on_cycle_complete`` callback when all three fetches succeed.
        """
        now = datetime.now(tz=timezone.utc)

        sab_result, radarr_result, sonarr_result = await asyncio.gather(
            self._poll_sabnzbd(),
            self._poll_radarr(),
            self._poll_sonarr(),
            return_exceptions=True,
        )

        if isinstance(sab_result, BaseException):
            await self._on_event(
                ServiceUnavailableEvent(
                    timestamp=now,
                    service="sabnzbd",
                    error=str(sab_result),
                )
            )
        else:
            self.last_sab_queue = sab_result
            await self._diff_sabnzbd(sab_result, now)

        if isinstance(radarr_result, BaseException):
            await self._on_event(
                ServiceUnavailableEvent(
                    timestamp=now,
                    service="radarr",
                    error=str(radarr_result),
                )
            )
        else:
            self.last_radarr_queue = radarr_result
            await self._diff_radarr(radarr_result, now)

        if isinstance(sonarr_result, BaseException):
            await self._on_event(
                ServiceUnavailableEvent(
                    timestamp=now,
                    service="sonarr",
                    error=str(sonarr_result),
                )
            )
        else:
            self.last_sonarr_queue = sonarr_result
            await self._diff_sonarr(sonarr_result, now)

        if self._on_cycle_complete is not None:
            try:
                await self._on_cycle_complete()
            except Exception:
                _LOGGER.exception("Error in on_cycle_complete callback")

    # ------------------------------------------------------------------
    # Poll helpers (fetch only — may raise)
    # ------------------------------------------------------------------

    async def _poll_sabnzbd(self) -> Queue:
        return await self._client.get_queue()

    async def _poll_radarr(self) -> list[RadarrQueueItem]:
        return await self._radarr_client.get_queue()

    async def _poll_sonarr(self) -> list[SonarrQueueItem]:
        return await self._sonarr_client.get_queue()

    # ------------------------------------------------------------------
    # Diff + emit helpers
    # ------------------------------------------------------------------

    async def _diff_sabnzbd(self, queue: Queue, now: datetime) -> None:
        """Diff SABnzbd queue against previous state and emit events."""
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

    async def _diff_radarr(self, items: list[RadarrQueueItem], now: datetime) -> None:
        """Diff Radarr queue against previous state and emit events."""
        await self._on_event(RadarrQueueSnapshotEvent(timestamp=now, items=items))

        if self._previous_radarr is not None:
            prev_by_id = {i.download_id: i for i in self._previous_radarr}
            curr_by_id = {i.download_id: i for i in items}

            for did, item in curr_by_id.items():
                if did not in prev_by_id:
                    await self._on_event(
                        RadarrQueueItemAddedEvent(timestamp=now, item=item)
                    )

            for did, item in prev_by_id.items():
                if did not in curr_by_id:
                    await self._on_event(
                        RadarrQueueItemRemovedEvent(
                            timestamp=now,
                            download_id=did,
                            title=item.title,
                        )
                    )

        self._previous_radarr = items

    async def _diff_sonarr(self, items: list[SonarrQueueItem], now: datetime) -> None:
        """Diff Sonarr queue against previous state and emit events."""
        await self._on_event(SonarrQueueSnapshotEvent(timestamp=now, items=items))

        if self._previous_sonarr is not None:
            prev_by_id = {i.download_id: i for i in self._previous_sonarr}
            curr_by_id = {i.download_id: i for i in items}

            for did, item in curr_by_id.items():
                if did not in prev_by_id:
                    await self._on_event(
                        SonarrQueueItemAddedEvent(timestamp=now, item=item)
                    )

            for did, item in prev_by_id.items():
                if did not in curr_by_id:
                    await self._on_event(
                        SonarrQueueItemRemovedEvent(
                            timestamp=now,
                            download_id=did,
                            title=item.title,
                        )
                    )

        self._previous_sonarr = items
