"""Central coordinator: owns the monitor and dispatches events to handlers."""

from __future__ import annotations

import logging

from conductarr.clients.sabnzbd import SABnzbdClient
from conductarr.config import Config
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
from conductarr.monitor import SabnzbdMonitor

_LOGGER = logging.getLogger(__name__)


class ConductarrEngine:
    """Central coordinator.

    Owns the monitor and dispatches events to handlers.
    Designed to be extended with queue managers and other handlers later.
    """

    def __init__(self, config: Config) -> None:
        self._config = config
        self._client = SABnzbdClient(
            url=config.sabnzbd.url,
            api_key=config.sabnzbd.api_key,
        )
        self._monitor = SabnzbdMonitor(
            client=self._client,
            poll_interval=config.sabnzbd.poll_interval,
            on_event=self._handle_event,
        )

    async def start(self) -> None:
        """Start the monitor and begin processing events."""
        await self._client.__aenter__()
        await self._monitor.start()
        _LOGGER.info("Conductarr engine started")

    async def stop(self) -> None:
        """Stop the monitor and close the client session."""
        await self._monitor.stop()
        await self._client.__aexit__(None, None, None)

    async def _handle_event(self, event: SabnzbdEvent) -> None:
        """Dispatch an event to the appropriate handler."""
        match event:
            case QueueSnapshotEvent():
                _LOGGER.info(
                    "Event: %s  slots=%d  paused=%s",
                    type(event).__name__,
                    event.queue.noofslots,
                    event.queue.paused,
                )
            case JobAddedEvent():
                _LOGGER.info(
                    "Event: %s  nzo_id=%s  filename=%s",
                    type(event).__name__,
                    event.slot.nzo_id,
                    event.slot.filename,
                )
            case JobRemovedEvent():
                _LOGGER.info(
                    "Event: %s  nzo_id=%s  filename=%s",
                    type(event).__name__,
                    event.nzo_id,
                    event.filename,
                )
            case JobStatusChangedEvent():
                _LOGGER.info(
                    "Event: %s  nzo_id=%s  %s → %s",
                    type(event).__name__,
                    event.nzo_id,
                    event.old_status,
                    event.new_status,
                )
            case JobPriorityChangedEvent():
                _LOGGER.info(
                    "Event: %s  nzo_id=%s  %s → %s",
                    type(event).__name__,
                    event.nzo_id,
                    event.old_priority,
                    event.new_priority,
                )
            case QueuePausedEvent():
                _LOGGER.info("Event: %s", type(event).__name__)
            case QueueResumedEvent():
                _LOGGER.info("Event: %s", type(event).__name__)
            case _:
                _LOGGER.info("Event: %s", type(event).__name__)
