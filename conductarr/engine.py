"""Central coordinator: owns the monitor and dispatches events to handlers."""

from __future__ import annotations

import logging

from conductarr.clients.radarr import RadarrClient
from conductarr.clients.sabnzbd import SABnzbdClient
from conductarr.clients.sonarr import SonarrClient
from conductarr.config import AnyDatabaseConfig, ConductarrConfig, SQLiteDatabaseConfig
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
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
from conductarr.monitor import SabnzbdMonitor
from conductarr.queue.manager import QueueManager
from conductarr.queue.models import VirtualQueue

_LOGGER = logging.getLogger(__name__)


class ConductarrEngine:
    """Central coordinator.

    Owns the monitor and dispatches events to handlers.
    Designed to be extended with queue managers and other handlers later.
    """

    def __init__(
        self, config: ConductarrConfig, database_config: AnyDatabaseConfig | None = None
    ) -> None:
        self._config = config
        self._client = SABnzbdClient(
            url=config.sabnzbd.url,
            api_key=config.sabnzbd.api_key,
        )
        self._radarr_client = RadarrClient(
            url=config.radarr.url,
            api_key=config.radarr.api_key,
        )
        self._sonarr_client = SonarrClient(
            url=config.sonarr.url,
            api_key=config.sonarr.api_key,
        )
        self._monitor = SabnzbdMonitor(
            client=self._client,
            radarr_client=self._radarr_client,
            sonarr_client=self._sonarr_client,
            poll_interval=config.poll_interval,
            on_event=self._handle_event,
        )

        # Database and queue management
        db_config = database_config or SQLiteDatabaseConfig()
        self._db = Database(db_config)
        self._repo = QueueRepository(self._db)
        virtual_queues = [
            VirtualQueue(
                name=q.name,
                priority=q.priority,
                enabled=q.enabled,
                matchers=[{"type": m.type, "tags": m.tags} for m in q.matchers],
            )
            for q in config.queues
        ]
        self._queue_manager = QueueManager(self._repo, virtual_queues)

    async def start(self) -> None:
        """Start the monitor and begin processing events."""
        await self._db.connect()
        await self._client.__aenter__()
        await self._monitor.start()
        _LOGGER.info("Conductarr engine started")

    async def stop(self) -> None:
        """Stop the monitor and close the client session."""
        await self._monitor.stop()
        await self._client.__aexit__(None, None, None)
        await self._db.disconnect()

    async def _handle_event(self, event: ConductarrEvent) -> None:
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
                await self._queue_manager.on_job_added(event.slot.nzo_id, event.slot)
            case JobRemovedEvent():
                _LOGGER.info(
                    "Event: %s  nzo_id=%s  filename=%s",
                    type(event).__name__,
                    event.nzo_id,
                    event.filename,
                )
                await self._queue_manager.on_job_removed(event.nzo_id)
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
            case RadarrQueueSnapshotEvent():
                _LOGGER.info(
                    "Event: %s  items=%d",
                    type(event).__name__,
                    len(event.items),
                )
            case RadarrQueueItemAddedEvent():
                _LOGGER.info(
                    "Event: %s  download_id=%s  title=%s",
                    type(event).__name__,
                    event.item.download_id,
                    event.item.title,
                )
            case RadarrQueueItemRemovedEvent():
                _LOGGER.info(
                    "Event: %s  download_id=%s  title=%s",
                    type(event).__name__,
                    event.download_id,
                    event.title,
                )
            case SonarrQueueSnapshotEvent():
                _LOGGER.info(
                    "Event: %s  items=%d",
                    type(event).__name__,
                    len(event.items),
                )
            case SonarrQueueItemAddedEvent():
                _LOGGER.info(
                    "Event: %s  download_id=%s  title=%s",
                    type(event).__name__,
                    event.item.download_id,
                    event.item.title,
                )
            case SonarrQueueItemRemovedEvent():
                _LOGGER.info(
                    "Event: %s  download_id=%s  title=%s",
                    type(event).__name__,
                    event.download_id,
                    event.title,
                )
            case ServiceUnavailableEvent():
                _LOGGER.info(
                    "Event: %s  service=%s  error=%s",
                    type(event).__name__,
                    event.service,
                    event.error,
                )
            case _:
                _LOGGER.info("Event: %s", type(event).__name__)
