"""Event dataclasses emitted by the Conductarr monitor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from conductarr.clients.radarr import RadarrQueueItem
from conductarr.clients.sabnzbd import Queue, QueueSlot
from conductarr.clients.sonarr import SonarrQueueItem


@dataclass(frozen=True)
class ConductarrEvent:
    """Base class for all Conductarr events."""

    timestamp: datetime


# ---------------------------------------------------------------------------
# SABnzbd events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class QueueSnapshotEvent(ConductarrEvent):
    """Emitted on every poll cycle with the full queue state."""

    queue: Queue


@dataclass(frozen=True)
class JobAddedEvent(ConductarrEvent):
    """Emitted when a new job appears in the queue."""

    slot: QueueSlot


@dataclass(frozen=True)
class JobRemovedEvent(ConductarrEvent):
    """Emitted when a job disappears from the queue."""

    nzo_id: str
    filename: str


@dataclass(frozen=True)
class JobStatusChangedEvent(ConductarrEvent):
    """Emitted when a job's status changes."""

    nzo_id: str
    filename: str
    old_status: str
    new_status: str


@dataclass(frozen=True)
class JobPriorityChangedEvent(ConductarrEvent):
    """Emitted when a job's priority changes."""

    nzo_id: str
    filename: str
    old_priority: str
    new_priority: str


@dataclass(frozen=True)
class QueuePausedEvent(ConductarrEvent):
    """Emitted when the queue transitions to paused."""


@dataclass(frozen=True)
class QueueResumedEvent(ConductarrEvent):
    """Emitted when the queue transitions from paused to running."""


# ---------------------------------------------------------------------------
# Radarr events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RadarrQueueSnapshotEvent(ConductarrEvent):
    items: list[RadarrQueueItem]

    def __repr__(self) -> str:
        return f"RadarrQueueSnapshotEvent(timestamp={self.timestamp!r}, items=[{len(self.items)} items])"


@dataclass(frozen=True)
class RadarrQueueItemAddedEvent(ConductarrEvent):
    item: RadarrQueueItem


@dataclass(frozen=True)
class RadarrQueueItemRemovedEvent(ConductarrEvent):
    download_id: str
    title: str


# ---------------------------------------------------------------------------
# Sonarr events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SonarrQueueSnapshotEvent(ConductarrEvent):
    items: list[SonarrQueueItem]

    def __repr__(self) -> str:
        return f"SonarrQueueSnapshotEvent(timestamp={self.timestamp!r}, items=[{len(self.items)} items])"


@dataclass(frozen=True)
class SonarrQueueItemAddedEvent(ConductarrEvent):
    item: SonarrQueueItem


@dataclass(frozen=True)
class SonarrQueueItemRemovedEvent(ConductarrEvent):
    download_id: str
    title: str


# ---------------------------------------------------------------------------
# General events
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ServiceUnavailableEvent(ConductarrEvent):
    service: str  # "sabnzbd", "radarr", "sonarr"
    error: str
