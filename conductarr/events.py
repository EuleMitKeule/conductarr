"""Event dataclasses emitted by the SABnzbd monitor."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from conductarr.clients.sabnzbd import Queue, QueueSlot


@dataclass(frozen=True)
class SabnzbdEvent:
    """Base class for all SABnzbd events."""

    timestamp: datetime


@dataclass(frozen=True)
class QueueSnapshotEvent(SabnzbdEvent):
    """Emitted on every poll cycle with the full queue state."""

    queue: Queue


@dataclass(frozen=True)
class JobAddedEvent(SabnzbdEvent):
    """Emitted when a new job appears in the queue."""

    slot: QueueSlot


@dataclass(frozen=True)
class JobRemovedEvent(SabnzbdEvent):
    """Emitted when a job disappears from the queue."""

    nzo_id: str
    filename: str


@dataclass(frozen=True)
class JobStatusChangedEvent(SabnzbdEvent):
    """Emitted when a job's status changes."""

    nzo_id: str
    filename: str
    old_status: str
    new_status: str


@dataclass(frozen=True)
class JobPriorityChangedEvent(SabnzbdEvent):
    """Emitted when a job's priority changes."""

    nzo_id: str
    filename: str
    old_priority: str
    new_priority: str


@dataclass(frozen=True)
class QueuePausedEvent(SabnzbdEvent):
    """Emitted when the queue transitions to paused."""


@dataclass(frozen=True)
class QueueResumedEvent(SabnzbdEvent):
    """Emitted when the queue transitions from paused to running."""
