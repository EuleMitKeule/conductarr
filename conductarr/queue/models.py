"""Dataclasses for virtual queues and queue items."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class VirtualQueue:
    """A named priority tier with matcher rules."""

    name: str
    priority: int
    enabled: bool = True
    fallback: bool = False
    matchers: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QueueItem:
    """A single tracked download item."""

    source: str
    source_id: str
    tags: list[str]
    status: str = "pending"
    virtual_queue: str | None = None
    attempts: int = 0
    last_tried_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    id: int | None = None
