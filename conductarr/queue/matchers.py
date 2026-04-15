"""Pluggable matcher system for assigning items to virtual queues."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from conductarr.queue.models import QueueItem


class BaseMatcher(ABC):
    """Base class for queue item matchers."""

    @abstractmethod
    def matches(self, item: QueueItem, config: dict[str, Any]) -> bool:
        """Return True if this item belongs to the queue based on matcher config."""


class TagMatcher(BaseMatcher):
    """Matches if item.tags contains any of the configured tags."""

    def matches(self, item: QueueItem, config: dict[str, Any]) -> bool:
        required_tags: list[str] = config.get("tags", [])
        return any(tag in item.tags for tag in required_tags)


MATCHER_REGISTRY: dict[str, type[BaseMatcher]] = {
    "tags": TagMatcher,
}


def register_matcher(name: str, matcher_class: type[BaseMatcher]) -> None:
    """Register a new matcher type."""
    MATCHER_REGISTRY[name] = matcher_class
