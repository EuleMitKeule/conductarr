"""Pluggable matcher system for assigning items to virtual queues."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from conductarr.queue.models import AssignContext, QueueItem


class BaseMatcher(ABC):
    """Base class for queue item matchers."""

    @abstractmethod
    def matches(
        self, item: QueueItem, config: dict[str, Any], context: AssignContext
    ) -> bool:
        """Return True if this item belongs to the queue based on matcher config."""


class TagMatcher(BaseMatcher):
    """Matches if item.tags contains any of the configured tags."""

    def matches(
        self, item: QueueItem, config: dict[str, Any], context: AssignContext
    ) -> bool:
        required_tags: list[str] = config.get("tags", [])
        return any(tag in item.tags for tag in required_tags)


class HasNoFileMatcher(BaseMatcher):
    """Matches if the media item currently has no file (first-time download)."""

    def matches(
        self, item: QueueItem, config: dict[str, Any], context: AssignContext
    ) -> bool:
        return not context.has_file


MATCHER_REGISTRY: dict[str, type[BaseMatcher]] = {
    "tags": TagMatcher,
    "has_no_file": HasNoFileMatcher,
}


def register_matcher(name: str, matcher_class: type[BaseMatcher]) -> None:
    """Register a new matcher type."""
    MATCHER_REGISTRY[name] = matcher_class
