"""Unit tests for the pluggable matcher system."""

from __future__ import annotations

from typing import Any

from conductarr.queue.matchers import MATCHER_REGISTRY, TagMatcher, register_matcher
from conductarr.queue.models import QueueItem


class TestTagMatcher:
    """Tests for TagMatcher."""

    def setup_method(self) -> None:
        self.matcher = TagMatcher()

    def test_matching_single_tag(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["request"])
        assert self.matcher.matches(item, {"tags": ["request"]}) is True

    def test_non_matching_tags(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["upgrade-de"])
        assert self.matcher.matches(item, {"tags": ["request"]}) is False

    def test_empty_required_tags(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["request"])
        assert self.matcher.matches(item, {"tags": []}) is False

    def test_empty_item_tags(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=[])
        assert self.matcher.matches(item, {"tags": ["request"]}) is False

    def test_multiple_tags_one_matches(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["kometa", "overseerr"])
        assert self.matcher.matches(item, {"tags": ["request", "overseerr"]}) is True

    def test_multiple_tags_none_match(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["kometa", "upgrade-de"])
        assert self.matcher.matches(item, {"tags": ["request", "overseerr"]}) is False

    def test_no_tags_key_in_config(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["request"])
        assert self.matcher.matches(item, {}) is False


class TestMatcherRegistry:
    """Tests for the matcher registry."""

    def test_tags_registered(self) -> None:
        assert "tags" in MATCHER_REGISTRY
        assert MATCHER_REGISTRY["tags"] is TagMatcher

    def test_register_custom_matcher(self) -> None:
        from conductarr.queue.matchers import BaseMatcher

        class DummyMatcher(BaseMatcher):
            def matches(self, item: QueueItem, config: dict[str, Any]) -> bool:
                return True

        register_matcher("dummy", DummyMatcher)
        assert MATCHER_REGISTRY["dummy"] is DummyMatcher
        # Cleanup
        del MATCHER_REGISTRY["dummy"]
