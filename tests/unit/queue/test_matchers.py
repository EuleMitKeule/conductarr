"""Unit tests for the pluggable matcher system."""

from __future__ import annotations

from typing import Any

from conductarr.queue.matchers import (
    MATCHER_REGISTRY,
    HasNoFileMatcher,
    TagMatcher,
    register_matcher,
)
from conductarr.queue.models import AssignContext, QueueItem

# Shared default context (file present, no custom formats) used by tests that
# don't exercise context-sensitive behaviour.
_DEFAULT_CONTEXT = AssignContext(has_file=True, existing_custom_formats=[])
_NO_FILE_CONTEXT = AssignContext(has_file=False, existing_custom_formats=[])


class TestTagMatcher:
    """Tests for TagMatcher."""

    def setup_method(self) -> None:
        self.matcher = TagMatcher()

    def test_matching_single_tag(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["request"])
        assert (
            self.matcher.matches(item, {"tags": ["request"]}, _DEFAULT_CONTEXT) is True
        )

    def test_non_matching_tags(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["upgrade-de"])
        assert (
            self.matcher.matches(item, {"tags": ["request"]}, _DEFAULT_CONTEXT) is False
        )

    def test_empty_required_tags(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["request"])
        assert self.matcher.matches(item, {"tags": []}, _DEFAULT_CONTEXT) is False

    def test_empty_item_tags(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=[])
        assert (
            self.matcher.matches(item, {"tags": ["request"]}, _DEFAULT_CONTEXT) is False
        )

    def test_multiple_tags_one_matches(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["kometa", "overseerr"])
        assert (
            self.matcher.matches(
                item, {"tags": ["request", "overseerr"]}, _DEFAULT_CONTEXT
            )
            is True
        )

    def test_multiple_tags_none_match(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["kometa", "upgrade-de"])
        assert (
            self.matcher.matches(
                item, {"tags": ["request", "overseerr"]}, _DEFAULT_CONTEXT
            )
            is False
        )

    def test_no_tags_key_in_config(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=["request"])
        assert self.matcher.matches(item, {}, _DEFAULT_CONTEXT) is False


class TestHasNoFileMatcher:
    """Tests for HasNoFileMatcher."""

    def setup_method(self) -> None:
        self.matcher = HasNoFileMatcher()

    def test_returns_true_when_no_file(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=[])
        assert self.matcher.matches(item, {}, _NO_FILE_CONTEXT) is True

    def test_returns_false_when_has_file(self) -> None:
        item = QueueItem(source="radarr", source_id="1", tags=[])
        assert self.matcher.matches(item, {}, _DEFAULT_CONTEXT) is False

    def test_tags_are_irrelevant(self) -> None:
        """has_no_file ignores item tags entirely."""
        item = QueueItem(source="radarr", source_id="1", tags=["request", "overseerr"])
        assert self.matcher.matches(item, {}, _NO_FILE_CONTEXT) is True

    def test_sonarr_episode_no_file(self) -> None:
        item = QueueItem(source="sonarr", source_id="42", tags=[])
        assert self.matcher.matches(item, {}, _NO_FILE_CONTEXT) is True


class TestMatcherRegistry:
    """Tests for the matcher registry."""

    def test_tags_registered(self) -> None:
        assert "tags" in MATCHER_REGISTRY
        assert MATCHER_REGISTRY["tags"] is TagMatcher

    def test_has_no_file_registered(self) -> None:
        assert "has_no_file" in MATCHER_REGISTRY
        assert MATCHER_REGISTRY["has_no_file"] is HasNoFileMatcher

    def test_register_custom_matcher(self) -> None:
        from conductarr.queue.matchers import BaseMatcher

        class DummyMatcher(BaseMatcher):
            def matches(
                self, item: QueueItem, config: dict[str, Any], context: AssignContext
            ) -> bool:
                return True

        register_matcher("dummy", DummyMatcher)
        assert MATCHER_REGISTRY["dummy"] is DummyMatcher
        # Cleanup
        del MATCHER_REGISTRY["dummy"]
