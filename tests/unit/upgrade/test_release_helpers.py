"""Unit tests for release-filtering and media-condition helpers in the orchestrator."""

from __future__ import annotations

from conductarr.clients.release import ReleaseResult
from conductarr.config import AcceptConditionConfig
from conductarr.orchestrator import _filter_releases, _media_satisfies_conditions


def _make_release(
    guid: str = "abc",
    title: str = "Test Release",
    custom_formats: list[str] | None = None,
    custom_format_score: int = 100,
    download_allowed: bool = True,
) -> ReleaseResult:
    return ReleaseResult(
        guid=guid,
        title=title,
        indexer_id=1,
        custom_formats=custom_formats or [],
        custom_format_score=custom_format_score,
        download_allowed=download_allowed,
    )


# ---------------------------------------------------------------------------
# _filter_releases
# ---------------------------------------------------------------------------


class TestFilterReleases:
    def test_no_conditions_returns_all(self) -> None:
        releases = [_make_release(guid="a"), _make_release(guid="b")]
        assert _filter_releases(releases, []) == releases

    def test_custom_format_condition(self) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        releases = [
            _make_release(guid="a", custom_formats=["German DL", "HI"]),
            _make_release(guid="b", custom_formats=["HI"]),
        ]
        result = _filter_releases(releases, [cond])
        assert len(result) == 1
        assert result[0].guid == "a"

    def test_custom_format_min_score_condition(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=80)
        releases = [
            _make_release(guid="a", custom_format_score=90),
            _make_release(guid="b", custom_format_score=50),
        ]
        result = _filter_releases(releases, [cond])
        assert len(result) == 1
        assert result[0].guid == "a"

    def test_multiple_conditions_and_combined(self) -> None:
        conds = [
            AcceptConditionConfig(type="custom_format", name="German DL"),
            AcceptConditionConfig(type="custom_format_min_score", value=80),
        ]
        releases = [
            _make_release(
                guid="a", custom_formats=["German DL"], custom_format_score=90
            ),
            _make_release(
                guid="b", custom_formats=["German DL"], custom_format_score=50
            ),
            _make_release(guid="c", custom_formats=[], custom_format_score=90),
        ]
        result = _filter_releases(releases, conds)
        assert len(result) == 1
        assert result[0].guid == "a"

    def test_unknown_condition_type_passes_through(self) -> None:
        cond = AcceptConditionConfig(type="unknown_type")
        release = _make_release(guid="a")
        result = _filter_releases([release], [cond])
        assert len(result) == 1


# ---------------------------------------------------------------------------
# _media_satisfies_conditions
# ---------------------------------------------------------------------------


class TestMediaSatisfiesConditions:
    def test_empty_conditions_always_satisfied(self) -> None:
        assert _media_satisfies_conditions([], 0, []) is True
        assert _media_satisfies_conditions(["German DL"], 200, []) is True

    def test_custom_format_present(self) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        assert _media_satisfies_conditions(["German DL"], 0, [cond]) is True

    def test_custom_format_absent(self) -> None:
        cond = AcceptConditionConfig(type="custom_format", name="German DL")
        assert _media_satisfies_conditions(["HI"], 0, [cond]) is False

    def test_score_at_threshold(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert _media_satisfies_conditions([], 100, [cond]) is True

    def test_score_above_threshold(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert _media_satisfies_conditions([], 150, [cond]) is True

    def test_score_below_threshold(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert _media_satisfies_conditions([], 50, [cond]) is False

    def test_multiple_conditions_all_must_match(self) -> None:
        conds = [
            AcceptConditionConfig(type="custom_format", name="German DL"),
            AcceptConditionConfig(type="custom_format_min_score", value=80),
        ]
        assert _media_satisfies_conditions(["German DL"], 90, conds) is True
        assert _media_satisfies_conditions(["German DL"], 50, conds) is False
        assert _media_satisfies_conditions([], 90, conds) is False

    def test_unknown_condition_type_passes(self) -> None:
        cond = AcceptConditionConfig(type="unknown_type")
        assert _media_satisfies_conditions([], 0, [cond]) is True
