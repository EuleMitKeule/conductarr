"""Unit tests for the release-selection pipeline (the grab safety net)."""

from __future__ import annotations

from typing import Any

import pytest

from conductarr.clients.release import MediaState, ReleaseResult
from conductarr.config import AcceptConditionConfig, UpgradeConfig
from conductarr.upgrade.selection import (
    Outcome,
    filter_releases,
    media_satisfies_conditions,
    select_release,
    unexpected_rejections,
)

GERMAN_DL = AcceptConditionConfig(type="custom_format", name="German DL")


def _upgrade(**kwargs: Any) -> UpgradeConfig:
    kwargs.setdefault("accept_conditions", [GERMAN_DL])
    return UpgradeConfig(**kwargs)


def _media(score: int = 100, resolution: int = 1080, media_id: int = 1) -> MediaState:
    return MediaState(
        media_id=media_id,
        title="Movie",
        monitored=True,
        has_file=True,
        custom_formats=[],
        custom_format_score=score,
        resolution=resolution,
    )


def _release(**kwargs: Any) -> ReleaseResult:
    kwargs.setdefault("guid", "g1")
    kwargs.setdefault("title", "Movie.German.DL.1080p")
    kwargs.setdefault("indexer_id", 1)
    kwargs.setdefault("custom_formats", ["German DL"])
    kwargs.setdefault("custom_format_score", 500)
    kwargs.setdefault("resolution", 1080)
    return ReleaseResult(**kwargs)


def _select(
    releases: list[ReleaseResult],
    media: MediaState | None = None,
    upgrade: UpgradeConfig | None = None,
    blocklist: set[str] | None = None,
    previously_grabbed: set[str] | None = None,
) -> Any:
    return select_release(
        releases,
        media=media or _media(),
        upgrade=upgrade or _upgrade(),
        blocklist=blocklist or set(),
        previously_grabbed=previously_grabbed,
    )


class TestConditions:
    def test_media_satisfies_custom_format(self) -> None:
        assert media_satisfies_conditions(["German DL"], 0, [GERMAN_DL])
        assert not media_satisfies_conditions(["English"], 0, [GERMAN_DL])

    def test_media_satisfies_min_score(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=100)
        assert media_satisfies_conditions([], 100, [cond])
        assert not media_satisfies_conditions([], 99, [cond])

    def test_filter_releases_requires_all_conditions(self) -> None:
        cond = AcceptConditionConfig(type="custom_format_min_score", value=300)
        good = _release(custom_format_score=400)
        low = _release(guid="g2", custom_format_score=200)
        wrong = _release(guid="g3", custom_formats=["English"])
        assert filter_releases([good, low, wrong], [GERMAN_DL, cond]) == [good]


class TestSelectRelease:
    def test_picks_best_scoring_release(self) -> None:
        a = _release(guid="a", title="a", custom_format_score=300)
        b = _release(guid="b", title="b", custom_format_score=700)
        result = _select([a, b])
        assert result.outcome == Outcome.WOULD_GRAB
        assert result.best == b

    def test_no_releases(self) -> None:
        assert _select([]).outcome == Outcome.NO_RELEASES

    def test_torrents_are_never_selected(self) -> None:
        result = _select([_release(protocol="torrent", custom_format_score=9999)])
        assert result.outcome == Outcome.NO_USENET
        assert result.best is None

    def test_unknown_protocol_is_treated_as_not_usenet(self) -> None:
        assert _select([_release(protocol="unknown")]).outcome == Outcome.NO_USENET

    def test_usenet_chosen_even_if_torrent_scores_higher(self) -> None:
        torrent = _release(
            guid="t", title="t", protocol="torrent", custom_format_score=900
        )
        usenet = _release(guid="u", title="u", custom_format_score=300)
        assert _select([torrent, usenet]).best == usenet

    def test_conditions_not_met(self) -> None:
        result = _select([_release(custom_formats=["English"])])
        assert result.outcome == Outcome.NO_CONDITION_MATCH

    def test_release_mapped_to_other_movie_is_dropped(self) -> None:
        result = _select([_release(mapped_media_ids=[99])], media=_media(media_id=1))
        assert result.outcome == Outcome.REJECTED

    def test_release_mapped_to_same_movie_is_kept(self) -> None:
        result = _select([_release(mapped_media_ids=[1])], media=_media(media_id=1))
        assert result.outcome == Outcome.WOULD_GRAB

    def test_season_pack_allowed_by_default(self) -> None:
        pack = _release(full_season=True, mapped_media_ids=[1, 2, 3])
        assert _select([pack]).outcome == Outcome.WOULD_GRAB

    def test_season_pack_can_be_disabled(self) -> None:
        pack = _release(full_season=True, mapped_media_ids=[1, 2, 3])
        multi = _release(guid="g2", mapped_media_ids=[1, 2])
        result = _select([pack, multi], upgrade=_upgrade(allow_season_packs=False))
        assert result.outcome == Outcome.REJECTED

    def test_pack_without_confirmed_mapping_is_dropped(self) -> None:
        unmapped = _release(full_season=True, mapped_media_ids=[])
        other_season = _release(guid="g2", full_season=True, mapped_media_ids=[7, 8])
        assert _select([unmapped, other_season]).outcome == Outcome.REJECTED

    def test_higher_scoring_pack_beats_single_episode(self) -> None:
        single = _release(guid="s", title="s", custom_format_score=500)
        pack = _release(
            guid="p",
            title="p",
            full_season=True,
            mapped_media_ids=[1, 2],
            custom_format_score=800,
        )
        assert _select([single, pack]).best == pack

    def test_single_episode_preferred_on_equal_score(self) -> None:
        pack = _release(guid="p", title="p", full_season=True, mapped_media_ids=[1, 2])
        single = _release(guid="s", title="s")
        assert _select([pack, single]).best == single

    @pytest.mark.parametrize(
        "rejection",
        [
            "Existing file on disk is of equal or higher preference: Bluray-1080p",
            "Existing file meets cutoff: Bluray-1080p",
            "Quality profile does not allow upgrades",
            "Existing file on disk has a equal or higher Custom Format score: 100",
            "Cutoff has already been met",
        ],
    )
    def test_upgrade_related_rejections_are_allowed(self, rejection: str) -> None:
        result = _select([_release(rejections=[rejection])])
        assert result.outcome == Outcome.WOULD_GRAB

    @pytest.mark.parametrize(
        "rejection",
        [
            "Wrong movie",
            "Unknown Movie",
            "WEBDL-480p is not wanted in profile",
            "Language is not wanted in profile",
            "Contains one or more restricted terms",
            "Release in queue already meets cutoff: Bluray-1080p",
            "Release is blocklisted",
            "150.0 GB is larger than maximum allowed 100.0 GB",
        ],
    )
    def test_other_rejections_block_the_grab(self, rejection: str) -> None:
        release = _release(
            rejections=["Existing file meets cutoff: Bluray-1080p", rejection]
        )
        result = _select([release])
        assert result.outcome == Outcome.REJECTED
        assert result.best is None

    def test_unexpected_rejections_helper(self) -> None:
        release = _release(rejections=["Existing file meets cutoff", "Wrong movie"])
        assert unexpected_rejections(release, ["cutoff"]) == ["Wrong movie"]

    def test_not_downloadable_is_transient(self) -> None:
        result = _select([_release(download_allowed=False)])
        assert result.outcome == Outcome.TRANSIENT

    def test_blocklisted_is_transient(self) -> None:
        release = _release()
        result = _select([release], blocklist={release.title})
        assert result.outcome == Outcome.TRANSIENT

    def test_previously_grabbed_title_is_not_grabbed_again(self) -> None:
        release = _release()
        result = _select([release], previously_grabbed={release.title})
        assert result.outcome == Outcome.TRANSIENT

    def test_resolution_downgrade_is_blocked(self) -> None:
        result = _select(
            [_release(resolution=1080, custom_format_score=5000)],
            media=_media(score=0, resolution=2160),
        )
        assert result.outcome == Outcome.NO_IMPROVEMENT

    def test_resolution_downgrade_allowed_when_configured(self) -> None:
        result = _select(
            [_release(resolution=1080)],
            media=_media(score=0, resolution=2160),
            upgrade=_upgrade(allow_resolution_downgrade=True),
        )
        assert result.outcome == Outcome.WOULD_GRAB

    def test_unknown_release_resolution_is_blocked_if_file_resolution_known(
        self,
    ) -> None:
        result = _select([_release(resolution=0)], media=_media(score=0))
        assert result.outcome == Outcome.NO_IMPROVEMENT

    def test_score_must_improve(self) -> None:
        result = _select([_release(custom_format_score=100)], media=_media(score=100))
        assert result.outcome == Outcome.NO_IMPROVEMENT

    def test_min_score_increase(self) -> None:
        upgrade = _upgrade(min_score_increase=50)
        media = _media(score=100)
        assert _select([_release(custom_format_score=149)], media, upgrade).outcome == (
            Outcome.NO_IMPROVEMENT
        )
        assert _select([_release(custom_format_score=150)], media, upgrade).outcome == (
            Outcome.WOULD_GRAB
        )

    def test_steps_are_reported(self) -> None:
        result = _select([_release()])
        assert [name for name, _ in result.steps] == [
            "usenet",
            "conditions",
            "mapping",
            "rejections",
            "downloadable",
            "blocklist",
            "resolution",
            "score",
        ]
