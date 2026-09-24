"""Pure release-selection pipeline for upgrade grabs.

Every release returned by an interactive Arr search runs through the same
ordered filter chain, both in the live scheduler and in ``debug-upgrades``.
The chain is deliberately conservative: when in doubt a release is dropped,
because a wrong grab can replace a good file in the library.

Filter order
------------
1. ``usenet``      - only usenet releases; torrents are never touched.
2. ``conditions``  - release satisfies every ``accept_condition``.
3. ``mapping``     - release maps to the searched movie/episode; season
                     packs only if allowed and confirmed to contain it.
4. ``rejections``  - every Arr rejection is on the allow-list (by default only
                     the "not an upgrade / cutoff met" family that exists
                     because the Arr itself is not allowed to upgrade).
5. ``downloadable``- ``downloadAllowed`` is true (transient).
6. ``blocklist``   - not blocklisted and not grabbed for this item before.
7. ``resolution``  - not lower than the current file's resolution.
8. ``score``       - custom-format score beats the current file.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import StrEnum

from conductarr.clients.release import MediaState, ReleaseResult
from conductarr.config import AcceptConditionConfig, UpgradeConfig

_LOGGER = logging.getLogger(__name__)


class Outcome(StrEnum):
    WOULD_GRAB = "would_grab"
    NO_RELEASES = "no_releases"
    NO_USENET = "no_usenet"
    NO_CONDITION_MATCH = "all_filtered_conditions"
    REJECTED = "all_filtered_rejected"
    TRANSIENT = "all_filtered_transient"
    NO_IMPROVEMENT = "no_score_improvement"


# Outcomes after which the item is retried after ``no_release_retry_days``
# instead of ``retry_after_days``.
TRANSIENT_OUTCOMES = frozenset({Outcome.TRANSIENT})


@dataclass
class SelectionResult:
    outcome: Outcome
    reason: str
    releases_total: int
    steps: list[tuple[str, int]] = field(default_factory=list)
    """``(filter_name, releases_remaining)`` after each filter step."""
    best: ReleaseResult | None = None


def media_satisfies_conditions(
    custom_formats: list[str],
    custom_format_score: int,
    conditions: list[AcceptConditionConfig],
) -> bool:
    """Return ``True`` if existing media already satisfies ALL *conditions*."""
    for cond in conditions:
        if cond.type == "custom_format":
            if cond.name not in custom_formats:
                return False
        elif cond.type == "custom_format_min_score":
            if custom_format_score < cond.value:
                return False
    return True


def release_matches_conditions(
    release: ReleaseResult, conditions: list[AcceptConditionConfig]
) -> bool:
    return media_satisfies_conditions(
        release.custom_formats, release.custom_format_score, conditions
    )


def filter_releases(
    releases: list[ReleaseResult], conditions: list[AcceptConditionConfig]
) -> list[ReleaseResult]:
    """Return only releases that satisfy ALL *conditions*."""
    return [r for r in releases if release_matches_conditions(r, conditions)]


# Rejections that always block a grab, even if an allow-list entry matches
# (e.g. "Release in queue already meets cutoff" contains "cutoff").
_ALWAYS_BLOCKING = ("in queue", "blocklist")


def unexpected_rejections(release: ReleaseResult, allowed: list[str]) -> list[str]:
    """Return the rejections of *release* that are not on the allow-list."""
    allowed_lower = [a.lower() for a in allowed if a]
    result: list[str] = []
    for rejection in release.rejections:
        text = rejection.lower()
        if any(b in text for b in _ALWAYS_BLOCKING) or not any(
            a in text for a in allowed_lower
        ):
            result.append(rejection)
    return result


def is_pack(release: ReleaseResult) -> bool:
    return release.full_season or len(release.mapped_media_ids) > 1


def _maps_to_single_item(
    release: ReleaseResult, media_id: int, allow_packs: bool
) -> bool:
    if release.mapped_media_ids and media_id not in release.mapped_media_ids:
        return False  # the Arr mapped it to a different movie/episode
    if not is_pack(release):
        return True
    # A pack is only safe if the Arr confirmed it contains the searched episode.
    return allow_packs and media_id in release.mapped_media_ids


def select_release(
    releases: list[ReleaseResult],
    *,
    media: MediaState,
    upgrade: UpgradeConfig,
    blocklist: set[str],
    previously_grabbed: set[str] | None = None,
) -> SelectionResult:
    """Run the filter chain and pick the best remaining release."""
    total = len(releases)
    steps: list[tuple[str, int]] = []
    previously_grabbed = previously_grabbed or set()

    def done(outcome: Outcome, reason: str) -> SelectionResult:
        return SelectionResult(outcome, reason, total, steps)

    if not releases:
        return done(Outcome.NO_RELEASES, "Indexer returned no releases")

    remaining = [r for r in releases if r.protocol == "usenet"]
    steps.append(("usenet", len(remaining)))
    if not remaining:
        return done(Outcome.NO_USENET, f"{total} release(s) but none via usenet")

    remaining = filter_releases(remaining, upgrade.accept_conditions)
    steps.append(("conditions", len(remaining)))
    if not remaining:
        return done(
            Outcome.NO_CONDITION_MATCH,
            f"{total} release(s) but none match accept_conditions",
        )

    remaining = [
        r
        for r in remaining
        if _maps_to_single_item(r, media.media_id, upgrade.allow_season_packs)
    ]
    steps.append(("mapping", len(remaining)))
    rejected_reasons: set[str] = set()
    kept: list[ReleaseResult] = []
    for release in remaining:
        unexpected = unexpected_rejections(release, upgrade.allowed_rejections)
        if unexpected:
            rejected_reasons.update(unexpected)
        else:
            kept.append(release)
    remaining = kept
    steps.append(("rejections", len(remaining)))
    if not remaining:
        detail = "; ".join(sorted(rejected_reasons)[:3])
        return done(
            Outcome.REJECTED,
            "matching release(s) are packs, map to another item or were "
            f"rejected by the Arr{f': {detail}' if detail else ''}",
        )

    remaining = [r for r in remaining if r.download_allowed]
    steps.append(("downloadable", len(remaining)))
    remaining = [
        r
        for r in remaining
        if r.title not in blocklist and r.title not in previously_grabbed
    ]
    steps.append(("blocklist", len(remaining)))
    if not remaining:
        return done(
            Outcome.TRANSIENT,
            "matching release(s) are not downloadable, blocklisted or were "
            "already grabbed for this item",
        )

    if not upgrade.allow_resolution_downgrade and media.resolution > 0:
        remaining = [r for r in remaining if r.resolution >= media.resolution]
    steps.append(("resolution", len(remaining)))
    required = media.custom_format_score + upgrade.min_score_increase
    remaining = [r for r in remaining if r.custom_format_score >= required]
    steps.append(("score", len(remaining)))
    if not remaining:
        return done(
            Outcome.NO_IMPROVEMENT,
            f"no release reaches score {required} without lowering the "
            f"resolution (current: score {media.custom_format_score}, "
            f"{media.resolution or '?'}p)",
        )

    # Best score wins; on a tie prefer the single episode over a pack.
    best = max(
        remaining,
        key=lambda r: (r.custom_format_score, r.resolution, not is_pack(r)),
    )
    result = done(
        Outcome.WOULD_GRAB,
        f"'{best.title}' (score {best.custom_format_score}, {best.quality or '?'})",
    )
    result.best = best
    return result
