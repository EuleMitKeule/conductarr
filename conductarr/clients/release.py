"""Shared release dataclass used by both Radarr and Sonarr clients."""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["ReleaseResult"]


@dataclass(frozen=True, slots=True)
class ReleaseResult:
    """A release candidate returned by /api/v3/release."""

    guid: str
    title: str
    indexer_id: int
    custom_formats: list[str] = field(default_factory=list)
    custom_format_score: int = 0
    quality: str = ""
    size: int = 0
    download_allowed: bool = True
