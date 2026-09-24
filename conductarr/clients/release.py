"""Shared value objects used by both the Radarr and Sonarr clients."""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["ArrQueueItem", "MediaState", "ReleaseResult"]


@dataclass(frozen=True, slots=True)
class ReleaseResult:
    """A release candidate returned by ``GET /api/v3/release``."""

    guid: str
    title: str
    indexer_id: int
    custom_formats: list[str] = field(default_factory=list)
    custom_format_score: int = 0
    quality: str = ""
    resolution: int = 0
    size: int = 0
    download_allowed: bool = True
    protocol: str = "usenet"
    """Lower-cased download protocol (``usenet`` / ``torrent`` / ``unknown``)."""
    rejections: list[str] = field(default_factory=list)
    indexer: str = ""
    full_season: bool = False
    """Sonarr only: the release is a full-season pack."""
    mapped_media_ids: list[int] = field(default_factory=list)
    """IDs of the movie/episodes the Arr mapped this release to (may be empty)."""


@dataclass(frozen=True, slots=True)
class MediaState:
    """Current library state of a single movie or episode."""

    media_id: int
    title: str
    monitored: bool
    has_file: bool
    custom_formats: list[str] = field(default_factory=list)
    custom_format_score: int = 0
    quality: str = ""
    resolution: int = 0


@dataclass(frozen=True, slots=True)
class ArrQueueItem:
    """A single record from an Arr ``/api/v3/queue`` response."""

    download_id: str  # = SABnzbd nzo_id for usenet downloads
    media_id: int  # movie_id (Radarr) or episode_id (Sonarr)
    title: str
    protocol: str = ""
