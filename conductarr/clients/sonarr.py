"""Async Sonarr API client."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

from pyarr import AsyncSonarr
from pyarr.exceptions import (
    PyarrConnectionError,
    PyarrResourceNotFound,
    PyarrUnauthorizedError,
)

from conductarr.clients.release import ReleaseResult

_LOGGER = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class SonarrError(Exception):
    """Base Sonarr client error."""


class SonarrConnectionError(SonarrError):
    """Raised when a connection to Sonarr cannot be established."""


class SonarrAuthError(SonarrError):
    """Raised when authentication with Sonarr fails."""


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SonarrQueueItem:
    download_id: str  # = SABnzbd nzo_id
    series_id: int
    episode_id: int
    title: str
    status: str
    quality: str
    custom_format_score: int


@dataclass(frozen=True, slots=True)
class SonarrSeries:
    id: int
    title: str
    tvdb_id: int
    monitored: bool
    status: str


@dataclass(frozen=True, slots=True)
class SonarrEpisode:
    id: int
    series_id: int
    episode_number: int
    season_number: int
    title: str
    monitored: bool
    has_file: bool
    custom_format_score: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_url(url: str) -> tuple[str, int, bool]:
    """Return (host, port, tls) from a full URL string."""
    parsed = urlparse(url)
    tls = parsed.scheme == "https"
    host = parsed.hostname or "localhost"
    if parsed.port:
        port = parsed.port
    else:
        port = 443 if tls else 8989
    return host, port, tls


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class SonarrClient:
    """Async Sonarr client wrapping :class:`pyarr.AsyncSonarr`."""

    def __init__(self, url: str, api_key: str) -> None:
        self._url = url
        self._api_key = api_key
        self._api: AsyncSonarr | None = None

    def _get_api(self) -> AsyncSonarr:
        """Return the underlying client, constructing it on first use."""
        if not self._api_key:
            raise SonarrAuthError("No Sonarr API key configured")
        if self._api is None:
            host, port, tls = _parse_url(self._url)
            self._api = AsyncSonarr(
                host=host, api_key=self._api_key, port=port, tls=tls
            )
        return self._api

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------

    async def get_queue(self) -> list[SonarrQueueItem]:
        """Return all current Sonarr queue items."""
        try:
            data = await self._get_api().queue.get(page_size=1000)
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        return [
            SonarrQueueItem(
                download_id=item.get("downloadId", ""),
                series_id=item.get("seriesId", 0),
                episode_id=item.get("episodeId", 0),
                title=item.get("title", ""),
                status=item.get("status", ""),
                quality=(item.get("quality", {}).get("quality", {}).get("name", "")),
                custom_format_score=item.get("customFormatScore", 0),
            )
            for item in data.get("records", [])
        ]

    # ------------------------------------------------------------------
    # Series
    # ------------------------------------------------------------------

    async def get_series(self, *, monitored: bool | None = None) -> list[SonarrSeries]:
        """Return all series, optionally filtered by *monitored*."""
        try:
            raw = cast(list[dict[str, Any]], await self._get_api().series.get())
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        series = [
            SonarrSeries(
                id=s["id"],
                title=s.get("title", ""),
                tvdb_id=s.get("tvdbId", 0),
                monitored=s.get("monitored", False),
                status=s.get("status", ""),
            )
            for s in raw
        ]
        if monitored is not None:
            series = [s for s in series if s.monitored is monitored]
        return series

    # ------------------------------------------------------------------
    # Episodes
    # ------------------------------------------------------------------

    async def get_episodes(
        self,
        series_id: int,
        *,
        monitored: bool | None = None,
        has_file: bool | None = None,
    ) -> list[SonarrEpisode]:
        """Return episodes for a series, optionally filtered."""
        try:
            raw = cast(
                list[dict[str, Any]],
                await self._get_api().episode.get(series_id=series_id),
            )
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        episodes = [self._to_episode(e) for e in raw]
        if monitored is not None:
            episodes = [e for e in episodes if e.monitored is monitored]
        if has_file is not None:
            episodes = [e for e in episodes if e.has_file is has_file]
        return episodes

    async def get_episode(self, episode_id: int) -> SonarrEpisode | None:
        """Look up a single episode by ID.  Returns ``None`` if not found."""
        try:
            raw = cast(
                dict[str, Any],
                await self._get_api().episode.get(item_id=episode_id),
            )
        except PyarrResourceNotFound:
            return None
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        return self._to_episode(raw)

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def trigger_episode_search(self, episode_id: int) -> bool:
        """Trigger an episode search.  Returns ``True`` on success."""
        try:
            await self._get_api().command.execute(
                "EpisodeSearch", episodeIds=[episode_id]
            )
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc
        return True

    async def trigger_season_search(self, series_id: int, season_number: int) -> bool:
        """Trigger a season search.  Returns ``True`` on success."""
        try:
            await self._get_api().command.execute(
                "SeasonSearch",
                seriesId=series_id,
                seasonNumber=season_number,
            )
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc
        return True

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    async def get_tags(self) -> dict[int, str]:
        """Return a mapping of tag_id → label for all Sonarr tags."""
        try:
            raw = cast(list[dict[str, Any]], await self._get_api().tag.get())
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc
        return {t["id"]: t["label"] for t in raw}

    async def get_episode_tags(self, episode_id: int) -> list[str]:
        """Return tag labels for the parent series of the given episode."""
        try:
            ep_raw = cast(
                dict[str, Any],
                await self._get_api().episode.get(item_id=episode_id),
            )
        except PyarrResourceNotFound:
            return []
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        series_id: int = ep_raw.get("seriesId", 0)
        if not series_id:
            return []

        try:
            series_raw = cast(
                dict[str, Any],
                await self._get_api().series.get(item_id=series_id),
            )
        except PyarrResourceNotFound:
            return []
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        tag_ids: list[int] = series_raw.get("tags", [])
        if not tag_ids:
            return []
        tag_map = await self.get_tags()
        return [tag_map[tid] for tid in tag_ids if tid in tag_map]

    # ------------------------------------------------------------------
    # Releases
    # ------------------------------------------------------------------

    async def search_releases(self, series_id: int) -> list[ReleaseResult]:
        """Search for available releases for *series_id* via GET /api/v3/release."""
        try:
            raw = await self._get_api().http_utils.request(
                "release", params={"seriesId": series_id}
            )
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

        return [self._to_release(item) for item in (raw or [])]

    async def grab_release(self, release: ReleaseResult) -> None:
        """Force-grab *release* via POST /api/v3/release."""
        payload: dict[str, Any] = {
            "guid": release.guid,
            "indexerId": release.indexer_id,
        }
        try:
            await self._get_api().http_utils.request(
                "release", method="POST", json_data=payload
            )
        except PyarrUnauthorizedError as exc:
            raise SonarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise SonarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise SonarrError(str(exc)) from exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_release(data: dict[str, Any]) -> ReleaseResult:
        return ReleaseResult(
            guid=data.get("guid", ""),
            title=data.get("title", ""),
            indexer_id=data.get("indexerId", 0),
            custom_formats=[cf.get("name", "") for cf in data.get("customFormats", [])],
            custom_format_score=data.get("customFormatScore", 0),
            quality=data.get("quality", {}).get("quality", {}).get("name", ""),
            size=data.get("size", 0),
            download_allowed=data.get("downloadAllowed", True),
        )

    @staticmethod
    def _to_episode(data: dict[str, Any]) -> SonarrEpisode:
        return SonarrEpisode(
            id=data["id"],
            series_id=data.get("seriesId", 0),
            episode_number=data.get("episodeNumber", 0),
            season_number=data.get("seasonNumber", 0),
            title=data.get("title", ""),
            monitored=data.get("monitored", False),
            has_file=data.get("hasFile", False),
            custom_format_score=data.get("customFormatScore", 0),
        )
