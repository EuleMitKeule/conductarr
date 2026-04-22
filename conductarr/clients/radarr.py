"""Async Radarr API client."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, cast
from urllib.parse import urlparse

from pyarr import AsyncRadarr
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


class RadarrError(Exception):
    """Base Radarr client error."""


class RadarrConnectionError(RadarrError):
    """Raised when a connection to Radarr cannot be established."""


class RadarrAuthError(RadarrError):
    """Raised when authentication with Radarr fails."""


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RadarrQueueItem:
    download_id: str  # = SABnzbd nzo_id
    movie_id: int
    title: str
    status: str
    quality: str
    custom_format_score: int


@dataclass(frozen=True, slots=True)
class RadarrMovie:
    id: int
    title: str
    tmdb_id: int
    has_file: bool
    monitored: bool
    custom_format_score: int
    quality_profile_id: int
    tag_ids: list[int] = field(default_factory=list)
    custom_formats: list[str] = field(default_factory=list)


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
        port = 443 if tls else 7878
    return host, port, tls


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class RadarrClient:
    """Async Radarr client wrapping :class:`pyarr.AsyncRadarr`."""

    def __init__(self, url: str, api_key: str) -> None:
        self._url = url
        self._api_key = api_key
        self._api: AsyncRadarr | None = None

    def _get_api(self) -> AsyncRadarr:
        """Return the underlying client, constructing it on first use."""
        if not self._api_key:
            raise RadarrAuthError("No Radarr API key configured")
        if self._api is None:
            host, port, tls = _parse_url(self._url)
            self._api = AsyncRadarr(
                host=host, api_key=self._api_key, port=port, tls=tls
            )
        return self._api

    # ------------------------------------------------------------------
    # Queue
    # ------------------------------------------------------------------

    async def get_queue(self) -> list[RadarrQueueItem]:
        """Return all current Radarr queue items."""
        _LOGGER.debug("Radarr get_queue: fetching up to 1000 records")
        try:
            data = await self._get_api().queue.get(page_size=1000)
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        records = [
            RadarrQueueItem(
                download_id=item.get("downloadId", ""),
                movie_id=item.get("movieId", 0),
                title=item.get("title", ""),
                status=item.get("status", ""),
                quality=(item.get("quality", {}).get("quality", {}).get("name", "")),
                custom_format_score=item.get("customFormatScore", 0),
            )
            for item in data.get("records", [])
        ]
        _LOGGER.debug("Radarr get_queue: got %d records", len(records))
        return records

    # ------------------------------------------------------------------
    # Movies
    # ------------------------------------------------------------------

    async def get_movies(
        self,
        *,
        monitored: bool | None = None,
        has_file: bool | None = None,
    ) -> list[RadarrMovie]:
        """Return movies, optionally filtered by *monitored* and *has_file*."""
        _LOGGER.debug(
            "Radarr get_movies: fetching all movies (monitored=%s, has_file=%s)",
            monitored,
            has_file,
        )
        try:
            raw = cast(list[dict[str, Any]], await self._get_api().movie.get())
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        movies = [self._to_movie(m) for m in raw]
        if monitored is not None:
            movies = [m for m in movies if m.monitored is monitored]
        if has_file is not None:
            movies = [m for m in movies if m.has_file is has_file]
        _LOGGER.debug("Radarr get_movies: got %d movies", len(movies))
        return movies

    async def get_movie(self, movie_id: int) -> RadarrMovie | None:
        """Look up a single movie by ID.  Returns ``None`` if not found."""
        _LOGGER.debug("Radarr get_movie: movie_id=%d", movie_id)
        try:
            raw = cast(
                dict[str, Any], await self._get_api().movie.get(item_id=movie_id)
            )
        except PyarrResourceNotFound:
            _LOGGER.debug("Radarr get_movie: movie_id=%d not found", movie_id)
            return None
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        movie = self._to_movie(raw)
        _LOGGER.debug("Radarr get_movie: found '%s'", movie.title)
        return movie

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def trigger_search(self, movie_id: int) -> bool:
        """Trigger a movie search in Radarr.  Returns ``True`` on success.

        .. warning::
            This initiates an actual download search in Radarr.  Only call
            from the :class:`~conductarr.upgrade.scheduler.UpgradeScheduler`.
        """
        _LOGGER.debug("Radarr trigger_search: movie_id=%d", movie_id)
        try:
            await self._get_api().command.execute("MoviesSearch", movieIds=[movie_id])
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc
        _LOGGER.debug(
            "Radarr trigger_search: command dispatched for movie_id=%d", movie_id
        )
        return True

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    async def get_tags(self) -> dict[int, str]:
        """Return a mapping of tag_id → label for all Radarr tags."""
        _LOGGER.debug("Radarr get_tags: fetching all tags")
        try:
            raw = cast(list[dict[str, Any]], await self._get_api().tag.get())
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc
        tag_map = {t["id"]: t["label"] for t in raw}
        _LOGGER.debug("Radarr get_tags: got %d tag(s)", len(tag_map))
        return tag_map

    async def get_movie_tags(self, movie_id: int) -> list[str]:
        """Return tag labels for the given movie."""
        _LOGGER.debug("Radarr get_movie_tags: movie_id=%d", movie_id)
        try:
            raw = cast(
                dict[str, Any], await self._get_api().movie.get(item_id=movie_id)
            )
        except PyarrResourceNotFound:
            return []
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        tag_ids: list[int] = raw.get("tags", [])
        if not tag_ids:
            return []
        tag_map = await self.get_tags()
        tags = [tag_map[tid] for tid in tag_ids if tid in tag_map]
        _LOGGER.debug(
            "Radarr get_movie_tags: movie_id=%d has %d tag(s)", movie_id, len(tags)
        )
        return tags

    # ------------------------------------------------------------------
    # Releases
    # ------------------------------------------------------------------

    async def search_releases(self, movie_id: int) -> list[ReleaseResult]:
        """Search for available releases for *movie_id* via GET /api/v3/release."""
        _LOGGER.debug("Radarr search_releases: movie_id=%d", movie_id)
        try:
            raw = await self._get_api().release.get(movie_id=movie_id)
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        results = [self._to_release(item) for item in raw]
        _LOGGER.debug(
            "Radarr search_releases: movie_id=%d got %d release(s)",
            movie_id,
            len(results),
        )
        return results

    async def grab_release(self, release: ReleaseResult) -> None:
        """Force-grab *release* via POST /api/v3/release."""
        _LOGGER.debug(
            "Radarr grab_release: guid=%s title='%s'", release.guid, release.title
        )
        try:
            await self._get_api().release.add(release.guid, release.indexer_id)
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc
        _LOGGER.debug("Radarr grab_release: successfully grabbed '%s'", release.title)

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
    def _to_movie(data: dict[str, Any]) -> RadarrMovie:
        return RadarrMovie(
            id=data["id"],
            title=data.get("title", ""),
            tmdb_id=data.get("tmdbId", 0),
            has_file=data.get("hasFile", False),
            monitored=data.get("monitored", False),
            custom_format_score=data.get("customFormatScore", 0),
            quality_profile_id=data.get("qualityProfileId", 0),
            tag_ids=list(data.get("tags", [])),
            custom_formats=[cf.get("name", "") for cf in data.get("customFormats", [])],
        )
