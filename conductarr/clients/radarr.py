"""Async Radarr API client."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import urlparse

from pyarr import AsyncRadarr
from pyarr.exceptions import (
    PyarrConnectionError,
    PyarrResourceNotFound,
    PyarrUnauthorizedError,
)

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
        try:
            data = await self._get_api().queue.get(page_size=1000)
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        return [
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
        return movies

    async def get_movie(self, movie_id: int) -> RadarrMovie | None:
        """Look up a single movie by ID.  Returns ``None`` if not found."""
        try:
            raw = cast(
                dict[str, Any], await self._get_api().movie.get(item_id=movie_id)
            )
        except PyarrResourceNotFound:
            return None
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc

        return self._to_movie(raw)

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------

    async def trigger_search(self, movie_id: int) -> bool:
        """Trigger a movie search in Radarr.  Returns ``True`` on success."""
        try:
            await self._get_api().command.execute("MoviesSearch", movieIds=[movie_id])
        except PyarrUnauthorizedError as exc:
            raise RadarrAuthError(str(exc)) from exc
        except (PyarrConnectionError, ConnectionError, OSError) as exc:
            raise RadarrConnectionError(str(exc)) from exc
        except Exception as exc:
            raise RadarrError(str(exc)) from exc
        return True

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
        )
