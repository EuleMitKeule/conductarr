"""Async Radarr API client."""

from __future__ import annotations

from typing import Any, ClassVar

from conductarr.clients.arr import (
    ArrClient,
    ArrNotFoundError,
    parse_custom_formats,
    parse_quality,
    parse_release,
)
from conductarr.clients.release import MediaState, ReleaseResult


class RadarrClient(ArrClient):
    source: ClassVar[str] = "radarr"

    def _queue_media_key(self) -> str:
        return "movieId"

    async def _get_movie(self, movie_id: int) -> dict[str, Any] | None:
        try:
            data = await self._request("GET", f"movie/{movie_id}")
        except ArrNotFoundError:
            return None
        return data if isinstance(data, dict) else None

    async def get_media_state(self, media_id: int) -> MediaState | None:
        movie = await self._get_movie(media_id)
        if movie is None:
            return None
        title = str(movie.get("title", ""))
        if year := movie.get("year"):
            title = f"{title} ({year})"
        has_file = bool(movie.get("hasFile", False))
        # /movieFile reliably carries customFormats + score of the file on disk;
        # the embedded movie.movieFile is not populated consistently.
        file_data: dict[str, Any] | None = None
        if has_file:
            files = await self._request(
                "GET", "movieFile", params={"movieId": media_id}
            )
            if isinstance(files, list) and files:
                file_data = files[0]
            elif isinstance(movie.get("movieFile"), dict):
                file_data = movie["movieFile"]
        quality, resolution = parse_quality(file_data or {})
        return MediaState(
            media_id=media_id,
            title=title,
            monitored=bool(movie.get("monitored", False)),
            has_file=has_file and file_data is not None,
            custom_formats=parse_custom_formats(file_data or {}),
            custom_format_score=int((file_data or {}).get("customFormatScore") or 0),
            quality=quality,
            resolution=resolution,
        )

    async def get_media_tags(self, media_id: int) -> list[str]:
        movie = await self._get_movie(media_id)
        if movie is None:
            return []
        return await self._labels_for([int(t) for t in movie.get("tags") or []])

    async def list_media_ids_with_files(self) -> list[int]:
        movies = await self._request("GET", "movie")
        return sorted(int(m["id"]) for m in movies or [] if m.get("hasFile"))

    async def search_releases(self, media_id: int) -> list[ReleaseResult]:
        raw = await self._request(
            "GET",
            "release",
            params={"movieId": media_id},
            timeout=self._search_timeout,
        )
        results: list[ReleaseResult] = []
        for item in raw or []:
            mapped = item.get("mappedMovieId") or item.get("movieId")
            results.append(parse_release(item, [int(mapped)] if mapped else []))
        return results
