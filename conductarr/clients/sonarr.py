"""Async Sonarr API client.

The Sonarr unit of work is a single *episode*: upgrade candidates, queue
resolution and release searches are all keyed by ``episode_id``.
"""

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


class SonarrClient(ArrClient):
    source: ClassVar[str] = "sonarr"

    def _queue_media_key(self) -> str:
        return "episodeId"

    async def _get_episode(self, episode_id: int) -> dict[str, Any] | None:
        try:
            data = await self._request("GET", f"episode/{episode_id}")
        except ArrNotFoundError:
            return None
        return data if isinstance(data, dict) else None

    async def _get_series(self, series_id: int) -> dict[str, Any] | None:
        try:
            data = await self._request("GET", f"series/{series_id}")
        except ArrNotFoundError:
            return None
        return data if isinstance(data, dict) else None

    async def get_media_state(self, media_id: int) -> MediaState | None:
        episode = await self._get_episode(media_id)
        if episode is None:
            return None
        label = (
            f"S{int(episode.get('seasonNumber') or 0):02d}"
            f"E{int(episode.get('episodeNumber') or 0):02d}"
        )
        series = episode.get("series")
        series_title = series.get("title") if isinstance(series, dict) else None
        title = f"{series_title} {label}" if series_title else label
        if episode.get("title"):
            title = f"{title} - {episode['title']}"

        has_file = bool(episode.get("hasFile", False))
        file_data: dict[str, Any] | None = None
        file_id = episode.get("episodeFileId")
        if has_file and file_id:
            try:
                data = await self._request("GET", f"episodeFile/{int(file_id)}")
                file_data = data if isinstance(data, dict) else None
            except ArrNotFoundError:
                file_data = None
        quality, resolution = parse_quality(file_data or {})
        return MediaState(
            media_id=media_id,
            title=title,
            monitored=bool(episode.get("monitored", False)),
            has_file=has_file and file_data is not None,
            custom_formats=parse_custom_formats(file_data or {}),
            custom_format_score=int((file_data or {}).get("customFormatScore") or 0),
            quality=quality,
            resolution=resolution,
        )

    async def get_media_tags(self, media_id: int) -> list[str]:
        """Tags live on the series, so resolve episode → series → tags."""
        episode = await self._get_episode(media_id)
        if episode is None or not episode.get("seriesId"):
            return []
        series = await self._get_series(int(episode["seriesId"]))
        if series is None:
            return []
        return await self._labels_for([int(t) for t in series.get("tags") or []])

    async def list_media_ids_with_files(self) -> list[int]:
        series_list = await self._request("GET", "series")
        ids: list[int] = []
        for series in series_list or []:
            stats = series.get("statistics") or {}
            if stats and not stats.get("episodeFileCount"):
                continue
            episodes = await self._request(
                "GET", "episode", params={"seriesId": int(series["id"])}
            )
            ids.extend(int(e["id"]) for e in episodes or [] if e.get("hasFile"))
        return sorted(ids)

    async def search_releases(self, media_id: int) -> list[ReleaseResult]:
        raw = await self._request(
            "GET",
            "release",
            params={"episodeId": media_id},
            timeout=self._search_timeout,
        )
        results: list[ReleaseResult] = []
        for item in raw or []:
            mapped = [
                int(e["id"])
                for e in item.get("mappedEpisodeInfo") or []
                if isinstance(e, dict) and e.get("id")
            ]
            results.append(parse_release(item, mapped))
        return results
