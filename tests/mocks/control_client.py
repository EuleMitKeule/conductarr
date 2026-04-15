"""Python client classes for mock service control APIs."""

from __future__ import annotations

from typing import Any, cast

import httpx


class _BaseControlClient:
    """Shared helpers for control clients."""

    def __init__(self, base_url: str) -> None:
        self._base = base_url.rstrip("/")

    async def _post(
        self, path: str, json: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=self._base) as client:
            resp = await client.post(path, json=json or {})
        resp.raise_for_status()
        return cast(dict[str, Any], resp.json())

    async def _get(self, path: str) -> dict[str, Any]:
        async with httpx.AsyncClient(base_url=self._base) as client:
            resp = await client.get(path)
        resp.raise_for_status()
        return cast(dict[str, Any], resp.json())


class SABnzbdControlClient(_BaseControlClient):
    def __init__(self, base_url: str = "http://localhost:8080") -> None:
        super().__init__(base_url)

    async def reset(self) -> None:
        await self._post("/control/reset")

    async def start_job(
        self,
        filename: str,
        cat: str,
        priority: str = "Normal",
        nzo_id: str | None = None,
    ) -> str:
        body: dict[str, Any] = {"filename": filename, "cat": cat, "priority": priority}
        if nzo_id is not None:
            body["nzo_id"] = nzo_id
        data = await self._post("/control/job/start", json=body)
        return str(data["nzo_id"])

    async def finish_job(self, nzo_id: str) -> None:
        await self._post("/control/job/finish", json={"nzo_id": nzo_id})

    async def cancel_job(self, nzo_id: str) -> None:
        await self._post("/control/job/cancel", json={"nzo_id": nzo_id})

    async def get_state(self) -> dict[str, Any]:
        return await self._get("/control/state")


class RadarrControlClient(_BaseControlClient):
    def __init__(self, base_url: str = "http://localhost:7878") -> None:
        super().__init__(base_url)

    async def reset(self) -> None:
        await self._post("/control/reset")

    async def add_movie(
        self,
        title: str,
        tmdb_id: int,
        monitored: bool = True,
        has_file: bool = False,
        custom_format_score: int = 0,
        tags: list[str] | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/control/movie/add",
            json={
                "title": title,
                "tmdb_id": tmdb_id,
                "monitored": monitored,
                "has_file": has_file,
                "custom_format_score": custom_format_score,
                "tags": tags or [],
            },
        )

    async def release_movie(self, tmdb_id: int, nzo_id: str) -> dict[str, Any]:
        return await self._post(
            "/control/movie/release",
            json={"tmdb_id": tmdb_id, "nzo_id": nzo_id},
        )

    async def finish_movie(
        self, tmdb_id: int, custom_format_score: int = 100
    ) -> dict[str, Any]:
        return await self._post(
            "/control/movie/finished",
            json={"tmdb_id": tmdb_id, "custom_format_score": custom_format_score},
        )

    async def cancel_movie(self, tmdb_id: int) -> None:
        await self._post("/control/movie/cancelled", json={"tmdb_id": tmdb_id})

    async def get_state(self) -> dict[str, Any]:
        return await self._get("/control/state")


class SonarrControlClient(_BaseControlClient):
    def __init__(self, base_url: str = "http://localhost:8989") -> None:
        super().__init__(base_url)

    async def reset(self) -> None:
        await self._post("/control/reset")

    async def add_series(
        self,
        title: str,
        tvdb_id: int,
        monitored: bool = True,
        tags: list[str] | None = None,
        episodes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        return await self._post(
            "/control/series/add",
            json={
                "title": title,
                "tvdb_id": tvdb_id,
                "monitored": monitored,
                "tags": tags or [],
                "episodes": episodes or [],
            },
        )

    async def release_episode(self, episode_id: int, nzo_id: str) -> dict[str, Any]:
        return await self._post(
            "/control/episode/release",
            json={"episode_id": episode_id, "nzo_id": nzo_id},
        )

    async def finish_episode(
        self, episode_id: int, custom_format_score: int = 100
    ) -> dict[str, Any]:
        return await self._post(
            "/control/episode/finished",
            json={"episode_id": episode_id, "custom_format_score": custom_format_score},
        )

    async def cancel_episode(self, episode_id: int) -> None:
        await self._post("/control/episode/cancelled", json={"episode_id": episode_id})

    async def get_state(self) -> dict[str, Any]:
        return await self._get("/control/state")
