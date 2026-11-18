"""Shared async HTTP client for the Radarr/Sonarr v3 API.

Both apps expose an almost identical API surface, so the transport, error
mapping, timeouts and the endpoints that are identical in both apps live
here.  :mod:`conductarr.clients.radarr` and :mod:`conductarr.clients.sonarr`
only add the media-specific bits.

Safety note: this client deliberately exposes **no** method that deletes
anything or triggers an Arr-side automatic search.  There are two write
operations: :meth:`ArrClient.grab_release` hands one explicitly selected
release to the Arr's download client, and
:meth:`ArrClient.import_completed_download` imports the files of one finished
download that conductarr grabbed itself.  The Arr replaces the episode/movie
file it had before (into its recycle bin, if one is configured), exactly as
its own completed-download handling would for an upgrade.
"""

from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, ClassVar

import aiohttp

from conductarr.clients.release import (
    ArrDownloadStatus,
    ArrQueueItem,
    MediaState,
    ReleaseResult,
)

_LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 30.0
DEFAULT_SEARCH_TIMEOUT = 180.0
_TAG_CACHE_TTL = 300.0
_PAGE_SIZE = 1000


class ArrError(Exception):
    """Base error for Radarr/Sonarr API failures."""


class ArrConnectionError(ArrError):
    """The Arr instance could not be reached or timed out."""


class ArrAuthError(ArrError):
    """The API key was rejected or is missing."""


class ArrNotFoundError(ArrError):
    """The requested resource does not exist (HTTP 404)."""


def _nested_get(data: dict[str, Any], *keys: str) -> Any:
    current: Any = data
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def parse_custom_formats(data: dict[str, Any]) -> list[str]:
    return [
        str(cf.get("name", ""))
        for cf in data.get("customFormats") or []
        if isinstance(cf, dict)
    ]


def parse_quality(data: dict[str, Any]) -> tuple[str, int]:
    """Return ``(quality_name, resolution)`` from an Arr ``quality`` object."""
    name = _nested_get(data, "quality", "quality", "name") or ""
    resolution = _nested_get(data, "quality", "quality", "resolution") or 0
    try:
        return str(name), int(resolution)
    except TypeError, ValueError:
        return str(name), 0


def _parse_rejections(raw: Any) -> list[str]:
    """Normalise ``rejections`` (list of strings or of ``{reason, message}``)."""
    result: list[str] = []
    for item in raw or []:
        if isinstance(item, str):
            result.append(item)
        elif isinstance(item, dict):
            text = item.get("message") or item.get("reason") or ""
            if text:
                result.append(str(text))
    return result


def parse_release(
    data: dict[str, Any], mapped_media_ids: list[int] | None = None
) -> ReleaseResult:
    quality, resolution = parse_quality(data)
    protocol = str(data.get("protocol") or "unknown").lower()
    return ReleaseResult(
        guid=str(data.get("guid", "")),
        title=str(data.get("title", "")),
        indexer_id=int(data.get("indexerId") or 0),
        custom_formats=parse_custom_formats(data),
        custom_format_score=int(data.get("customFormatScore") or 0),
        quality=quality,
        resolution=resolution,
        size=int(data.get("size") or 0),
        download_allowed=bool(data.get("downloadAllowed", True)),
        protocol=protocol,
        rejections=_parse_rejections(data.get("rejections")),
        indexer=str(data.get("indexer") or ""),
        full_season=bool(data.get("fullSeason", False)),
        mapped_media_ids=mapped_media_ids or [],
    )


class ArrClient(ABC):
    """Async client for one Radarr or Sonarr instance."""

    source: ClassVar[str]

    def __init__(
        self,
        url: str,
        api_key: str,
        *,
        timeout: float = DEFAULT_TIMEOUT,
        search_timeout: float = DEFAULT_SEARCH_TIMEOUT,
    ) -> None:
        self._base_url = url.rstrip("/")
        self._api_key = api_key
        self._timeout = timeout
        self._search_timeout = search_timeout
        self._session: aiohttp.ClientSession | None = None
        self._tag_cache: dict[int, str] = {}
        self._tag_cache_at: float = 0.0

    # ------------------------------------------------------------------
    # Transport
    # ------------------------------------------------------------------

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
        timeout: float | None = None,
    ) -> Any:
        if not self._api_key:
            raise ArrAuthError(f"No {self.source} API key configured")
        url = f"{self._base_url}/api/v3/{path.lstrip('/')}"
        client_timeout = aiohttp.ClientTimeout(total=timeout or self._timeout)
        try:
            async with self._get_session().request(
                method,
                url,
                params={k: str(v) for k, v in (params or {}).items()},
                json=json,
                headers={"X-Api-Key": self._api_key},
                timeout=client_timeout,
            ) as resp:
                if resp.status in (401, 403):
                    raise ArrAuthError(f"{self.source} rejected the API key")
                if resp.status == 404:
                    raise ArrNotFoundError(f"{self.source}: {path} not found")
                if resp.status >= 400:
                    body = (await resp.text())[:300]
                    raise ArrError(
                        f"{self.source} {method} {path} failed "
                        f"(HTTP {resp.status}): {body}"
                    )
                if resp.status == 204:
                    return None
                return await resp.json(content_type=None)
        except ArrError:
            raise
        except TimeoutError as exc:
            raise ArrConnectionError(
                f"{self.source} {method} {path} timed out"
            ) from exc
        except aiohttp.ClientError as exc:
            raise ArrConnectionError(f"{self.source}: {exc}") from exc
        except ValueError as exc:  # invalid JSON
            raise ArrError(f"{self.source} returned invalid JSON: {exc}") from exc

    async def _get_paged_records(
        self, path: str, params: dict[str, Any] | None = None
    ) -> list[dict[str, Any]]:
        records: list[dict[str, Any]] = []
        page = 1
        while True:
            data = await self._request(
                "GET",
                path,
                params={**(params or {}), "page": page, "pageSize": _PAGE_SIZE},
            )
            batch = list((data or {}).get("records") or [])
            records.extend(batch)
            total = int((data or {}).get("totalRecords") or 0)
            if len(batch) < _PAGE_SIZE or len(records) >= total:
                return records
            page += 1

    # ------------------------------------------------------------------
    # Shared endpoints
    # ------------------------------------------------------------------

    async def get_tags(self) -> dict[int, str]:
        """Return ``tag_id → label``; cached for a few minutes."""
        now = time.monotonic()
        if self._tag_cache and now - self._tag_cache_at < _TAG_CACHE_TTL:
            return self._tag_cache
        raw = await self._request("GET", "tag")
        self._tag_cache = {int(t["id"]): str(t["label"]) for t in raw or []}
        self._tag_cache_at = now
        return self._tag_cache

    async def _labels_for(self, tag_ids: list[int]) -> list[str]:
        if not tag_ids:
            return []
        tag_map = await self.get_tags()
        return [tag_map[tid] for tid in tag_ids if tid in tag_map]

    async def get_blocklist_source_titles(self) -> set[str]:
        """Return the ``sourceTitle`` of every blocklisted release."""
        records = await self._get_paged_records("blocklist")
        return {str(r["sourceTitle"]) for r in records if r.get("sourceTitle")}

    async def get_custom_format_names(self) -> set[str]:
        """Return the names of all custom formats defined in the Arr."""
        data = await self._request("GET", "customformat")
        return {str(cf.get("name", "")) for cf in data or []}

    async def grab_release(self, release: ReleaseResult) -> None:
        """Hand *release* (from a previous search) to the Arr's download client."""
        _LOGGER.debug(
            "%s grab_release: guid=%s title=%r",
            self.source,
            release.guid,
            release.title,
        )
        await self._request(
            "POST",
            "release",
            json={"guid": release.guid, "indexerId": release.indexer_id},
            timeout=self._search_timeout,
        )

    async def get_queue(self) -> list[ArrQueueItem]:
        records = await self._get_paged_records("queue", self._queue_params())
        items: list[ArrQueueItem] = []
        for record in records:
            download_id = record.get("downloadId")
            media_id = record.get(self._queue_media_key())
            if not download_id or not media_id:
                continue
            items.append(
                ArrQueueItem(
                    download_id=str(download_id),
                    media_id=int(media_id),
                    title=str(record.get("title", "")),
                    protocol=str(record.get("protocol") or "").lower(),
                )
            )
        return items

    async def get_download_status(self, download_id: str) -> list[ArrDownloadStatus]:
        """Return the queue records of one download (several for season packs)."""
        records = await self._get_paged_records("queue", self._queue_params())
        wanted = download_id.lower()
        result: list[ArrDownloadStatus] = []
        for record in records:
            if str(record.get("downloadId") or "").lower() != wanted:
                continue
            messages = [
                str(message)
                for entry in record.get("statusMessages") or []
                if isinstance(entry, dict)
                for message in entry.get("messages") or []
                if message
            ]
            result.append(
                ArrDownloadStatus(
                    download_id=str(record.get("downloadId")),
                    status=str(record.get("status") or ""),
                    state=str(record.get("trackedDownloadState") or ""),
                    messages=messages,
                )
            )
        return result

    async def import_completed_download(
        self, download_id: str, is_allowed: Callable[[list[str]], bool]
    ) -> int:
        """Import the files of a finished download the Arr refuses to import.

        Only files whose rejections all pass *is_allowed* are imported (the
        caller allows the "not an upgrade" family).  Returns the number of
        files handed to the Arr; ``0`` means nothing was imported.
        """
        items = await self._request(
            "GET",
            "manualimport",
            params={"downloadId": download_id, "filterExistingFiles": "false"},
            timeout=self._search_timeout,
        )
        files: list[dict[str, Any]] = []
        for item in items or []:
            if not isinstance(item, dict):
                continue
            if not is_allowed(_parse_rejections(item.get("rejections"))):
                _LOGGER.info(
                    "%s: not importing %s (%s)",
                    self.source,
                    item.get("path"),
                    "; ".join(_parse_rejections(item.get("rejections"))),
                )
                continue
            payload = self._manual_import_file(item)
            if payload is None:
                continue
            payload["downloadId"] = download_id
            files.append(payload)
        if not files:
            return 0
        await self._request(
            "POST",
            "command",
            json={"name": "ManualImport", "importMode": "auto", "files": files},
            timeout=self._search_timeout,
        )
        return len(files)

    @staticmethod
    def _common_import_fields(item: dict[str, Any]) -> dict[str, Any]:
        return {
            "path": item.get("path"),
            "folderName": item.get("folderName"),
            "quality": item.get("quality"),
            "languages": item.get("languages") or [],
            "releaseGroup": item.get("releaseGroup") or "",
            "indexerFlags": int(item.get("indexerFlags") or 0),
        }

    # ------------------------------------------------------------------
    # Media-specific API (implemented by subclasses)
    # ------------------------------------------------------------------

    @abstractmethod
    def _manual_import_file(self, item: dict[str, Any]) -> dict[str, Any] | None:
        """Build one ``ManualImport`` file entry, or ``None`` if unmapped."""

    def _queue_params(self) -> dict[str, Any]:
        return {}

    @abstractmethod
    def _queue_media_key(self) -> str:
        """Key of the movie/episode id in a queue record."""

    @abstractmethod
    async def get_media_state(self, media_id: int) -> MediaState | None:
        """Return the current file state or ``None`` when the item is gone."""

    @abstractmethod
    async def get_media_tags(self, media_id: int) -> list[str]:
        """Return the tag labels that apply to *media_id*."""

    @abstractmethod
    async def list_media_ids_with_files(self) -> list[int]:
        """Return the ids of all movies/episodes that currently have a file."""

    @abstractmethod
    async def search_releases(self, media_id: int) -> list[ReleaseResult]:
        """Run an interactive indexer search for *media_id*."""
