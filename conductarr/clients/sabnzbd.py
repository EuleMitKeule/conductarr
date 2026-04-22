"""Async SABnzbd API client."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

import aiohttp


def _loads(data: str | bytes) -> Any:
    return json.loads(data)


_LOGGER = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Priority constants
# ---------------------------------------------------------------------------


class SABnzbdPriority(IntEnum):
    FORCE = 2
    HIGH = 1
    NORMAL = 0
    LOW = -1
    STOP = -3


# ---------------------------------------------------------------------------
# Custom exceptions
# ---------------------------------------------------------------------------


class SABnzbdError(Exception):
    """Base SABnzbd client error."""


class SABnzbdConnectionError(SABnzbdError):
    """Raised when a connection to SABnzbd cannot be established."""


class SABnzbdTimeoutError(SABnzbdError):
    """Raised when a request to SABnzbd times out."""


class SABnzbdInvalidAPIKeyError(SABnzbdError):
    """Raised when the provided API key is rejected by SABnzbd."""


class SABnzbdMissingAPIKeyError(SABnzbdError):
    """Raised when no API key was provided and SABnzbd requires one."""


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class QueueSlot:
    nzo_id: str
    filename: str
    cat: str
    priority: str
    status: str
    index: int
    mb: str
    mbleft: str
    percentage: str
    timeleft: str
    labels: list[str]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueueSlot:
        return cls(
            nzo_id=data["nzo_id"],
            filename=data["filename"],
            cat=data["cat"],
            priority=data["priority"],
            status=data["status"],
            index=int(data["index"]),
            mb=data["mb"],
            mbleft=data["mbleft"],
            percentage=data["percentage"],
            timeleft=data["timeleft"],
            labels=list(data.get("labels", [])),
        )


@dataclass(frozen=True, slots=True)
class Queue:
    status: str
    paused: bool
    noofslots: int
    slots: list[QueueSlot]

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Queue:
        queue = data["queue"]
        return cls(
            status=queue["status"],
            paused=bool(queue["paused"]),
            noofslots=int(queue["noofslots"]),
            slots=[QueueSlot.from_dict(s) for s in queue.get("slots", [])],
        )


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


@dataclass
class SABnzbdClient:
    """Async client for the SABnzbd API.

    Parameters
    ----------
    url:
        Base URL of the SABnzbd instance, e.g. ``http://localhost:8080``.
    api_key:
        SABnzbd API key.
    """

    url: str
    api_key: str
    _session: aiohttp.ClientSession | None = field(default=None, init=False, repr=False)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> SABnzbdClient:
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _request(self, **params: Any) -> Any:
        """Perform a GET request to ``/api`` and return the parsed JSON body.

        The ``output=json`` and ``apikey`` params are always injected.
        Raises the appropriate :class:`SABnzbdError` subclass on failure.
        """
        base = self.url.rstrip("/")
        endpoint = f"{base}/api"
        all_params: dict[str, Any] = {
            "output": "json",
            "apikey": self.api_key,
            **params,
        }

        _LOGGER.debug(
            "SABnzbd GET %s params=%s",
            endpoint,
            {k: v for k, v in all_params.items() if k != "apikey"},
        )

        owns_session = self._session is None
        session: aiohttp.ClientSession = (
            self._session if self._session is not None else aiohttp.ClientSession()
        )

        try:
            async with session.get(endpoint, params=all_params) as resp:
                text = await resp.text()
        except aiohttp.ServerTimeoutError as exc:
            raise SABnzbdTimeoutError(str(exc)) from exc
        except aiohttp.ClientConnectionError as exc:
            raise SABnzbdConnectionError(str(exc)) from exc
        except aiohttp.ClientError as exc:
            raise SABnzbdConnectionError(str(exc)) from exc
        finally:
            if owns_session:
                await session.close()

        # Detect API key errors before attempting JSON parse
        stripped = text.strip()
        if stripped == "API Key Incorrect":
            raise SABnzbdInvalidAPIKeyError("API key was rejected by SABnzbd")
        if stripped == "API Key Required":
            raise SABnzbdMissingAPIKeyError("SABnzbd requires an API key")
        if not stripped:
            raise SABnzbdConnectionError(
                "SABnzbd returned an empty response (likely a refused hostname)"
            )

        _LOGGER.debug("SABnzbd response: %s", text[:2000])

        try:
            data: Any = _loads(text)
        except Exception as exc:
            raise SABnzbdConnectionError(
                f"SABnzbd returned non-JSON response: {text[:200]!r}"
            ) from exc

        if isinstance(data, dict) and "error" in data:
            raise SABnzbdError(data["error"])

        return data

    # ------------------------------------------------------------------
    # Public API methods
    # ------------------------------------------------------------------

    async def get_queue(self) -> Queue:
        """Return the current download queue."""
        data = await self._request(mode="queue")
        return Queue.from_dict(data)

    async def pause_job(self, nzo_id: str) -> bool:
        """Pause a specific job."""
        data = await self._request(mode="queue", name="pause", value=nzo_id)
        return bool(data.get("status", False))

    async def resume_job(self, nzo_id: str) -> bool:
        """Resume a specific job."""
        data = await self._request(mode="queue", name="resume", value=nzo_id)
        return bool(data.get("status", False))

    async def delete_job(self, nzo_id: str, del_files: bool = False) -> bool:
        """Delete a job, optionally removing its downloaded files."""
        data = await self._request(
            mode="queue",
            name="delete",
            value=nzo_id,
            del_files=1 if del_files else 0,
        )
        return bool(data.get("status", False))

    async def set_priority(self, nzo_id: str, priority: int) -> int:
        """Set the priority of a job and return its new position."""
        data = await self._request(
            mode="queue",
            name="priority",
            value=nzo_id,
            value2=priority,
        )
        return int(data["position"])

    async def switch(self, nzo_id: str, other_nzo_id: str) -> tuple[int, int]:
        """Move *nzo_id* to directly above *other_nzo_id*.

        Returns ``(position, priority)`` from the ``result`` field, or
        ``(-1, -1)`` when the response is not in the expected list format
        (some SABnzbd versions return ``{"result": "ok"}`` or a bare number).
        """
        data = await self._request(mode="switch", value=nzo_id, value2=other_nzo_id)
        result = data.get("result")
        if isinstance(result, list) and len(result) >= 2:
            return (int(result[0]), int(result[1]))
        _LOGGER.debug(
            "switch(%s, %s) returned unexpected result format: %r",
            nzo_id,
            other_nzo_id,
            result,
        )
        return (-1, -1)

    async def pause_queue(self) -> bool:
        """Pause the entire download queue."""
        data = await self._request(mode="pause")
        return bool(data.get("status", False))

    async def resume_queue(self) -> bool:
        """Resume the entire download queue."""
        data = await self._request(mode="resume")
        return bool(data.get("status", False))

    async def get_history(self) -> list[dict[str, Any]]:
        """Return raw history slots."""
        data = await self._request(mode="history")
        return list(data["history"]["slots"])

    async def version(self) -> str:
        """Return the SABnzbd version string."""
        data = await self._request(mode="version")
        return str(data["version"])
