"""Unit tests for the SABnzbd API client."""

from __future__ import annotations

import re
from typing import Any

import aiohttp
import pytest
from aioresponses import aioresponses

from conductarr.clients.sabnzbd import (
    Queue,
    QueueSlot,
    SABnzbdClient,
    SABnzbdConnectionError,
    SABnzbdError,
    SABnzbdInvalidAPIKeyError,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "http://localhost:8080"
API_KEY = "testapikey"
API_PATTERN = re.compile(rf"{re.escape(BASE_URL)}/api.*")

QUEUE_RESPONSE: dict[str, Any] = {
    "queue": {
        "status": "Downloading",
        "paused": False,
        "noofslots": 1,
        "slots": [
            {
                "nzo_id": "SABnzbd_nzo_abc123",
                "filename": "Ubuntu.22.04.iso",
                "cat": "software",
                "priority": "Normal",
                "status": "Downloading",
                "index": 0,
                "mb": "1024.00",
                "mbleft": "512.00",
                "percentage": "50",
                "timeleft": "0:05:00",
                "labels": [],
            }
        ],
    }
}

HISTORY_RESPONSE: dict[str, Any] = {
    "history": {
        "noofslots": 1,
        "slots": [
            {
                "nzo_id": "SABnzbd_nzo_done123",
                "name": "Ubuntu.22.04.iso",
                "status": "Completed",
                "category": "software",
                "bytes": 1_073_741_824,
                "completed": 1700000000,
            }
        ],
    }
}

VERSION_RESPONSE: dict[str, Any] = {"version": "4.3.0"}

BOOL_OK_RESPONSE: dict[str, Any] = {"status": True}

SET_PRIORITY_RESPONSE: dict[str, Any] = {"position": 2}

SWITCH_RESPONSE: dict[str, Any] = {"result": [3, 1], "status": True}

ERROR_RESPONSE: dict[str, Any] = {"error": "Something went wrong"}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> SABnzbdClient:
    return SABnzbdClient(url=BASE_URL, api_key=API_KEY)


@pytest.fixture
def mocked() -> Any:
    with aioresponses() as m:
        yield m


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _query(mocked: Any) -> dict[str, str]:
    """Return the query params of the first intercepted request as a dict."""
    url = list(mocked.requests.keys())[0][1]
    return dict(url.query)


# ---------------------------------------------------------------------------
# get_queue
# ---------------------------------------------------------------------------


class TestGetQueue:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=QUEUE_RESPONSE)

        result = await client.get_queue()

        assert isinstance(result, Queue)
        assert result.status == "Downloading"
        assert result.paused is False
        assert result.noofslots == 1
        assert len(result.slots) == 1
        slot = result.slots[0]
        assert isinstance(slot, QueueSlot)
        assert slot.nzo_id == "SABnzbd_nzo_abc123"
        assert slot.filename == "Ubuntu.22.04.iso"
        assert slot.cat == "software"
        params = _query(mocked)
        assert params["mode"] == "queue"
        assert params["output"] == "json"
        assert params["apikey"] == API_KEY

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.get_queue()

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.get_queue()

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.get_queue()


# ---------------------------------------------------------------------------
# pause_job
# ---------------------------------------------------------------------------


class TestPauseJob:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=BOOL_OK_RESPONSE)

        result = await client.pause_job("SABnzbd_nzo_abc123")

        assert result is True
        params = _query(mocked)
        assert params["mode"] == "queue"
        assert params["name"] == "pause"
        assert params["value"] == "SABnzbd_nzo_abc123"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.pause_job("SABnzbd_nzo_abc123")

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.pause_job("SABnzbd_nzo_abc123")

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.pause_job("SABnzbd_nzo_abc123")


# ---------------------------------------------------------------------------
# resume_job
# ---------------------------------------------------------------------------


class TestResumeJob:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=BOOL_OK_RESPONSE)

        result = await client.resume_job("SABnzbd_nzo_abc123")

        assert result is True
        params = _query(mocked)
        assert params["mode"] == "queue"
        assert params["name"] == "resume"
        assert params["value"] == "SABnzbd_nzo_abc123"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.resume_job("SABnzbd_nzo_abc123")

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.resume_job("SABnzbd_nzo_abc123")

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.resume_job("SABnzbd_nzo_abc123")


# ---------------------------------------------------------------------------
# delete_job
# ---------------------------------------------------------------------------


class TestDeleteJob:
    async def test_happy_path_del_files_true(
        self, client: SABnzbdClient, mocked: Any
    ) -> None:
        mocked.get(API_PATTERN, payload=BOOL_OK_RESPONSE)

        result = await client.delete_job("SABnzbd_nzo_abc123", del_files=True)

        assert result is True
        params = _query(mocked)
        assert params["mode"] == "queue"
        assert params["name"] == "delete"
        assert params["value"] == "SABnzbd_nzo_abc123"
        assert params["del_files"] == "1"

    async def test_happy_path_del_files_false(
        self, client: SABnzbdClient, mocked: Any
    ) -> None:
        mocked.get(API_PATTERN, payload=BOOL_OK_RESPONSE)

        result = await client.delete_job("SABnzbd_nzo_abc123", del_files=False)

        assert result is True
        params = _query(mocked)
        assert params["del_files"] == "0"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.delete_job("SABnzbd_nzo_abc123")

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.delete_job("SABnzbd_nzo_abc123")

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.delete_job("SABnzbd_nzo_abc123")


# ---------------------------------------------------------------------------
# set_priority
# ---------------------------------------------------------------------------


class TestSetPriority:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=SET_PRIORITY_RESPONSE)

        result = await client.set_priority("SABnzbd_nzo_abc123", 1)

        assert result == 2
        params = _query(mocked)
        assert params["mode"] == "queue"
        assert params["name"] == "priority"
        assert params["value"] == "SABnzbd_nzo_abc123"
        assert params["value2"] == "1"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.set_priority("SABnzbd_nzo_abc123", 1)

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.set_priority("SABnzbd_nzo_abc123", 1)

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.set_priority("SABnzbd_nzo_abc123", 1)


# ---------------------------------------------------------------------------
# switch
# ---------------------------------------------------------------------------


class TestSwitch:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=SWITCH_RESPONSE)

        result = await client.switch("SABnzbd_nzo_abc123", "SABnzbd_nzo_xyz789")

        assert result == (3, 1)
        params = _query(mocked)
        assert params["mode"] == "switch"
        assert params["value"] == "SABnzbd_nzo_abc123"
        assert params["value2"] == "SABnzbd_nzo_xyz789"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.switch("SABnzbd_nzo_abc123", "SABnzbd_nzo_xyz789")

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.switch("SABnzbd_nzo_abc123", "SABnzbd_nzo_xyz789")

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.switch("SABnzbd_nzo_abc123", "SABnzbd_nzo_xyz789")


# ---------------------------------------------------------------------------
# pause_queue
# ---------------------------------------------------------------------------


class TestPauseQueue:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=BOOL_OK_RESPONSE)

        result = await client.pause_queue()

        assert result is True
        params = _query(mocked)
        assert params["mode"] == "pause"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.pause_queue()

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.pause_queue()

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.pause_queue()


# ---------------------------------------------------------------------------
# resume_queue
# ---------------------------------------------------------------------------


class TestResumeQueue:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=BOOL_OK_RESPONSE)

        result = await client.resume_queue()

        assert result is True
        params = _query(mocked)
        assert params["mode"] == "resume"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.resume_queue()

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.resume_queue()

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.resume_queue()


# ---------------------------------------------------------------------------
# get_history
# ---------------------------------------------------------------------------


class TestGetHistory:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=HISTORY_RESPONSE)

        result = await client.get_history()

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["nzo_id"] == "SABnzbd_nzo_done123"
        assert result[0]["status"] == "Completed"
        params = _query(mocked)
        assert params["mode"] == "history"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.get_history()

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.get_history()

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.get_history()


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------


class TestVersion:
    async def test_happy_path(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=VERSION_RESPONSE)

        result = await client.version()

        assert result == "4.3.0"
        params = _query(mocked)
        assert params["mode"] == "version"
        assert params["output"] == "json"

    async def test_invalid_api_key(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, body="API Key Incorrect")
        with pytest.raises(SABnzbdInvalidAPIKeyError):
            await client.version()

    async def test_connection_error(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, exception=aiohttp.ClientError("connection failed"))
        with pytest.raises(SABnzbdConnectionError):
            await client.version()

    async def test_error_in_response(self, client: SABnzbdClient, mocked: Any) -> None:
        mocked.get(API_PATTERN, payload=ERROR_RESPONSE)
        with pytest.raises(SABnzbdError, match="Something went wrong"):
            await client.version()
