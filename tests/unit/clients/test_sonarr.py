"""Unit tests for the Sonarr API client."""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiohttp
import pytest
from aioresponses import aioresponses
from pyarr.exceptions import (
    PyarrConnectionError,
    PyarrResourceNotFound,
    PyarrUnauthorizedError,
)

from conductarr.clients.release import ReleaseResult
from conductarr.clients.sonarr import (
    SonarrAuthError,
    SonarrClient,
    SonarrConnectionError,
    SonarrError,
    SonarrEpisode,
    SonarrQueueItem,
    SonarrSeries,
    _parse_url,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "http://localhost:8989"
API_KEY = "testapikey"

BLOCKLIST_URL_PATTERN = re.compile(
    rf"{re.escape(BASE_URL)}/api/v3/blocklist.*"
)
EPISODE_FILE_URL_PATTERN = re.compile(
    rf"{re.escape(BASE_URL)}/api/v3/episodeFile/.*"
)

QUEUE_RESPONSE: dict[str, Any] = {
    "records": [
        {
            "downloadId": "SABnzbd_nzo_abc123",
            "seriesId": 10,
            "episodeId": 20,
            "title": "Breaking.Bad.S01E01",
            "status": "downloading",
            "quality": {"quality": {"name": "HDTV-720p"}},
            "customFormatScore": 50,
        }
    ]
}

SERIES_RESPONSE: list[dict[str, Any]] = [
    {
        "id": 1,
        "title": "Breaking Bad",
        "tvdbId": 81189,
        "monitored": True,
        "status": "ended",
        "tags": [10, 20],
    },
    {
        "id": 2,
        "title": "Better Call Saul",
        "tvdbId": 273181,
        "monitored": False,
        "status": "ended",
        "tags": [],
    },
]

EPISODE_RAW: dict[str, Any] = {
    "id": 100,
    "seriesId": 1,
    "episodeNumber": 1,
    "seasonNumber": 1,
    "title": "Pilot",
    "monitored": True,
    "hasFile": True,
    "customFormatScore": 100,
    "customFormats": [{"name": "BluRay"}],
    "episodeFileId": 55,
}

TAGS_RESPONSE: list[dict[str, Any]] = [
    {"id": 10, "label": "upgrade"},
    {"id": 20, "label": "monitored"},
]

RELEASE_RAW: dict[str, Any] = {
    "guid": "guid-123",
    "title": "Breaking.Bad.S01E01.720p",
    "indexerId": 3,
    "customFormats": [{"name": "WEB-DL"}],
    "customFormatScore": 80,
    "quality": {"quality": {"name": "WEBDL-720p"}},
    "size": 1_500_000_000,
    "downloadAllowed": True,
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def client() -> SonarrClient:
    return SonarrClient(url=BASE_URL, api_key=API_KEY)


@pytest.fixture
def mock_api(client: SonarrClient) -> MagicMock:
    """Inject a MagicMock as the underlying pyarr API client."""
    api = MagicMock()
    client._api = api
    return api


# ---------------------------------------------------------------------------
# _parse_url
# ---------------------------------------------------------------------------


class TestParseUrl:
    def test_http_with_port(self) -> None:
        host, port, tls = _parse_url("http://myhost:1234")
        assert host == "myhost"
        assert port == 1234
        assert tls is False

    def test_https_default_port(self) -> None:
        host, port, tls = _parse_url("https://sonarr.example.com")
        assert host == "sonarr.example.com"
        assert port == 443
        assert tls is True

    def test_http_default_port(self) -> None:
        host, port, tls = _parse_url("http://localhost")
        assert host == "localhost"
        assert port == 8989
        assert tls is False


# ---------------------------------------------------------------------------
# _get_api / auth guard
# ---------------------------------------------------------------------------


class TestGetApi:
    def test_no_api_key_raises_auth_error(self) -> None:
        c = SonarrClient(url=BASE_URL, api_key="")
        with pytest.raises(SonarrAuthError):
            c._get_api()

    def test_lazy_init_creates_api(self) -> None:
        c = SonarrClient(url=BASE_URL, api_key=API_KEY)
        assert c._api is None
        api = c._get_api()
        assert api is not None
        # Second call returns same instance
        assert c._get_api() is api


# ---------------------------------------------------------------------------
# get_queue
# ---------------------------------------------------------------------------


class TestGetQueue:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(return_value=QUEUE_RESPONSE)

        result = await client.get_queue()

        assert len(result) == 1
        item = result[0]
        assert isinstance(item, SonarrQueueItem)
        assert item.download_id == "SABnzbd_nzo_abc123"
        assert item.series_id == 10
        assert item.episode_id == 20
        assert item.title == "Breaking.Bad.S01E01"
        assert item.status == "downloading"
        assert item.quality == "HDTV-720p"
        assert item.custom_format_score == 50
        mock_api.queue.get.assert_awaited_once_with(page_size=1000)

    async def test_empty_queue(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(return_value={"records": []})
        result = await client.get_queue()
        assert result == []

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauthorized"))
        with pytest.raises(SonarrAuthError):
            await client.get_queue()

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(side_effect=PyarrConnectionError("timeout"))
        with pytest.raises(SonarrConnectionError):
            await client.get_queue()

    async def test_os_error_wrapped(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(side_effect=OSError("unreachable"))
        with pytest.raises(SonarrConnectionError):
            await client.get_queue()

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(side_effect=RuntimeError("unexpected"))
        with pytest.raises(SonarrError):
            await client.get_queue()


# ---------------------------------------------------------------------------
# get_series
# ---------------------------------------------------------------------------


class TestGetSeries:
    async def test_happy_path_no_filter(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.series.get = AsyncMock(return_value=SERIES_RESPONSE)

        result = await client.get_series()

        assert len(result) == 2
        s = result[0]
        assert isinstance(s, SonarrSeries)
        assert s.id == 1
        assert s.title == "Breaking Bad"
        assert s.tvdb_id == 81189
        assert s.monitored is True
        assert s.status == "ended"
        assert s.tag_ids == [10, 20]

    async def test_filter_monitored_true(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.series.get = AsyncMock(return_value=SERIES_RESPONSE)

        result = await client.get_series(monitored=True)

        assert len(result) == 1
        assert result[0].title == "Breaking Bad"

    async def test_filter_monitored_false(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.series.get = AsyncMock(return_value=SERIES_RESPONSE)

        result = await client.get_series(monitored=False)

        assert len(result) == 1
        assert result[0].title == "Better Call Saul"

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.series.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauthorized"))
        with pytest.raises(SonarrAuthError):
            await client.get_series()

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.series.get = AsyncMock(side_effect=ConnectionError("failed"))
        with pytest.raises(SonarrConnectionError):
            await client.get_series()

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.series.get = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(SonarrError):
            await client.get_series()


# ---------------------------------------------------------------------------
# get_episodes
# ---------------------------------------------------------------------------


class TestGetEpisodes:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(return_value=[EPISODE_RAW])

        result = await client.get_episodes(1)

        assert len(result) == 1
        ep = result[0]
        assert isinstance(ep, SonarrEpisode)
        assert ep.id == 100
        assert ep.series_id == 1
        assert ep.episode_number == 1
        assert ep.season_number == 1
        assert ep.title == "Pilot"
        assert ep.monitored is True
        assert ep.has_file is True
        assert ep.custom_format_score == 100
        assert ep.custom_formats == ["BluRay"]
        assert ep.episode_file_id == 55
        mock_api.episode.get.assert_awaited_once_with(series_id=1)

    async def test_filter_monitored(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        unmonitored = {**EPISODE_RAW, "id": 200, "monitored": False}
        mock_api.episode.get = AsyncMock(return_value=[EPISODE_RAW, unmonitored])

        result = await client.get_episodes(1, monitored=True)

        assert len(result) == 1
        assert result[0].id == 100

    async def test_filter_has_file(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        no_file = {**EPISODE_RAW, "id": 201, "hasFile": False}
        mock_api.episode.get = AsyncMock(return_value=[EPISODE_RAW, no_file])

        result = await client.get_episodes(1, has_file=False)

        assert len(result) == 1
        assert result[0].id == 201

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.get_episodes(1)

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(side_effect=PyarrConnectionError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.get_episodes(1)

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(side_effect=RuntimeError("oops"))
        with pytest.raises(SonarrError):
            await client.get_episodes(1)


# ---------------------------------------------------------------------------
# get_episode
# ---------------------------------------------------------------------------


class TestGetEpisode:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(return_value=EPISODE_RAW)

        result = await client.get_episode(100)

        assert result is not None
        assert result.id == 100
        assert result.title == "Pilot"
        mock_api.episode.get.assert_awaited_once_with(item_id=100)

    async def test_not_found_returns_none(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(side_effect=PyarrResourceNotFound("404"))

        result = await client.get_episode(999)

        assert result is None

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.get_episode(1)

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.get_episode(1)

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.episode.get = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(SonarrError):
            await client.get_episode(1)

    async def test_episode_file_id_none_when_zero(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        raw = {**EPISODE_RAW, "episodeFileId": 0}
        mock_api.episode.get = AsyncMock(return_value=raw)
        result = await client.get_episode(100)
        assert result is not None
        assert result.episode_file_id is None


# ---------------------------------------------------------------------------
# trigger_episode_search
# ---------------------------------------------------------------------------


class TestTriggerEpisodeSearch:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(return_value=None)

        result = await client.trigger_episode_search(100)

        assert result is True
        mock_api.command.execute.assert_awaited_once_with(
            "EpisodeSearch", episodeIds=[100]
        )

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.trigger_episode_search(1)

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(side_effect=PyarrConnectionError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.trigger_episode_search(1)

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(SonarrError):
            await client.trigger_episode_search(1)


# ---------------------------------------------------------------------------
# trigger_season_search
# ---------------------------------------------------------------------------


class TestTriggerSeasonSearch:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(return_value=None)

        result = await client.trigger_season_search(series_id=1, season_number=2)

        assert result is True
        mock_api.command.execute.assert_awaited_once_with(
            "SeasonSearch", seriesId=1, seasonNumber=2
        )

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.trigger_season_search(1, 1)

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(side_effect=OSError("unreachable"))
        with pytest.raises(SonarrConnectionError):
            await client.trigger_season_search(1, 1)

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(SonarrError):
            await client.trigger_season_search(1, 1)


# ---------------------------------------------------------------------------
# get_tags
# ---------------------------------------------------------------------------


class TestGetTags:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(return_value=TAGS_RESPONSE)

        result = await client.get_tags()

        assert result == {10: "upgrade", 20: "monitored"}

    async def test_empty(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(return_value=[])
        result = await client.get_tags()
        assert result == {}

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.get_tags()

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(side_effect=PyarrConnectionError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.get_tags()

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(SonarrError):
            await client.get_tags()


# ---------------------------------------------------------------------------
# get_episode_tags
# ---------------------------------------------------------------------------


class TestGetEpisodeTags:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        ep_raw = {**EPISODE_RAW, "seriesId": 1}
        series_raw = {"id": 1, "tags": [10, 20]}

        mock_api.episode.get = AsyncMock(return_value=ep_raw)
        mock_api.series.get = AsyncMock(return_value=series_raw)
        mock_api.tag.get = AsyncMock(return_value=TAGS_RESPONSE)

        result = await client.get_episode_tags(100)

        assert result == ["upgrade", "monitored"]

    async def test_episode_not_found_returns_empty(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(side_effect=PyarrResourceNotFound("404"))

        result = await client.get_episode_tags(999)

        assert result == []

    async def test_no_series_id_returns_empty(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(return_value={"id": 100, "seriesId": 0})

        result = await client.get_episode_tags(100)

        assert result == []

    async def test_series_not_found_returns_empty(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(return_value={**EPISODE_RAW, "seriesId": 1})
        mock_api.series.get = AsyncMock(side_effect=PyarrResourceNotFound("404"))

        result = await client.get_episode_tags(100)

        assert result == []

    async def test_series_has_no_tags_returns_empty(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(return_value={**EPISODE_RAW, "seriesId": 1})
        mock_api.series.get = AsyncMock(return_value={"id": 1, "tags": []})

        result = await client.get_episode_tags(100)

        assert result == []

    async def test_episode_auth_error(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.get_episode_tags(1)

    async def test_series_connection_error(
        self, client: SonarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.episode.get = AsyncMock(return_value={**EPISODE_RAW, "seriesId": 1})
        mock_api.series.get = AsyncMock(side_effect=PyarrConnectionError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.get_episode_tags(100)


# ---------------------------------------------------------------------------
# search_releases
# ---------------------------------------------------------------------------


class TestSearchReleases:
    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(return_value=[RELEASE_RAW])

        result = await client.search_releases(100)

        assert len(result) == 1
        r = result[0]
        assert isinstance(r, ReleaseResult)
        assert r.guid == "guid-123"
        assert r.title == "Breaking.Bad.S01E01.720p"
        assert r.indexer_id == 3
        assert r.custom_formats == ["WEB-DL"]
        assert r.custom_format_score == 80
        assert r.quality == "WEBDL-720p"
        assert r.size == 1_500_000_000
        assert r.download_allowed is True
        mock_api.release.get.assert_awaited_once_with(episode_id=100)

    async def test_empty_result(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(return_value=None)
        result = await client.search_releases(100)
        assert result == []

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.search_releases(1)

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.search_releases(1)

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(SonarrError):
            await client.search_releases(1)


# ---------------------------------------------------------------------------
# grab_release
# ---------------------------------------------------------------------------


class TestGrabRelease:
    def _make_release(self) -> ReleaseResult:
        return ReleaseResult(
            guid="guid-123",
            title="Breaking.Bad.S01E01.720p",
            indexer_id=3,
            custom_formats=[],
            custom_format_score=0,
            quality="WEBDL-720p",
            size=0,
            download_allowed=True,
        )

    async def test_happy_path(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.add = AsyncMock(return_value=None)
        release = self._make_release()

        await client.grab_release(release)

        mock_api.release.add.assert_awaited_once_with("guid-123", 3)

    async def test_auth_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.add = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(SonarrAuthError):
            await client.grab_release(self._make_release())

    async def test_connection_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.add = AsyncMock(side_effect=OSError("down"))
        with pytest.raises(SonarrConnectionError):
            await client.grab_release(self._make_release())

    async def test_generic_error(self, client: SonarrClient, mock_api: MagicMock) -> None:
        mock_api.release.add = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(SonarrError):
            await client.grab_release(self._make_release())


# ---------------------------------------------------------------------------
# get_blocklist_source_titles
# ---------------------------------------------------------------------------


class TestGetBlocklistSourceTitles:
    async def test_happy_path(self, client: SonarrClient) -> None:
        payload = {
            "records": [
                {"sourceTitle": "Breaking.Bad.S01E01.HDTV"},
                {"sourceTitle": "Breaking.Bad.S01E02.HDTV"},
                {"sourceTitle": ""},  # empty title should be skipped
            ]
        }
        with aioresponses() as m:
            m.get(BLOCKLIST_URL_PATTERN, payload=payload)

            result = await client.get_blocklist_source_titles()

        assert result == {
            "Breaking.Bad.S01E01.HDTV",
            "Breaking.Bad.S01E02.HDTV",
        }

    async def test_empty_blocklist(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(BLOCKLIST_URL_PATTERN, payload={"records": []})

            result = await client.get_blocklist_source_titles()

        assert result == set()

    async def test_pagination(self, client: SonarrClient) -> None:
        # First page returns 1000 records → triggers page 2
        page1_records = [{"sourceTitle": f"title-{i}"} for i in range(1000)]
        page2_records = [{"sourceTitle": "title-last"}]

        with aioresponses() as m:
            m.get(BLOCKLIST_URL_PATTERN, payload={"records": page1_records})
            m.get(BLOCKLIST_URL_PATTERN, payload={"records": page2_records})

            result = await client.get_blocklist_source_titles()

        assert len(result) == 1001
        assert "title-0" in result
        assert "title-last" in result

    async def test_connection_error(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(
                BLOCKLIST_URL_PATTERN,
                exception=aiohttp.ClientError("connection failed"),
            )
            with pytest.raises(SonarrConnectionError):
                await client.get_blocklist_source_titles()

    async def test_timeout_error(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(
                BLOCKLIST_URL_PATTERN,
                exception=aiohttp.ServerTimeoutError(),
            )
            with pytest.raises(SonarrConnectionError):
                await client.get_blocklist_source_titles()

    async def test_generic_error(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(
                BLOCKLIST_URL_PATTERN,
                exception=RuntimeError("unexpected"),
            )
            with pytest.raises(SonarrError):
                await client.get_blocklist_source_titles()


# ---------------------------------------------------------------------------
# get_episode_file
# ---------------------------------------------------------------------------


class TestGetEpisodeFile:
    async def test_happy_path(self, client: SonarrClient) -> None:
        payload: dict[str, Any] = {
            "id": 55,
            "customFormatScore": 200,
            "customFormats": [{"name": "BluRay"}],
        }
        with aioresponses() as m:
            m.get(EPISODE_FILE_URL_PATTERN, payload=payload)

            result = await client.get_episode_file(55)

        assert result is not None
        assert result["id"] == 55
        assert result["customFormatScore"] == 200

    async def test_not_found_returns_none(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(EPISODE_FILE_URL_PATTERN, status=404, payload={})

            result = await client.get_episode_file(999)

        assert result is None

    async def test_connection_error(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(
                EPISODE_FILE_URL_PATTERN,
                exception=aiohttp.ClientError("down"),
            )
            with pytest.raises(SonarrConnectionError):
                await client.get_episode_file(55)

    async def test_timeout_error(self, client: SonarrClient) -> None:
        with aioresponses() as m:
            m.get(
                EPISODE_FILE_URL_PATTERN,
                exception=aiohttp.ServerTimeoutError(),
            )
            with pytest.raises(SonarrConnectionError):
                await client.get_episode_file(55)
