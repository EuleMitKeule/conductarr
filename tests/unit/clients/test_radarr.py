"""Unit tests for the Radarr API client."""

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

from conductarr.clients.radarr import (
    RadarrAuthError,
    RadarrClient,
    RadarrConnectionError,
    RadarrError,
    RadarrMovie,
    RadarrQueueItem,
    _parse_url,
)
from conductarr.clients.release import ReleaseResult

BASE_URL = "http://localhost:7878"
API_KEY = "testapikey"

BLOCKLIST_URL_PATTERN = re.compile(rf"{re.escape(BASE_URL)}/api/v3/blocklist.*")
MOVIE_FILE_URL_PATTERN = re.compile(rf"{re.escape(BASE_URL)}/api/v3/movieFile.*")

QUEUE_RESPONSE: dict[str, Any] = {
    "records": [
        {
            "downloadId": "SABnzbd_nzo_abc123",
            "movieId": 10,
            "title": "The.Matrix.1999.1080p",
            "status": "downloading",
            "quality": {"quality": {"name": "BluRay-1080p"}},
            "customFormatScore": 50,
        }
    ]
}

MOVIES_RESPONSE: list[dict[str, Any]] = [
    {
        "id": 1,
        "title": "The Matrix",
        "tmdbId": 603,
        "hasFile": True,
        "monitored": True,
        "customFormatScore": 125,
        "qualityProfileId": 7,
        "tags": [10, 20],
        "customFormats": [{"name": "Remux"}],
    },
    {
        "id": 2,
        "title": "Inception",
        "tmdbId": 27205,
        "hasFile": False,
        "monitored": False,
        "customFormatScore": 0,
        "qualityProfileId": 8,
        "tags": [],
        "customFormats": [],
    },
]

TAGS_RESPONSE: list[dict[str, Any]] = [
    {"id": 10, "label": "upgrade"},
    {"id": 20, "label": "favorite"},
]

RELEASE_RAW: dict[str, Any] = {
    "guid": "guid-123",
    "title": "The.Matrix.1999.1080p.BluRay",
    "indexerId": 3,
    "customFormats": [{"name": "Remux"}],
    "customFormatScore": 80,
    "quality": {"quality": {"name": "BluRay-1080p"}},
    "size": 1_500_000_000,
    "downloadAllowed": True,
}


@pytest.fixture
def client() -> RadarrClient:
    return RadarrClient(url=BASE_URL, api_key=API_KEY)


@pytest.fixture
def mock_api(client: RadarrClient) -> MagicMock:
    api = MagicMock()
    client._api = api
    return api


class TestParseUrl:
    def test_http_with_port(self) -> None:
        host, port, tls = _parse_url("http://myhost:1234")
        assert host == "myhost"
        assert port == 1234
        assert tls is False

    def test_https_default_port(self) -> None:
        host, port, tls = _parse_url("https://radarr.example.com")
        assert host == "radarr.example.com"
        assert port == 443
        assert tls is True

    def test_http_default_port(self) -> None:
        host, port, tls = _parse_url("http://localhost")
        assert host == "localhost"
        assert port == 7878
        assert tls is False


class TestGetApi:
    def test_no_api_key_raises_auth_error(self) -> None:
        client = RadarrClient(url=BASE_URL, api_key="")
        with pytest.raises(RadarrAuthError):
            client._get_api()

    def test_lazy_init_creates_api_once(self) -> None:
        client = RadarrClient(url=BASE_URL, api_key=API_KEY)
        fake_api = MagicMock()

        with patch(
            "conductarr.clients.radarr.AsyncRadarr", return_value=fake_api
        ) as ctor:
            assert client._get_api() is fake_api
            assert client._get_api() is fake_api

        ctor.assert_called_once_with(
            host="localhost",
            api_key=API_KEY,
            port=7878,
            tls=False,
        )


class TestGetQueue:
    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(return_value=QUEUE_RESPONSE)

        result = await client.get_queue()

        assert len(result) == 1
        item = result[0]
        assert isinstance(item, RadarrQueueItem)
        assert item.download_id == "SABnzbd_nzo_abc123"
        assert item.movie_id == 10
        assert item.title == "The.Matrix.1999.1080p"
        assert item.status == "downloading"
        assert item.quality == "BluRay-1080p"
        assert item.custom_format_score == 50
        mock_api.queue.get.assert_awaited_once_with(page_size=1000)

    async def test_empty_queue(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(return_value={"records": []})
        assert await client.get_queue() == []

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.queue.get = AsyncMock(
            side_effect=PyarrUnauthorizedError("unauthorized")
        )
        with pytest.raises(RadarrAuthError):
            await client.get_queue()

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.queue.get = AsyncMock(side_effect=PyarrConnectionError("timeout"))
        with pytest.raises(RadarrConnectionError):
            await client.get_queue()

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.queue.get = AsyncMock(side_effect=RuntimeError("unexpected"))
        with pytest.raises(RadarrError):
            await client.get_queue()


class TestGetMovies:
    async def test_happy_path_no_filter(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(return_value=MOVIES_RESPONSE)

        result = await client.get_movies()

        assert len(result) == 2
        movie = result[0]
        assert isinstance(movie, RadarrMovie)
        assert movie.id == 1
        assert movie.title == "The Matrix"
        assert movie.tmdb_id == 603
        assert movie.has_file is True
        assert movie.monitored is True
        assert movie.custom_format_score == 125
        assert movie.quality_profile_id == 7
        assert movie.tag_ids == [10, 20]
        assert movie.custom_formats == ["Remux"]

    async def test_filter_monitored_true(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(return_value=MOVIES_RESPONSE)
        result = await client.get_movies(monitored=True)
        assert [movie.title for movie in result] == ["The Matrix"]

    async def test_filter_has_file_false(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(return_value=MOVIES_RESPONSE)
        result = await client.get_movies(has_file=False)
        assert [movie.title for movie in result] == ["Inception"]

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.movie.get = AsyncMock(
            side_effect=PyarrUnauthorizedError("unauthorized")
        )
        with pytest.raises(RadarrAuthError):
            await client.get_movies()

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=ConnectionError("failed"))
        with pytest.raises(RadarrConnectionError):
            await client.get_movies()

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RadarrError):
            await client.get_movies()


class TestGetMovie:
    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.movie.get = AsyncMock(return_value=MOVIES_RESPONSE[0])

        result = await client.get_movie(1)

        assert result is not None
        assert result.id == 1
        assert result.title == "The Matrix"
        mock_api.movie.get.assert_awaited_once_with(item_id=1)

    async def test_not_found_returns_none(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=PyarrResourceNotFound("404"))
        assert await client.get_movie(999) is None

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.movie.get = AsyncMock(
            side_effect=PyarrUnauthorizedError("unauthorized")
        )
        with pytest.raises(RadarrAuthError):
            await client.get_movie(1)

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=OSError("down"))
        with pytest.raises(RadarrConnectionError):
            await client.get_movie(1)

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(RadarrError):
            await client.get_movie(1)


class TestTriggerSearch:
    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(return_value=None)

        result = await client.trigger_search(55)

        assert result is True
        mock_api.command.execute.assert_awaited_once_with("MoviesSearch", movieIds=[55])

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.command.execute = AsyncMock(
            side_effect=PyarrUnauthorizedError("unauth")
        )
        with pytest.raises(RadarrAuthError):
            await client.trigger_search(1)

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.command.execute = AsyncMock(side_effect=PyarrConnectionError("down"))
        with pytest.raises(RadarrConnectionError):
            await client.trigger_search(1)

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.command.execute = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(RadarrError):
            await client.trigger_search(1)


class TestGetTags:
    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(return_value=TAGS_RESPONSE)
        assert await client.get_tags() == {10: "upgrade", 20: "favorite"}

    async def test_empty(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(return_value=[])
        assert await client.get_tags() == {}

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.tag.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(RadarrAuthError):
            await client.get_tags()

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.tag.get = AsyncMock(side_effect=PyarrConnectionError("down"))
        with pytest.raises(RadarrConnectionError):
            await client.get_tags()

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.tag.get = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(RadarrError):
            await client.get_tags()


class TestGetMovieTags:
    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.movie.get = AsyncMock(return_value={"id": 1, "tags": [10, 20]})
        mock_api.tag.get = AsyncMock(return_value=TAGS_RESPONSE)

        result = await client.get_movie_tags(1)

        assert result == ["upgrade", "favorite"]

    async def test_not_found_returns_empty(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=PyarrResourceNotFound("404"))
        assert await client.get_movie_tags(999) == []

    async def test_no_tags_returns_empty(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(return_value={"id": 1, "tags": []})
        assert await client.get_movie_tags(1) == []

    async def test_unknown_tag_ids_are_skipped(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(return_value={"id": 1, "tags": [10, 99]})
        mock_api.tag.get = AsyncMock(return_value=TAGS_RESPONSE)
        assert await client.get_movie_tags(1) == ["upgrade"]

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.movie.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(RadarrAuthError):
            await client.get_movie_tags(1)

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.movie.get = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(RadarrConnectionError):
            await client.get_movie_tags(1)


class TestSearchReleases:
    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(return_value=[RELEASE_RAW])

        result = await client.search_releases(1)

        assert len(result) == 1
        release = result[0]
        assert isinstance(release, ReleaseResult)
        assert release.guid == "guid-123"
        assert release.title == "The.Matrix.1999.1080p.BluRay"
        assert release.indexer_id == 3
        assert release.custom_formats == ["Remux"]
        assert release.custom_format_score == 80
        assert release.quality == "BluRay-1080p"
        assert release.size == 1_500_000_000
        assert release.download_allowed is True
        mock_api.release.get.assert_awaited_once_with(movie_id=1)

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.release.get = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(RadarrAuthError):
            await client.search_releases(1)

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.release.get = AsyncMock(side_effect=ConnectionError("down"))
        with pytest.raises(RadarrConnectionError):
            await client.search_releases(1)

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.release.get = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(RadarrError):
            await client.search_releases(1)


class TestGrabRelease:
    @staticmethod
    def make_release() -> ReleaseResult:
        return ReleaseResult(
            guid="guid-123",
            title="The.Matrix.1999.1080p.BluRay",
            indexer_id=3,
            custom_formats=[],
            custom_format_score=0,
            quality="BluRay-1080p",
            size=0,
            download_allowed=True,
        )

    async def test_happy_path(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.release.add = AsyncMock(return_value=None)

        await client.grab_release(self.make_release())

        mock_api.release.add.assert_awaited_once_with("guid-123", 3)

    async def test_auth_error(self, client: RadarrClient, mock_api: MagicMock) -> None:
        mock_api.release.add = AsyncMock(side_effect=PyarrUnauthorizedError("unauth"))
        with pytest.raises(RadarrAuthError):
            await client.grab_release(self.make_release())

    async def test_connection_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.release.add = AsyncMock(side_effect=OSError("down"))
        with pytest.raises(RadarrConnectionError):
            await client.grab_release(self.make_release())

    async def test_generic_error(
        self, client: RadarrClient, mock_api: MagicMock
    ) -> None:
        mock_api.release.add = AsyncMock(side_effect=RuntimeError("crash"))
        with pytest.raises(RadarrError):
            await client.grab_release(self.make_release())


class TestGetBlocklistSourceTitles:
    async def test_happy_path(self, client: RadarrClient) -> None:
        payload = {
            "records": [
                {"sourceTitle": "The.Matrix.1999.1080p"},
                {"sourceTitle": "Inception.2010.1080p"},
                {"sourceTitle": ""},
            ]
        }
        with aioresponses() as mocked:
            mocked.get(BLOCKLIST_URL_PATTERN, payload=payload)
            result = await client.get_blocklist_source_titles()

        assert result == {"The.Matrix.1999.1080p", "Inception.2010.1080p"}

    async def test_empty_blocklist(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(BLOCKLIST_URL_PATTERN, payload={"records": []})
            assert await client.get_blocklist_source_titles() == set()

    async def test_pagination(self, client: RadarrClient) -> None:
        page_1 = [{"sourceTitle": f"title-{index}"} for index in range(1000)]
        page_2 = [{"sourceTitle": "title-last"}]

        with aioresponses() as mocked:
            mocked.get(BLOCKLIST_URL_PATTERN, payload={"records": page_1})
            mocked.get(BLOCKLIST_URL_PATTERN, payload={"records": page_2})
            result = await client.get_blocklist_source_titles()

        assert len(result) == 1001
        assert "title-0" in result
        assert "title-last" in result

    async def test_connection_error(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(
                BLOCKLIST_URL_PATTERN,
                exception=aiohttp.ClientError("connection failed"),
            )
            with pytest.raises(RadarrConnectionError):
                await client.get_blocklist_source_titles()

    async def test_timeout_error(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(
                BLOCKLIST_URL_PATTERN,
                exception=aiohttp.ServerTimeoutError(),
            )
            with pytest.raises(RadarrConnectionError):
                await client.get_blocklist_source_titles()

    async def test_generic_error(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(
                BLOCKLIST_URL_PATTERN,
                exception=RuntimeError("unexpected"),
            )
            with pytest.raises(RadarrError):
                await client.get_blocklist_source_titles()


class TestGetMovieFile:
    async def test_happy_path(self, client: RadarrClient) -> None:
        payload = [
            {
                "id": 55,
                "customFormatScore": 200,
                "customFormats": [{"name": "BluRay"}],
            }
        ]
        with aioresponses() as mocked:
            mocked.get(MOVIE_FILE_URL_PATTERN, payload=payload)
            result = await client.get_movie_file(1)

        assert result is not None
        assert result["id"] == 55
        assert result["customFormatScore"] == 200

    async def test_not_found_returns_none(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(MOVIE_FILE_URL_PATTERN, status=404, payload={})
            assert await client.get_movie_file(999) is None

    async def test_empty_payload_returns_none(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(MOVIE_FILE_URL_PATTERN, payload=[])
            assert await client.get_movie_file(1) is None

    async def test_connection_error(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(
                MOVIE_FILE_URL_PATTERN,
                exception=aiohttp.ClientError("down"),
            )
            with pytest.raises(RadarrConnectionError):
                await client.get_movie_file(1)

    async def test_timeout_error(self, client: RadarrClient) -> None:
        with aioresponses() as mocked:
            mocked.get(
                MOVIE_FILE_URL_PATTERN,
                exception=aiohttp.ServerTimeoutError(),
            )
            with pytest.raises(RadarrConnectionError):
                await client.get_movie_file(1)
