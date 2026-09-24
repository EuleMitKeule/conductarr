"""Unit tests for the Radarr/Sonarr HTTP clients."""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from typing import Any

import aiohttp
import pytest
import pytest_asyncio
from aioresponses import aioresponses

from conductarr.clients.arr import (
    ArrAuthError,
    ArrClient,
    ArrConnectionError,
    ArrError,
)
from conductarr.clients.radarr import RadarrClient
from conductarr.clients.release import ReleaseResult
from conductarr.clients.sonarr import SonarrClient

pytestmark = pytest.mark.asyncio

RADARR = "http://radarr:7878"
SONARR = "http://sonarr:8989/sonarr"  # with URL base path


def _url(base: str, path: str) -> re.Pattern[str]:
    return re.compile(rf"^{re.escape(base)}/api/v3/{re.escape(path)}(\?.*)?$")


@pytest_asyncio.fixture
async def radarr() -> AsyncGenerator[RadarrClient, None]:
    client = RadarrClient(RADARR, "key")
    yield client
    await client.close()


@pytest_asyncio.fixture
async def sonarr() -> AsyncGenerator[SonarrClient, None]:
    client = SonarrClient(SONARR, "key")
    yield client
    await client.close()


@pytest.fixture
def mocked() -> Any:
    with aioresponses() as m:
        yield m


def _release_json(**overrides: Any) -> dict[str, Any]:
    data: dict[str, Any] = {
        "guid": "g1",
        "title": "Movie.2020.German.DL.1080p.BluRay.x264-GRP",
        "indexerId": 3,
        "indexer": "NZBgeek",
        "protocol": "usenet",
        "customFormats": [{"id": 1, "name": "German DL"}],
        "customFormatScore": 1500,
        "quality": {"quality": {"name": "Bluray-1080p", "resolution": 1080}},
        "size": 9_000_000_000,
        "downloadAllowed": True,
        "rejections": ["Existing file meets cutoff: Bluray-1080p"],
    }
    data.update(overrides)
    return data


class TestTransport:
    async def test_api_key_header_and_base_path(
        self, sonarr: SonarrClient, mocked: Any
    ) -> None:
        mocked.get(_url(SONARR, "tag"), payload=[])
        assert await sonarr.get_tags() == {}
        request = next(iter(mocked.requests.values()))[0]
        assert request.kwargs["headers"]["X-Api-Key"] == "key"

    async def test_unauthorized(self, radarr: RadarrClient, mocked: Any) -> None:
        mocked.get(_url(RADARR, "tag"), status=401)
        with pytest.raises(ArrAuthError):
            await radarr.get_tags()

    async def test_server_error(self, radarr: RadarrClient, mocked: Any) -> None:
        mocked.get(_url(RADARR, "tag"), status=500, body="boom")
        with pytest.raises(ArrError, match="HTTP 500"):
            await radarr.get_tags()

    async def test_connection_error(self, radarr: RadarrClient, mocked: Any) -> None:
        mocked.get(_url(RADARR, "tag"), exception=aiohttp.ClientConnectionError("x"))
        with pytest.raises(ArrConnectionError):
            await radarr.get_tags()

    async def test_timeout(self, radarr: RadarrClient, mocked: Any) -> None:
        mocked.get(_url(RADARR, "tag"), exception=TimeoutError())
        with pytest.raises(ArrConnectionError, match="timed out"):
            await radarr.get_tags()

    async def test_missing_api_key(self) -> None:
        client = RadarrClient(RADARR, "")
        with pytest.raises(ArrAuthError):
            await client.get_tags()

    async def test_tags_are_cached(self, radarr: RadarrClient, mocked: Any) -> None:
        mocked.get(_url(RADARR, "tag"), payload=[{"id": 1, "label": "request"}])
        assert await radarr.get_tags() == {1: "request"}
        assert await radarr.get_tags() == {1: "request"}  # second call: cache

    def test_no_destructive_or_auto_search_methods(self) -> None:
        forbidden = re.compile(r"delete|remove|trigger|command|search_missing")
        for cls in (ArrClient, RadarrClient, SonarrClient):
            assert not [n for n in dir(cls) if forbidden.search(n)], cls


class TestRadarr:
    async def test_queue_is_paged_and_parsed(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        page1 = [
            {"downloadId": f"nzo_{i}", "movieId": i, "title": "t", "protocol": "usenet"}
            for i in range(1, 1001)
        ]
        page2 = [
            {
                "downloadId": "nzo_x",
                "movieId": 5000,
                "title": "x",
                "protocol": "usenet",
            },
            {"movieId": 9, "title": "no download id"},
        ]
        mocked.get(
            _url(RADARR, "queue"), payload={"totalRecords": 1002, "records": page1}
        )
        mocked.get(
            _url(RADARR, "queue"), payload={"totalRecords": 1002, "records": page2}
        )
        items = await radarr.get_queue()
        assert len(items) == 1001
        assert items[-1].download_id == "nzo_x" and items[-1].media_id == 5000

    async def test_media_state_uses_movie_file(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(RADARR, "movie/7"),
            payload={
                "id": 7,
                "title": "Dune",
                "year": 2021,
                "hasFile": True,
                "monitored": True,
            },
        )
        mocked.get(
            _url(RADARR, "movieFile"),
            payload=[
                {
                    "customFormats": [{"name": "German DL"}],
                    "customFormatScore": 900,
                    "quality": {"quality": {"name": "Remux-2160p", "resolution": 2160}},
                }
            ],
        )
        state = await radarr.get_media_state(7)
        assert state is not None
        assert state.title == "Dune (2021)"
        assert state.custom_formats == ["German DL"]
        assert state.custom_format_score == 900
        assert state.resolution == 2160

    async def test_media_state_not_found(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        mocked.get(_url(RADARR, "movie/7"), status=404)
        assert await radarr.get_media_state(7) is None

    async def test_media_state_without_file(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        mocked.get(_url(RADARR, "movie/7"), payload={"id": 7, "hasFile": False})
        state = await radarr.get_media_state(7)
        assert state is not None and not state.has_file

    async def test_media_tags(self, radarr: RadarrClient, mocked: Any) -> None:
        mocked.get(_url(RADARR, "movie/7"), payload={"id": 7, "tags": [1, 2]})
        mocked.get(_url(RADARR, "tag"), payload=[{"id": 1, "label": "request"}])
        assert await radarr.get_media_tags(7) == ["request"]

    async def test_list_media_with_files(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(RADARR, "movie"),
            payload=[{"id": 3, "hasFile": True}, {"id": 1, "hasFile": True}, {"id": 2}],
        )
        assert await radarr.list_media_ids_with_files() == [1, 3]

    async def test_search_releases_parses_safety_fields(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(RADARR, "release"),
            payload=[
                _release_json(mappedMovieId=7),
                _release_json(
                    guid="g2",
                    protocol="torrent",
                    rejections=[{"reason": "x", "message": "Wrong movie"}],
                ),
            ],
        )
        usenet, torrent = await radarr.search_releases(7)
        assert usenet.protocol == "usenet"
        assert usenet.mapped_media_ids == [7]
        assert usenet.resolution == 1080
        assert usenet.custom_formats == ["German DL"]
        assert usenet.indexer == "NZBgeek"
        assert torrent.protocol == "torrent"
        assert torrent.rejections == ["Wrong movie"]

    async def test_missing_protocol_is_unknown(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        raw = _release_json()
        del raw["protocol"]
        mocked.get(_url(RADARR, "release"), payload=[raw])
        (release,) = await radarr.search_releases(7)
        assert release.protocol == "unknown"

    async def test_grab_posts_guid_and_indexer(
        self, radarr: RadarrClient, mocked: Any
    ) -> None:
        mocked.post(_url(RADARR, "release"), payload={})
        await radarr.grab_release(ReleaseResult(guid="g1", title="t", indexer_id=3))
        request = next(iter(mocked.requests.values()))[0]
        assert request.kwargs["json"] == {"guid": "g1", "indexerId": 3}

    async def test_blocklist_is_paged(self, radarr: RadarrClient, mocked: Any) -> None:
        records = [{"sourceTitle": f"t{i}"} for i in range(1000)]
        mocked.get(
            _url(RADARR, "blocklist"),
            payload={"totalRecords": 1001, "records": records},
        )
        mocked.get(
            _url(RADARR, "blocklist"),
            payload={"totalRecords": 1001, "records": [{"sourceTitle": "last"}]},
        )
        titles = await radarr.get_blocklist_source_titles()
        assert len(titles) == 1001 and "last" in titles


class TestSonarr:
    async def test_queue_uses_episode_id(
        self, sonarr: SonarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(SONARR, "queue"),
            payload={
                "totalRecords": 1,
                "records": [{"downloadId": "nzo_1", "episodeId": 42, "seriesId": 3}],
            },
        )
        (item,) = await sonarr.get_queue()
        assert item.media_id == 42

    async def test_media_state_uses_episode_file(
        self, sonarr: SonarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(SONARR, "episode/42"),
            payload={
                "id": 42,
                "seasonNumber": 1,
                "episodeNumber": 2,
                "title": "Pilot",
                "hasFile": True,
                "episodeFileId": 9,
                "monitored": True,
                "series": {"title": "Dark"},
            },
        )
        mocked.get(
            _url(SONARR, "episodeFile/9"),
            payload={
                "customFormats": [{"name": "German DL"}],
                "customFormatScore": 50,
                "quality": {"quality": {"name": "WEBDL-1080p", "resolution": 1080}},
            },
        )
        state = await sonarr.get_media_state(42)
        assert state is not None
        assert state.title == "Dark S01E02 - Pilot"
        assert state.custom_format_score == 50
        assert state.resolution == 1080

    async def test_tags_come_from_series(
        self, sonarr: SonarrClient, mocked: Any
    ) -> None:
        mocked.get(_url(SONARR, "episode/42"), payload={"id": 42, "seriesId": 3})
        mocked.get(_url(SONARR, "series/3"), payload={"id": 3, "tags": [5]})
        mocked.get(_url(SONARR, "tag"), payload=[{"id": 5, "label": "anime"}])
        assert await sonarr.get_media_tags(42) == ["anime"]

    async def test_list_media_with_files_skips_empty_series(
        self, sonarr: SonarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(SONARR, "series"),
            payload=[
                {"id": 1, "statistics": {"episodeFileCount": 0}},
                {"id": 2, "statistics": {"episodeFileCount": 2}},
            ],
        )
        mocked.get(
            _url(SONARR, "episode"),
            payload=[{"id": 20, "hasFile": True}, {"id": 21, "hasFile": False}],
        )
        assert await sonarr.list_media_ids_with_files() == [20]
        assert len(mocked.requests) == 2  # series 1 was not queried

    async def test_search_marks_packs_and_mapping(
        self, sonarr: SonarrClient, mocked: Any
    ) -> None:
        mocked.get(
            _url(SONARR, "release"),
            payload=[
                _release_json(
                    fullSeason=True, mappedEpisodeInfo=[{"id": 1}, {"id": 2}]
                ),
                _release_json(guid="g2", mappedEpisodeInfo=[{"id": 42}]),
            ],
        )
        pack, single = await sonarr.search_releases(42)
        assert pack.full_season and pack.mapped_media_ids == [1, 2]
        assert single.mapped_media_ids == [42]
