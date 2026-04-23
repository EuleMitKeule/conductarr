"""In-memory state for the Sonarr mock."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MockSeries:
    id: int
    title: str
    tvdb_id: int
    monitored: bool = True
    status: str = "continuing"
    tags: list[int] = field(default_factory=list)


@dataclass
class MockEpisode:
    id: int
    series_id: int
    season_number: int
    episode_number: int
    title: str
    monitored: bool = True
    has_file: bool = False
    custom_format_score: int = 0
    custom_formats: list[str] = field(default_factory=list)


@dataclass
class MockTag:
    id: int
    label: str


@dataclass
class MockQueueItem:
    id: int
    series_id: int
    episode_id: int
    title: str
    download_id: str
    status: str = "downloading"
    quality: str = "Bluray-1080p"
    custom_format_score: int = 0


@dataclass
class MockRelease:
    guid: str
    title: str
    indexer_id: int = 1
    custom_formats: list[str] = field(default_factory=list)
    custom_format_score: int = 0
    download_allowed: bool = True


class SonarrState:
    def __init__(self) -> None:
        self.series: dict[int, MockSeries] = {}
        self.episodes: dict[int, MockEpisode] = {}
        self.tags: dict[int, MockTag] = {}
        self.queue: dict[int, MockQueueItem] = {}
        # episode_id → list of available releases
        self.releases: dict[int, list[MockRelease]] = {}
        # Identifiers (guid or sourceTitle) that appear in the blocklist
        self.blocklist: list[str] = []
        # GUIDs that were grabbed via POST /api/v3/release (for test assertions)
        self.grabbed: list[str] = []
        self._series_counter: int = 0
        self._episode_counter: int = 0
        self._tag_counter: int = 0
        self._queue_counter: int = 0
        self._blocklist_counter: int = 0

    def reset(self) -> None:
        self.series.clear()
        self.episodes.clear()
        self.tags.clear()
        self.queue.clear()
        self.releases.clear()
        self.blocklist.clear()
        self.grabbed.clear()
        self._series_counter = 0
        self._episode_counter = 0
        self._tag_counter = 0
        self._queue_counter = 0
        self._blocklist_counter = 0

    # -- tag helpers --

    def find_or_create_tag(self, label: str) -> int:
        for tag in self.tags.values():
            if tag.label == label:
                return tag.id
        self._tag_counter += 1
        tag = MockTag(id=self._tag_counter, label=label)
        self.tags[tag.id] = tag
        return tag.id

    # -- series operations --

    def add_series(
        self,
        title: str,
        tvdb_id: int,
        monitored: bool = True,
        status: str = "continuing",
        tag_labels: list[str] | None = None,
        episodes: list[dict[str, Any]] | None = None,
    ) -> tuple[MockSeries, list[MockEpisode]]:
        self._series_counter += 1
        tag_ids = [self.find_or_create_tag(label) for label in (tag_labels or [])]
        s = MockSeries(
            id=self._series_counter,
            title=title,
            tvdb_id=tvdb_id,
            monitored=monitored,
            status=status,
            tags=tag_ids,
        )
        self.series[s.id] = s

        created_episodes: list[MockEpisode] = []
        for ep_data in episodes or []:
            self._episode_counter += 1
            ep = MockEpisode(
                id=self._episode_counter,
                series_id=s.id,
                season_number=ep_data.get("season_number", 0),
                episode_number=ep_data.get("episode_number", 0),
                title=ep_data.get("title", ""),
                monitored=ep_data.get("monitored", True),
            )
            self.episodes[ep.id] = ep
            created_episodes.append(ep)
        return s, created_episodes

    # -- queue operations --

    def add_queue_item(
        self, series_id: int, episode_id: int, title: str, download_id: str
    ) -> MockQueueItem:
        self._queue_counter += 1
        item = MockQueueItem(
            id=self._queue_counter,
            series_id=series_id,
            episode_id=episode_id,
            title=title,
            download_id=download_id,
        )
        self.queue[item.id] = item
        return item

    def remove_queue_items_for_episode(self, episode_id: int) -> None:
        to_remove = [qid for qid, q in self.queue.items() if q.episode_id == episode_id]
        for qid in to_remove:
            del self.queue[qid]

    # -- serialisation helpers --

    def series_to_dict(self, s: MockSeries) -> dict[str, Any]:
        ep_count = sum(1 for e in self.episodes.values() if e.series_id == s.id)
        ep_file_count = sum(
            1 for e in self.episodes.values() if e.series_id == s.id and e.has_file
        )
        return {
            "id": s.id,
            "title": s.title,
            "tvdbId": s.tvdb_id,
            "monitored": s.monitored,
            "status": s.status,
            "tags": s.tags,
            "statistics": {
                "episodeCount": ep_count,
                "episodeFileCount": ep_file_count,
            },
        }

    def episode_to_dict(self, ep: MockEpisode) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": ep.id,
            "seriesId": ep.series_id,
            "seasonNumber": ep.season_number,
            "episodeNumber": ep.episode_number,
            "title": ep.title,
            "monitored": ep.monitored,
            "hasFile": ep.has_file,
            "customFormatScore": ep.custom_format_score,
            "customFormats": [
                {"id": i + 1, "name": name} for i, name in enumerate(ep.custom_formats)
            ],
        }
        if ep.has_file:
            result["episodeFileId"] = ep.id
        return result

    def episode_file_to_dict(self, ep: MockEpisode) -> dict[str, Any]:
        """Return a /api/v3/episodeFile record for the given episode."""
        return {
            "id": ep.id,
            "episodeId": ep.id,
            "seriesId": ep.series_id,
            "relativePath": f"Season {ep.season_number}/{ep.title}.mkv",
            "customFormatScore": ep.custom_format_score,
            "customFormats": [
                {"id": i + 1, "name": name} for i, name in enumerate(ep.custom_formats)
            ],
        }

    def queue_item_to_dict(self, item: MockQueueItem) -> dict[str, Any]:
        return {
            "id": item.id,
            "seriesId": item.series_id,
            "episodeId": item.episode_id,
            "title": item.title,
            "downloadId": item.download_id,
            "status": item.status,
            "quality": {"quality": {"name": item.quality}},
            "customFormatScore": item.custom_format_score,
        }

    def tag_to_dict(self, tag: MockTag) -> dict[str, Any]:
        return {"id": tag.id, "label": tag.label}

    def to_dict(self) -> dict[str, Any]:
        return {
            "series": {sid: self.series_to_dict(s) for sid, s in self.series.items()},
            "episodes": {
                eid: self.episode_to_dict(e) for eid, e in self.episodes.items()
            },
            "tags": {tid: self.tag_to_dict(t) for tid, t in self.tags.items()},
            "queue": {qid: self.queue_item_to_dict(q) for qid, q in self.queue.items()},
            "grabbed": list(self.grabbed),
        }

    # -- release operations --

    def add_release(self, episode_id: int, release: MockRelease) -> None:
        self.releases.setdefault(episode_id, []).append(release)

    def release_to_dict(self, release: MockRelease) -> dict[str, Any]:
        return {
            "guid": release.guid,
            "title": release.title,
            "indexerId": release.indexer_id,
            "customFormats": [
                {"id": i + 1, "name": name}
                for i, name in enumerate(release.custom_formats)
            ],
            "customFormatScore": release.custom_format_score,
            "quality": {"quality": {"name": "Bluray-1080p"}},
            "size": 4_000_000_000,
            "downloadAllowed": release.download_allowed,
        }

    # -- blocklist operations --

    def add_to_blocklist(self, identifier: str) -> None:
        self._blocklist_counter += 1
        self.blocklist.append(identifier)

    def blocklist_to_page(self, page: int, page_size: int) -> dict[str, Any]:
        start = (page - 1) * page_size
        records = [
            {
                "id": i + 1,
                "guid": identifier,
                "sourceTitle": identifier,
            }
            for i, identifier in enumerate(self.blocklist)
        ][start : start + page_size]
        return {"totalRecords": len(self.blocklist), "records": records}
