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


class SonarrState:
    def __init__(self) -> None:
        self.series: dict[int, MockSeries] = {}
        self.episodes: dict[int, MockEpisode] = {}
        self.tags: dict[int, MockTag] = {}
        self.queue: dict[int, MockQueueItem] = {}
        self._series_counter: int = 0
        self._episode_counter: int = 0
        self._tag_counter: int = 0
        self._queue_counter: int = 0

    def reset(self) -> None:
        self.series.clear()
        self.episodes.clear()
        self.tags.clear()
        self.queue.clear()
        self._series_counter = 0
        self._episode_counter = 0
        self._tag_counter = 0
        self._queue_counter = 0

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
        return {
            "id": ep.id,
            "seriesId": ep.series_id,
            "seasonNumber": ep.season_number,
            "episodeNumber": ep.episode_number,
            "title": ep.title,
            "monitored": ep.monitored,
            "hasFile": ep.has_file,
            "customFormatScore": ep.custom_format_score,
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
        }
