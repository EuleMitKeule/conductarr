"""In-memory state for the Radarr mock."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class MockMovie:
    id: int
    title: str
    tmdb_id: int
    monitored: bool = True
    has_file: bool = False
    custom_format_score: int = 0
    quality_profile_id: int = 1
    tags: list[int] = field(default_factory=list)


@dataclass
class MockTag:
    id: int
    label: str


@dataclass
class MockQueueItem:
    id: int
    movie_id: int
    title: str
    download_id: str
    status: str = "downloading"
    quality: str = "Bluray-1080p"
    custom_format_score: int = 0


class RadarrState:
    def __init__(self) -> None:
        self.movies: dict[int, MockMovie] = {}
        self.tags: dict[int, MockTag] = {}
        self.queue: dict[int, MockQueueItem] = {}
        self._movie_counter: int = 0
        self._tag_counter: int = 0
        self._queue_counter: int = 0

    def reset(self) -> None:
        self.movies.clear()
        self.tags.clear()
        self.queue.clear()
        self._movie_counter = 0
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

    # -- movie operations --

    def add_movie(
        self,
        title: str,
        tmdb_id: int,
        monitored: bool = True,
        has_file: bool = False,
        custom_format_score: int = 0,
        tag_labels: list[str] | None = None,
    ) -> MockMovie:
        self._movie_counter += 1
        tag_ids = [self.find_or_create_tag(label) for label in (tag_labels or [])]
        movie = MockMovie(
            id=self._movie_counter,
            title=title,
            tmdb_id=tmdb_id,
            monitored=monitored,
            has_file=has_file,
            custom_format_score=custom_format_score,
            tags=tag_ids,
        )
        self.movies[movie.id] = movie
        return movie

    def find_movie_by_tmdb(self, tmdb_id: int) -> MockMovie | None:
        for movie in self.movies.values():
            if movie.tmdb_id == tmdb_id:
                return movie
        return None

    # -- queue operations --

    def add_queue_item(
        self, movie_id: int, title: str, download_id: str
    ) -> MockQueueItem:
        self._queue_counter += 1
        item = MockQueueItem(
            id=self._queue_counter,
            movie_id=movie_id,
            title=title,
            download_id=download_id,
        )
        self.queue[item.id] = item
        return item

    def remove_queue_items_for_movie(self, movie_id: int) -> None:
        to_remove = [qid for qid, q in self.queue.items() if q.movie_id == movie_id]
        for qid in to_remove:
            del self.queue[qid]

    # -- serialisation helpers --

    def movie_to_dict(self, movie: MockMovie) -> dict[str, Any]:
        result: dict[str, Any] = {
            "id": movie.id,
            "title": movie.title,
            "tmdbId": movie.tmdb_id,
            "monitored": movie.monitored,
            "hasFile": movie.has_file,
            "qualityProfileId": movie.quality_profile_id,
            "tags": movie.tags,
            "customFormatScore": movie.custom_format_score,
            "statistics": {"movieFileCount": 1 if movie.has_file else 0},
        }
        if movie.has_file:
            result["movieFile"] = {
                "quality": {"quality": {"name": "Bluray-1080p"}},
                "customFormatScore": movie.custom_format_score,
            }
        else:
            result["movieFile"] = None
        return result

    def queue_item_to_dict(self, item: MockQueueItem) -> dict[str, Any]:
        return {
            "id": item.id,
            "movieId": item.movie_id,
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
            "movies": {mid: self.movie_to_dict(m) for mid, m in self.movies.items()},
            "tags": {tid: self.tag_to_dict(t) for tid, t in self.tags.items()},
            "queue": {qid: self.queue_item_to_dict(q) for qid, q in self.queue.items()},
        }
