"""In-memory fakes for SABnzbd and the Arr apps used by unit tests."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, ClassVar

from conductarr.clients.arr import ArrClient, ArrConnectionError
from conductarr.clients.release import ArrQueueItem, MediaState, ReleaseResult
from conductarr.clients.sabnzbd import Queue, QueueSlot, SABnzbdClient
from conductarr.config import (
    AcceptConditionConfig,
    ConductarrConfig,
    MatcherConfig,
    RadarrConfig,
    SabnzbdConfig,
    SonarrConfig,
    UpgradeConfig,
    VirtualQueueConfig,
)
from conductarr.config import MemoryDatabaseConfig
from conductarr.db.database import Database


@dataclass
class FakeJob:
    nzo_id: str
    status: str = "Downloading"


class FakeSab(SABnzbdClient):
    def __init__(self) -> None:
        super().__init__(url="http://sab", api_key="key")
        self.jobs: list[FakeJob] = []
        self.history: list[dict[str, Any]] = []
        self.paused = False
        self.diskspace: float | None = 500.0
        self.calls: list[tuple[str, ...]] = []
        self.offline = False

    def add(self, nzo_id: str, status: str = "Downloading") -> None:
        self.jobs.append(FakeJob(nzo_id, status))

    def finish(self, nzo_id: str, status: str = "Completed") -> None:
        self.jobs = [j for j in self.jobs if j.nzo_id != nzo_id]
        self.history.append({"nzo_id": nzo_id, "status": status})

    def remove(self, nzo_id: str) -> None:
        self.jobs = [j for j in self.jobs if j.nzo_id != nzo_id]

    def status_of(self, nzo_id: str) -> str:
        return next(j.status for j in self.jobs if j.nzo_id == nzo_id)

    @property
    def order(self) -> list[str]:
        return [j.nzo_id for j in self.jobs]

    @property
    def write_calls(self) -> list[tuple[str, ...]]:
        return [c for c in self.calls if c[0] in ("pause", "resume", "switch")]

    async def get_queue(self) -> Queue:
        if self.offline:
            raise ConnectionError("offline")
        return Queue(
            status="Downloading",
            paused=self.paused,
            noofslots=len(self.jobs),
            slots=[
                QueueSlot(
                    nzo_id=j.nzo_id,
                    filename=j.nzo_id,
                    cat="*",
                    priority="Normal",
                    status=j.status,
                    index=i,
                    mb="1",
                    mbleft="1",
                    percentage="0",
                    timeleft="0",
                    labels=[],
                )
                for i, j in enumerate(self.jobs)
            ],
            diskspace_free_gb=self.diskspace,
        )

    async def pause_job(self, nzo_id: str) -> bool:
        self.calls.append(("pause", nzo_id))
        for j in self.jobs:
            if j.nzo_id == nzo_id:
                j.status = "Paused"
        return True

    async def resume_job(self, nzo_id: str) -> bool:
        self.calls.append(("resume", nzo_id))
        for j in self.jobs:
            if j.nzo_id == nzo_id:
                j.status = "Downloading"
        return True

    async def switch(self, nzo_id: str, other_nzo_id: str) -> tuple[int, int]:
        self.calls.append(("switch", nzo_id, other_nzo_id))
        job = next(j for j in self.jobs if j.nzo_id == nzo_id)
        self.jobs.remove(job)
        idx = next(i for i, j in enumerate(self.jobs) if j.nzo_id == other_nzo_id)
        self.jobs.insert(idx, job)
        return (idx, 0)

    async def get_history(
        self, nzo_ids: list[str] | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        self.calls.append(("history",))
        return [h for h in self.history if not nzo_ids or h["nzo_id"] in nzo_ids]


class FakeArr(ArrClient):
    source: ClassVar[str] = "radarr"

    def __init__(self) -> None:
        super().__init__(f"http://{self.source}", "key")
        self.queue: list[ArrQueueItem] = []
        self.media: dict[int, MediaState] = {}
        self.tags: dict[int, list[str]] = {}
        self.releases: dict[int, list[ReleaseResult]] = {}
        self.blocklist: set[str] = set()
        self.custom_format_names = {"German DL", "OV/ENG/GER", "OV/GER"}
        self.grabbed: list[ReleaseResult] = []
        self.searches: list[int] = []
        self.fail_grab = False
        self.fail_search = False
        self.fail_queue = False
        self.on_grab: Callable[[ReleaseResult], Awaitable[None]] | None = None

    def add_media(
        self,
        media_id: int,
        *,
        score: int = 0,
        formats: list[str] | None = None,
        resolution: int = 1080,
        has_file: bool = True,
        monitored: bool = True,
        tags: list[str] | None = None,
    ) -> None:
        self.media[media_id] = MediaState(
            media_id=media_id,
            title=f"{self.source}-{media_id}",
            monitored=monitored,
            has_file=has_file,
            custom_formats=formats or [],
            custom_format_score=score,
            quality=f"{resolution}p",
            resolution=resolution,
        )
        self.tags[media_id] = tags or []

    def add_release(self, media_id: int, **kwargs: Any) -> ReleaseResult:
        kwargs.setdefault(
            "guid", f"guid-{media_id}-{len(self.releases.get(media_id, []))}"
        )
        kwargs.setdefault("title", f"Release.{media_id}.{kwargs['guid']}")
        kwargs.setdefault("indexer_id", 1)
        kwargs.setdefault("resolution", 1080)
        release = ReleaseResult(**kwargs)
        self.releases.setdefault(media_id, []).append(release)
        return release

    def _queue_media_key(self) -> str:
        return "id"

    async def get_queue(self) -> list[ArrQueueItem]:
        if self.fail_queue:
            raise ArrConnectionError("down")
        return list(self.queue)

    async def get_media_state(self, media_id: int) -> MediaState | None:
        return self.media.get(media_id)

    async def get_media_tags(self, media_id: int) -> list[str]:
        return self.tags.get(media_id, [])

    async def list_media_ids_with_files(self) -> list[int]:
        return sorted(i for i, m in self.media.items() if m.has_file)

    async def search_releases(self, media_id: int) -> list[ReleaseResult]:
        self.searches.append(media_id)
        if self.fail_search:
            raise ArrConnectionError("indexer down")
        return list(self.releases.get(media_id, []))

    async def grab_release(self, release: ReleaseResult) -> None:
        if self.on_grab is not None:
            await self.on_grab(release)
        if self.fail_grab:
            raise ArrConnectionError("grab failed")
        self.grabbed.append(release)

    async def get_custom_format_names(self) -> set[str]:
        return set(self.custom_format_names)

    async def get_blocklist_source_titles(self) -> set[str]:
        return set(self.blocklist)


class FakeRadarr(FakeArr):
    source: ClassVar[str] = "radarr"


class FakeSonarr(FakeArr):
    source: ClassVar[str] = "sonarr"


def make_config(**overrides: Any) -> ConductarrConfig:
    """Standard test config: requests(100) > upgrade(50) > fallback(0)."""
    upgrade_kwargs: dict[str, Any] = {
        "sources": ["radarr", "sonarr"],
        "max_active": 1,
        "search_interval": 0,
        "max_searches_per_day": 0,
        "defer_to_other_downloads": False,
        "accept_conditions": [
            AcceptConditionConfig(type="custom_format", name="German DL")
        ],
        **overrides.pop("upgrade", {}),
    }
    upgrade = UpgradeConfig(**upgrade_kwargs)
    return ConductarrConfig(
        poll_interval=1.0,
        sabnzbd=SabnzbdConfig(url="http://sab", api_key="key"),
        radarr=RadarrConfig(url="http://radarr", api_key="key"),
        sonarr=SonarrConfig(url="http://sonarr", api_key="key"),
        queues=[
            VirtualQueueConfig(
                name="requests",
                priority=100,
                matchers=[MatcherConfig(type="tags", tags=["request"])],
            ),
            VirtualQueueConfig(
                name="german_upgrade",
                priority=50,
                matchers=[MatcherConfig(type="tags", tags=["upgrade-de"])],
                upgrade=upgrade,
            ),
            VirtualQueueConfig(name="other", priority=0, fallback=True),
        ],
        **overrides,
    )


async def make_db() -> Database:
    db = Database(MemoryDatabaseConfig())
    await db.connect()
    return db
