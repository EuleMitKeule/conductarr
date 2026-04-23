"""Sonarr mock FastAPI application."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from .state import MockRelease, SonarrState

API_KEY = "sonarr-test-key"

app = FastAPI(title="Mock Sonarr")
state = SonarrState()


# ---------------------------------------------------------------------------
# API key dependency
# ---------------------------------------------------------------------------


def _require_api_key(request: Request) -> None:
    key = request.headers.get("X-Api-Key") or request.query_params.get("apikey")
    if not key or key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")


# ---------------------------------------------------------------------------
# Real API endpoints — /api/v3/
# ---------------------------------------------------------------------------


@app.get("/api")
async def api_version_detect() -> dict:
    """pyarr calls this to auto-detect the API version."""
    return {"current": "v3", "deprecated": []}


@app.get("/api/v3/system/status")
async def system_status() -> dict:
    return {"version": "4.0.0", "appName": "Sonarr"}


@app.get("/api/v3/series/{series_id}")
async def get_series_by_id(series_id: int, _: None = Depends(_require_api_key)) -> dict:
    s = state.series.get(series_id)
    if s is None:
        raise HTTPException(status_code=404, detail="Series not found")
    return state.series_to_dict(s)


@app.get("/api/v3/series")
async def get_series(_: None = Depends(_require_api_key)) -> list[dict]:
    return [state.series_to_dict(s) for s in state.series.values()]


@app.get("/api/v3/episode/{episode_id}")
async def get_episode_by_id(
    episode_id: int, _: None = Depends(_require_api_key)
) -> dict:
    ep = state.episodes.get(episode_id)
    if ep is None:
        raise HTTPException(status_code=404, detail="Episode not found")
    return state.episode_to_dict(ep)


@app.get("/api/v3/episode")
async def get_episodes(
    seriesId: int = Query(...),  # noqa: N803
    _: None = Depends(_require_api_key),
) -> list[dict]:
    return [
        state.episode_to_dict(ep)
        for ep in state.episodes.values()
        if ep.series_id == seriesId
    ]


@app.get("/api/v3/episodeFile/{episode_file_id}")
async def get_episode_file(
    episode_file_id: int, _: None = Depends(_require_api_key)
) -> dict:
    ep = state.episodes.get(episode_file_id)
    if ep is None or not ep.has_file:
        raise HTTPException(status_code=404, detail="EpisodeFile not found")
    return state.episode_file_to_dict(ep)


@app.get("/api/v3/tag")
async def get_tags(_: None = Depends(_require_api_key)) -> list[dict]:
    return [state.tag_to_dict(t) for t in state.tags.values()]


@app.get("/api/v3/queue")
async def get_queue(_: None = Depends(_require_api_key)) -> dict:
    records = [state.queue_item_to_dict(q) for q in state.queue.values()]
    return {"totalRecords": len(records), "records": records}


@app.post("/api/v3/command")
async def post_command(body: dict, _: None = Depends(_require_api_key)) -> dict:
    name = body.get("name", "")
    return {"id": 1, "name": name, "status": "started"}


@app.get("/api/v3/release")
async def get_releases(
    episodeId: int = Query(...),  # noqa: N803
    _: None = Depends(_require_api_key),
) -> list[dict]:
    releases = state.releases.get(episodeId, [])
    return [state.release_to_dict(r) for r in releases]


@app.post("/api/v3/release")
async def grab_release(body: dict, _: None = Depends(_require_api_key)) -> dict:
    guid = body.get("guid", "")
    if guid:
        state.grabbed.append(guid)
    return {"id": 1, "guid": guid, "status": "grabbed"}


@app.get("/api/v3/blocklist")
async def get_blocklist(
    pageSize: int = Query(default=10),  # noqa: N803
    page: int = Query(default=1),
    _: None = Depends(_require_api_key),
) -> dict:
    return state.blocklist_to_page(page, pageSize)


# ---------------------------------------------------------------------------
# Pydantic models for control endpoints
# ---------------------------------------------------------------------------


class EpisodeSpec(BaseModel):
    season_number: int
    episode_number: int
    title: str
    monitored: bool = True


class AddSeriesRequest(BaseModel):
    title: str
    tvdb_id: int
    monitored: bool = True
    status: str = "continuing"
    tags: list[str] = []
    episodes: list[EpisodeSpec] = []


class AddReleaseRequest(BaseModel):
    episode_id: int
    guid: str
    title: str
    indexer_id: int = 1
    custom_formats: list[str] = []
    custom_format_score: int = 0
    download_allowed: bool = True


class BlocklistAddRequest(BaseModel):
    identifier: str  # guid or sourceTitle to blocklist


class ReleaseEpisodeRequest(BaseModel):
    episode_id: int
    nzo_id: str


class FinishEpisodeRequest(BaseModel):
    episode_id: int
    custom_format_score: int = 100
    custom_formats: list[str] = []


class CancelEpisodeRequest(BaseModel):
    episode_id: int


# ---------------------------------------------------------------------------
# Control endpoints
# ---------------------------------------------------------------------------


@app.post("/control/reset")
async def control_reset() -> dict:
    state.reset()
    return {"ok": True}


@app.post("/control/series/add")
async def control_series_add(body: AddSeriesRequest) -> dict:
    s, episodes = state.add_series(
        title=body.title,
        tvdb_id=body.tvdb_id,
        monitored=body.monitored,
        status=body.status,
        tag_labels=body.tags,
        episodes=[ep.model_dump() for ep in body.episodes],
    )
    result = state.series_to_dict(s)
    result["episodes"] = [state.episode_to_dict(ep) for ep in episodes]
    return result


@app.post("/control/episode/release")
async def control_episode_release(body: ReleaseEpisodeRequest) -> dict:
    ep = state.episodes.get(body.episode_id)
    if ep is None:
        raise HTTPException(status_code=404, detail="Episode not found")
    s = state.series.get(ep.series_id)
    title = s.title if s else ""
    item = state.add_queue_item(
        series_id=ep.series_id,
        episode_id=ep.id,
        title=title,
        download_id=body.nzo_id,
    )
    return state.queue_item_to_dict(item)


@app.post("/control/episode/finished")
async def control_episode_finished(body: FinishEpisodeRequest) -> dict:
    ep = state.episodes.get(body.episode_id)
    if ep is None:
        raise HTTPException(status_code=404, detail="Episode not found")
    ep.has_file = True
    ep.custom_format_score = body.custom_format_score
    ep.custom_formats = body.custom_formats
    state.remove_queue_items_for_episode(ep.id)
    return state.episode_to_dict(ep)


@app.post("/control/episode/cancelled")
async def control_episode_cancelled(body: CancelEpisodeRequest) -> dict:
    ep = state.episodes.get(body.episode_id)
    if ep is None:
        raise HTTPException(status_code=404, detail="Episode not found")
    state.remove_queue_items_for_episode(ep.id)
    return {"ok": True}


@app.get("/control/state")
async def control_state() -> dict:
    return state.to_dict()


@app.post("/control/release/add")
async def control_release_add(body: AddReleaseRequest) -> dict:
    ep = state.episodes.get(body.episode_id)
    if ep is None:
        raise HTTPException(status_code=404, detail="Episode not found")
    release = MockRelease(
        guid=body.guid,
        title=body.title,
        indexer_id=body.indexer_id,
        custom_formats=body.custom_formats,
        custom_format_score=body.custom_format_score,
        download_allowed=body.download_allowed,
    )
    state.add_release(ep.id, release)
    return state.release_to_dict(release)


@app.post("/control/blocklist/add")
async def control_blocklist_add(body: BlocklistAddRequest) -> dict:
    state.add_to_blocklist(body.identifier)
    return {"ok": True, "identifier": body.identifier}
