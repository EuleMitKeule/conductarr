"""Radarr mock FastAPI application."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from .state import RadarrState

API_KEY = "radarr-test-key"

app = FastAPI(title="Mock Radarr")
state = RadarrState()


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
    return {"version": "5.3.0", "appName": "Radarr"}


@app.get("/api/v3/movie/{movie_id}")
async def get_movie(movie_id: int, _: None = Depends(_require_api_key)) -> dict:
    movie = state.movies.get(movie_id)
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    return state.movie_to_dict(movie)


@app.get("/api/v3/movie")
async def get_movies(_: None = Depends(_require_api_key)) -> list[dict]:
    return [state.movie_to_dict(m) for m in state.movies.values()]


@app.get("/api/v3/tag")
async def get_tags(_: None = Depends(_require_api_key)) -> list[dict]:
    return [state.tag_to_dict(t) for t in state.tags.values()]


@app.get("/api/v3/movieFile")
async def get_movie_file(
    movieId: int = Query(...),  # noqa: N803
    _: None = Depends(_require_api_key),
) -> list[dict]:
    movie = state.movies.get(movieId)
    if movie is None or not movie.has_file:
        return []
    return [state.movie_file_to_dict(movie)]


@app.get("/api/v3/queue")
async def get_queue(_: None = Depends(_require_api_key)) -> dict:
    records = [state.queue_item_to_dict(q) for q in state.queue.values()]
    return {"totalRecords": len(records), "records": records}


@app.post("/api/v3/command")
async def post_command(body: dict, _: None = Depends(_require_api_key)) -> dict:
    name = body.get("name", "")
    return {"id": 1, "name": name, "status": "started"}


# ---------------------------------------------------------------------------
# Pydantic models for control endpoints
# ---------------------------------------------------------------------------


class AddMovieRequest(BaseModel):
    title: str
    tmdb_id: int
    monitored: bool = True
    has_file: bool = False
    custom_format_score: int = 0
    tags: list[str] = []


class ReleaseMovieRequest(BaseModel):
    tmdb_id: int
    nzo_id: str


class FinishMovieRequest(BaseModel):
    tmdb_id: int
    custom_format_score: int = 100


class CancelMovieRequest(BaseModel):
    tmdb_id: int


# ---------------------------------------------------------------------------
# Control endpoints
# ---------------------------------------------------------------------------


@app.post("/control/reset")
async def control_reset() -> dict:
    state.reset()
    return {"ok": True}


@app.post("/control/movie/add")
async def control_movie_add(body: AddMovieRequest) -> dict:
    movie = state.add_movie(
        title=body.title,
        tmdb_id=body.tmdb_id,
        monitored=body.monitored,
        has_file=body.has_file,
        custom_format_score=body.custom_format_score,
        tag_labels=body.tags,
    )
    return state.movie_to_dict(movie)


@app.post("/control/movie/release")
async def control_movie_release(body: ReleaseMovieRequest) -> dict:
    movie = state.find_movie_by_tmdb(body.tmdb_id)
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    item = state.add_queue_item(
        movie_id=movie.id, title=movie.title, download_id=body.nzo_id
    )
    return state.queue_item_to_dict(item)


@app.post("/control/movie/finished")
async def control_movie_finished(body: FinishMovieRequest) -> dict:
    movie = state.find_movie_by_tmdb(body.tmdb_id)
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    movie.has_file = True
    movie.custom_format_score = body.custom_format_score
    state.remove_queue_items_for_movie(movie.id)
    return state.movie_to_dict(movie)


@app.post("/control/movie/cancelled")
async def control_movie_cancelled(body: CancelMovieRequest) -> dict:
    movie = state.find_movie_by_tmdb(body.tmdb_id)
    if movie is None:
        raise HTTPException(status_code=404, detail="Movie not found")
    state.remove_queue_items_for_movie(movie.id)
    return {"ok": True}


@app.get("/control/state")
async def control_state() -> dict:
    return state.to_dict()
