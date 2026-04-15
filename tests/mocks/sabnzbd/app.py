"""SABnzbd mock FastAPI application."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from pydantic import BaseModel

from .state import SABnzbdState

API_KEY = "sabnzbd-test-key"

app = FastAPI(title="Mock SABnzbd")
state = SABnzbdState()


# ---------------------------------------------------------------------------
# Pydantic models for control endpoints
# ---------------------------------------------------------------------------


class StartJobRequest(BaseModel):
    filename: str
    cat: str = "*"
    priority: str = "Normal"
    nzo_id: str | None = None


class JobIdRequest(BaseModel):
    nzo_id: str


# ---------------------------------------------------------------------------
# Real API — all via /api with query parameters
# ---------------------------------------------------------------------------


@app.api_route("/api", methods=["GET", "POST"], response_model=None)
async def api_handler(request: Request) -> JSONResponse | PlainTextResponse:
    params = dict(request.query_params)
    mode = params.get("mode", "")
    apikey = params.get("apikey", "")

    # API key validation
    if not apikey:
        return PlainTextResponse("API Key Required")
    if apikey != API_KEY:
        return PlainTextResponse("API Key Incorrect")

    if mode == "version":
        return JSONResponse({"version": "4.5.5"})

    if mode == "queue":
        name = params.get("name")
        if name is None:
            return JSONResponse(state.get_queue_response())
        value = params.get("value")
        value2 = params.get("value2")
        if name == "pause":
            if value:
                return JSONResponse(state.pause_job(value))
            return JSONResponse(state.pause_queue())
        if name == "resume":
            if value:
                return JSONResponse(state.resume_job(value))
            return JSONResponse(state.resume_queue())
        if name == "delete":
            return JSONResponse(state.delete_job(value or ""))
        if name == "priority":
            return JSONResponse(state.set_priority(value or "", int(value2 or "0")))
        return JSONResponse({"error": f"unknown queue name: {name}"}, status_code=400)

    if mode == "switch":
        value = params.get("value", "")
        value2 = params.get("value2", "")
        return JSONResponse(state.switch_jobs(value, value2))

    if mode == "pause":
        return JSONResponse(state.pause_queue())

    if mode == "resume":
        return JSONResponse(state.resume_queue())

    if mode == "history":
        return JSONResponse(state.get_history_response())

    if mode == "addfile":
        async with request.form() as form:
            uploaded = form.get("name")
            cat = str(form.get("cat", "*"))
            filename = "unknown.nzb"
            if uploaded is not None and hasattr(uploaded, "filename"):
                filename = uploaded.filename or filename
        nzo_id = state.add_job(filename=filename, cat=cat)
        return JSONResponse({"status": True, "nzo_ids": [nzo_id]})

    return JSONResponse({"error": f"unknown mode: {mode}"}, status_code=400)


# ---------------------------------------------------------------------------
# Control endpoints
# ---------------------------------------------------------------------------


@app.post("/control/reset")
async def control_reset() -> dict:
    state.reset()
    return {"ok": True}


@app.post("/control/job/start")
async def control_job_start(body: StartJobRequest) -> dict:
    nzo_id = state.add_job(
        filename=body.filename,
        cat=body.cat,
        priority=body.priority,
        nzo_id=body.nzo_id,
    )
    return {"nzo_id": nzo_id}


@app.post("/control/job/finish")
async def control_job_finish(body: JobIdRequest) -> dict:
    state.remove_job(body.nzo_id)
    return {"ok": True}


@app.post("/control/job/cancel")
async def control_job_cancel(body: JobIdRequest) -> dict:
    state.remove_job(body.nzo_id)
    return {"ok": True}


@app.get("/control/state")
async def control_state() -> dict:
    return state.to_dict()
