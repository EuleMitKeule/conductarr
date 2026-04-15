"""Example flow: exercise mock SABnzbd, Radarr, and Sonarr via control clients."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import aiohttp

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from conductarr.clients.sabnzbd import SABnzbdClient
from tests.mocks.control_client import (
    RadarrControlClient,
    SABnzbdControlClient,
    SonarrControlClient,
)


async def wait_for_service(url: str, name: str, *, timeout: int = 120) -> None:
    """Poll a service URL until it responds or *timeout* seconds elapse."""
    base = url.rstrip("/")
    import time

    deadline = time.monotonic() + timeout
    attempt = 0
    while True:
        attempt += 1
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    base + "/", timeout=aiohttp.ClientTimeout(total=4)
                ) as resp:
                    if resp.status < 500:
                        print(f"{name} ready (attempt {attempt}).")
                        return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{name} not available after {timeout}s")
        print(f"Waiting for {name}… (attempt {attempt})")
        await asyncio.sleep(3)


async def main() -> None:
    sab_url = os.environ.get("SABNZBD_URL", "http://localhost:8080")
    sab_key = os.environ.get("SABNZBD_API_KEY", "sabnzbd-test-key")
    radarr_url = os.environ.get("RADARR_URL", "http://localhost:7878")
    sonarr_url = os.environ.get("SONARR_URL", "http://localhost:8989")

    # Wait for services to be available
    await wait_for_service(sab_url, "SABnzbd")

    # Control clients for orchestrating the scenario
    sab_ctrl = SABnzbdControlClient(sab_url)
    radarr_ctrl = RadarrControlClient(radarr_url)
    sonarr_ctrl = SonarrControlClient(sonarr_url)

    # 1. Reset all mocks
    await sab_ctrl.reset()
    await radarr_ctrl.reset()
    await sonarr_ctrl.reset()
    print("All mocks reset.")

    # 2. Print versions from all three services
    async with SABnzbdClient(url=sab_url, api_key=sab_key) as sab_client:
        version = await sab_client.version()
        print(f"SABnzbd version: {version}")

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{radarr_url}/api/v3/system/status") as resp:
            radarr_status = await resp.json()
            print(f"Radarr version: {radarr_status['version']}")

        async with session.get(f"{sonarr_url}/api/v3/system/status") as resp:
            sonarr_status = await resp.json()
            print(f"Sonarr version: {sonarr_status['version']}")

    # 3. Simulate a user request
    movie = await radarr_ctrl.add_movie(
        title="The Matrix", tmdb_id=603, tags=["request"]
    )
    print(
        f"Added movie: {movie['title']} (id={movie['id']}, tags={movie.get('tags', [])})"
    )

    nzo_id = await sab_ctrl.start_job(
        filename="The.Matrix.1999.1080p.BluRay.nzb", cat="movies"
    )
    print(f"Started SABnzbd job: {nzo_id}")

    await radarr_ctrl.release_movie(tmdb_id=603, nzo_id=nzo_id)

    async with SABnzbdClient(url=sab_url, api_key=sab_key) as sab_client:
        queue = await sab_client.get_queue()
        print(f"SABnzbd queue — slots: {queue.noofslots}")
        for slot in queue.slots:
            print(
                f"  nzo_id={slot.nzo_id}  filename={slot.filename}"
                f"  status={slot.status}  priority={slot.priority}"
            )

    async with aiohttp.ClientSession() as session:
        async with session.get(
            f"{radarr_url}/api/v3/queue",
            params={"pageSize": 100},
            headers={"X-Api-Key": os.environ.get("RADARR_API_KEY", "radarr-test-key")},
        ) as resp:
            radarr_queue = await resp.json()
            print(f"Radarr queue — records: {len(radarr_queue.get('records', []))}")
            for rec in radarr_queue.get("records", []):
                print(
                    f"  downloadId={rec.get('downloadId')}  title={rec.get('title')}"
                    f"  status={rec.get('status')}"
                )

    # 4. Wait for Conductarr to poll and reconcile
    print("Waiting 5 seconds for Conductarr to poll and reconcile…")
    await asyncio.sleep(5)

    # 5. Check Radarr mock state to confirm the nzo_id linkage survived the poll
    state = await radarr_ctrl.get_state()
    queue_entries = state.get("queue", {})
    linked = [q for q in queue_entries.values() if q.get("download_id") == nzo_id]
    if linked:
        print(f"Reconcile confirmed: Radarr queue entry linked to nzo_id={nzo_id}")
    else:
        print(f"Warning: no Radarr queue entry found for nzo_id={nzo_id}")

    # 6. Finish the job
    await sab_ctrl.finish_job(nzo_id)
    await radarr_ctrl.finish_movie(tmdb_id=603)
    print("Finished SABnzbd job and Radarr movie.")

    # 7. Wait for Conductarr to reconcile the completion
    print("Waiting 5 seconds for Conductarr to reconcile completion…")
    await asyncio.sleep(5)

    async with SABnzbdClient(url=sab_url, api_key=sab_key) as sab_client:
        queue = await sab_client.get_queue()
        print(f"SABnzbd queue after finish — slots: {queue.noofslots}")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
