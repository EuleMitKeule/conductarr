"""Example flow: exercise SABnzbd, Radarr, and Sonarr dev stack."""

from __future__ import annotations

import asyncio
import base64
import os
import sys
import time
from pathlib import Path

import aiohttp

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from conductarr.clients.radarr import RadarrClient
from conductarr.clients.sabnzbd import SABnzbdClient
from conductarr.clients.sonarr import SonarrClient
from tests.integration.clients.test_sabnzbd import MINIMAL_TEST_NZB


async def wait_for_service(url: str, name: str, *, timeout: int = 120) -> None:
    """Poll a service URL until it responds or *timeout* seconds elapse."""
    base = url.rstrip("/")
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
    sab_key = os.environ.get("SABNZBD_API_KEY", "")
    radarr_url = os.environ.get("RADARR_URL", "http://localhost:7878")
    radarr_key = os.environ.get("RADARR_API_KEY", "")
    sonarr_url = os.environ.get("SONARR_URL", "http://localhost:8989")
    sonarr_key = os.environ.get("SONARR_API_KEY", "")

    await wait_for_service(sab_url, "SABnzbd")

    # ---- SABnzbd ----
    async with SABnzbdClient(url=sab_url, api_key=sab_key) as client:
        version = await client.version()
        print(f"SABnzbd version: {version}")

        queue = await client.get_queue()
        print(f"SABnzbd queue — paused: {queue.paused}, slots: {queue.noofslots}")

        # Upload minimal test NZB
        nzb_bytes = base64.b64decode(MINIMAL_TEST_NZB)
        base_url = sab_url.rstrip("/")
        async with aiohttp.ClientSession() as session:
            form = aiohttp.FormData()
            form.add_field(
                "name",
                nzb_bytes,
                filename="conductarr-test.nzb",
                content_type="application/x-nzb",
            )
            async with session.post(
                f"{base_url}/api",
                params={"mode": "addfile", "output": "json", "apikey": sab_key},
                data=form,
            ) as resp:
                upload_result = await resp.json()

        assert upload_result.get("status") is True, f"Upload failed: {upload_result}"
        nzo_id: str = upload_result["nzo_ids"][0]
        print(f"Uploaded NZB — nzo_id: {nzo_id}")

        await asyncio.sleep(2)
        queue = await client.get_queue()
        print(f"Queue after upload — paused: {queue.paused}, slots: {queue.noofslots}")
        for slot in queue.slots:
            print(
                f"  nzo_id={slot.nzo_id}  filename={slot.filename}"
                f"  status={slot.status}  priority={slot.priority}"
            )

        deleted = await client.delete_job(nzo_id, del_files=True)
        print(f"Deleted {nzo_id}: {deleted}")

    # ---- Radarr ----
    radarr = RadarrClient(url=radarr_url, api_key=radarr_key)
    try:
        radarr_queue = await radarr.get_queue()
        print(f"Radarr queue items: {len(radarr_queue)}")
    except Exception as exc:
        print(f"Radarr queue error: {exc}")

    try:
        movies = await radarr.get_movies()
        print(f"Radarr total movies: {len(movies)}")
    except Exception as exc:
        print(f"Radarr movies error: {exc}")

    # ---- Sonarr ----
    sonarr = SonarrClient(url=sonarr_url, api_key=sonarr_key)
    try:
        sonarr_queue = await sonarr.get_queue()
        print(f"Sonarr queue items: {len(sonarr_queue)}")
    except Exception as exc:
        print(f"Sonarr queue error: {exc}")

    try:
        series = await sonarr.get_series()
        print(f"Sonarr total series: {len(series)}")
    except Exception as exc:
        print(f"Sonarr series error: {exc}")

    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
