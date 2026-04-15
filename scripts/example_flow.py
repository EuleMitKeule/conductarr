"""Example flow: upload a test NZB to SABnzbd and inspect the queue."""

from __future__ import annotations

import asyncio
import base64
import os
import re
import sys
import time
from pathlib import Path

import aiohttp

# Allow running from the repo root without installing the package.
sys.path.insert(0, str(Path(__file__).parent.parent))

from conductarr.clients.sabnzbd import SABnzbdClient
from tests.integration.clients.test_sabnzbd import MINIMAL_TEST_NZB

_SABNZBD_INI = Path(__file__).parent.parent / "dev" / "sabnzbd" / "sabnzbd.ini"
_API_KEY_RE = re.compile(r"^api_key\s*=\s*(\w+)", re.MULTILINE)


def _resolve_api_key() -> str:
    key = os.environ.get("SABNZBD_API_KEY", "")
    if key:
        return key
    if _SABNZBD_INI.exists():
        match = _API_KEY_RE.search(_SABNZBD_INI.read_text(encoding="utf-8"))
        if match:
            return match.group(1)
    return ""


async def wait_for_sabnzbd(url: str, *, timeout: int = 120) -> None:
    """Poll SABnzbd's root URL until it responds or *timeout* seconds elapse."""
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
                        print(f"SABnzbd ready (attempt {attempt}).")
                        return
        except Exception:
            pass
        if time.monotonic() >= deadline:
            raise RuntimeError(f"SABnzbd not available after {timeout}s")
        print(f"Waiting for SABnzbd… (attempt {attempt})")
        await asyncio.sleep(3)


async def main() -> None:
    url = os.environ.get("SABNZBD_URL", "http://localhost:8080")
    api_key = _resolve_api_key()

    await wait_for_sabnzbd(url)

    async with SABnzbdClient(url=url, api_key=api_key) as client:
        # 1. Version
        version = await client.version()
        print(f"SABnzbd version: {version}")

        # 2. Current queue status
        queue = await client.get_queue()
        print(f"Queue — paused: {queue.paused}, slots: {queue.noofslots}")

        # 3. Upload the minimal test NZB via multipart POST (mode=addfile)
        nzb_bytes = base64.b64decode(MINIMAL_TEST_NZB)
        base_url = url.rstrip("/")
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
                params={"mode": "addfile", "output": "json", "apikey": api_key},
                data=form,
            ) as resp:
                upload_result = await resp.json()

        assert upload_result.get("status") is True, f"Upload failed: {upload_result}"
        nzo_id: str = upload_result["nzo_ids"][0]
        print(f"Uploaded NZB — nzo_id: {nzo_id}")

        # 4. Wait 2 seconds, then print the queue again
        await asyncio.sleep(2)
        queue = await client.get_queue()
        print(f"Queue after upload — paused: {queue.paused}, slots: {queue.noofslots}")

        # 5. Print each slot
        for slot in queue.slots:
            print(
                f"  nzo_id={slot.nzo_id}  filename={slot.filename}"
                f"  status={slot.status}  priority={slot.priority}"
            )

        # 6. Delete the job
        deleted = await client.delete_job(nzo_id, del_files=True)
        print(f"Deleted {nzo_id}: {deleted}")

    # 7. Done
    print("Done.")


if __name__ == "__main__":
    asyncio.run(main())
