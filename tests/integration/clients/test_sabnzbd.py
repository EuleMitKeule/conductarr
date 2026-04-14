"""Integration tests for the SABnzbd API client.

Requires a running SABnzbd instance. Set SABNZBD_URL and SABNZBD_API_KEY
environment variables before running, or start SABnzbd via docker compose
so that ./dev/sabnzbd/sabnzbd.ini is created automatically.
"""

from __future__ import annotations

import base64
import os
import re
from pathlib import Path

import aiohttp
import pytest

from conductarr.clients.sabnzbd import Queue, SABnzbdClient, SABnzbdPriority

_SABNZBD_INI = (
    Path(__file__).parent.parent.parent.parent / "dev" / "sabnzbd" / "sabnzbd.ini"
)
_API_KEY_RE = re.compile(r"^api_key\s*=\s*(\w+)", re.MULTILINE)


def _has_api_key() -> bool:
    if os.getenv("SABNZBD_API_KEY"):
        return True
    if _SABNZBD_INI.exists():
        return bool(_API_KEY_RE.search(_SABNZBD_INI.read_text(encoding="utf-8")))
    return False


pytestmark = pytest.mark.skipif(
    not _has_api_key(),
    reason="SABNZBD_API_KEY not set and ./dev/sabnzbd/sabnzbd.ini not found",
)

# ---------------------------------------------------------------------------
# Minimal valid NZB for upload tests
# ---------------------------------------------------------------------------

_MINIMAL_NZB_XML = b"""\
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN"
  "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <file poster="test@conductarr.invalid" date="1700000000"
        subject="[1/1] conductarr-test.nfo (1/1)">
    <groups>
      <group>alt.binaries.test</group>
    </groups>
    <segments>
      <segment bytes="100" number="1">conductarr-test-abc123@test.invalid</segment>
    </segments>
  </file>
</nzb>
"""

MINIMAL_TEST_NZB = base64.b64encode(_MINIMAL_NZB_XML).decode()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_version(sabnzbd_client: SABnzbdClient) -> None:
    result = await sabnzbd_client.version()
    assert isinstance(result, str)
    assert len(result) > 0


async def test_get_queue(sabnzbd_client: SABnzbdClient) -> None:
    result = await sabnzbd_client.get_queue()
    assert isinstance(result, Queue)


async def test_get_history(sabnzbd_client: SABnzbdClient) -> None:
    result = await sabnzbd_client.get_history()
    assert isinstance(result, list)


async def test_pause_resume_queue(sabnzbd_client: SABnzbdClient) -> None:
    paused = await sabnzbd_client.pause_queue()
    assert paused is True

    resumed = await sabnzbd_client.resume_queue()
    assert resumed is True


async def test_add_and_manage_job(sabnzbd_client: SABnzbdClient) -> None:
    """Upload a minimal NZB and exercise the full per-job API surface."""
    url = sabnzbd_client.url.rstrip("/")
    api_key = sabnzbd_client.api_key

    # Upload via multipart POST (mode=addfile)
    nzb_bytes = base64.b64decode(MINIMAL_TEST_NZB)
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field(
            "name",
            nzb_bytes,
            filename="conductarr-test.nzb",
            content_type="application/x-nzb",
        )
        async with session.post(
            f"{url}/api",
            params={"mode": "addfile", "output": "json", "apikey": api_key},
            data=form,
        ) as resp:
            upload_result = await resp.json()

    assert upload_result.get("status") is True, f"Upload failed: {upload_result}"
    nzo_ids: list[str] = upload_result["nzo_ids"]
    assert nzo_ids, "No nzo_ids returned after upload"
    nzo_id = nzo_ids[0]

    # Job should appear in queue
    queue = await sabnzbd_client.get_queue()
    nzo_ids_in_queue = [s.nzo_id for s in queue.slots]
    assert nzo_id in nzo_ids_in_queue, (
        f"{nzo_id} not found in queue: {nzo_ids_in_queue}"
    )

    # Pause
    paused = await sabnzbd_client.pause_job(nzo_id)
    assert paused is True

    # Resume
    resumed = await sabnzbd_client.resume_job(nzo_id)
    assert resumed is True

    # Set priority
    new_pos = await sabnzbd_client.set_priority(nzo_id, SABnzbdPriority.HIGH)
    assert isinstance(new_pos, int)

    # Delete with files
    deleted = await sabnzbd_client.delete_job(nzo_id, del_files=True)
    assert deleted is True

    # Job should be gone from queue
    queue_after = await sabnzbd_client.get_queue()
    remaining = [s.nzo_id for s in queue_after.slots]
    assert nzo_id not in remaining
