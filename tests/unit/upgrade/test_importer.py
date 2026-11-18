"""Unit tests for finishing the imports of conductarr's own upgrade grabs."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from dataclasses import dataclass
from typing import Any
from unittest.mock import patch

import pytest
import pytest_asyncio

from conductarr.clients.arr import ArrClient
from conductarr.clients.release import ArrDownloadStatus
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.upgrade import importer as importer_module
from conductarr.upgrade.importer import (
    MAX_ATTEMPTS,
    RETRY_SECONDS,
    SETTLE_SECONDS,
    STATE_KEY,
    ImportFinisher,
    add_pending_import,
)
from tests.unit.fakes import FakeRadarr, FakeSonarr, make_config, make_db

pytestmark = pytest.mark.asyncio

NOT_UPGRADE = (
    "Not an upgrade for existing episode file(s). Existing quality: "
    "Bluray-1080p. New Quality WEBDL-1080p."
)
CF_NOT_UPGRADE = "Not a Custom Format upgrade for existing episode file(s)."
MORE_EPISODES = "Episode file on disk contains more episodes than this file contains"


@dataclass
class Harness:
    db: Database
    repo: QueueRepository
    sonarr: FakeSonarr
    finisher: ImportFinisher
    clock: list[float]

    async def finish_grab(self, nzo_id: str) -> None:
        await add_pending_import(self.repo, nzo_id, "sonarr", "german_upgrade")

    def block(self, nzo_id: str, *messages: str, state: str = "importPending") -> None:
        self.sonarr.download_status[nzo_id] = [
            ArrDownloadStatus(nzo_id, "completed", state, list(messages))
        ]
        self.sonarr.import_files[nzo_id] = [list(messages)]

    async def run(self, after: float = 0.0) -> None:
        self.clock[0] += after
        with patch.object(importer_module.time, "time", lambda: self.clock[0]):
            await self.finisher.run_cycle()

    async def pending(self) -> set[str]:
        return set((await importer_module._load(self.repo)).keys())


async def _build(**overrides: Any) -> Harness:
    config = make_config(**overrides)
    db = await make_db()
    repo = QueueRepository(db)
    sonarr = FakeSonarr()
    arr: dict[str, ArrClient] = {"radarr": FakeRadarr(), "sonarr": sonarr}
    clock = [1_000_000.0]
    return Harness(db, repo, sonarr, ImportFinisher(config, repo, arr), clock)


@pytest_asyncio.fixture
async def h() -> AsyncGenerator[Harness, None]:
    harness = await _build()
    with patch.object(importer_module.time, "time", lambda: harness.clock[0]):
        yield harness
    await harness.db.disconnect()


async def test_waits_for_the_arr_before_acting(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    h.block("nzo_1", NOT_UPGRADE)
    await h.run(after=SETTLE_SECONDS - 1)
    assert h.sonarr.imports == []
    assert await h.pending() == {"nzo_1"}


async def test_imports_not_an_upgrade_rejection(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    h.block("nzo_1", NOT_UPGRADE)
    await h.run(after=SETTLE_SECONDS)
    assert h.sonarr.imports == [("nzo_1", 1)]
    # next cycle sees the download gone from the Arr queue and forgets it
    await h.run(after=RETRY_SECONDS)
    assert await h.pending() == set()


async def test_custom_format_rejection_is_allowed_too(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    h.block("nzo_1", CF_NOT_UPGRADE, state="importBlocked")
    await h.run(after=SETTLE_SECONDS)
    assert h.sonarr.imports == [("nzo_1", 1)]


async def test_other_rejection_is_left_for_a_human(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    h.block("nzo_1", NOT_UPGRADE, MORE_EPISODES)
    await h.run(after=SETTLE_SECONDS)
    assert h.sonarr.imports == []
    assert await h.pending() == set()


async def test_imported_by_the_arr_itself_is_forgotten(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    await h.run(after=SETTLE_SECONDS)
    assert h.sonarr.imports == []
    assert await h.pending() == set()


async def test_still_importing_is_not_touched(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    h.block("nzo_1", state="importing")
    await h.run(after=SETTLE_SECONDS)
    assert h.sonarr.imports == []
    assert await h.pending() == {"nzo_1"}


async def test_retries_are_spaced_and_capped(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    h.block("nzo_1", NOT_UPGRADE)
    h.sonarr.fail_import = True
    await h.run(after=SETTLE_SECONDS)
    await h.run(after=1)  # too soon for a retry
    assert await h.pending() == {"nzo_1"}
    for _ in range(MAX_ATTEMPTS):
        await h.run(after=RETRY_SECONDS)
    assert await h.pending() == set()


async def test_dry_run_never_imports() -> None:
    harness = await _build(dry_run=True)
    await harness.finish_grab("nzo_1")
    harness.block("nzo_1", NOT_UPGRADE)
    await harness.run(after=SETTLE_SECONDS)
    assert harness.sonarr.imports == []
    await harness.db.disconnect()


async def test_state_survives_reload(h: Harness) -> None:
    await h.finish_grab("nzo_1")
    await h.finish_grab("nzo_2")
    assert await h.repo.get_state(STATE_KEY)
    assert await h.pending() == {"nzo_1", "nzo_2"}
