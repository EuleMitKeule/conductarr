"""Finish the imports of conductarr's own upgrade downloads.

Conductarr grabs releases the Arr itself rejected as "not an upgrade" (for
example a German dual-language WEB-DL replacing an English-only Blu-ray).
When such a download finishes, the Arr's completed-download handling runs the
same upgrade check again, refuses the import and leaves the download in its
queue forever (``importPending`` / ``importBlocked``).

:class:`ImportFinisher` remembers every successfully finished conductarr grab
and, once the Arr has had time to try on its own, imports the files with a
``ManualImport`` command, but only when every rejection is in the upgrade
queue's ``allowed_rejections`` (the same allow-list the grab used).  Any other
rejection (wrong episode, sample, destination conflict, ...) is left alone for
a human to look at.

State lives in the ``kv_state`` table under :data:`STATE_KEY`, so a restart
does not forget a pending import.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import asdict, dataclass

from conductarr.clients.arr import ArrClient, ArrError
from conductarr.config import ConductarrConfig, UpgradeConfig
from conductarr.db.repository import QueueRepository
from conductarr.upgrade.selection import unexpected_messages

_LOGGER = logging.getLogger(__name__)

STATE_KEY = "pending_imports"
# The Arr needs a moment to notice a finished job and try its own import.
SETTLE_SECONDS = 120.0
# Minimum time between two ManualImport attempts for one download.
RETRY_SECONDS = 600.0
MAX_ATTEMPTS = 3
# Forget a pending import the Arr never reports as blocked after this long.
GIVE_UP_SECONDS = 86400.0
# A download that is gone from the Arr queue is done (imported or removed).
_BLOCKED_STATES = frozenset({"importpending", "importblocked"})
# Serialises read-modify-write of the state between the queue and upgrade loops
# (one lock per event loop, so tests with fresh loops never share one).
_STATE_LOCKS: dict[int, asyncio.Lock] = {}


def _state_lock() -> asyncio.Lock:
    return _STATE_LOCKS.setdefault(id(asyncio.get_running_loop()), asyncio.Lock())


@dataclass
class PendingImport:
    download_id: str
    source: str
    virtual_queue: str | None
    finished_at: float = 0.0
    attempts: int = 0
    last_attempt_at: float = 0.0


async def _load(repo: QueueRepository) -> dict[str, PendingImport]:
    raw = await repo.get_state(STATE_KEY)
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return {k: PendingImport(**v) for k, v in data.items()}
    except ValueError, TypeError:
        _LOGGER.warning("Discarding unreadable %s state", STATE_KEY)
        return {}


async def _save(repo: QueueRepository, pending: dict[str, PendingImport]) -> None:
    if pending:
        await repo.set_state(
            STATE_KEY, json.dumps({k: asdict(v) for k, v in pending.items()})
        )
    else:
        await repo.delete_state(STATE_KEY)


async def add_pending_import(
    repo: QueueRepository, download_id: str, source: str, virtual_queue: str | None
) -> None:
    """Remember a finished conductarr grab so its import can be finished."""
    async with _state_lock():
        pending = await _load(repo)
        pending[download_id] = PendingImport(
            download_id, source, virtual_queue, finished_at=time.time()
        )
        await _save(repo, pending)


class ImportFinisher:
    def __init__(
        self,
        config: ConductarrConfig,
        repo: QueueRepository,
        arr_clients: dict[str, ArrClient],
    ) -> None:
        self._config = config
        self._repo = repo
        self._arr = arr_clients
        self._upgrade_by_queue: dict[str, UpgradeConfig] = {
            q.name: q.upgrade for q in config.upgrade_queues if q.upgrade is not None
        }

    def _allowed(self, entry: PendingImport) -> list[str]:
        upgrade = self._upgrade_by_queue.get(entry.virtual_queue or "")
        if upgrade is None and self._upgrade_by_queue:
            upgrade = next(iter(self._upgrade_by_queue.values()))
        return list(upgrade.allowed_rejections) if upgrade else []

    async def run_cycle(self) -> None:
        pending = await _load(self._repo)
        if not pending:
            return
        now = time.time()
        results: dict[str, PendingImport | None] = {}
        for download_id, entry in pending.items():
            results[download_id] = None if await self._process(entry, now) else entry
        # The queue loop may have added entries while we talked to the Arrs.
        async with _state_lock():
            latest = await _load(self._repo)
            for download_id, result in results.items():
                if result is None:
                    latest.pop(download_id, None)
                else:
                    latest[download_id] = result
            await _save(self._repo, latest)

    async def _process(self, entry: PendingImport, now: float) -> bool:
        """Handle one pending import; return ``True`` when it can be forgotten."""
        client = self._arr.get(entry.source)
        if client is None:
            return True
        age = now - entry.finished_at
        if age < SETTLE_SECONDS:
            return False
        try:
            records = await client.get_download_status(entry.download_id)
        except ArrError as exc:
            _LOGGER.debug("Import check for %s failed: %s", entry.download_id, exc)
            return age > GIVE_UP_SECONDS
        if not records:
            return True  # imported by the Arr, or removed by someone
        if any(r.state.lower() not in _BLOCKED_STATES for r in records):
            # still importing / not yet evaluated
            return age > GIVE_UP_SECONDS

        allowed = self._allowed(entry)
        messages = [m for r in records for m in r.messages]
        blocking = unexpected_messages(messages, allowed)
        if blocking or not messages:
            _LOGGER.warning(
                "%s download %s needs a manual import: %s",
                entry.source,
                entry.download_id,
                "; ".join(sorted(set(blocking))) or "no reason given",
            )
            return True
        if entry.attempts >= MAX_ATTEMPTS:
            _LOGGER.warning(
                "%s download %s still not imported after %d attempts; giving up",
                entry.source,
                entry.download_id,
                entry.attempts,
            )
            return True
        if entry.attempts and now - entry.last_attempt_at < RETRY_SECONDS:
            return False

        if self._config.dry_run:
            _LOGGER.info(
                "[dry-run] would import %s download %s (%s)",
                entry.source,
                entry.download_id,
                "; ".join(sorted(set(messages))),
            )
            return True

        entry.attempts += 1
        entry.last_attempt_at = now
        try:
            count = await client.import_completed_download(
                entry.download_id,
                lambda rejections: not unexpected_messages(rejections, allowed),
            )
        except ArrError as exc:
            _LOGGER.warning(
                "Importing %s download %s failed: %s",
                entry.source,
                entry.download_id,
                exc,
            )
            return False
        if count:
            _LOGGER.info(
                "Imported %d file(s) of %s download %s despite: %s",
                count,
                entry.source,
                entry.download_id,
                "; ".join(sorted(set(messages))),
            )
        else:
            _LOGGER.warning(
                "%s download %s has no importable file; leaving it for a manual import",
                entry.source,
                entry.download_id,
            )
            return True
        return False  # verify on a later cycle that it left the Arr queue
