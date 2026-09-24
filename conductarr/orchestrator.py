"""Central orchestrator: wires clients, database and the two work loops.

Two independent asyncio tasks run while ``conductarr watch`` is active:

* **queue loop** (every ``poll_interval``): :class:`QueueManager` reads
  SABnzbd, maps jobs to virtual queues, finalises finished jobs and keeps the
  queue ordered.  It never waits for an indexer search, so user downloads are
  prioritised within one poll interval even while an upgrade search runs.
* **upgrade loop**: :class:`UpgradeScheduler` rescans the libraries when due
  and performs at most one rate-limited indexer search per upgrade queue and
  cycle, based on the latest queue snapshot.

Each cycle is wrapped in a watchdog timeout so a hanging HTTP call can never
freeze the loop.  On shutdown every SABnzbd job that conductarr paused is
resumed.
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from conductarr.clients.arr import ArrClient
from conductarr.clients.radarr import RadarrClient
from conductarr.clients.sabnzbd import SABnzbdClient
from conductarr.clients.sonarr import SonarrClient
from conductarr.config import ConductarrConfig, Config
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.queue.manager import NzoEntry, QueueManager, QueueSnapshot
from conductarr.upgrade.scheduler import DryRunCandidateResult, UpgradeScheduler

__all__ = ["DryRunCandidateResult", "Orchestrator"]

_LOGGER = logging.getLogger(__name__)

HEARTBEAT_FILE_NAME = "heartbeat"


class Orchestrator:
    def __init__(self, config: Config, conductarr_config: ConductarrConfig) -> None:
        self._config = config
        self._conductarr_config = conductarr_config
        self._database = Database(config.database)
        self._repository = QueueRepository(self._database)
        self._sab_client = SABnzbdClient(
            url=conductarr_config.sabnzbd.url,
            api_key=conductarr_config.sabnzbd.api_key,
            timeout=conductarr_config.sabnzbd.timeout,
        )
        self._arr_clients: dict[str, ArrClient] = {}
        if (radarr := conductarr_config.radarr) is not None:
            self._arr_clients["radarr"] = RadarrClient(
                radarr.url,
                radarr.api_key,
                timeout=radarr.timeout,
                search_timeout=radarr.search_timeout,
            )
        if (sonarr := conductarr_config.sonarr) is not None:
            self._arr_clients["sonarr"] = SonarrClient(
                sonarr.url,
                sonarr.api_key,
                timeout=sonarr.timeout,
                search_timeout=sonarr.search_timeout,
            )
        self._queue_manager = QueueManager(
            conductarr_config, self._repository, self._sab_client, self._arr_clients
        )
        self._scheduler = UpgradeScheduler(
            conductarr_config, self._repository, self._arr_clients, self._queue_manager
        )
        self._snapshot: QueueSnapshot | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._connected = False
        self._started = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open DB and HTTP connections and load persisted state."""
        await self._database.connect()
        await self._sab_client.__aenter__()
        self._connected = True
        await self._queue_manager.load_state()

    async def start(self) -> None:
        await self.connect()
        self._started = True
        cfg = self._conductarr_config
        if cfg.dry_run:
            _LOGGER.warning(
                "DRY-RUN mode: conductarr will not reorder, pause, resume or grab"
            )
        self._tasks = [
            asyncio.create_task(self._queue_loop(), name="conductarr-queue-loop"),
            asyncio.create_task(self._upgrade_loop(), name="conductarr-upgrade-loop"),
        ]
        _LOGGER.info(
            "Orchestrator started (poll_interval=%.1fs, upgrade queues: %s)",
            cfg.poll_interval,
            ", ".join(q.name for q in cfg.upgrade_queues) or "none",
        )

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks = []
        # Only the instance that ran the loop releases its paused jobs; a
        # one-off command (debug-upgrades) must not touch a running instance.
        if self._connected and self._started:
            try:
                await asyncio.wait_for(
                    self._queue_manager.release_paused_jobs(), timeout=30
                )
            except Exception:
                _LOGGER.warning("Could not release paused jobs", exc_info=True)
        await self._sab_client.__aexit__(None, None, None)
        for client in self._arr_clients.values():
            await client.close()
        await self._database.disconnect()
        self._connected = False
        self._started = False
        _LOGGER.info("Orchestrator stopped")

    async def poll_once(self) -> None:
        """Run one queue cycle followed by one upgrade cycle (used by tests)."""
        self._snapshot = await self._queue_manager.run_cycle()
        await self._scheduler.run_cycle(self._snapshot)

    async def scan_library(self) -> None:
        """Force a library scan for all upgrade queues."""
        await self._scheduler.seed_if_due(force=True)

    async def dry_run_upgrades(
        self, source_filter: str | None = None, source_id_filter: str | None = None
    ) -> list[DryRunCandidateResult]:
        return await self._scheduler.dry_run(source_filter, source_id_filter)

    @property
    def repo(self) -> QueueRepository:
        return self._repository

    @property
    def _nzo_cache_entries(self) -> dict[str, NzoEntry]:
        return self._queue_manager.entries

    # ------------------------------------------------------------------
    # Loops
    # ------------------------------------------------------------------

    @property
    def _cycle_timeout(self) -> float:
        return max(120.0, 4 * self._conductarr_config.poll_interval)

    async def _queue_loop(self) -> None:
        interval = self._conductarr_config.poll_interval
        while True:
            try:
                self._snapshot = await asyncio.wait_for(
                    self._queue_manager.run_cycle(), timeout=self._cycle_timeout
                )
                self._write_heartbeat()
            except TimeoutError:
                _LOGGER.error(
                    "Queue cycle exceeded %.0fs and was aborted", self._cycle_timeout
                )
            except Exception:
                _LOGGER.exception("Unhandled error in queue cycle")
            await asyncio.sleep(interval)

    async def _upgrade_loop(self) -> None:
        interval = self._conductarr_config.poll_interval
        cfg = self._conductarr_config
        search_timeout = max(
            (c.search_timeout for c in (cfg.radarr, cfg.sonarr) if c is not None),
            default=180.0,
        )
        # pre-checks + one search + one grab per upgrade queue
        cycle_timeout = (self._cycle_timeout + 2 * search_timeout) * max(
            1, len(cfg.upgrade_queues)
        )
        scan_timeout = 3600.0
        first = True
        while True:
            try:
                await asyncio.wait_for(
                    self._scheduler.seed_if_due(force=first), timeout=scan_timeout
                )
                first = False
                await self._repository.prune_search_log()
                snapshot = self._snapshot
                if (
                    snapshot is not None
                    and time.monotonic() - snapshot.taken_at < 3 * interval
                ):
                    await asyncio.wait_for(
                        self._scheduler.run_cycle(snapshot), timeout=cycle_timeout
                    )
            except TimeoutError:
                _LOGGER.error("Upgrade cycle timed out and was aborted")
            except Exception:
                _LOGGER.exception("Unhandled error in upgrade cycle")
            await asyncio.sleep(interval)

    def _write_heartbeat(self) -> None:
        path = heartbeat_path(self._config)
        try:
            path.write_text(str(time.time()), encoding="utf-8")
        except OSError:
            _LOGGER.debug("Could not write heartbeat %s", path, exc_info=True)


def heartbeat_path(config: Config) -> Path:
    return config.config_dir / HEARTBEAT_FILE_NAME
