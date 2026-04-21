"""Upgrade scheduler: keeps upgrade virtual queues filled with release grabs."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from conductarr.clients.release import ReleaseResult
from conductarr.config import AcceptConditionConfig, UpgradeConfig, VirtualQueueConfig
from conductarr.db.repository import QueueRepository
from conductarr.queue.models import QueueItem

if TYPE_CHECKING:
    from conductarr.clients.radarr import RadarrClient
    from conductarr.clients.sonarr import SonarrClient

_LOGGER = logging.getLogger(__name__)

__all__ = ["UpgradeScheduler"]


class UpgradeScheduler:
    """Keeps upgrade queues filled by searching and force-grabbing releases.

    Two triggers:
    1. ``on_job_completed(virtual_queue)`` — called when a download finishes
       and the freed slot should be refilled immediately.
    2. A daily scan loop that wakes every ``daily_scan_interval`` seconds and
       searches all due candidates regardless of current slot count.
    """

    def __init__(
        self,
        repo: QueueRepository,
        upgrade_queues: list[VirtualQueueConfig],
        radarr_client: RadarrClient | None = None,
        sonarr_client: SonarrClient | None = None,
    ) -> None:
        self._repo = repo
        self._upgrade_queues = upgrade_queues
        self._radarr = radarr_client
        self._sonarr = sonarr_client
        # Per-queue source-turn counter (monotonically increasing index)
        self._source_turn: dict[str, int] = {}
        self._daily_tasks: list[asyncio.Task[None]] = []
        # Prevents concurrent _fill_slots calls from over-filling a queue
        self._fill_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start per-queue daily scan loops."""
        await self.seed_upgrade_queues()
        for qc in self._upgrade_queues:
            if qc.upgrade and qc.upgrade.enabled:
                task = asyncio.create_task(
                    self._daily_loop(qc),
                    name=f"upgrade-daily-{qc.name}",
                )
                self._daily_tasks.append(task)
        _LOGGER.info(
            "UpgradeScheduler started (%d upgrade queue(s))", len(self._daily_tasks)
        )

    async def stop(self) -> None:
        """Cancel all running daily scan tasks."""
        for task in self._daily_tasks:
            task.cancel()
        if self._daily_tasks:
            await asyncio.gather(*self._daily_tasks, return_exceptions=True)
        self._daily_tasks.clear()
        _LOGGER.info("UpgradeScheduler stopped")

    # ------------------------------------------------------------------
    # Public trigger
    # ------------------------------------------------------------------

    async def on_job_completed(self, virtual_queue: str) -> None:
        """React to a completed download: try to fill the freed slot."""
        for qc in self._upgrade_queues:
            if (
                qc.name == virtual_queue
                and qc.upgrade is not None
                and qc.upgrade.enabled
            ):
                _LOGGER.debug(
                    "Job completed in upgrade queue '%s', triggering fill",
                    virtual_queue,
                )
                await self._fill_slots(qc)
                return

    async def on_reconcile_complete(self) -> None:
        """Called after every reconcile cycle; fills any open upgrade slots."""
        for qc in self._upgrade_queues:
            if qc.upgrade is None or not qc.upgrade.enabled:
                continue
            active = await self._count_active(qc.name)
            slots_to_fill = qc.upgrade.max_active - active
            _LOGGER.debug(
                "Queue '%s': %d/%d active, %d slot(s) to fill",
                qc.name,
                active,
                qc.upgrade.max_active,
                slots_to_fill,
            )
            if slots_to_fill > 0:
                try:
                    await self._fill_slots(qc)
                except Exception:
                    _LOGGER.exception("Error filling slots for queue '%s'", qc.name)

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    async def _daily_loop(self, queue_config: VirtualQueueConfig) -> None:
        """Sleep-scan loop for one upgrade queue."""
        upgrade = queue_config.upgrade
        assert upgrade is not None  # guaranteed by caller
        while True:
            await asyncio.sleep(upgrade.daily_scan_interval)
            try:
                await self._daily_scan(queue_config)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception(
                    "Unexpected error in daily scan for queue '%s'", queue_config.name
                )

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    async def _fill_slots(self, queue_config: VirtualQueueConfig) -> None:
        """Fill as many empty upgrade slots as possible up to *max_active*."""
        upgrade = queue_config.upgrade
        if upgrade is None or not upgrade.enabled:
            return

        async with self._fill_lock:
            active = await self._count_active(queue_config.name)
            _LOGGER.debug(
                "Filling slots for queue '%s': %d/%d active",
                queue_config.name,
                active,
                upgrade.max_active,
            )
            slots_to_fill = upgrade.max_active - active
            if slots_to_fill <= 0:
                return

            sources = upgrade.sources
            if not sources:
                return

            queue_name = queue_config.name
            turn = self._source_turn.get(queue_name, 0)
            consecutive_misses = 0  # sources tried in a row without filling a slot

            while slots_to_fill > 0 and consecutive_misses < len(sources):
                source = sources[turn % len(sources)]
                turn += 1
                filled = await self._try_fill_from_source(upgrade, queue_name, source)
                if filled:
                    slots_to_fill -= 1
                    consecutive_misses = 0
                else:
                    consecutive_misses += 1

            self._source_turn[queue_name] = turn

    async def _daily_scan(self, queue_config: VirtualQueueConfig) -> None:
        """Search all due candidates and grab up to *max_active* matches."""
        upgrade = queue_config.upgrade
        if upgrade is None or not upgrade.enabled:
            return

        queue_name = queue_config.name
        _LOGGER.debug("Daily scan starting for queue '%s'", queue_name)

        for source in upgrade.sources:
            active = await self._count_active(queue_name)
            if active >= upgrade.max_active:
                break
            try:
                candidates = await self._repo.get_upgrade_candidates(
                    queue_name, source, upgrade.retry_after_days
                )
            except Exception:
                _LOGGER.exception(
                    "Daily scan: failed to get candidates for queue '%s' source '%s'",
                    queue_name,
                    source,
                )
                continue

            _LOGGER.debug(
                "Daily scan: %d due candidate(s) for queue '%s' source '%s'",
                len(candidates),
                queue_name,
                source,
            )

            for candidate in candidates:
                active = await self._count_active(queue_name)
                if active >= upgrade.max_active:
                    break
                if candidate.metadata.get("upgrade_grabbed"):
                    continue
                if candidate.id is None:
                    continue

                now_str = datetime.now(UTC).isoformat()
                _LOGGER.debug(
                    "Searching releases for %s/%s", source, candidate.source_id
                )
                try:
                    releases = await self._search_releases(
                        source, int(candidate.source_id)
                    )
                    matching = _filter_releases(releases, upgrade.accept_conditions)
                except Exception:
                    _LOGGER.warning(
                        "Daily scan: error searching releases for %s/%s",
                        source,
                        candidate.source_id,
                        exc_info=True,
                    )
                    await asyncio.sleep(upgrade.search_interval)
                    continue

                _LOGGER.debug(
                    "Found %d releases, %d matching conditions for %s/%s",
                    len(releases),
                    len(matching),
                    source,
                    candidate.source_id,
                )

                if matching:
                    best = max(matching, key=lambda r: r.custom_format_score)
                    try:
                        await self._grab_release(source, best)
                    except Exception:
                        _LOGGER.warning(
                            "Daily scan: error grabbing release for %s/%s: %s",
                            source,
                            candidate.source_id,
                            best.title,
                            exc_info=True,
                        )
                        await asyncio.sleep(upgrade.search_interval)
                        continue
                    candidate.metadata["upgrade_grabbed"] = True
                    candidate.metadata["upgrade_last_searched_at"] = now_str
                    await self._repo.update_metadata(candidate.id, candidate.metadata)
                    _LOGGER.info(
                        "Daily scan: grabbed upgrade for %s/%s: %s",
                        source,
                        candidate.source_id,
                        best.title,
                    )
                else:
                    candidate.metadata["upgrade_no_match_at"] = now_str
                    candidate.metadata["upgrade_last_searched_at"] = now_str
                    await self._repo.update_metadata(candidate.id, candidate.metadata)
                await asyncio.sleep(upgrade.search_interval)

    async def _try_fill_from_source(
        self,
        upgrade: UpgradeConfig,
        queue_name: str,
        source: str,
    ) -> bool:
        """Try each due candidate for *source* until one is grabbed.

        Returns ``True`` if a release was grabbed, ``False`` if the source
        was fully exhausted without finding a matching release.
        """
        try:
            candidates = await self._repo.get_upgrade_candidates(
                queue_name, source, upgrade.retry_after_days
            )
        except Exception:
            _LOGGER.exception(
                "Failed to get upgrade candidates for queue '%s' source '%s'",
                queue_name,
                source,
            )
            return False

        if not candidates:
            _LOGGER.debug(
                "No due candidates for queue '%s' source '%s'",
                queue_name,
                source,
            )
            return False

        for candidate in candidates:
            if candidate.metadata.get("upgrade_grabbed"):
                continue
            if candidate.id is None:
                continue

            now_str = datetime.now(UTC).isoformat()
            _LOGGER.debug("Searching releases for %s/%s", source, candidate.source_id)
            try:
                releases = await self._search_releases(source, int(candidate.source_id))
                matching = _filter_releases(releases, upgrade.accept_conditions)
            except Exception:
                _LOGGER.warning(
                    "Error searching releases for %s/%s",
                    source,
                    candidate.source_id,
                    exc_info=True,
                )
                await asyncio.sleep(upgrade.search_interval)
                continue

            _LOGGER.debug(
                "Found %d releases, %d matching conditions for %s/%s",
                len(releases),
                len(matching),
                source,
                candidate.source_id,
            )

            if matching:
                best = max(matching, key=lambda r: r.custom_format_score)
                try:
                    await self._grab_release(source, best)
                except Exception:
                    _LOGGER.warning(
                        "Error grabbing release for %s/%s: %s",
                        source,
                        candidate.source_id,
                        best.title,
                        exc_info=True,
                    )
                    await asyncio.sleep(upgrade.search_interval)
                    continue
                candidate.metadata["upgrade_grabbed"] = True
                candidate.metadata["upgrade_last_searched_at"] = now_str
                await self._repo.update_metadata(candidate.id, candidate.metadata)
                _LOGGER.info(
                    "Grabbed upgrade for %s/%s: %s",
                    source,
                    candidate.source_id,
                    best.title,
                )
                await asyncio.sleep(upgrade.search_interval)
                return True
            else:
                # No matching release found for this candidate
                candidate.metadata["upgrade_no_match_at"] = now_str
                candidate.metadata["upgrade_last_searched_at"] = now_str
                await self._repo.update_metadata(candidate.id, candidate.metadata)
                await asyncio.sleep(upgrade.search_interval)

        return False  # source exhausted

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _count_active(self, virtual_queue: str) -> int:
        """Count currently active job mappings for *virtual_queue*."""
        all_maps = await self._repo.get_all_job_maps()
        count = sum(1 for jm in all_maps if jm["virtual_queue"] == virtual_queue)
        _LOGGER.debug("Queue '%s': %d active job(s)", virtual_queue, count)
        return count

    # ------------------------------------------------------------------
    # Seed
    # ------------------------------------------------------------------

    async def seed_upgrade_queues(self) -> None:
        """Pre-populate the DB with upgrade candidates from Radarr/Sonarr.

        For each upgrade queue, fetches all movies/episodes that have a file
        but do not yet satisfy the queue's ``accept_conditions``.  Those items
        are inserted as :class:`~conductarr.queue.models.QueueItem` entries so
        the scheduler can search for better releases.  Existing items are never
        overwritten.
        """
        for qc in self._upgrade_queues:
            if qc.upgrade and qc.upgrade.enabled:
                try:
                    await self._seed_queue(qc)
                except Exception:
                    _LOGGER.exception("Seed: unexpected error for queue '%s'", qc.name)

    async def _seed_queue(self, queue_config: VirtualQueueConfig) -> None:
        """Seed one upgrade queue from Radarr/Sonarr."""
        upgrade = queue_config.upgrade
        assert upgrade is not None  # guaranteed by caller

        for source in upgrade.sources:
            if source == "radarr" and self._radarr is not None:
                await self._seed_from_radarr(queue_config.name, upgrade)
            elif source == "sonarr" and self._sonarr is not None:
                await self._seed_from_sonarr(queue_config.name, upgrade)

    async def _seed_from_radarr(
        self, queue_name: str, upgrade_config: UpgradeConfig
    ) -> None:
        """Create QueueItems for Radarr movies that do not satisfy upgrade conditions."""
        assert self._radarr is not None  # caller guarantees this
        try:
            movies = await self._radarr.get_movies(has_file=True)
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Radarr movies")
            return

        seeded = 0
        for movie in movies:
            if _media_satisfies_conditions(
                movie.custom_formats,
                movie.custom_format_score,
                upgrade_config.accept_conditions,
            ):
                continue
            existing = await self._repo.get_item("radarr", str(movie.id))
            if existing is None:
                item = QueueItem(
                    source="radarr",
                    source_id=str(movie.id),
                    virtual_queue=queue_name,
                    tags=[],
                )
                await self._repo.upsert_item(item)
                seeded += 1
        _LOGGER.info(
            "Seed: inserted %d new Radarr item(s) into '%s'", seeded, queue_name
        )

    async def _seed_from_sonarr(
        self, queue_name: str, upgrade_config: UpgradeConfig
    ) -> None:
        """Create QueueItems for Sonarr episodes that do not satisfy upgrade conditions."""
        assert self._sonarr is not None  # caller guarantees this
        try:
            all_series = await self._sonarr.get_series()
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Sonarr series")
            return

        seeded = 0
        for series in all_series:
            try:
                episodes = await self._sonarr.get_episodes(series.id, has_file=True)
            except Exception:
                _LOGGER.warning(
                    "Seed: failed to fetch episodes for Sonarr series %d", series.id
                )
                continue
            for episode in episodes:
                if _media_satisfies_conditions(
                    episode.custom_formats,
                    episode.custom_format_score,
                    upgrade_config.accept_conditions,
                ):
                    continue
                existing = await self._repo.get_item("sonarr", str(episode.id))
                if existing is None:
                    item = QueueItem(
                        source="sonarr",
                        source_id=str(episode.id),
                        virtual_queue=queue_name,
                        tags=[],
                    )
                    await self._repo.upsert_item(item)
                    seeded += 1
        _LOGGER.info(
            "Seed: inserted %d new Sonarr episode(s) into '%s'", seeded, queue_name
        )

    async def _search_releases(self, source: str, item_id: int) -> list[ReleaseResult]:
        """Dispatch search_releases to the right client based on *source*."""
        if source == "radarr":
            if self._radarr is None:
                _LOGGER.warning("No Radarr client configured, skipping search")
                return []
            return await self._radarr.search_releases(item_id)
        if source == "sonarr":
            if self._sonarr is None:
                _LOGGER.warning("No Sonarr client configured, skipping search")
                return []
            return await self._sonarr.search_releases(item_id)
        _LOGGER.warning("Unknown upgrade source: %s", source)
        return []

    async def _grab_release(self, source: str, release: ReleaseResult) -> None:
        """Dispatch grab_release to the right client based on *source*."""
        if source == "radarr":
            if self._radarr is None:
                raise RuntimeError("No Radarr client configured")
            await self._radarr.grab_release(release)
        elif source == "sonarr":
            if self._sonarr is None:
                raise RuntimeError("No Sonarr client configured")
            await self._sonarr.grab_release(release)
        else:
            raise ValueError(f"Unknown upgrade source: {source}")


def _media_satisfies_conditions(
    custom_formats: list[str],
    custom_format_score: int,
    conditions: list[AcceptConditionConfig],
) -> bool:
    """Return ``True`` if existing media already satisfies ALL *conditions*.

    Used during seeding to skip items that have already been upgraded.
    """
    for cond in conditions:
        if cond.type == "custom_format":
            if cond.name not in custom_formats:
                return False
        elif cond.type == "custom_format_min_score":
            if custom_format_score < cond.value:
                return False
    return True


def _filter_releases(
    releases: list[ReleaseResult],
    conditions: list[AcceptConditionConfig],
) -> list[ReleaseResult]:
    """Return only releases that satisfy ALL *conditions*.

    Supported condition types:
    - ``custom_format``: release must have a custom format with this name.
    - ``custom_format_min_score``: release ``custom_format_score >= value``.
    """
    return [r for r in releases if _matches_all_conditions(r, conditions)]


def _matches_all_conditions(
    release: ReleaseResult,
    conditions: list[AcceptConditionConfig],
) -> bool:
    for cond in conditions:
        if cond.type == "custom_format":
            if cond.name not in release.custom_formats:
                return False
        elif cond.type == "custom_format_min_score":
            if release.custom_format_score < cond.value:
                return False
        else:
            _LOGGER.warning("Unknown accept_condition type: %s", cond.type)
    return True
