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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start per-queue daily scan loops."""
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
        await self.seed_upgrade_queues()

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

    # ------------------------------------------------------------------
    # Internal loops
    # ------------------------------------------------------------------

    async def _daily_loop(self, queue_config: VirtualQueueConfig) -> None:
        """Sleep-scan loop for one upgrade queue."""
        upgrade = queue_config.upgrade
        assert upgrade is not None  # guaranteed by caller
        while True:
            try:
                await self._daily_scan(queue_config)
            except asyncio.CancelledError:
                raise
            except Exception:
                _LOGGER.exception(
                    "Unexpected error in daily scan for queue '%s'", queue_config.name
                )
            await asyncio.sleep(upgrade.daily_scan_interval)

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    async def _fill_slots(self, queue_config: VirtualQueueConfig) -> None:
        """Fill as many empty upgrade slots as possible up to *max_active*."""
        upgrade = queue_config.upgrade
        if upgrade is None or not upgrade.enabled:
            return

        active = await self._count_active(queue_config.name)
        slots_to_fill = upgrade.max_active - active
        if slots_to_fill <= 0:
            _LOGGER.debug(
                "Queue '%s': no free slots (%d/%d active)",
                queue_config.name,
                active,
                upgrade.max_active,
            )
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
        grabbed_total = 0

        for source in upgrade.sources:
            if grabbed_total >= upgrade.max_active:
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

            for candidate in candidates:
                if grabbed_total >= upgrade.max_active:
                    break
                if candidate.metadata.get("upgrade_grabbed"):
                    continue
                if candidate.id is None:
                    continue

                now_str = datetime.now(UTC).isoformat()
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
                    continue

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
                        continue
                    candidate.metadata["upgrade_grabbed"] = True
                    candidate.metadata["upgrade_last_searched_at"] = now_str
                    await self._repo.update_metadata(candidate.id, candidate.metadata)
                    grabbed_total += 1
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

        for candidate in candidates:
            if candidate.metadata.get("upgrade_grabbed"):
                continue
            if candidate.id is None:
                continue

            now_str = datetime.now(UTC).isoformat()
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
                continue

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
                return True
            else:
                # No matching release found for this candidate
                candidate.metadata["upgrade_no_match_at"] = now_str
                candidate.metadata["upgrade_last_searched_at"] = now_str
                await self._repo.update_metadata(candidate.id, candidate.metadata)

        return False  # source exhausted

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _count_active(self, virtual_queue: str) -> int:
        """Count currently active job mappings for *virtual_queue*."""
        all_maps = await self._repo.get_all_job_maps()
        return sum(1 for jm in all_maps if jm["virtual_queue"] == virtual_queue)

    # ------------------------------------------------------------------
    # Seed
    # ------------------------------------------------------------------

    async def seed_upgrade_queues(self) -> None:
        """Pre-populate the DB with all tagged items from Radarr/Sonarr.

        For each upgrade queue, reads the tag labels from its ``tags`` matchers
        and creates :class:`~conductarr.queue.models.QueueItem` entries for any
        Radarr movies / Sonarr episodes that carry those tags but have not yet
        been seen in the SABnzbd queue.  Existing items are never overwritten.
        """
        for qc in self._upgrade_queues:
            if qc.upgrade and qc.upgrade.enabled:
                try:
                    await self._seed_queue(qc)
                except Exception:
                    _LOGGER.exception("Seed: unexpected error for queue '%s'", qc.name)

    async def _seed_queue(self, queue_config: VirtualQueueConfig) -> None:
        """Seed one upgrade queue from Radarr/Sonarr."""
        tag_filter = self._get_tag_filter(queue_config)
        if not tag_filter:
            _LOGGER.debug(
                "Queue '%s': no 'tags' matchers configured, skipping seed",
                queue_config.name,
            )
            return

        upgrade = queue_config.upgrade
        assert upgrade is not None  # guaranteed by caller

        for source in upgrade.sources:
            if source == "radarr" and self._radarr is not None:
                await self._seed_from_radarr(queue_config.name, tag_filter)
            elif source == "sonarr" and self._sonarr is not None:
                await self._seed_from_sonarr(queue_config.name, tag_filter)

    async def _seed_from_radarr(self, queue_name: str, tag_filter: list[str]) -> None:
        """Create QueueItems for all Radarr movies matching *tag_filter*."""
        try:
            movies = await self._radarr.get_movies(has_file=True)  # type: ignore[union-attr]
            tag_map = await self._radarr.get_tags()  # type: ignore[union-attr]
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Radarr movies or tags")
            return

        seeded = 0
        for movie in movies:
            movie_tags = [tag_map[tid] for tid in movie.tag_ids if tid in tag_map]
            if not any(t in tag_filter for t in movie_tags):
                continue
            existing = await self._repo.get_item("radarr", str(movie.id))
            if existing is None:
                item = QueueItem(
                    source="radarr",
                    source_id=str(movie.id),
                    virtual_queue=queue_name,
                    tags=movie_tags,
                )
                await self._repo.upsert_item(item)
                seeded += 1
        _LOGGER.info(
            "Seed: inserted %d new Radarr item(s) into '%s'", seeded, queue_name
        )

    async def _seed_from_sonarr(self, queue_name: str, tag_filter: list[str]) -> None:
        """Create QueueItems for all monitored Sonarr episodes matching *tag_filter*."""
        try:
            all_series = await self._sonarr.get_series()  # type: ignore[union-attr]
            tag_map = await self._sonarr.get_tags()  # type: ignore[union-attr]
        except Exception:
            _LOGGER.exception("Seed: failed to fetch Sonarr series or tags")
            return

        seeded = 0
        for series in all_series:
            series_tags = [tag_map[tid] for tid in series.tag_ids if tid in tag_map]
            if not any(t in tag_filter for t in series_tags):
                continue
            try:
                episodes = await self._sonarr.get_episodes(  # type: ignore[union-attr]
                    series.id, monitored=True, has_file=True
                )
            except Exception:
                _LOGGER.warning(
                    "Seed: failed to fetch episodes for Sonarr series %d", series.id
                )
                continue
            for episode in episodes:
                existing = await self._repo.get_item("sonarr", str(episode.id))
                if existing is None:
                    item = QueueItem(
                        source="sonarr",
                        source_id=str(episode.id),
                        virtual_queue=queue_name,
                        tags=series_tags,
                    )
                    await self._repo.upsert_item(item)
                    seeded += 1
        _LOGGER.info(
            "Seed: inserted %d new Sonarr episode(s) into '%s'", seeded, queue_name
        )

    @staticmethod
    def _get_tag_filter(queue_config: VirtualQueueConfig) -> list[str]:
        """Extract unique tag labels from all ``tags`` matchers in *queue_config*."""
        seen: set[str] = set()
        tags: list[str] = []
        for matcher in queue_config.matchers:
            if matcher.type == "tags":
                for tag in matcher.tags:
                    if tag not in seen:
                        seen.add(tag)
                        tags.append(tag)
        return tags

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


def _filter_releases(
    releases: list[ReleaseResult],
    conditions: list[AcceptConditionConfig],
) -> list[ReleaseResult]:
    """Return only releases that satisfy ALL *conditions*.

    Supported condition types:
    - ``custom_format``: release must have a custom format with this name.
    - ``custom_format_min_score``: release ``custom_format_score >= value``.
    """
    result = []
    for release in releases:
        if not release.download_allowed:
            _LOGGER.debug(
                "Release '%s' excluded: download_allowed=False", release.title
            )
            continue
        if _matches_all_conditions(release, conditions):
            result.append(release)
    return result


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
