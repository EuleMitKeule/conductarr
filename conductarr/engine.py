"""ConductarrEngine: public facade that delegates to the new Orchestrator.

The legacy monitor, queue manager, and upgrade scheduler modules are kept
intact but are no longer wired into the main loop.  They can be removed in
a future cleanup pass.
"""

from __future__ import annotations

import logging

from conductarr.config import AnyDatabaseConfig, ConductarrConfig
from conductarr.db.repository import QueueRepository
from conductarr.orchestrator import Orchestrator

_LOGGER = logging.getLogger(__name__)


class ConductarrEngine:
    """Public facade over the new :class:`~conductarr.orchestrator.Orchestrator`.

    Preserves the external API used by ``__main__.py`` and integration tests
    while delegating all logic to the orchestrator.
    """

    def __init__(
        self,
        config: ConductarrConfig,
        database_config: AnyDatabaseConfig | None = None,
    ) -> None:
        self._orchestrator = Orchestrator(config, database_config)

    async def connect(self) -> None:
        """Open DB and HTTP connections without starting the poll loop."""
        await self._orchestrator.connect()

    async def start(self) -> None:
        """Connect, seed, and start the poll loop."""
        await self._orchestrator.start()

    async def stop(self) -> None:
        """Stop the poll loop and close all connections."""
        await self._orchestrator.stop()

    async def poll_once(self) -> None:
        """Execute exactly one poll cycle (for integration tests)."""
        await self._orchestrator.poll_once()

    @property
    def repo(self) -> QueueRepository:
        """Expose the repository for test introspection."""
        return self._orchestrator.repo
