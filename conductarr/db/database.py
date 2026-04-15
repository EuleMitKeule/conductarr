"""Database connection management and migration runner."""

from __future__ import annotations

import logging
from importlib import resources
from typing import Any

import aiosqlite

from conductarr.config import AnyDatabaseConfig, MemoryDatabaseConfig

_LOGGER = logging.getLogger(__name__)

_MIGRATIONS_PACKAGE = "conductarr.db.migrations"


class Database:
    """Async SQLite database wrapper with automatic migrations."""

    def __init__(self, config: AnyDatabaseConfig) -> None:
        self._config = config
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        """Open the database connection and run pending migrations."""
        if isinstance(self._config, MemoryDatabaseConfig):
            db_path = ":memory:"
        else:
            db_dir = self._config.dir
            db_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(self._config.db_file)

        self._conn = await aiosqlite.connect(db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA foreign_keys=ON")
        _LOGGER.info("Database connected: %s", db_path)
        await self._run_migrations()

    async def disconnect(self) -> None:
        """Close the database connection."""
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            _LOGGER.info("Database disconnected")

    @property
    def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        return self._conn

    async def execute(self, sql: str, params: tuple[Any, ...] = ()) -> None:
        """Execute a single SQL statement."""
        await self._db.execute(sql, params)
        await self._db.commit()

    async def fetchall(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[aiosqlite.Row]:
        """Execute a query and return all rows."""
        cursor = await self._db.execute(sql, params)
        return list(await cursor.fetchall())

    async def fetchone(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> aiosqlite.Row | None:
        """Execute a query and return a single row."""
        cursor = await self._db.execute(sql, params)
        return await cursor.fetchone()

    async def _run_migrations(self) -> None:
        """Scan migrations/ for *.sql files and apply missing ones."""
        # Ensure schema_versions table exists (bootstrap)
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS schema_versions ("
            "  version INTEGER PRIMARY KEY,"
            "  applied_at DATETIME NOT NULL DEFAULT (datetime('now'))"
            ")"
        )
        await self._db.commit()

        # Fetch already-applied versions
        cursor = await self._db.execute("SELECT version FROM schema_versions")
        applied = {row[0] for row in await cursor.fetchall()}

        # Discover migration files from the package
        migration_files: list[tuple[int, str]] = []
        migration_ref = resources.files(_MIGRATIONS_PACKAGE)
        for item in migration_ref.iterdir():
            name = item.name
            if name.endswith(".sql"):
                version = int(name.split("_", 1)[0])
                migration_files.append((version, name))

        migration_files.sort(key=lambda x: x[0])

        for version, filename in migration_files:
            if version in applied:
                continue

            _LOGGER.info("Applying migration %s", filename)
            sql = resources.files(_MIGRATIONS_PACKAGE).joinpath(filename).read_text()
            await self._db.executescript(sql)
            await self._db.execute(
                "INSERT INTO schema_versions (version) VALUES (?)", (version,)
            )
            await self._db.commit()
            _LOGGER.info("Migration %s applied", filename)
