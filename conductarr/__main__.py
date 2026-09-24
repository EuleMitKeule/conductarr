"""Main entry point for Conductarr CLI.

Defines the Typer CLI application and top-level commands. Serves as the
main execution point when Conductarr is invoked from the command line.
"""

import asyncio
import logging
import signal
import time
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from conductarr.config import (
    ConductarrConfig,
    ConfigError,
    Config,
    SQLiteDatabaseConfig,
    load_config,
)
from conductarr.const import (
    APP_DESCRIPTION,
    APP_NAME,
    DEFAULT_CONFIG_DIR,
    DEFAULT_CONFIG_FILE_NAME,
    DEFAULT_LOG_DIR,
    DEFAULT_LOG_FILE_NAME,
    ENV_CONFIG_DIR,
    ENV_CONFIG_FILE_NAME,
    ENV_LOG_DIR,
    ENV_LOG_FILE_NAME,
    ENV_LOG_LEVEL,
    VERSION,
    LogLevel,
)
from conductarr.db.database import Database
from conductarr.db.repository import QueueRepository
from conductarr.log import setup_logging
from conductarr.orchestrator import (
    HEARTBEAT_FILE_NAME,
    DryRunCandidateResult,
    Orchestrator,
)

_LOGGER = logging.getLogger(APP_NAME)

app = typer.Typer(
    name=APP_NAME,
    help=APP_DESCRIPTION,
    no_args_is_help=True,
)

ConfigDirOpt = Annotated[
    Path,
    typer.Option(
        "--config-dir",
        envvar=ENV_CONFIG_DIR,
        help=f"Config/state directory. Default: '{DEFAULT_CONFIG_DIR}'.",
    ),
]
ConfigFileNameOpt = Annotated[
    str,
    typer.Option(
        "--config-file-name",
        envvar=ENV_CONFIG_FILE_NAME,
        help=f"Config file name. Default: '{DEFAULT_CONFIG_FILE_NAME}'.",
    ),
]
LogLevelOpt = Annotated[
    LogLevel | None,
    typer.Option(
        "--log-level",
        envvar=ENV_LOG_LEVEL,
        help="Log level.",
        case_sensitive=False,
    ),
]
LogDirOpt = Annotated[
    Path | None,
    typer.Option(
        "--log-dir",
        envvar=ENV_LOG_DIR,
        help=f"Log file directory. Default: '{DEFAULT_LOG_DIR}'. Set to empty string to disable file logging.",
    ),
]
LogFileNameOpt = Annotated[
    str | None,
    typer.Option(
        "--log-file-name",
        envvar=ENV_LOG_FILE_NAME,
        help=f"Log file name. Default: '{DEFAULT_LOG_FILE_NAME}'. Set to empty string to disable file logging.",
    ),
]


def _init_config(
    config_dir: Path,
    config_file_name: str,
    log_level: LogLevel | None,
    log_dir: Path | None,
    log_file_name: str | None,
) -> Config:
    """Initialize a new config file with default values."""
    config = load_config(
        config_dir, config_file_name, log_level, log_dir, log_file_name
    )
    setup_logging(config.logging, tz=config.general.tz)
    _LOGGER.info("conductarr version %s startup complete", VERSION)
    _LOGGER.debug("Loaded config: %s", config.model_dump_json(indent=2))
    return config


@app.command("version", help="Show version information.")
def version() -> None:
    """Show version information.

    Displays the current conductarr version string.
    """
    typer.echo(f"conductarr version {VERSION}")


def _load_service_config(config: Config) -> ConductarrConfig:
    try:
        return ConductarrConfig.from_yaml(config.config_dir / config.config_file)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc


@app.command("watch", help="Start continuous watch mode.")
def watch(
    config_dir: ConfigDirOpt = DEFAULT_CONFIG_DIR,
    config_file_name: ConfigFileNameOpt = DEFAULT_CONFIG_FILE_NAME,
    log_level: LogLevelOpt = None,
    log_dir: LogDirOpt = None,
    log_file_name: LogFileNameOpt = None,
) -> None:
    """Start continuous watch mode.

    Monitors SABnzbd and orchestrates the download queue based on configured
    priority rules.  SIGTERM/SIGINT trigger a graceful shutdown that resumes
    every job conductarr paused.
    """
    config = _init_config(
        config_dir, config_file_name, log_level, log_dir, log_file_name
    )
    conductarr_config = _load_service_config(config)
    orchestrator = Orchestrator(config, conductarr_config)

    async def _run() -> None:
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError, RuntimeError:
                pass  # Windows: KeyboardInterrupt still cancels the run
        await orchestrator.start()
        try:
            await stop_event.wait()
            _LOGGER.info("Shutdown requested")
        finally:
            await orchestrator.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


@app.command("paths", help="Print all writable paths from config, one per line.")
def paths(
    config_dir: ConfigDirOpt = DEFAULT_CONFIG_DIR,
    config_file_name: ConfigFileNameOpt = DEFAULT_CONFIG_FILE_NAME,
    log_dir: LogDirOpt = None,
    log_file_name: LogFileNameOpt = None,
) -> None:
    """Print all writable paths from config, one per line.

    Suitable for use in the Docker entrypoint to chown writable mounts.
    """
    config = load_config(
        config_dir, config_file_name, log_dir=log_dir, log_file_name=log_file_name
    )

    seen: set[Path] = set()

    def _emit(p: Path | None) -> None:
        if p is not None and p not in seen:
            seen.add(p)
            typer.echo(p)

    _emit(
        config.logging.log_file.parent
        if config.logging.log_file
        else config.logging.dir
    )

    if isinstance(config.database, SQLiteDatabaseConfig):
        _emit(config.database.db_file.parent)


@app.command("healthcheck", help="Exit 0 if the watch loop is alive.")
def healthcheck(
    config_dir: ConfigDirOpt = DEFAULT_CONFIG_DIR,
    max_age: Annotated[
        float,
        typer.Option("--max-age", help="Maximum heartbeat age in seconds."),
    ] = 300.0,
) -> None:
    """Check the heartbeat file written after every successful queue cycle."""
    path = config_dir.resolve() / HEARTBEAT_FILE_NAME
    try:
        age = time.time() - float(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError) as exc:
        typer.echo(f"unhealthy: no heartbeat ({exc})", err=True)
        raise typer.Exit(code=1) from exc
    if age > max_age:
        typer.echo(f"unhealthy: last queue cycle {age:.0f}s ago", err=True)
        raise typer.Exit(code=1)
    typer.echo(f"healthy: last queue cycle {age:.0f}s ago")


@app.command("status", help="Show upgrade progress and search budget.")
def status(
    config_dir: ConfigDirOpt = DEFAULT_CONFIG_DIR,
    config_file_name: ConfigFileNameOpt = DEFAULT_CONFIG_FILE_NAME,
    log_level: LogLevelOpt = None,
    log_dir: LogDirOpt = None,
    log_file_name: LogFileNameOpt = None,
) -> None:
    """Show per-queue candidate counts and indexer search usage (read-only)."""
    config = _init_config(
        config_dir,
        config_file_name,
        log_level or LogLevel.WARNING,
        log_dir,
        log_file_name,
    )
    conductarr_config = _load_service_config(config)

    async def _run() -> None:
        database = Database(config.database)
        await database.connect()
        repo = QueueRepository(database)
        try:
            typer.echo(
                f"{'queue':<20} {'source':<8} {'items':>7} {'due':>7} "
                f"{'grabbed':>8} {'ok':>7} {'no match':>9} {'upgraded':>9}"
            )
            upgrade_cfg = {q.name: q.upgrade for q in conductarr_config.upgrade_queues}
            for row in await repo.get_stats():
                cfg = upgrade_cfg.get(row["queue"])
                due = (
                    await repo.count_upgrade_candidates(
                        row["queue"],
                        row["source"],
                        cfg.retry_after_days,
                        cfg.no_release_retry_days,
                    )
                    if cfg
                    else 0
                )
                typer.echo(
                    f"{row['queue']:<20} {row['source']:<8} {row['items']:>7} "
                    f"{due:>7} {row['grabbed']:>8} {row['satisfied']:>7} "
                    f"{row['no_match']:>9} {row['upgraded']:>9}"
                )
            typer.echo("")
            for queue in conductarr_config.upgrade_queues:
                assert queue.upgrade is not None
                used = await repo.count_searches_last_day(queue.name)
                limit = queue.upgrade.max_searches_per_day or "unlimited"
                typer.echo(f"Searches last 24h for '{queue.name}': {used} / {limit}")
            paused = await repo.get_paused_jobs()
            typer.echo(f"Jobs currently paused by conductarr: {len(paused)}")
        finally:
            await database.disconnect()

    asyncio.run(_run())


def _print_dry_run_results(results: list[DryRunCandidateResult]) -> None:
    """Pretty-print dry-run upgrade results to stdout."""
    if not results:
        typer.echo(
            "No eligible upgrade candidates found "
            "(all already satisfied, downloading or not due)."
        )
        return

    for r in results:
        title_str = f"  ({r.media_title})" if r.media_title else ""
        typer.echo(f"\n[{r.outcome}]  [{r.queue}]  {r.source}/{r.source_id}{title_str}")
        typer.echo(f"       {r.reason}")
        if r.current_score is not None:
            typer.echo(
                f"       Current: score={r.current_score}, "
                f"resolution={r.current_resolution or '?'}p"
            )
        if r.steps:
            chain = " -> ".join(f"{name}: {n}" for name, n in r.steps)
            typer.echo(f"       Releases: {r.releases_total} total -> {chain}")
        if r.best_release is not None:
            size_mb = r.best_release.size // (1024 * 1024)
            typer.echo(
                f"       Best release:  {r.best_release.title}"
                f"  (score={r.best_release.custom_format_score},"
                f" quality={r.best_release.quality},"
                f" size={size_mb:,} MB, indexer={r.best_release.indexer or '?'})"
            )


@app.command(
    "debug-upgrades", help="Dry-run upgrade selection without grabbing anything."
)
def debug_upgrades(
    config_dir: ConfigDirOpt = DEFAULT_CONFIG_DIR,
    config_file_name: ConfigFileNameOpt = DEFAULT_CONFIG_FILE_NAME,
    log_level: LogLevelOpt = None,
    log_dir: LogDirOpt = None,
    log_file_name: LogFileNameOpt = None,
    source: Annotated[
        str | None,
        typer.Option(
            "--source",
            help="Restrict to a specific source: 'radarr' or 'sonarr'.",
        ),
    ] = None,
    source_id: Annotated[
        str | None,
        typer.Option(
            "--id",
            help="Test a specific movie/episode ID within the source (e.g. '42').",
        ),
    ] = None,
) -> None:
    """Dry-run upgrade selection without grabbing anything.

    Picks the next candidate the scheduler would search (or the given id),
    runs an indexer search and every filter step, but writes nothing and
    grabs nothing.  Note: this does perform one real indexer search.
    """
    config = _init_config(
        config_dir, config_file_name, log_level, log_dir, log_file_name
    )
    conductarr_config = _load_service_config(config)
    orchestrator = Orchestrator(config, conductarr_config)

    async def _run() -> list[DryRunCandidateResult]:
        await orchestrator.connect()
        try:
            return await orchestrator.dry_run_upgrades(
                source_filter=source,
                source_id_filter=source_id,
            )
        finally:
            await orchestrator.stop()

    results = asyncio.run(_run())
    _print_dry_run_results(results)


def main() -> None:
    """Main entry point.

    Initializes and runs the CLI application.
    """
    load_dotenv(override=False)
    app()


if __name__ == "__main__":
    main()
