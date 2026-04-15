"""Main entry point for Conductarr CLI.

Defines the Typer CLI application and top-level commands. Serves as the
main execution point when Conductarr is invoked from the command line.
"""

import asyncio
import logging
from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from conductarr.config import (
    Config,
    ConductarrConfig,
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
from conductarr.log import setup_logging

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
    priority rules.
    """
    config = _init_config(
        config_dir, config_file_name, log_level, log_dir, log_file_name
    )

    conductarr_config = ConductarrConfig.from_yaml(
        config.config_dir / config.config_file
    )

    from conductarr.engine import ConductarrEngine

    engine = ConductarrEngine(conductarr_config)

    async def _run() -> None:
        await engine.start()
        try:
            await asyncio.sleep(float("inf"))
        finally:
            await engine.stop()

    _LOGGER.info("Starting watch mode")
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

    Outputs the resolved log directory and all symlink library output paths.
    Each path appears exactly once (duplicates are suppressed).
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


@app.command("status", help="Show cache stats and last run info.")
def status(
    config_dir: ConfigDirOpt = DEFAULT_CONFIG_DIR,
    config_file_name: ConfigFileNameOpt = DEFAULT_CONFIG_FILE_NAME,
    log_level: LogLevelOpt = None,
    log_dir: LogDirOpt = None,
    log_file_name: LogFileNameOpt = None,
) -> None:
    """Show cache stats and last run info.

    Displays a summary of the current cache state and when the last scan
    was performed.
    """
    _config = _init_config(
        config_dir, config_file_name, log_level, log_dir, log_file_name
    )
    _LOGGER.info("Showing status")


def main() -> None:
    """Main entry point.

    Initializes and runs the CLI application.
    """
    load_dotenv(override=False)
    app()


if __name__ == "__main__":
    main()
