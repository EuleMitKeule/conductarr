"""Logging setup for conductarr.

Configures the ``conductarr`` logger hierarchy from a :class:`~conductarr.config.LoggingConfig`
instance. All sub-module loggers are children of ``conductarr`` and inherit its
handlers automatically.
"""

import logging
import sys
import time
import zoneinfo
from collections.abc import Callable
from datetime import datetime
from logging.handlers import RotatingFileHandler

from conductarr.config import LoggingConfig
from conductarr.const import APP_NAME, DEFAULT_TZ

_LOGGER = logging.getLogger(APP_NAME)

_LEVEL_COLORS: dict[int, str] = {
    logging.DEBUG: "\033[36m",  # Cyan
    logging.INFO: "\033[32m",  # Green
    logging.WARNING: "\033[33m",  # Yellow
    logging.ERROR: "\033[31m",  # Red
    logging.CRITICAL: "\033[1;31m",  # Bold red
}
_ANSI_RESET = "\033[0m"


def _make_tz_converter(
    tz_name: str,
) -> Callable[[float | None], time.struct_time]:
    """Create a ``time.localtime``-compatible converter for the given timezone."""
    tz = zoneinfo.ZoneInfo(tz_name)

    def converter(timestamp: float | None = None) -> time.struct_time:
        if timestamp is None:
            timestamp = time.time()
        return datetime.fromtimestamp(timestamp, tz=tz).timetuple()

    return converter


class _ColoredFormatter(logging.Formatter):
    """Formatter that wraps each log line with an ANSI color based on level."""

    def format(self, record: logging.LogRecord) -> str:
        """Return the formatted log line wrapped in the appropriate ANSI color."""
        color = _LEVEL_COLORS.get(record.levelno, "")
        message = super().format(record)
        return f"{color}{message}{_ANSI_RESET}" if color else message


def setup_logging(config: LoggingConfig, tz: str = DEFAULT_TZ) -> None:
    """Configure the ``conductarr`` root logger from *config*.

    Sets up a console handler (stderr) and, optionally, a file handler
    (plain or rotating). Color is only applied to the console handler and
    only when stderr is a TTY.

    Args:
        config: The validated :class:`~conductarr.config.LoggingConfig` to apply.
        tz: IANA timezone name used for log timestamps.
    """
    root = logging.getLogger(APP_NAME)
    root.setLevel(config.level.value)
    root.propagate = False
    root.handlers.clear()

    converter = _make_tz_converter(tz)

    plain_formatter = logging.Formatter(fmt=config.format, datefmt=config.date_format)
    plain_formatter.converter = converter

    use_color = config.color and getattr(sys.stderr, "isatty", lambda: False)()
    console_handler = logging.StreamHandler(sys.stderr)
    if use_color:
        colored_formatter = _ColoredFormatter(
            fmt=config.format, datefmt=config.date_format
        )
        colored_formatter.converter = converter
        console_handler.setFormatter(colored_formatter)
    else:
        console_handler.setFormatter(plain_formatter)
    root.addHandler(console_handler)

    if config.log_file is not None:
        config.log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler: logging.FileHandler
        if config.rotation.enabled:
            file_handler = RotatingFileHandler(
                config.log_file,
                maxBytes=config.rotation.max_bytes,
                backupCount=config.rotation.backup_count,
                encoding="utf-8",
            )
            if (
                config.rotation.rotate_on_start
                and config.log_file.exists()
                and config.log_file.stat().st_size > 0
            ):
                file_handler.doRollover()
        else:
            file_handler = logging.FileHandler(config.log_file, encoding="utf-8")
        file_handler.setFormatter(plain_formatter)
        root.addHandler(file_handler)
