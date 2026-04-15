"""Constants for Conductarr."""

from enum import StrEnum
from pathlib import Path

VERSION = "0.0.0-dev"
APP_NAME = "conductarr"
APP_DESCRIPTION = "🎼 Priority-based download queue orchestrator for Radarr, Sonarr & SABnzbd — automatically manages and reorders your download queue across multiple priority tiers."


class LogLevel(StrEnum):
    """Log level options for console output."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class DatabaseType(StrEnum):
    """Discriminator values for database backend config types."""

    MEMORY = "memory"
    SQLITE = "sqlite"


ENV_CONFIG_DIR = "CONFIG_DIR"
ENV_CONFIG_FILE_NAME = "CONFIG_FILE_NAME"
ENV_LOG_LEVEL = "LOG_LEVEL"
ENV_LOG_DIR = "LOG_DIR"
ENV_LOG_FILE_NAME = "LOG_FILE_NAME"
ENV_DEV_SAB_INI = "DEV_SAB_INI"
ENV_DEV_RADARR_CONFIG = "DEV_RADARR_CONFIG"
ENV_DEV_SONARR_CONFIG = "DEV_SONARR_CONFIG"

ENV_PREFIX_GENERAL = ""
ENV_PREFIX_LOGGING = "LOG"
ENV_PREFIX_LOG_ROTATION = "LOG_ROTATION"
ENV_PREFIX_SABNZBD = "SABNZBD"

DEFAULT_CONFIG_DIR = Path("./config")
DEFAULT_CONFIG_FILE_NAME = "conductarr.yml"

DEFAULT_TZ = "UTC"
DEFAULT_PUID = 1000
DEFAULT_PGID = 1000
DEFAULT_UMASK = "022"

DEFAULT_LOG_LEVEL = LogLevel.INFO
DEFAULT_LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s: %(message)s"
DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
DEFAULT_LOG_DIR: Path = Path("./config/logs")
DEFAULT_LOG_FILE_NAME: str = "conductarr.log"
DEFAULT_LOG_COLOR = True
DEFAULT_LOG_ROTATION_ENABLED = True
DEFAULT_LOG_ROTATION_MAX_BYTES = 10_485_760  # 10 MB
DEFAULT_LOG_ROTATION_BACKUP_COUNT = 3
DEFAULT_LOG_ROTATION_ROTATE_ON_START = True

DEFAULT_DB_DIR: Path = Path("./config")
DEFAULT_DB_FILE_NAME: str = "conductarr.db"

CONF_CONFIG_DIR = "config_dir"

CONF_GENERAL = "general"
CONF_GENERAL_TZ = "tz"
CONF_GENERAL_PUID = "puid"
CONF_GENERAL_PGID = "pgid"
CONF_GENERAL_UMASK = "umask"
CONF_LOGGING = "logging"
CONF_LOGGING_LEVEL = "level"
CONF_LOGGING_FORMAT = "format"
CONF_LOGGING_DATE_FORMAT = "date_format"
CONF_LOGGING_DIR = "dir"
CONF_LOGGING_FILE_NAME = "file_name"
CONF_LOGGING_COLOR = "color"
CONF_LOGGING_ROTATION = "rotation"
CONF_LOGGING_ROTATION_ENABLED = "enabled"
CONF_LOGGING_ROTATION_MAX_BYTES = "max_bytes"
CONF_LOGGING_ROTATION_BACKUP_COUNT = "backup_count"
CONF_LOGGING_ROTATION_ROTATE_ON_START = "rotate_on_start"

CONF_DATABASE = "database"
CONF_DATABASE_TYPE = "type"
CONF_DATABASE_DIR = "dir"
CONF_DATABASE_FILE_NAME = "file_name"

DEFAULT_POLL_INTERVAL: float = 15.0
