"""Configuration management module.

Handles loading, parsing, and validation of conductarr configuration.
"""

import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
import zoneinfo
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Literal

import typer
import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator

from conductarr.const import (
    APP_NAME,
    CONF_CONFIG_DIR,
    CONF_DATABASE,
    CONF_DATABASE_DIR,
    CONF_GENERAL_TZ,
    CONF_GENERAL_UMASK,
    CONF_LOGGING,
    CONF_LOGGING_DIR,
    CONF_LOGGING_FILE_NAME,
    CONF_LOGGING_LEVEL,
    DEFAULT_DB_DIR,
    DEFAULT_DB_FILE_NAME,
    DEFAULT_LOG_COLOR,
    DEFAULT_LOG_DATE_FORMAT,
    DEFAULT_LOG_DIR,
    DEFAULT_LOG_FILE_NAME,
    DEFAULT_LOG_FORMAT,
    DEFAULT_LOG_LEVEL,
    DEFAULT_LOG_ROTATION_BACKUP_COUNT,
    DEFAULT_LOG_ROTATION_ENABLED,
    DEFAULT_LOG_ROTATION_MAX_BYTES,
    DEFAULT_LOG_ROTATION_ROTATE_ON_START,
    DEFAULT_PGID,
    DEFAULT_POLL_INTERVAL,
    DEFAULT_PUID,
    DEFAULT_TZ,
    DEFAULT_UMASK,
    ENV_DEV_RADARR_CONFIG,
    ENV_DEV_SAB_INI,
    ENV_DEV_SONARR_CONFIG,
    ENV_PREFIX_GENERAL,
    ENV_PREFIX_LOG_ROTATION,
    ENV_PREFIX_LOGGING,
    DatabaseType,
    LogLevel,
)

__all__ = [
    "AcceptConditionConfig",
    "AnyDatabaseConfig",
    "ConductarrConfig",
    "ConfigError",
    "DatabaseType",
    "MatcherConfig",
    "MemoryDatabaseConfig",
    "RadarrConfig",
    "SabnzbdConfig",
    "SonarrConfig",
    "SQLiteDatabaseConfig",
    "UpgradeConfig",
    "VirtualQueueConfig",
]

_LOGGER = logging.getLogger(APP_NAME)


def _read_sabnzbd_api_key(ini_path: Path) -> str:
    """Read the SABnzbd API key from a sabnzbd.ini file.

    SABnzbd's ini starts with bare ``__version__`` / ``__encoding__`` lines
    before any section header, which configparser cannot handle.  A simple
    regex search avoids that problem entirely.
    """
    if not ini_path.exists():
        return ""
    text = ini_path.read_text(encoding="utf-8")
    m = re.search(r"^api_key\s*=\s*(\S+)", text, re.MULTILINE)
    return m.group(1) if m else ""


def _read_arr_api_key(xml_path: Path) -> str:
    """Read the API key from a Radarr/Sonarr config.xml file."""
    if not xml_path.exists():
        return ""
    tree = ET.parse(xml_path)  # noqa: S314 – trusted local dev file
    root = tree.getroot()
    elem = root.find("ApiKey")
    return elem.text.strip() if elem is not None and elem.text else ""


class GeneralConfig(BaseModel):
    """General sub-configuration for timezone, user/group IDs, and umask."""

    _env_prefix: ClassVar[str] = ENV_PREFIX_GENERAL

    tz: str = Field(default=DEFAULT_TZ, validate_default=True)
    puid: int = Field(default=DEFAULT_PUID, validate_default=True)
    pgid: int = Field(default=DEFAULT_PGID, validate_default=True)
    umask: str = Field(default=DEFAULT_UMASK, validate_default=True)

    @field_validator(CONF_GENERAL_TZ, mode="before")
    @classmethod
    def _coerce_tz(cls, v: object) -> object:
        """Treat empty/whitespace-only strings as None (falls back to default)."""
        if isinstance(v, str) and not v.strip():
            return DEFAULT_TZ
        return v

    @field_validator(CONF_GENERAL_TZ, mode="after")
    @classmethod
    def _validate_tz(cls, v: str) -> str:
        """Validate that the timezone is a known IANA timezone."""
        try:
            zoneinfo.ZoneInfo(v)
        except KeyError:
            raise ValueError(f"Invalid timezone: '{v}'")
        return v

    @field_validator(CONF_GENERAL_UMASK, mode="before")
    @classmethod
    def _coerce_umask(cls, v: object) -> object:
        """Convert integer umask values (from YAML) to zero-padded strings."""
        if isinstance(v, int):
            return f"{v:03d}"
        return v

    @field_validator(CONF_GENERAL_UMASK, mode="after")
    @classmethod
    def _validate_umask(cls, v: str) -> str:
        """Validate that the umask is a valid octal string in range 000-777."""
        try:
            val = int(v, 8)
        except ValueError:
            raise ValueError(
                f"Invalid umask '{v}': must be a valid octal string (e.g. '022')"
            )
        if val < 0 or val > 0o777:
            raise ValueError(f"Umask value '{v}' out of range (000-777)")
        return v


class LogRotationConfig(BaseModel):
    """Log file rotation sub-configuration."""

    _env_prefix: ClassVar[str] = ENV_PREFIX_LOG_ROTATION

    enabled: bool = Field(default=DEFAULT_LOG_ROTATION_ENABLED, validate_default=True)
    max_bytes: int = Field(
        default=DEFAULT_LOG_ROTATION_MAX_BYTES, validate_default=True
    )
    backup_count: int = Field(
        default=DEFAULT_LOG_ROTATION_BACKUP_COUNT, validate_default=True
    )
    rotate_on_start: bool = Field(
        default=DEFAULT_LOG_ROTATION_ROTATE_ON_START, validate_default=True
    )


class LoggingConfig(BaseModel):
    """Logging sub-configuration."""

    _env_prefix: ClassVar[str] = ENV_PREFIX_LOGGING
    _yaml_excluded: ClassVar[frozenset[str]] = frozenset({CONF_LOGGING_DIR})

    level: LogLevel = Field(default=DEFAULT_LOG_LEVEL, validate_default=True)
    format: str = Field(default=DEFAULT_LOG_FORMAT, validate_default=True)
    date_format: str = Field(default=DEFAULT_LOG_DATE_FORMAT, validate_default=True)
    dir: Path | None = Field(default=DEFAULT_LOG_DIR, validate_default=True)
    file_name: str | None = Field(default=DEFAULT_LOG_FILE_NAME, validate_default=True)
    color: bool = Field(default=DEFAULT_LOG_COLOR, validate_default=True)
    rotation: LogRotationConfig = Field(
        default_factory=LogRotationConfig, validate_default=True
    )

    @property
    def log_file(self) -> Path | None:
        """Return the resolved log file path, or None if file logging is disabled."""
        if self.dir is None or self.file_name is None:
            return None
        return self.dir / self.file_name

    @field_validator(CONF_LOGGING_LEVEL, mode="before")
    @classmethod
    def _coerce_level(cls, v: object) -> object:
        if isinstance(v, str):
            return v.upper()
        return v

    @field_validator(CONF_LOGGING_DIR, CONF_LOGGING_FILE_NAME, mode="before")
    @classmethod
    def _coerce_nullable_str(cls, v: object) -> object:
        """Treat empty-string values as None (disables file logging)."""
        if isinstance(v, str) and not v.strip():
            return None
        return v

    @field_validator(CONF_LOGGING_DIR, mode="after")
    @classmethod
    def _resolve_dir_to_absolute(cls, v: Path | None) -> Path | None:
        if v is not None:
            return v.resolve()
        return v


def _coerce_typed_list(v: object) -> object:
    """Coerce plain string items in a list to ``{'type': item}`` dicts.

    Allows YAML entries like ``- ffprobe`` in addition to
    ``- type: ffprobe`` for configs without required arguments.
    StrEnum members are also strings, so they are handled by the same branch.
    """
    if isinstance(v, list):
        return [{"type": item} if isinstance(item, str) else item for item in v]
    return v


class MemoryDatabaseConfig(BaseModel):
    """In-memory (non-persistent) database backend configuration."""

    type: Literal[DatabaseType.MEMORY] = DatabaseType.MEMORY


class SQLiteDatabaseConfig(BaseModel):
    """SQLite database backend configuration."""

    type: Literal[DatabaseType.SQLITE] = DatabaseType.SQLITE
    dir: Path = Field(default=DEFAULT_DB_DIR, validate_default=True)
    file_name: str = Field(default=DEFAULT_DB_FILE_NAME)

    @property
    def db_file(self) -> Path:
        """Return the resolved path to the SQLite database file."""
        return self.dir / self.file_name

    @field_validator(CONF_DATABASE_DIR, mode="after")
    @classmethod
    def _resolve_dir_to_absolute(cls, v: Path) -> Path:
        return v.resolve()


AnyDatabaseConfig = MemoryDatabaseConfig | SQLiteDatabaseConfig


# ---------------------------------------------------------------------------
# Service configuration (plain dataclasses)
# ---------------------------------------------------------------------------


class ConfigError(Exception):
    """Raised when the configuration file is invalid or incomplete."""


@dataclass
class SabnzbdConfig:
    url: str
    api_key: str


@dataclass
class RadarrConfig:
    url: str
    api_key: str


@dataclass
class SonarrConfig:
    url: str
    api_key: str


@dataclass
class MatcherConfig:
    type: str
    tags: list[str] = field(default_factory=list)


@dataclass
class AcceptConditionConfig:
    """A single condition that a release must satisfy to be eligible for upgrade."""

    type: str  # "custom_format" | "custom_format_min_score"
    name: str = ""  # used by custom_format
    value: int = 0  # used by custom_format_min_score


@dataclass
class UpgradeConfig:
    """Upgrade-scheduler settings attached to a virtual queue."""

    enabled: bool = True
    sources: list[str] = field(default_factory=lambda: ["radarr", "sonarr"])
    max_active: int = 1
    daily_scan_interval: int = 86400  # seconds
    retry_after_days: int = 7
    search_interval: float = 30.0  # seconds between indexer search calls
    accept_conditions: list[AcceptConditionConfig] = field(default_factory=list)


@dataclass
class VirtualQueueConfig:
    name: str
    priority: int
    enabled: bool = True
    fallback: bool = False
    matchers: list[MatcherConfig] = field(default_factory=list)
    upgrade: UpgradeConfig | None = None


@dataclass
class ConductarrConfig:
    poll_interval: float
    sabnzbd: SabnzbdConfig
    radarr: RadarrConfig
    sonarr: SonarrConfig
    queues: list[VirtualQueueConfig] = field(default_factory=list)

    @classmethod
    def from_yaml(cls, path: Path) -> "ConductarrConfig":
        """Load service configuration from a YAML file.

        Raises:
            ConfigError: If any required section is missing.
        """
        with open(path, encoding="utf-8") as fh:
            data = yaml.safe_load(fh) or {}

        conductarr_data = data.get("conductarr", {})
        poll_interval = float(
            conductarr_data.get("poll_interval", DEFAULT_POLL_INTERVAL)
        )

        for section in ("sabnzbd", "radarr", "sonarr"):
            if section not in data:
                raise ConfigError(f"Missing required config section: '{section}'")

        sab_data: dict[str, Any] = dict(data["sabnzbd"])
        if not sab_data.get("api_key"):
            if ini_path := os.environ.get(ENV_DEV_SAB_INI):
                sab_data["api_key"] = _read_sabnzbd_api_key(Path(ini_path))
                _LOGGER.debug("SABnzbd API key loaded from %s", ini_path)

        radarr_data: dict[str, Any] = dict(data["radarr"])
        if not radarr_data.get("api_key"):
            if xml_path := os.environ.get(ENV_DEV_RADARR_CONFIG):
                radarr_data["api_key"] = _read_arr_api_key(Path(xml_path))
                _LOGGER.debug("Radarr API key loaded from %s", xml_path)

        sonarr_data: dict[str, Any] = dict(data["sonarr"])
        if not sonarr_data.get("api_key"):
            if xml_path := os.environ.get(ENV_DEV_SONARR_CONFIG):
                sonarr_data["api_key"] = _read_arr_api_key(Path(xml_path))
                _LOGGER.debug("Sonarr API key loaded from %s", xml_path)

        # Parse virtual queues
        queues: list[VirtualQueueConfig] = []
        for q in data.get("queues", []):
            matchers = [MatcherConfig(**m) for m in q.get("matchers", [])]
            upgrade: UpgradeConfig | None = None
            if raw_upgrade := q.get("upgrade"):
                conditions = [
                    AcceptConditionConfig(
                        type=c["type"],
                        name=c.get("name", ""),
                        value=int(c.get("value", 0)),
                    )
                    for c in raw_upgrade.get("accept_conditions", [])
                ]
                upgrade = UpgradeConfig(
                    enabled=bool(raw_upgrade.get("enabled", True)),
                    sources=list(raw_upgrade.get("sources", ["radarr", "sonarr"])),
                    max_active=int(raw_upgrade.get("max_active", 1)),
                    daily_scan_interval=int(
                        raw_upgrade.get("daily_scan_interval", 86400)
                    ),
                    retry_after_days=int(raw_upgrade.get("retry_after_days", 7)),
                    search_interval=float(raw_upgrade.get("search_interval", 30.0)),
                    accept_conditions=conditions,
                )
            queues.append(
                VirtualQueueConfig(
                    name=q["name"],
                    priority=int(q["priority"]),
                    enabled=q.get("enabled", True),
                    fallback=bool(q.get("fallback", False)),
                    matchers=matchers,
                    upgrade=upgrade,
                )
            )

        return cls(
            poll_interval=poll_interval,
            sabnzbd=SabnzbdConfig(**sab_data),
            radarr=RadarrConfig(**radarr_data),
            sonarr=SonarrConfig(**sonarr_data),
            queues=queues,
        )


class Config(BaseModel):
    """conductarr root configuration."""

    config_dir: Path
    config_file: str
    general: GeneralConfig
    logging: LoggingConfig
    database: AnyDatabaseConfig = Field(
        default_factory=SQLiteDatabaseConfig, discriminator="type"
    )
    output_path: Path | None = None

    @field_validator(CONF_CONFIG_DIR, mode="after")
    @classmethod
    def _resolve_config_dir_to_absolute(cls, v: Path) -> Path:
        """Convert config directory path to absolute."""
        return v.resolve()


_config: Config | None = None


def get_config() -> Config:
    """Return the loaded Config singleton.

    Raises:
        RuntimeError: If load_config() has not been called yet.
    """
    if _config is None:
        raise RuntimeError("Config has not been loaded yet. Call load_config() first.")
    return _config


def _to_yaml_serializable(obj: Any) -> Any:
    """Recursively convert enums and other non-YAML types to YAML-serializable forms."""
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, dict):
        return {k: _to_yaml_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_yaml_serializable(v) for v in obj]
    return obj


def _apply_env_vars(
    model_cls: type[BaseModel],
    yaml_data: dict[str, Any],
    config_file_name: str,
    field_name: str,
) -> dict[str, Any]:
    """Overlay environment variables onto ``yaml_data`` for ``model_cls``.

    The env var prefix is read from the model class's ``_env_prefix`` attribute
    if present, otherwise defaults to the field name in uppercase.
    Field names are derived as ``f"{prefix}_{field_name}".upper()``.

    Warns when the same field is supplied by both the config file
    and an env var (the env var wins).

    Args:
        model_cls: The Pydantic model whose fields to inspect.
        yaml_data: Values already loaded from the config file.
        config_file_name: File name used in the warning message.
        field_name: The name of the field in the parent config class.

    Returns:
        A new dict with env var values overlaid on top of yaml_data.
    """
    prefix: str = getattr(model_cls, "_env_prefix", field_name.upper())
    merged = dict(yaml_data)
    for name, field_info in model_cls.model_fields.items():
        if isinstance(field_info.annotation, type) and issubclass(
            field_info.annotation, BaseModel
        ):
            nested_yaml = merged.get(name) or {}
            merged[name] = _apply_env_vars(
                field_info.annotation, nested_yaml, config_file_name, name
            )
            continue
        env_var = f"{prefix}_{name}".upper() if prefix else name.upper()
        env_value = os.environ.get(env_var)
        if env_value is not None:
            if name in yaml_data:
                display_prefix = prefix if prefix else field_name.upper()
                _LOGGER.warning(
                    "'%s %s' is set in both '%s' (%r) and %s (%r). Env var takes precedence.",
                    display_prefix,
                    name,
                    config_file_name,
                    yaml_data[name],
                    env_var,
                    env_value,
                )
            merged[name] = env_value
    return merged


def load_config(
    config_dir: Path,
    config_file_name: str,
    log_level: LogLevel | None = None,
    log_dir: Path | None = None,
    log_file_name: str | None = None,
) -> Config:
    """Load and validate configuration from all sources.

    Exits:
        Calls sys.exit() with a human-readable error on validation failure.

    Returns:
        The validated Config singleton.
    """
    global _config

    config_path = config_dir / config_file_name
    cli_values: dict[str, dict[str, Any]] = {}
    logging_cli: dict[str, Any] = {}
    if log_level is not None:
        logging_cli[CONF_LOGGING_LEVEL] = log_level
    if log_dir is not None:
        logging_cli[CONF_LOGGING_DIR] = log_dir
    if log_file_name is not None:
        logging_cli[CONF_LOGGING_FILE_NAME] = log_file_name
    if logging_cli:
        cli_values[CONF_LOGGING] = logging_cli

    if not config_path.is_file():
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.touch(exist_ok=True)
        # TODO: Write template config file

    with config_path.open(encoding="utf-8") as config_file:
        yaml_data = yaml.safe_load(config_file) or {}

    sub_configs: dict[str, Any] = {}
    for field_name, field_info in Config.model_fields.items():
        if not (
            isinstance(field_info.annotation, type)
            and issubclass(field_info.annotation, BaseModel)
        ):
            continue
        yaml_sub = yaml_data.get(field_name) or {}
        yaml_excluded: frozenset[str] = getattr(
            field_info.annotation, "_yaml_excluded", frozenset()
        )
        yaml_sub = {k: v for k, v in yaml_sub.items() if k not in yaml_excluded}
        sub_merged = _apply_env_vars(
            field_info.annotation, yaml_sub, config_file_name, field_name
        )
        sub_merged.update(cli_values.get(field_name) or {})
        sub_configs[field_name] = sub_merged

    extra_fields: dict[str, Any] = {}
    output_path_raw = yaml_data.get("output_path")
    if output_path_raw is not None:
        extra_fields["output_path"] = output_path_raw
    database_raw = yaml_data.get(CONF_DATABASE)
    resolved_config_dir = config_dir.resolve()
    if database_raw is None:
        # Default: sqlite stored alongside the config file
        extra_fields[CONF_DATABASE] = {
            "type": DatabaseType.SQLITE,
            "dir": str(resolved_config_dir),
        }
    else:
        db_raw = dict(database_raw)
        db_type = str(db_raw.get("type", DatabaseType.SQLITE))
        if db_type == DatabaseType.SQLITE and "dir" not in db_raw:
            db_raw["dir"] = str(resolved_config_dir)
        extra_fields[CONF_DATABASE] = db_raw

    try:
        _config = Config(
            config_dir=config_dir,
            config_file=config_file_name,
            **sub_configs,
            **extra_fields,
        )
    except ValidationError as exc:
        typer.echo(f"Error validating config file '{config_path}':", err=True)
        typer.echo(exc, err=True)
        sys.exit(1)

    return _config
