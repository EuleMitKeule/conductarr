"""Unit tests for service-configuration loading and validation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
import yaml

from conductarr.config import ConductarrConfig, ConfigError

ROOT = Path(__file__).parent.parent.parent


def _write(tmp_path: Path, data: dict[str, Any]) -> Path:
    path = tmp_path / "conductarr.yml"
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    return path


def _base(**queue_upgrade: Any) -> dict[str, Any]:
    upgrade: dict[str, Any] = {
        "sources": ["radarr"],
        "accept_conditions": [{"type": "custom_format", "name": "German DL"}],
    }
    upgrade.update(queue_upgrade)
    return {
        "conductarr": {"poll_interval": 10},
        "sabnzbd": {"url": "http://sab:8080", "api_key": "k"},
        "radarr": {"url": "http://radarr:7878", "api_key": "k"},
        "queues": [
            {"name": "requests", "priority": 100},
            {"name": "upgrade", "priority": 1, "upgrade": upgrade},
        ],
    }


def test_example_config_is_valid() -> None:
    config = ConductarrConfig.from_yaml(ROOT / "config.example.yml")
    assert config.upgrade_queues


def test_defaults_are_conservative(tmp_path: Path) -> None:
    config = ConductarrConfig.from_yaml(_write(tmp_path, _base()))
    upgrade = config.upgrade_queues[0].upgrade
    assert upgrade is not None
    assert upgrade.search_interval >= 300
    assert upgrade.max_searches_per_day > 0
    assert upgrade.allow_season_packs
    assert not upgrade.allow_resolution_downgrade
    assert upgrade.defer_to_other_downloads
    assert config.sonarr is None


def test_legacy_daily_scan_interval_alias(tmp_path: Path) -> None:
    config = ConductarrConfig.from_yaml(
        _write(tmp_path, _base(daily_scan_interval=123))
    )
    assert config.upgrade_queues[0].upgrade is not None
    assert config.upgrade_queues[0].upgrade.rescan_interval == 123


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda d: d["queues"][1]["upgrade"].update(max_actve=2), "max_actve"),
        (
            lambda d: d["queues"][1]["upgrade"].update(accept_conditions=[]),
            "accept_conditions",
        ),
        (lambda d: d["queues"][1]["upgrade"].update(sources=["lidarr"]), "sources"),
        (
            lambda d: d["queues"][1]["upgrade"].update(sources=["sonarr"]),
            "no 'sonarr' section",
        ),
        (
            lambda d: d["queues"].append({"name": "requests", "priority": 5}),
            "duplicate",
        ),
        (
            lambda d: d["queues"][1]["upgrade"].update(
                accept_conditions=[{"type": "custom_format"}]
            ),
            "requires 'name'",
        ),
        (lambda d: d["queues"][0].update(matchers=[{"type": "tag"}]), "matchers"),
        (lambda d: d["conductarr"].update(poll_intervall=3), "poll_intervall"),
    ],
)
def test_invalid_configs_are_rejected(
    tmp_path: Path, mutate: Any, message: str
) -> None:
    data = _base()
    mutate(data)
    with pytest.raises(ConfigError, match=message):
        ConductarrConfig.from_yaml(_write(tmp_path, data))


def test_missing_sabnzbd_section(tmp_path: Path) -> None:
    data = _base()
    del data["sabnzbd"]
    with pytest.raises(ConfigError, match="sabnzbd"):
        ConductarrConfig.from_yaml(_write(tmp_path, data))
