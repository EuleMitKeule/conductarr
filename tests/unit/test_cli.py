"""Smoke tests for the CLI commands that need no running services."""

from __future__ import annotations

import time
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from conductarr.__main__ import app

runner = CliRunner()

CONFIG = {
    "logging": {"dir": "", "color": False},
    "sabnzbd": {"url": "http://sab:8080", "api_key": "k"},
    "radarr": {"url": "http://radarr:7878", "api_key": "k"},
    "queues": [
        {
            "name": "german_upgrade",
            "priority": 1,
            "upgrade": {
                "sources": ["radarr"],
                "accept_conditions": [{"type": "custom_format", "name": "German DL"}],
            },
        }
    ],
}


@pytest.fixture
def config_dir(tmp_path: Path) -> Path:
    (tmp_path / "conductarr.yml").write_text(yaml.safe_dump(CONFIG), encoding="utf-8")
    return tmp_path


def test_healthcheck_without_heartbeat_fails(config_dir: Path) -> None:
    result = runner.invoke(app, ["healthcheck", "--config-dir", str(config_dir)])
    assert result.exit_code == 1


def test_healthcheck_fresh_and_stale(config_dir: Path) -> None:
    heartbeat = config_dir / "heartbeat"
    heartbeat.write_text(str(time.time()), encoding="utf-8")
    ok = runner.invoke(app, ["healthcheck", "--config-dir", str(config_dir)])
    assert ok.exit_code == 0

    heartbeat.write_text(str(time.time() - 3600), encoding="utf-8")
    stale = runner.invoke(app, ["healthcheck", "--config-dir", str(config_dir)])
    assert stale.exit_code == 1


def test_status_on_empty_database(config_dir: Path) -> None:
    result = runner.invoke(
        app,
        [
            "status",
            "--config-dir",
            str(config_dir),
            "--log-dir",
            str(config_dir / "logs"),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Searches last 24h for 'german_upgrade': 0 / 100" in result.output
    assert "paused by conductarr: 0" in result.output


def test_invalid_config_exits_with_message(config_dir: Path) -> None:
    broken = dict(CONFIG, conductarr={"dry_rn": True})
    (config_dir / "conductarr.yml").write_text(yaml.safe_dump(broken), encoding="utf-8")
    result = runner.invoke(
        app,
        [
            "status",
            "--config-dir",
            str(config_dir),
            "--log-dir",
            str(config_dir / "logs"),
        ],
    )
    assert result.exit_code == 1
    assert "dry_rn" in result.output
