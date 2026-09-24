# Conductarr

![PyPI - Version](https://img.shields.io/pypi/v/conductarr?logo=python&logoColor=green&color=blue)
![GitHub License](https://img.shields.io/github/license/eulemitkeule/conductarr?color=blue)
![GitHub Sponsors](https://img.shields.io/github/sponsors/eulemitkeule?logo=GitHub-Sponsors)

[![Code Quality](https://github.com/EuleMitKeule/conductarr/actions/workflows/quality.yml/badge.svg)](https://github.com/EuleMitKeule/conductarr/actions/workflows/quality.yml)
[![Publish](https://github.com/EuleMitKeule/conductarr/actions/workflows/publish.yml/badge.svg)](https://github.com/EuleMitKeule/conductarr/actions/workflows/publish.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?project=EuleMitKeule_conductarr&metric=coverage)](https://sonarcloud.io/summary/new_code?id=EuleMitKeule_conductarr)
[![Bugs](https://sonarcloud.io/api/project_badges/measure?project=EuleMitKeule_conductarr&metric=bugs)](https://sonarcloud.io/summary/new_code?id=EuleMitKeule_conductarr)
[![Vulnerabilities](https://sonarcloud.io/api/project_badges/measure?project=EuleMitKeule_conductarr&metric=vulnerabilities)](https://sonarcloud.io/summary/new_code?id=EuleMitKeule_conductarr)
[![Code Smells](https://sonarcloud.io/api/project_badges/measure?project=EuleMitKeule_conductarr&metric=code_smells)](https://sonarcloud.io/summary/new_code?id=EuleMitKeule_conductarr)
[![Technical Debt](https://sonarcloud.io/api/project_badges/measure?project=EuleMitKeule_conductarr&metric=sqale_index)](https://sonarcloud.io/summary/new_code?id=EuleMitKeule_conductarr)

🎼 Priority-based download queue orchestrator for Radarr, Sonarr & SABnzbd — automatically manages and reorders your download queue across multiple priority tiers.

---


## What it does

Conductarr sits between Radarr/Sonarr and SABnzbd and does two things:

- **Queue ordering** — every SABnzbd job is mapped to a *virtual queue* (e.g. *user requests*, *other*, *upgrades*) via tag matchers.  The SABnzbd queue is kept in priority order, and by default only the top job downloads while the rest are paused.  Conductarr-initiated upgrades always sort below everything else.
- **Gradual library upgrades** — for queues with an `upgrade` section, conductarr walks your whole library, finds movies/episodes whose file does not yet satisfy your `accept_conditions` (e.g. one of the German-audio custom formats), runs an interactive search through the Arr, and grabs the best release that is a real improvement — one item at a time, rate-limited, and only when nothing else is downloading.

Radarr/Sonarr themselves never need to upgrade anything; conductarr decides *what* to grab, the Arr does the actual import.

---

## Safety model

Conductarr is built so that it cannot damage your library or disturb normal downloads:

| Guarantee | How |
|---|---|
| Never deletes anything | The clients have no delete/remove methods at all (enforced by a unit test). The only writes are: SABnzbd `switch`/`pause`/`resume` of single jobs, and Arr `POST /release` for one selected release. |
| Never touches torrents | Only releases with `protocol: usenet` are considered; unknown protocols are rejected. |
| Never grabs the wrong item | Releases the Arr mapped to a different movie/episode are dropped. Season packs are used when they are the best option, but only if the Arr confirmed they contain the searched episode; the other episodes of a grabbed pack are not searched meanwhile (`allow_season_packs: false` disables packs). |
| Respects Arr rejections | Only the "not an upgrade / cutoff met" family of rejections (which exist because the Arr itself may not upgrade) is overridden. Anything else — wrong quality, language, size, restricted terms, "already in queue" — blocks the release. |
| No downgrades | The release must beat the current custom-format score (`min_score_increase`) and may not have a lower resolution than the current file. |
| Never retries a bad release | Every grabbed release title is remembered per item and never grabbed again (e.g. after a failed import). |
| User downloads first | Upgrades wait while any other job is in SABnzbd (`defer_to_other_downloads`), sort last (`upgrades_last`) and are limited by `max_active`. Jobs you paused yourself are never resumed; everything conductarr paused is resumed on shutdown (SIGTERM). |
| Indexer friendly | At most one search per `search_interval` and `max_searches_per_day` per upgrade queue, persisted across restarts. |
| No wasted searches | Every custom-format name used in `accept_conditions` must exist in the Arr; otherwise the source is skipped with an error listing the available names. |
| Disk safety | No new upgrades below `min_free_space_gb` (as reported by SABnzbd) or while the SABnzbd queue is paused. |
| Stable | Every HTTP call has a timeout, every cycle has a watchdog, the queue loop never waits for an indexer search, and a heartbeat file backs the Docker `HEALTHCHECK`. |
| Observe first | `dry_run: true` runs everything but never reorders, pauses, resumes or grabs. |

---

## Quick start (Docker Compose)

```yaml
services:
  conductarr:
    image: ghcr.io/eulemitkeule/conductarr:latest
    container_name: conductarr
    restart: unless-stopped
    environment:
      PUID: 1000
      PGID: 1000
      UMASK: "022"
      TZ: Europe/Berlin
      LOG_LEVEL: info
    volumes:
      - /path/to/conductarr/config:/config
    command: conductarr watch
```

Conductarr only needs its own config directory — it never needs access to your media or download folders.

Place a `conductarr.yml` inside the config directory; [`config.example.yml`](config.example.yml) documents every option.  Unknown keys are rejected at startup, so typos cannot silently disable a safety setting.

### Recommended rollout

1. Start with `dry_run: true` and watch the log for a day: every "would grab", "would pause" and "would move" line shows exactly what conductarr would do.
2. Try a single item: `conductarr debug-upgrades --source radarr --id 42` (performs one real indexer search, grabs nothing).
3. Switch to `dry_run: false` with `max_active: 1`.

---

## Commands

| Command | Purpose |
|---|---|
| `conductarr watch` | Run the queue and upgrade loops. |
| `conductarr status` | Read-only overview: candidates per queue, grabs in flight, upgrades done, search budget used. |
| `conductarr debug-upgrades [--source radarr] [--id 42]` | Evaluate the next candidate (or one item) and print every filter step. No writes, no grab. |
| `conductarr healthcheck` | Exit 0 if the queue loop completed a cycle recently (used by the Docker `HEALTHCHECK`). |

---

## How it works

Two independent loops run every `poll_interval`:

**Queue loop**

1. Read the SABnzbd queue.
2. Map new `nzo_id`s to Radarr/Sonarr items via their queues (one fetch per cycle).  Jobs that could not be mapped are retried every minute.
3. Finalise jobs that left the queue once SABnzbd history reports `Completed`/`Failed` (post-processing is waited for).  Jobs deleted without history are released after 30 minutes.
4. Reorder by virtual-queue rank and keep one job active.

**Upgrade loop** (per upgrade queue)

1. Rescan the libraries every `rescan_interval`: every movie/episode with a file becomes a candidate, including items that were first downloaded as a user request.
2. Skip if `max_active` grabs are in flight, SABnzbd is paused/low on space, other downloads are waiting, or the search budget is used.
3. Walk candidates from a persisted cursor; items that already satisfy the conditions, have no file or are downloading are skipped without searching.
4. Search the first real candidate, run the [selection pipeline](conductarr/upgrade/selection.py) (usenet → conditions → mapping → rejections → downloadable → blocklist → resolution → score) and grab the best remaining release.

The grab is recorded in the database *before* the Arr is asked to grab it, so a download can never go untracked.

---

## Development

```bash
uv sync --all-groups
uv run pytest tests/unit                 # fast, no services needed
uv run pytest tests/integration          # starts mock SABnzbd/Radarr/Sonarr via docker compose
uv run ruff check . && uv run ty check
```
