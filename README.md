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

Conductarr sits between your \*arr apps and SABnzbd and enforces a strict priority order on the download queue:

- **Virtual queues** — you define named tiers (e.g. *user requests*, *upgrade queue*, *kometa*) each with a priority number and tag-based matchers.  Every SABnzbd job is assigned to the highest-matching tier.
- **Automatic reordering** — each poll cycle the SABnzbd queue is reordered so that higher-priority tiers always run first.  Only one job downloads at a time; everything else is paused.
- **Upgrade scanning** — queues marked `upgrade: enabled: true` automatically search Radarr/Sonarr for candidates that don't yet satisfy your `accept_conditions` (e.g. missing a custom format or below a score threshold), grab a release, and track it through to completion.  A rate-limited cursor ensures candidates are visited round-robin without hammering the APIs.
- **Duplicate prevention** — before grabbing a release Conductarr checks both the live SABnzbd queue and the database, so the same movie or episode is never downloaded twice concurrently.

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
      TZ: UTC
      LOG_LEVEL: info
    volumes:
      - /path/to/conductarr/config:/config
    command: conductarr watch
```

Place a `conductarr.yml` inside the mounted config directory.  A minimal example:

```yaml
conductarr:
  poll_interval: 15.0

sabnzbd:
  url: http://sabnzbd:8080
  api_key: YOUR_KEY

radarr:
  url: http://radarr:7878
  api_key: YOUR_KEY

sonarr:
  url: http://sonarr:8989
  api_key: YOUR_KEY

queues:
  - name: user_requests
    priority: 100
    matchers:
      - type: tags
        tags: ["request"]

  - name: german_upgrade
    priority: 50
    matchers:
      - type: tags
        tags: ["upgrade-de"]
    upgrade:
      enabled: true
      sources: [radarr, sonarr]
      max_active: 2
      search_interval: 30.0
      accept_conditions:
        - type: custom_format
          name: German DL

  - name: other
    priority: 1
    fallback: true
```

Tag your Radarr/Sonarr items with the matching tags and Conductarr will automatically place them in the right tier.

---

## Poll loop internals

Every `poll_interval` seconds the orchestrator runs a single cycle:

1. **Read SABnzbd queue** — fetch current slots (silently; no log noise when nothing changes).
2. **Resolve identities** — map each `nzo_id` to a virtual queue using an in-memory cache.  On a cache miss, Radarr and Sonarr queues are fetched *once* for the whole cycle, not once per item.
3. **Handle completions** — jobs that disappeared from SABnzbd are marked complete in the database.
4. **Reorder & enforce** — slots are sorted by virtual-queue priority and `switch()` is called only when the order is actually wrong.  The top slot is resumed; all other pausable slots are paused.
5. **Fill upgrade slots** — for each upgrade-enabled queue that has capacity, the next unprocessed candidate is fetched from the database, checked against live Radarr/Sonarr data, and if still eligible a release is grabbed and added to SABnzbd.

