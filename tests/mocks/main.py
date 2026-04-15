"""Start one or all mock services based on the SERVICE env var."""

from __future__ import annotations

import asyncio
import os

import uvicorn

from sabnzbd.app import app as sabnzbd_app
from radarr.app import app as radarr_app
from sonarr.app import app as sonarr_app

_ALL_SERVICES = [
    ("SABnzbd", sabnzbd_app, 8080),
    ("Radarr", radarr_app, 7878),
    ("Sonarr", sonarr_app, 8989),
]

_SERVICE_MAP: dict[str, tuple[str, object, int]] = {
    "sabnzbd": ("SABnzbd", sabnzbd_app, 8080),
    "radarr": ("Radarr", radarr_app, 7878),
    "sonarr": ("Sonarr", sonarr_app, 8989),
}


async def main() -> None:
    service_name = os.environ.get("SERVICE", "").lower()
    if service_name in _SERVICE_MAP:
        to_start = [_SERVICE_MAP[service_name]]
    else:
        to_start = _ALL_SERVICES

    servers: list[uvicorn.Server] = []
    for name, application, port in to_start:
        config = uvicorn.Config(
            app=application,
            host="0.0.0.0",
            port=port,
            log_level="info",
            loop="auto",
        )
        server = uvicorn.Server(config)
        servers.append(server)
        print(f"Starting {name} on port {port}")

    await asyncio.gather(*(s.serve() for s in servers))


if __name__ == "__main__":
    asyncio.run(main())
