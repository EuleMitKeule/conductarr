"""Extract an API key from a SABnzbd .ini file or an *arr config.xml file.

Usage:
    python scripts/get_api_key.py <path> sabnzbd
    python scripts/get_api_key.py <path> arr
"""

from __future__ import annotations

import sys
from pathlib import Path

from conductarr.config import _read_sabnzbd_api_key, _read_arr_api_key


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <file_path> <sabnzbd|arr>", file=sys.stderr)
        sys.exit(1)

    path = Path(sys.argv[1])
    service_type = sys.argv[2].lower()

    if not path.is_file():
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    if service_type == "sabnzbd":
        key = _read_sabnzbd_api_key(path)
    elif service_type == "arr":
        key = _read_arr_api_key(path)
    else:
        print(
            f"Unknown service type: {service_type!r}  (use 'sabnzbd' or 'arr')",
            file=sys.stderr,
        )
        sys.exit(1)

    if not key:
        print(f"No API key found in {path}", file=sys.stderr)
        sys.exit(1)

    print(key)


if __name__ == "__main__":
    main()
