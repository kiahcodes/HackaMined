"""
utils/proxy_rotator.py
======================
Reads a plain-text proxy list and rotates through them round-robin.
If no proxies are configured, returns None (direct connection).

Proxy file format (one per line):
    http://user:pass@host:port
    http://host:port
    socks5://host:port
"""

from __future__ import annotations
import itertools
import os
from pathlib import Path
from typing import Iterator, Optional

from utils.logger import get_logger

log = get_logger("utils.proxy_rotator")


class ProxyRotator:
    def __init__(self, proxy_file: Optional[str] = None) -> None:
        self._proxies: list[str] = []
        self._cycle: Iterator[str] | None = None

        if proxy_file and Path(proxy_file).exists():
            raw = Path(proxy_file).read_text().strip().splitlines()
            self._proxies = [p.strip() for p in raw if p.strip() and not p.startswith("#")]
            log.info("Loaded %d proxies from %s", len(self._proxies), proxy_file)
            self._cycle = itertools.cycle(self._proxies)
        else:
            log.info("No proxy file found — running direct (no proxy)")

    def next(self) -> Optional[str]:
        """Return the next proxy string, or None for direct connection."""
        if self._cycle:
            return next(self._cycle)
        return None

    @property
    def count(self) -> int:
        return len(self._proxies)
