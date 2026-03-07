"""
utils/logger.py
===============
Structured, coloured logger shared across the entire pipeline.
"""

from __future__ import annotations
import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger that emits structured records to stdout.
    Format: 2026-01-01 12:00:00 | INFO     | scraper.naukri | message
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Avoid attaching duplicate handlers in notebooks / reload scenarios
        return logger

    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
