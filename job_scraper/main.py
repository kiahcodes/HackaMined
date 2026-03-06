"""
main.py
=======
Entry point for the job market intelligence scraper.

Pipeline stages:
    1.  Build scraper instances (LinkedIn, Naukri, Internshala)
    2.  Launch all scrapers concurrently via asyncio.gather
    3.  Feed raw Job objects through Preprocessor
    4.  Aggregate all sources → jobs_combined.csv

Usage:
    python main.py                          # full scrape (all keywords × cities)
    python main.py --keywords "data analyst" --cities "Pune,Jaipur"
    python main.py --max-pages 3            # quick test run
"""

from __future__ import annotations
import argparse
import asyncio
import sys
import time
from typing import List

import nest_asyncio
nest_asyncio.apply()   # needed for notebook/Jupyter environments

from config.settings import SEARCH_KEYWORDS, TARGET_CITIES, USE_PROXIES, PROXY_FILE, DB_NAME, DB_TABLE
from scrapers.linkedin_scraper import LinkedInScraper
from scrapers.naukri_scraper import NaukriScraper
from scrapers.base_scraper import Job
from pipeline.preprocessor import Preprocessor
from pipeline.aggregator import Aggregator
from utils.logger import get_logger
from utils.proxy_rotator import ProxyRotator

log = get_logger("main")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Async job scraper — LinkedIn + Naukri + Internshala → CSV"
    )
    parser.add_argument(
        "--keywords", type=str, default=None,
        help="Comma-separated keywords (overrides settings.py)"
    )
    parser.add_argument(
        "--cities", type=str, default=None,
        help="Comma-separated cities (overrides settings.py)"
    )
    parser.add_argument(
        "--max-pages", type=int, default=None,
        help="Override MAX_PAGES_PER_KEYWORD for a quick test run"
    )
    parser.add_argument(
        "--no-linkedin", action="store_true",
        help="Skip LinkedIn (useful when throttled)"
    )
    parser.add_argument(
        "--no-naukri", action="store_true",
        help="Skip Naukri"
    )
    parser.add_argument(
        "--show-browser", action="store_true",
        help="Run Playwright in visible mode (useful for debugging blocks)"
    )
    return parser.parse_args()


# ─────────────────────────────────────────────────────────────────────────────
# Async pipeline
# ─────────────────────────────────────────────────────────────────────────────

async def run_scraper(scraper_cls, keywords, cities, proxy_rotator, label: str, headless: bool = True) -> List[Job]:
    """Instantiate and run one scraper inside its own context manager."""
    log.info("▶  Starting %s scraper", label)
    from scrapers.playwright_scraper import PlaywrightScraper
    if issubclass(scraper_cls, PlaywrightScraper):
        async with scraper_cls(headless=headless) as scraper:
            return await scraper.scrape_all(keywords, cities)
    else:
        kwargs = {"proxy_rotator": proxy_rotator} if proxy_rotator else {}
        async with scraper_cls(**kwargs) as scraper:
            return await scraper.scrape_all(keywords, cities)


async def main_async(args: argparse.Namespace) -> None:
    t0 = time.monotonic()

    # ── Resolve keywords / cities ──────────────────────────────────────────
    keywords = (
        [k.strip() for k in args.keywords.split(",")]
        if args.keywords else SEARCH_KEYWORDS
    )
    cities = (
        [c.strip() for c in args.cities.split(",")]
        if args.cities else TARGET_CITIES
    )

    log.info("Keywords : %s", keywords)
    log.info("Cities   : %s", cities)

    # ── Override max pages for test runs ──────────────────────────────────
    if args.max_pages:
        import config.settings as _cfg
        _cfg.MAX_PAGES_PER_KEYWORD = args.max_pages
        log.info("MAX_PAGES_PER_KEYWORD overridden to %d", args.max_pages)

    # ── Proxy setup ────────────────────────────────────────────────────────
    proxy_rotator = ProxyRotator(PROXY_FILE) if USE_PROXIES else None

    # ── Launch scrapers concurrently ───────────────────────────────────────
    scraper_tasks = []

    if not args.no_linkedin:
        scraper_tasks.append(
            run_scraper(LinkedInScraper, keywords, cities, None, "LinkedIn", headless=not args.show_browser)
        )
    if not args.no_naukri:
        scraper_tasks.append(
            run_scraper(NaukriScraper, keywords, cities, None, "Naukri", headless=not args.show_browser)
        )


    if not scraper_tasks:
        log.error("All scrapers disabled — nothing to do. Remove --no-* flags.")
        sys.exit(1)

    # Run all scrapers concurrently
    raw_results = await asyncio.gather(*scraper_tasks, return_exceptions=True)

    # ── Collect all jobs ───────────────────────────────────────────────────
    all_jobs: List[Job] = []
    for result in raw_results:
        if isinstance(result, Exception):
            log.error("Scraper raised exception: %s", result)
        elif isinstance(result, list):
            all_jobs.extend(result)

    log.info("Total raw jobs collected: %d", len(all_jobs))

    # ── Preprocess ─────────────────────────────────────────────────────────
    preprocessor = Preprocessor()

    # Split by source for cleaner logging, then pass all together
    df = preprocessor.run(all_jobs)

    # ── Aggregate & save ───────────────────────────────────────────────────
    aggregator = Aggregator()
    final_df = aggregator.run([df])

    elapsed = time.monotonic() - t0
    log.info("=" * 60)
    log.info("Pipeline complete in %.1f seconds", elapsed)
    log.info("Final record count : %d", len(final_df))
    log.info("Columns            : %s", list(final_df.columns))
    log.info("AI mention stats   : mean=%.1f  max=%d",
             final_df["ai_mention_count"].mean(), final_df["ai_mention_count"].max())
    log.info("Database           : %s  |  Table: %s", DB_NAME, DB_TABLE)
    log.info("=" * 60)

    # ── Preview ────────────────────────────────────────────────────────────
    print("\n── Sample output (first 5 rows) ──────────────────────────")
    print(final_df.head(5).to_string(index=False))
    print()


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
