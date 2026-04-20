"""
scrapers/linkedin_scraper.py
=============================
LinkedIn Jobs scraper — selectors verified live 2026-03-06.
Collects: job_title, company, city, sector, posted_date, skills_desc, url
"""

from __future__ import annotations
from typing import List, Optional
from urllib.parse import urlencode

from scrapers.playwright_scraper import PlaywrightScraper
from scrapers.base_scraper import Job, infer_sector
from config.settings import MAX_PAGES_PER_KEYWORD, RESULTS_PER_PAGE, LINKEDIN_CONCURRENCY
from utils.logger import get_logger

log = get_logger("scraper.linkedin")

_BASE = "https://www.linkedin.com/jobs/search/"

# ── CSS Selectors — verified live 2026-03-06 ─────────────────────────────────
_SEL = {
    "job_card":    "div.base-card.relative",
    "title":       "h3.base-search-card__title",
    "company":     "h4.base-search-card__subtitle",
    "city":        "span.job-search-card__location",
    "description": "div.base-search-card__metadata",
    "posted_date": "time.job-search-card__listdate",
    "url":         "a.base-card__full-link",
}


class LinkedInScraper(PlaywrightScraper):

    SOURCE = "linkedin"

    def __init__(self, **kwargs) -> None:
        super().__init__(
            concurrency=kwargs.pop("concurrency", LINKEDIN_CONCURRENCY),
            rate_per_sec=0.5,
            headless=kwargs.pop("headless", True),
        )

    def build_search_urls(self, keywords: List[str], cities: List[str]) -> List[str]:
        urls = []
        for kw in keywords:
            for city in cities:
                for page in range(MAX_PAGES_PER_KEYWORD):
                    params = {
                        "keywords": kw,
                        "location": f"{city}, India",
                        "start":    page * RESULTS_PER_PAGE,
                        "f_TPR":    "r2592000",
                        "f_JT":     "F",
                    }
                    urls.append(f"{_BASE}?{urlencode(params)}")
        return urls

    async def parse_listing_page(self, html: str, url: str) -> List[Job]:
        soup = self.parse_html(html)
        cards = soup.select(_SEL["job_card"])
        if not cards:
            log.debug("[linkedin] No cards on page: %s", url)
            return []
        jobs = [j for card in cards for j in [self._parse_card(card)] if j]
        log.info("[linkedin] Parsed %d jobs from %s", len(jobs), url)
        return jobs

    def _parse_card(self, card) -> Optional[Job]:
        title_el = card.select_one(_SEL["title"])
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        company_el = card.select_one(_SEL["company"])
        company = company_el.get_text(strip=True) if company_el else ""

        city_el  = card.select_one(_SEL["city"])
        city_raw = city_el.get_text(strip=True) if city_el else ""
        city     = city_raw.split(",")[0].strip()

        desc_el    = card.select_one(_SEL["description"])
        skills_desc = desc_el.get_text(" ", strip=True) if desc_el else ""

        # posted_date — try both regular and "new" date elements
        date_el = card.select_one(_SEL["posted_date"]) or \
                  card.select_one("time.job-search-card__listdate--new")
        posted_date = date_el.get("datetime", date_el.get_text(strip=True)) \
                      if date_el else ""

        # URL — direct link to job posting
        url_el  = card.select_one(_SEL["url"])
        job_url = url_el.get("href", "").split("?")[0] if url_el else ""

        sector    = infer_sector(title)
        full_text = " ".join([title, company, city, skills_desc])

        return Job(
            source="linkedin",
            job_title=title,
            company=company,
            city=city,
            sector=sector,
            posted_date=posted_date,
            skills=[],          # LinkedIn SERP doesn't expose structured skills
            skills_desc=skills_desc,
            url=job_url,
            full_text=full_text,
        )
