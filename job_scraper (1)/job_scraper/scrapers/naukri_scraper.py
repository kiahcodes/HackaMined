"""
scrapers/naukri_scraper.py
==========================
Naukri.com scraper — selectors verified live 2026-03-06.
Collects: job_title, company, city, sector, posted_date, skills, skills_desc, url
"""

from __future__ import annotations
from typing import List, Optional

from scrapers.playwright_scraper import PlaywrightScraper
from scrapers.base_scraper import Job, infer_sector
from config.settings import MAX_PAGES_PER_KEYWORD, NAUKRI_CONCURRENCY
from utils.logger import get_logger

log = get_logger("scraper.naukri")

# ── CSS Selectors — verified live 2026-03-06 ─────────────────────────────────
_SEL = {
    "job_card":    "div.srp-jobtuple-wrapper",
    "title":       "a.title",
    "company":     "a.comp-name",
    "city":        "span.locWdth",
    "skills":      "ul.tags-gt li",
    "description": "span.job-desc",
    "posted_date": "span.job-post-day",
    "experience":  "span.expwdth",          # e.g. "2-5 Yrs"
    "url":         "a.title",               # href attribute
}


class NaukriScraper(PlaywrightScraper):

    SOURCE = "naukri"

    def __init__(self, **kwargs) -> None:
        super().__init__(
            concurrency=kwargs.pop("concurrency", NAUKRI_CONCURRENCY),
            rate_per_sec=1.0,
            headless=kwargs.pop("headless", True),
        )

    def build_search_urls(self, keywords: List[str], cities: List[str]) -> List[str]:
        urls = []
        for kw in keywords:
            kw_slug = kw.lower().replace(" ", "-")
            for city in cities:
                city_slug = city.lower().replace(" ", "-")
                for page in range(1, MAX_PAGES_PER_KEYWORD + 1):
                    if page == 1:
                        url = f"https://www.naukri.com/{kw_slug}-jobs-in-{city_slug}"
                    else:
                        url = f"https://www.naukri.com/{kw_slug}-jobs-in-{city_slug}-{page}"
                    urls.append(url)
        return urls

    async def parse_listing_page(self, html: str, url: str) -> List[Job]:
        soup = self.parse_html(html)
        cards = soup.select(_SEL["job_card"])
        if not cards:
            log.debug("[naukri] No cards on page: %s", url)
            return []
        jobs = [j for card in cards for j in [self._parse_card(card)] if j]
        log.info("[naukri] Parsed %d jobs from %s", len(jobs), url)
        return jobs

    def _parse_card(self, card) -> Optional[Job]:
        title_el = card.select_one(_SEL["title"])
        if not title_el:
            return None
        title = title_el.get_text(strip=True)

        # Company
        company_el = card.select_one(_SEL["company"])
        company = company_el.get_text(strip=True) if company_el else ""

        # City
        city_el = card.select_one(_SEL["city"])
        city = city_el.get_text(strip=True).split("/")[0].strip() if city_el else ""

        # Experience
        exp_el = card.select_one(_SEL["experience"])
        experience = exp_el.get_text(strip=True) if exp_el else ""

        # Skills list
        skill_els = card.select(_SEL["skills"])
        skills = [s.get_text(strip=True) for s in skill_els if s.get_text(strip=True)]

        # Skills description (the short JD snippet)
        desc_el = card.select_one(_SEL["description"])
        skills_desc = desc_el.get_text(" ", strip=True) if desc_el else ""

        # Posted date
        date_el = card.select_one(_SEL["posted_date"])
        posted_date = date_el.get_text(strip=True) if date_el else ""

        # URL
        url_el = card.select_one(_SEL["url"])
        job_url = url_el.get("href", "") if url_el else ""
        # Naukri sometimes gives relative URLs
        if job_url and not job_url.startswith("http"):
            job_url = "https://www.naukri.com" + job_url

        # Sector inferred from title
        sector = infer_sector(title)

        full_text = " ".join([title, company, city, " ".join(skills), skills_desc])

        return Job(
            source="naukri",
            job_title=title,
            company=company,
            city=city,
            sector=sector,
            experience=experience,
            posted_date=posted_date,
            skills=skills,
            skills_desc=skills_desc,
            url=job_url,
            full_text=full_text,
        )
