"""
scrapers/base_scraper.py
========================
Abstract base class every platform scraper must inherit from.
"""

from __future__ import annotations
import asyncio
import random
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import List, Optional
import time

try:
    import aiohttp
except ImportError:
    aiohttp = None

from bs4 import BeautifulSoup

try:
    from fake_useragent import UserAgent
    _ua = UserAgent()
except Exception:
    class _FallbackUA:
        random = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    _ua = _FallbackUA()

from config.settings import (
    AI_KEYWORDS,
    INTER_REQUEST_DELAY,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECS,
    RETRY_MAX_WAIT_SECS,
    RETRY_MIN_WAIT_SECS,
)
from utils.logger import get_logger
from utils.proxy_rotator import ProxyRotator
from utils.rate_limiter import RateLimiter

log = get_logger("scraper.base")

_AI_PATTERN = re.compile(
    "|".join(AI_KEYWORDS),
    flags=re.IGNORECASE,
)

# ── Sector inference map ──────────────────────────────────────────────────────
# Maps keywords in job title → sector/domain label
_SECTOR_MAP = [
    (["machine learning", "ml engineer", "ai engineer", "deep learning", "nlp", "data scientist"], "AI / ML"),
    (["data analyst", "data engineer", "business analyst", "bi developer", "tableau", "power bi"], "Data & Analytics"),
    (["frontend", "front-end", "react", "angular", "vue", "ui developer", "ux"], "Frontend"),
    (["backend", "back-end", "node", "django", "flask", "spring", "api developer"], "Backend"),
    (["full stack", "fullstack"], "Full Stack"),
    (["devops", "cloud", "aws", "azure", "gcp", "sre", "infrastructure", "kubernetes", "docker"], "DevOps / Cloud"),
    (["mobile", "android", "ios", "flutter", "react native", "swift", "kotlin"], "Mobile"),
    (["cybersecurity", "security engineer", "penetration", "infosec", "soc analyst"], "Cybersecurity"),
    (["product manager", "product owner", "program manager"], "Product Management"),
    (["bpo", "customer support", "call centre", "call center", "voice process"], "BPO / Customer Support"),
    (["digital marketing", "seo", "sem", "content writer", "social media", "copywriter"], "Digital Marketing"),
    (["sales", "business development", "account executive", "bdm"], "Sales / BD"),
    (["finance", "accounting", "chartered", "ca ", "cfa", "financial analyst"], "Finance"),
    (["hr", "human resources", "recruiter", "talent acquisition"], "HR"),
    (["software engineer", "software developer", "sde", "swe"], "Software Engineering"),
    (["embedded", "firmware", "iot", "vlsi", "hardware"], "Embedded / Hardware"),
    (["qa", "quality assurance", "test engineer", "automation tester", "sdet"], "QA / Testing"),
    (["blockchain", "web3", "solidity", "smart contract"], "Blockchain / Web3"),
    (["game", "unity", "unreal"], "Game Development"),
]


def infer_sector(title: str) -> str:
    title_lower = title.lower()
    for keywords, sector in _SECTOR_MAP:
        if any(kw in title_lower for kw in keywords):
            return sector
    return "Other"


# ─────────────────────────────────────────────────────────────────────────────
# Shared Data Model
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Job:
    """
    Normalised job record.
    Final CSV columns:
        job_title | company | city | sector | posted_date | experience | skills | ai_mention_count | url
    """
    source:           str
    job_title:        str = ""
    company:          str = ""
    city:             str = ""
    sector:           str = ""
    posted_date:      str = ""
    experience:       str = ""
    skills:           List[str] = field(default_factory=list)
    skills_desc:      str = ""
    url:              str = ""
    full_text:        str = ""
    ai_mention_count: int = 0

    def compute_ai_mentions(self) -> None:
        self.ai_mention_count = len(_AI_PATTERN.findall(self.full_text))


# ─────────────────────────────────────────────────────────────────────────────
# Base Scraper
# ─────────────────────────────────────────────────────────────────────────────

class BaseScraper(ABC):

    SOURCE: str = "unknown"

    def __init__(
        self,
        concurrency:   int = 5,
        proxy_rotator: Optional[ProxyRotator] = None,
        rate_per_sec:  float = 1.0,
    ) -> None:
        self._sem           = asyncio.Semaphore(concurrency)
        self._proxy_rotator = proxy_rotator
        self._rate_limiter  = RateLimiter(rate=rate_per_sec, per=1.0)
        self._session       = None

    async def __aenter__(self) -> "BaseScraper":
        connector = aiohttp.TCPConnector(limit=30, ttl_dns_cache=300, ssl=False)
        timeout   = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECS)
        self._session = aiohttp.ClientSession(
            connector=connector, timeout=timeout, headers=self._base_headers()
        )
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._session:
            await self._session.close()

    @staticmethod
    def _base_headers() -> dict:
        return {
            "User-Agent":      _ua.random,
            "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection":      "keep-alive",
            "DNT":             "1",
            "Upgrade-Insecure-Requests": "1",
        }

    def _fresh_headers(self) -> dict:
        h = self._base_headers()
        h["User-Agent"] = _ua.random
        return h

    async def fetch(self, url: str) -> Optional[str]:
        async with self._sem:
            async with self._rate_limiter:
                await asyncio.sleep(random.uniform(*INTER_REQUEST_DELAY))
                return await self._fetch_with_retry(url)

    async def _fetch_with_retry(self, url: str) -> Optional[str]:
        proxy = self._proxy_rotator.next() if self._proxy_rotator else None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with self._session.get(
                    url, headers=self._fresh_headers(),
                    proxy=proxy, allow_redirects=True,
                ) as resp:
                    if resp.status in (429, 503):
                        wait = min(RETRY_MIN_WAIT_SECS * (2 ** (attempt - 1)), RETRY_MAX_WAIT_SECS)
                        log.warning("[%s] HTTP %s attempt %d/%d — sleeping %.1fs", self.SOURCE, resp.status, attempt, MAX_RETRIES, wait)
                        await asyncio.sleep(wait)
                        continue
                    if resp.status != 200:
                        log.warning("[%s] HTTP %s | %s", self.SOURCE, resp.status, url)
                        return None
                    return await resp.text(errors="replace")
            except Exception as exc:
                wait = min(RETRY_MIN_WAIT_SECS * (2 ** (attempt - 1)), RETRY_MAX_WAIT_SECS)
                log.warning("[%s] %s attempt %d/%d — sleeping %.1fs", self.SOURCE, type(exc).__name__, attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
        log.error("[%s] Giving up after %d attempts | %s", self.SOURCE, MAX_RETRIES, url)
        return None

    @staticmethod
    def parse_html(html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "lxml")

    @abstractmethod
    def build_search_urls(self, keywords: List[str], cities: List[str]) -> List[str]: ...

    @abstractmethod
    async def parse_listing_page(self, html: str, url: str) -> List[Job]: ...

    async def scrape_all(self, keywords: List[str], cities: List[str]) -> List[Job]:
        urls = self.build_search_urls(keywords, cities)
        log.info("[%s] Starting scrape — %d URLs", self.SOURCE, len(urls))
        start = time.monotonic()
        tasks  = [self._scrape_one(url) for url in urls]
        nested = await asyncio.gather(*tasks, return_exceptions=True)
        jobs, errors = [], 0
        for item in nested:
            if isinstance(item, Exception):
                log.error("[%s] Task exception: %s", self.SOURCE, item)
                errors += 1
            elif isinstance(item, list):
                jobs.extend(item)
        log.info("[%s] Done — %d jobs, %d errors, %.1fs", self.SOURCE, len(jobs), errors, time.monotonic() - start)
        return jobs

    async def _scrape_one(self, url: str) -> List[Job]:
        html = await self.fetch(url)
        if not html:
            return []
        jobs = await self.parse_listing_page(html, url)
        for job in jobs:
            job.compute_ai_mentions()
        return jobs
