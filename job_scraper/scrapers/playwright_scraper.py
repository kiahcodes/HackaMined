"""
scrapers/playwright_scraper.py
===============================
Playwright-powered base scraper for bot-protected sites (LinkedIn, Naukri).

Full stealth technique stack:
  1. Chromium launched with all automation flags disabled
  2. navigator.webdriver + 15 other bot-detection properties patched
  3. Realistic WebGL, Canvas, AudioContext fingerprints
  4. Randomised viewport, User-Agent, locale, timezone per context
  5. Cookie warmup — visits homepage first before job search URL
  6. Human-like: random scroll, mouse movement, typing delays
  7. Cloudflare JS challenge wait (up to 8s) before giving up
  8. Rate limiter + semaphore to avoid burst detection

Install:
    pip install playwright
    playwright install chromium
"""

from __future__ import annotations
import asyncio
import random
import time
from abc import abstractmethod
from typing import List, Optional

from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PWTimeout,
)

from scrapers.base_scraper import Job
from bs4 import BeautifulSoup
from config.settings import (
    INTER_REQUEST_DELAY,
    MAX_RETRIES,
    REQUEST_TIMEOUT_SECS,
    RETRY_MIN_WAIT_SECS,
    RETRY_MAX_WAIT_SECS,
)
from utils.logger import get_logger
from utils.rate_limiter import RateLimiter

log = get_logger("scraper.playwright")

# ── Fingerprint pools ─────────────────────────────────────────────────────────

_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1280, "height": 800},
]

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

# ── Full stealth JS — patches every known Playwright detection signal ─────────
_STEALTH_JS = """
// 1. Hide webdriver flag
Object.defineProperty(navigator, 'webdriver', { get: () => undefined });

// 2. Realistic plugin list (empty = headless giveaway)
Object.defineProperty(navigator, 'plugins', {
    get: () => {
        const arr = [
            { name: 'Chrome PDF Plugin',     filename: 'internal-pdf-viewer' },
            { name: 'Chrome PDF Viewer',     filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai' },
            { name: 'Native Client',         filename: 'internal-nacl-plugin' },
        ];
        arr.__proto__ = PluginArray.prototype;
        return arr;
    }
});

// 3. Realistic language list
Object.defineProperty(navigator, 'languages', { get: () => ['en-IN', 'en-US', 'en'] });

// 4. Chrome runtime object (missing in headless)
window.chrome = {
    app: { isInstalled: false },
    runtime: {
        OnInstalledReason: {},
        OnRestartRequiredReason: {},
        PlatformArch: {},
        PlatformNaclArch: {},
        PlatformOs: {},
        RequestUpdateCheckStatus: {},
    },
};

// 5. Permissions API — headless returns 'denied', real Chrome returns 'prompt'
const originalQuery = window.navigator.permissions.query;
window.navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications'
        ? Promise.resolve({ state: Notification.permission })
        : originalQuery(parameters)
);

// 6. WebGL vendor — headless shows 'Google SwiftShader'
const getParameter = WebGLRenderingContext.prototype.getParameter;
WebGLRenderingContext.prototype.getParameter = function(parameter) {
    if (parameter === 37445) return 'Intel Inc.';
    if (parameter === 37446) return 'Intel Iris OpenGL Engine';
    return getParameter.call(this, parameter);
};

// 7. Hide automation-related properties
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;

// 8. Realistic screen dimensions
Object.defineProperty(screen, 'availWidth',  { get: () => window.outerWidth });
Object.defineProperty(screen, 'availHeight', { get: () => window.outerHeight });

// 9. Make iframe contentWindow look normal
Object.defineProperty(HTMLIFrameElement.prototype, 'contentWindow', {
    get: function() {
        return window;
    }
});
"""

# Homepages to warm up cookies on before hitting job search pages
_HOMEPAGES = {
    "naukri":    "https://www.naukri.com",
    "linkedin":  "https://www.linkedin.com",
}


class PlaywrightScraper:
    """
    Drop-in replacement for BaseScraper for bot-protected sites.
    Subclasses implement build_search_urls() + parse_listing_page() only.
    """

    SOURCE: str = "unknown"

    def __init__(
        self,
        concurrency:  int   = 2,
        rate_per_sec: float = 0.4,
        headless:     bool  = True,
    ) -> None:
        self._sem          = asyncio.Semaphore(concurrency)
        self._rate_limiter = RateLimiter(rate=rate_per_sec, per=1.0)
        self._headless     = headless
        self._browser: Optional[Browser] = None
        self._playwright   = None
        # One persistent context per scraper — shares cookies across pages
        self._context: Optional[BrowserContext] = None

    # ── lifecycle ─────────────────────────────────────────────────────────

    async def __aenter__(self) -> "PlaywrightScraper":
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self._headless,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--disable-web-security",
                "--disable-features=IsolateOrigins,site-per-process",
                "--allow-running-insecure-content",
                "--disable-extensions",
                "--disable-plugins-discovery",
                "--no-first-run",
                "--no-default-browser-check",
                "--password-store=basic",
                "--use-mock-keychain",
            ],
        )
        self._context = await self._new_context()
        await self._warmup_cookies()
        log.info("[%s] Playwright ready (headless=%s)", self.SOURCE, self._headless)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._context:
            await self._context.close()
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    # ── stealth context ───────────────────────────────────────────────────

    async def _new_context(self) -> BrowserContext:
        ua       = random.choice(_USER_AGENTS)
        viewport = random.choice(_VIEWPORTS)

        context = await self._browser.new_context(
            user_agent=ua,
            viewport=viewport,
            locale="en-IN",
            timezone_id="Asia/Kolkata",
            geolocation={"latitude": 12.9716, "longitude": 77.5946},
            permissions=["geolocation"],
            java_script_enabled=True,
            accept_downloads=False,
            extra_http_headers={
                "Accept-Language":           "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
                "Accept-Encoding":           "gzip, deflate, br",
                "DNT":                       "1",
                "Sec-Ch-Ua":                 '"Chromium";v="123", "Not:A-Brand";v="8"',
                "Sec-Ch-Ua-Mobile":          "?0",
                "Sec-Ch-Ua-Platform":        '"Windows"',
                "Sec-Fetch-Dest":            "document",
                "Sec-Fetch-Mode":            "navigate",
                "Sec-Fetch-Site":            "none",
                "Sec-Fetch-User":            "?1",
                "Upgrade-Insecure-Requests": "1",
            },
        )
        await context.add_init_script(_STEALTH_JS)
        return context

    async def _warmup_cookies(self) -> None:
        """
        Visit the site homepage first — just like a real user would.
        This establishes a legitimate Cloudflare/CDN cookie before
        we hit any job search pages.
        """
        homepage = _HOMEPAGES.get(self.SOURCE)
        if not homepage:
            return

        page = await self._context.new_page()
        try:
            log.info("[%s] Warming up cookies on %s …", self.SOURCE, homepage)
            await page.goto(homepage, wait_until="domcontentloaded",
                            timeout=REQUEST_TIMEOUT_SECS * 1000)
            # Wait for Cloudflare JS challenge to resolve (up to 8s)
            await asyncio.sleep(random.uniform(4.0, 8.0))
            # Simulate quick scroll
            await page.evaluate("window.scrollBy(0, 400)")
            await asyncio.sleep(random.uniform(1.0, 2.5))
            log.info("[%s] Cookie warmup complete", self.SOURCE)
        except Exception as exc:
            log.warning("[%s] Cookie warmup failed (non-fatal): %s", self.SOURCE, exc)
        finally:
            await page.close()

    # ── core fetch ────────────────────────────────────────────────────────

    async def fetch(self, url: str) -> Optional[str]:
        async with self._sem:
            async with self._rate_limiter:
                await asyncio.sleep(random.uniform(*INTER_REQUEST_DELAY))
                return await self._fetch_with_retry(url)

    async def _fetch_with_retry(self, url: str) -> Optional[str]:
        for attempt in range(1, MAX_RETRIES + 1):
            page = await self._context.new_page()
            try:
                html = await self._fetch_page(page, url)
                if html:
                    return html
                # Block detected — back off longer before retry
                wait = RETRY_MIN_WAIT_SECS * (2 ** attempt)
                wait = min(wait, RETRY_MAX_WAIT_SECS)
                log.warning("[%s] Blocked attempt %d/%d — sleeping %.1fs",
                            self.SOURCE, attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
            except PWTimeout:
                wait = RETRY_MIN_WAIT_SECS * (2 ** (attempt - 1))
                log.warning("[%s] Timeout attempt %d/%d — sleeping %.1fs | %s",
                            self.SOURCE, attempt, MAX_RETRIES, wait, url)
                await asyncio.sleep(wait)
            except Exception as exc:
                log.warning("[%s] Error attempt %d/%d: %s | %s",
                            self.SOURCE, attempt, MAX_RETRIES, exc, url)
                await asyncio.sleep(RETRY_MIN_WAIT_SECS)
            finally:
                await page.close()

        log.error("[%s] Giving up after %d attempts | %s", self.SOURCE, MAX_RETRIES, url)
        return None

    async def _fetch_page(self, page: Page, url: str) -> Optional[str]:
        timeout_ms = REQUEST_TIMEOUT_SECS * 1000

        await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

        # Wait for Cloudflare challenge to pass if present
        await self._wait_for_cloudflare(page)

        # Human-like behaviour
        await asyncio.sleep(random.uniform(2.0, 4.0))
        await self._human_scroll(page)
        await self._human_mouse(page)

        # Check for blocks AFTER waiting
        page_text = (await page.inner_text("body")).lower()
        block_signals = [
            "verify you are human",
            "access denied",
            "just a moment",
            "checking your browser",
            "please enable cookies",
            "enable javascript and cookies",
            "captcha",
            "are you a robot",
            "unusual traffic",
        ]
        for signal in block_signals:
            if signal in page_text:
                log.warning("[%s] Block signal '%s' detected: %s", self.SOURCE, signal, url)
                return None

        return await page.content()

    async def _wait_for_cloudflare(self, page: Page, max_wait: float = 8.0) -> None:
        """Poll until Cloudflare challenge clears or timeout."""
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            try:
                text = (await page.inner_text("body")).lower()
            except Exception:
                break
            if "just a moment" in text or "checking your browser" in text:
                log.debug("[%s] Cloudflare challenge active — waiting …", self.SOURCE)
                await asyncio.sleep(1.5)
            else:
                break

    async def _human_scroll(self, page: Page) -> None:
        """Scroll down in 2-3 natural steps."""
        steps = random.randint(2, 4)
        for _ in range(steps):
            dist = random.randint(200, 600)
            await page.evaluate(f"window.scrollBy(0, {dist})")
            await asyncio.sleep(random.uniform(0.3, 0.9))

    async def _human_mouse(self, page: Page) -> None:
        """Move mouse to a few random positions."""
        vp = page.viewport_size or {"width": 1366, "height": 768}
        for _ in range(random.randint(2, 4)):
            await page.mouse.move(
                random.randint(100, vp["width"]  - 100),
                random.randint(100, vp["height"] - 100),
            )
            await asyncio.sleep(random.uniform(0.1, 0.4))

    # ── helpers ───────────────────────────────────────────────────────────

    @staticmethod
    def parse_html(html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "lxml")

    # ── abstract interface ────────────────────────────────────────────────

    @abstractmethod
    def build_search_urls(self, keywords: List[str], cities: List[str]) -> List[str]:
        ...

    @abstractmethod
    async def parse_listing_page(self, html: str, url: str) -> List[Job]:
        ...

    # ── orchestrator ──────────────────────────────────────────────────────

    async def scrape_all(
        self,
        keywords: List[str],
        cities: List[str],
        checkpoint_callback=None,   # called after every page with List[Job]
    ) -> List[Job]:
        """
        checkpoint_callback: optional async function(jobs: List[Job]) → None
        Called after every page so data can be saved mid-run.
        If you press Ctrl+C, all data up to that point is already saved.
        """
        urls = self.build_search_urls(keywords, cities)
        log.info("[%s] Starting Playwright scrape — %d URLs", self.SOURCE, len(urls))
        start = time.monotonic()

        tasks  = [self._scrape_one(url, checkpoint_callback) for url in urls]
        nested = await asyncio.gather(*tasks, return_exceptions=True)

        jobs: List[Job] = []
        errors = 0
        for item in nested:
            if isinstance(item, Exception):
                log.error("[%s] Task exception: %s", self.SOURCE, item)
                errors += 1
            elif isinstance(item, list):
                jobs.extend(item)

        elapsed = time.monotonic() - start
        log.info("[%s] Done — %d jobs, %d errors, %.1fs", self.SOURCE, len(jobs), errors, elapsed)
        return jobs

    async def _scrape_one(self, url: str, checkpoint_callback=None) -> List[Job]:
        html = await self.fetch(url)
        if not html:
            return []
        jobs = await self.parse_listing_page(html, url)
        for job in jobs:
            job.compute_ai_mentions()
        # Save immediately after each page
        if jobs and checkpoint_callback:
            try:
                await checkpoint_callback(jobs)
            except Exception as e:
                log.warning("[%s] Checkpoint save failed (non-fatal): %s", self.SOURCE, e)
        return jobs
