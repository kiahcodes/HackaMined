"""
detect_selectors.py
====================
Fetches a live Naukri page using Playwright and auto-detects
the correct CSS selectors for job cards, titles, cities, skills.

Run this ONCE whenever Naukri updates its frontend:
    python detect_selectors.py

It will:
1. Open Naukri in a real browser
2. Save the raw HTML to debug_naukri.html
3. Try 20+ known selector patterns against the HTML
4. Print the ones that actually find elements
5. Auto-patch naukri_scraper.py with the working selectors
"""

import asyncio
import sys
import os
import re
sys.path.insert(0, os.path.dirname(__file__))

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup

TEST_URL = "https://www.naukri.com/python-jobs-in-bangalore"

# ── All known Naukri selector variants across different site versions ─────────
CANDIDATE_SELECTORS = {
    "job_card": [
        "article.jobTuple",
        "article.job-container",
        "div.jobTuple",
        "div.job-container",
        "div.srp-jobtuple-wrapper",
        "div.cust-job-tuple",
        "li.jobTuple",
        '[data-job-id]',
        '[class*="jobTuple"]',
        '[class*="job-tuple"]',
        '[class*="jobtuple"]',
        "div.list > article",
        "section.list > article",
    ],
    "title": [
        "a.title",
        "a.jobTitle",
        "a.job-title",
        '[class*="title"] a',
        "h2.title a",
        "h2 a.jobTitle",
        "a[title]",
        '[class*="jobTitle"]',
        "a.row1",
    ],
    "city": [
        "li.location span",
        "span.location",
        "li.location",
        '[class*="location"]',
        "span.loc",
        "li.loc span",
        '[class*="loc"] span',
        "span.city",
    ],
    "skills": [
        "ul.tags li",
        "ul.skill-tags li",
        "ul.skills li",
        "div.tags span",
        '[class*="tags"] li',
        '[class*="skill"] li',
        "div.skill-list span",
        "ul.tag-list li",
    ],
    "description": [
        "div.job-desc",
        "div.jobDesc",
        "div.job-description",
        "p.job-desc",
        '[class*="job-desc"]',
        '[class*="jobDesc"]',
        "div.desc",
    ],
}


def detect_best_selectors(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    best = {}

    for field, candidates in CANDIDATE_SELECTORS.items():
        print(f"\n  [{field}]")
        for sel in candidates:
            try:
                found = soup.select(sel)
                if found:
                    sample = found[0].get_text(strip=True)[:60]
                    print(f"    ✅  {sel!r:45s} → {len(found)} elements  |  sample: '{sample}'")
                    if field not in best:
                        best[field] = sel   # first match wins
            except Exception:
                pass
        if field not in best:
            print(f"    ❌  No working selector found for '{field}'")

    return best


def patch_naukri_scraper(best: dict) -> None:
    """Overwrite the _SEL dict in naukri_scraper.py with detected selectors."""
    scraper_path = os.path.join(os.path.dirname(__file__), "scrapers", "naukri_scraper.py")
    with open(scraper_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Build new _SEL block
    lines = ["_SEL = {"]
    field_map = {
        "job_card":    ("job_card",    "individual job card"),
        "title":       ("title",       "job title link"),
        "city":        ("city",        "city text"),
        "skills":      ("skills",      "skill pills"),
        "description": ("description", "short JD snippet"),
    }
    for key, (field, comment) in field_map.items():
        sel = best.get(field, CANDIDATE_SELECTORS[field][0])
        lines.append(f'    "{key}":  "{sel}",  # {comment}')
    lines.append("}")
    new_sel_block = "\n".join(lines)

    # Replace existing _SEL block using regex
    pattern = r"_SEL\s*=\s*\{[^}]+\}"
    if re.search(pattern, content, re.DOTALL):
        new_content = re.sub(pattern, new_sel_block, content, flags=re.DOTALL)
        with open(scraper_path, "w", encoding="utf-8") as f:
            f.write(new_content)
        print(f"\n✅  Patched selectors written to {scraper_path}")
    else:
        print(f"\n⚠️  Could not auto-patch — update _SEL in {scraper_path} manually")


async def main():
    print("=" * 60)
    print("  Naukri Selector Auto-Detector")
    print("=" * 60)
    print(f"\nFetching: {TEST_URL}")
    print("Browser will open — please wait ~10 seconds …\n")

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,   # visible so you can see if it gets blocked
            args=["--no-sandbox", "--disable-blink-features=AutomationControlled"],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1366, "height": 768},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        page = await context.new_page()

        # Warmup
        await page.goto("https://www.naukri.com", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)

        # Real page
        await page.goto(TEST_URL, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(6)
        await page.evaluate("window.scrollBy(0, 600)")
        await asyncio.sleep(2)

        html = await page.content()
        await browser.close()

    # Save for manual inspection
    debug_path = "debug_naukri.html"
    with open(debug_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"HTML saved → {debug_path}  (open in browser to inspect manually)\n")

    # Detect selectors
    print("Detecting working selectors …")
    best = detect_best_selectors(html)

    print("\n" + "=" * 60)
    print("  DETECTED SELECTORS")
    print("=" * 60)
    for field, sel in best.items():
        print(f"  {field:15s} → {sel}")

    if len(best) >= 3:
        print("\nAuto-patching naukri_scraper.py …")
        patch_naukri_scraper(best)
    else:
        print("\n⚠️  Too few selectors found — check debug_naukri.html manually")
        print("    The page may have been blocked or returned no results.")

    print("\nDone. Run your scraper again:\n")
    print("  python main.py --max-pages 1 --cities 'Bangalore' --no-linkedin --no-internshala\n")


if __name__ == "__main__":
    asyncio.run(main())
