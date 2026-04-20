"""
debug_linkedin.py
=================
Deep diagnostic for LinkedIn Jobs — opens a VISIBLE browser,
loads the jobs search page, saves HTML, and prints all tag+class
combos so we can find the correct selectors.

Run:
    python debug_linkedin.py
"""

import asyncio
import os
import sys
sys.path.insert(0, os.path.dirname(__file__))

from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from collections import Counter

TEST_URL = "https://www.linkedin.com/jobs/search/?keywords=python+developer&location=Bangalore%2C+India&f_TPR=r2592000"
OUTPUT_HTML = "debug_linkedin.html"


async def fetch_with_browser() -> str:
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
            ],
        )
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
            viewport={"width": 1440, "height": 900},
            locale="en-IN",
            timezone_id="Asia/Kolkata",
        )
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        page = await context.new_page()

        print("Step 1: Warming up on linkedin.com homepage...")
        await page.goto("https://www.linkedin.com", wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(5)

        print(f"Step 2: Loading {TEST_URL} ...")
        await page.goto(TEST_URL, wait_until="networkidle", timeout=45000)

        print("Step 3: Waiting 8s for JS to fully render...")
        await asyncio.sleep(8)

        print("Step 4: Scrolling to trigger lazy-loaded cards...")
        for _ in range(5):
            await page.evaluate("window.scrollBy(0, 400)")
            await asyncio.sleep(1.0)

        print("Step 5: Capturing final HTML...")
        html = await page.content()

        title = await page.title()
        final_url = page.url
        print(f"  Page title : {title}")
        print(f"  Final URL  : {final_url}")
        print(f"  HTML size  : {len(html):,} bytes")

        await browser.close()
        return html


def analyse_html(html: str):
    soup = BeautifulSoup(html, "lxml")

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n✅ HTML saved → {OUTPUT_HTML}")

    print("\n" + "="*60)
    print("  TOP 40 TAG+CLASS COMBOS ON THE PAGE")
    print("="*60)
    counter = Counter()
    for tag in soup.find_all(True):
        classes = tag.get("class", [])
        if classes:
            key = f"{tag.name}.{'.'.join(classes[:2])}"
            counter[key] += 1
    for combo, count in counter.most_common(40):
        print(f"  {count:4d}x  {combo}")

    print("\n" + "="*60)
    print("  ELEMENTS WITH 'job' IN CLASS NAME")
    print("="*60)
    job_tags = soup.find_all(True, class_=lambda c: c and any("job" in cls.lower() for cls in c))
    seen = set()
    for tag in job_tags[:30]:
        classes = " ".join(tag.get("class", []))
        key = f"{tag.name}|{classes}"
        if key not in seen:
            seen.add(key)
            sample = tag.get_text(strip=True)[:60]
            print(f"  <{tag.name}> class='{classes}'")
            print(f"    text: '{sample}'")

    print("\n" + "="*60)
    print("  ELEMENTS WITH data-job-id OR data-entity-urn")
    print("="*60)
    for tag in soup.find_all(attrs={"data-job-id": True})[:10]:
        print(f"  <{tag.name}> data-job-id='{tag['data-job-id']}' class='{' '.join(tag.get('class', []))}'")
    for tag in soup.find_all(attrs={"data-entity-urn": True})[:10]:
        print(f"  <{tag.name}> data-entity-urn='{tag['data-entity-urn']}' class='{' '.join(tag.get('class', []))}'")

    print("\n" + "="*60)
    print("  ELEMENTS WITH 'card', 'result', 'listing' IN CLASS")
    print("="*60)
    for kw in ["card", "result", "listing", "scaffold", "base-card"]:
        matches = soup.find_all(True, class_=lambda c: c and any(kw in cls.lower() for cls in c))
        if matches:
            tag = matches[0]
            classes = " ".join(tag.get("class", []))
            print(f"  '{kw}' → <{tag.name}> class='{classes}'  ({len(matches)} total)")

    print("\n" + "="*60)
    print("  WHAT TO DO NEXT")
    print("="*60)
    print(f"  1. Open {OUTPUT_HTML} in your browser")
    print("  2. Right-click a job card → Inspect")
    print("  3. Paste the output above here to get selectors updated")
    print()


async def main():
    print("="*60)
    print("  LINKEDIN DEEP DEBUGGER")
    print("="*60 + "\n")
    html = await fetch_with_browser()
    analyse_html(html)


if __name__ == "__main__":
    asyncio.run(main())
