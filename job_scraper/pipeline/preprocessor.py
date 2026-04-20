"""
pipeline/preprocessor.py
========================
Cleans and normalises raw Job objects into a pandas DataFrame
with the 8 required columns:

    job_title | company | city | sector | posted_date | skills | ai_mention_count | url
"""

from __future__ import annotations
import re
from typing import List

import pandas as pd

from scrapers.base_scraper import Job
from utils.logger import get_logger

log = get_logger("pipeline.preprocessor")

# ── City normalisation ────────────────────────────────────────────────────────
_CITY_ALIASES: dict[str, str] = {
    "bengaluru": "Bangalore", "bangalore": "Bangalore", "bengalore": "Bangalore",
    "bombay": "Mumbai", "mumbai": "Mumbai",
    "delhi": "Delhi", "new delhi": "Delhi", "ncr": "Delhi", "delhi ncr": "Delhi",
    "hyderabad": "Hyderabad", "secunderabad": "Hyderabad",
    "chennai": "Chennai", "madras": "Chennai",
    "kolkata": "Kolkata", "calcutta": "Kolkata",
    "pune": "Pune", "ahmedabad": "Ahmedabad", "jaipur": "Jaipur",
    "lucknow": "Lucknow", "indore": "Indore", "nagpur": "Nagpur",
    "bhopal": "Bhopal", "surat": "Surat", "coimbatore": "Coimbatore",
    "kochi": "Kochi", "cochin": "Kochi", "ernakulam": "Kochi",
    "chandigarh": "Chandigarh", "patna": "Patna",
    "bhubaneswar": "Bhubaneswar", "bhubaneshwar": "Bhubaneswar",
    "guwahati": "Guwahati", "gauhati": "Guwahati",
    "remote": "Remote", "work from home": "Remote", "wfh": "Remote",
    "pan india": "Pan India", "india": "Pan India", "anywhere": "Remote",
}

# ── Title noise stripping ─────────────────────────────────────────────────────
_TITLE_NOISE = re.compile(
    r"""
    \s*[\(\[].*?[\)\]]          |
    \s*[-–—]\s*urgent\b         |
    \s*\|\s*[A-Z][a-zA-Z\s]+$  |
    \s*,\s*[A-Z][a-zA-Z\s]+$   |
    \s{2,}
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _normalise_city(raw: str) -> str:
    key = raw.lower().strip()
    key = re.sub(r"\s+", " ", key)
    if key in _CITY_ALIASES:
        return _CITY_ALIASES[key]
    key = re.sub(r",.*$", "", key).strip()
    if key in _CITY_ALIASES:
        return _CITY_ALIASES[key]
    key = key.split("/")[0].strip()
    if key in _CITY_ALIASES:
        return _CITY_ALIASES[key]
    return key.title()


def _normalise_title(raw: str) -> str:
    title = _TITLE_NOISE.sub(" ", raw).strip()
    return re.sub(r"\s+", " ", title).strip(" -–—,")


def _normalise_company(raw: str) -> str:
    company = raw.strip()
    # Remove trailing noise like "| India" or "(MNC)"
    company = re.sub(r"\s*[\|•·]\s*.*$", "", company).strip()
    return company


def _clean_skills(raw_skills: List[str]) -> List[str]:
    seen, cleaned = set(), []
    for s in raw_skills:
        n = re.sub(r"[^\w\s\.\+\#]", "", s.lower().strip())
        n = re.sub(r"\s+", " ", n).strip()
        if n and n not in seen and len(n) > 1:
            seen.add(n)
            cleaned.append(n)
    return cleaned


def _normalise_date(raw: str) -> str:
    """Normalise relative dates like '2 days ago' and ISO dates."""
    raw = raw.strip()
    if not raw:
        return ""
    # Already ISO format
    if re.match(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    # "X days/hours/weeks ago" — keep as-is, readable enough
    return raw


# ─────────────────────────────────────────────────────────────────────────────

class Preprocessor:

    def run(self, jobs: List[Job]) -> pd.DataFrame:
        FINAL_COLS = ["job_title", "company", "city", "sector",
                      "posted_date", "experience", "skills", "ai_mention_count", "url"]

        if not jobs:
            log.warning("No jobs to preprocess — returning empty DataFrame")
            return pd.DataFrame(columns=FINAL_COLS)

        log.info("Preprocessing %d raw job records …", len(jobs))

        # ── 1. Build DataFrame ─────────────────────────────────────────
        df = pd.DataFrame([{
            "source":           j.source,
            "job_title":        j.job_title,
            "company":          j.company,
            "city":             j.city,
            "sector":           j.sector,
            "posted_date":      j.posted_date,
            "experience":       j.experience,
            "skills":           j.skills,
            "skills_desc":      j.skills_desc,
            "url":              j.url,
            "full_text":        j.full_text,
            "ai_mention_count": j.ai_mention_count,
        } for j in jobs])

        original_count = len(df)

        # ── 2. Normalise titles ────────────────────────────────────────
        df["job_title"] = df["job_title"].fillna("").apply(_normalise_title)
        df = df[df["job_title"].str.len() > 2]

        # ── 3. Normalise companies ─────────────────────────────────────
        df["company"] = df["company"].fillna("").apply(_normalise_company)

        # ── 4. Normalise cities ────────────────────────────────────────
        df["city"] = df["city"].fillna("").apply(_normalise_city)
        df["city"] = df["city"].replace("", "Unknown")

        # ── 5. Sector — infer from title if not set by scraper ────────
        from scrapers.base_scraper import infer_sector as _infer
        df["sector"] = df.apply(
            lambda r: r["sector"] if r["sector"] else _infer(r["job_title"]), axis=1
        )
        df["sector"] = df["sector"].fillna("Other").replace("", "Other")

        # ── 6. Normalise dates ─────────────────────────────────────────
        df["posted_date"] = df["posted_date"].fillna("").apply(_normalise_date)

        # ── 7. Normalise experience ───────────────────────────────────
        df["experience"] = df["experience"].fillna("").str.strip()

        # ── 8. Normalise skills → pipe-separated string ────────────────
        df["skills"] = df["skills"].apply(_clean_skills)
        df["skills"] = df["skills"].apply(lambda lst: " | ".join(lst) if lst else "")

        # ── 9. Deduplicate ─────────────────────────────────────────────
        df["_title_key"] = df["job_title"].str.lower().str.strip()
        df["_city_key"]  = df["city"].str.lower().str.strip()
        df = df.drop_duplicates(subset=["_title_key", "_city_key", "source"])
        df["_skill_count"] = df["skills"].str.count(r"\|") + 1
        df = df.sort_values(["_skill_count", "ai_mention_count"], ascending=[False, False])
        df = df.drop_duplicates(subset=["_title_key", "_city_key"], keep="first")
        df = df.drop(columns=["_skill_count", "_title_key", "_city_key"])

        # ── 10. Final columns ───────────────────────────────────────────
        df = df[FINAL_COLS].copy().reset_index(drop=True)

        log.info(
            "Preprocessing complete — %d → %d records (dropped %d)",
            original_count, len(df), original_count - len(df),
        )
        return df
