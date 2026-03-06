"""
pipeline/aggregator.py
======================
Merges DataFrames from all sources and upserts into PostgreSQL.

Strategy: ON CONFLICT (url) DO NOTHING
- url is the unique key — same job posted twice = skip, not duplicate
- If url is empty (LinkedIn sometimes), falls back to (job_title, company, city)

Table is auto-created on first run if it doesn't exist.
"""

from __future__ import annotations
from typing import List

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from config.settings import (
    DB_HOST, DB_PORT, DB_NAME,
    DB_USER, DB_PASSWORD, DB_TABLE,
)
from utils.logger import get_logger

log = get_logger("pipeline.aggregator")

FINAL_COLS = ["job_title", "company", "city", "sector",
              "posted_date", "experience", "skills", "ai_mention_count", "url"]

# ── SQL ───────────────────────────────────────────────────────────────────────

_CREATE_TABLE = f"""
CREATE TABLE IF NOT EXISTS {DB_TABLE} (
    id               SERIAL PRIMARY KEY,
    job_title        TEXT        NOT NULL,
    company          TEXT,
    city             TEXT,
    sector           TEXT,
    posted_date      TEXT,
    experience       TEXT,
    skills           TEXT,
    ai_mention_count INTEGER     DEFAULT 0,
    url              TEXT,
    scraped_at       TIMESTAMPTZ DEFAULT NOW(),

    -- Unique constraint: same job title + company + city = duplicate
    CONSTRAINT uq_job UNIQUE (job_title, company, city)
);
"""

# Index for fast queries by city and sector (used by Layer 1 dashboard)
_CREATE_INDEXES = f"""
CREATE INDEX IF NOT EXISTS idx_jobs_city   ON {DB_TABLE} (city);
CREATE INDEX IF NOT EXISTS idx_jobs_sector ON {DB_TABLE} (sector);
CREATE INDEX IF NOT EXISTS idx_jobs_ai     ON {DB_TABLE} (ai_mention_count DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_url    ON {DB_TABLE} (url);
"""

_UPSERT = f"""
INSERT INTO {DB_TABLE}
    (job_title, company, city, sector, posted_date, experience,
     skills, ai_mention_count, url)
VALUES %s
ON CONFLICT (job_title, company, city) DO NOTHING;
"""


class Aggregator:

    def _connect(self):
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )

    def _ensure_table(self, cur) -> None:
        cur.execute(_CREATE_TABLE)
        cur.execute(_CREATE_INDEXES)

    def run(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        if not dataframes:
            raise ValueError("No DataFrames provided to aggregator")

        non_empty = [df for df in dataframes if not df.empty]
        if not non_empty:
            log.warning("All source DataFrames are empty — nothing to insert")
            return pd.DataFrame(columns=FINAL_COLS)

        combined = pd.concat(non_empty, ignore_index=True)
        log.info("Combined %d total records from %d sources", len(combined), len(non_empty))

        # ── In-memory dedup before DB insert ──────────────────────────────
        # Keep richest record per (job_title, company, city)
        combined["_skill_count"] = combined["skills"].str.count(r"\|") + 1
        combined = combined.sort_values(
            ["_skill_count", "ai_mention_count"], ascending=[False, False]
        )
        combined = combined.drop_duplicates(
            subset=["job_title", "company", "city"], keep="first"
        )
        combined = combined.drop(columns=["_skill_count"])
        combined = combined.sort_values(
            ["city", "sector", "ai_mention_count"], ascending=[True, True, False]
        ).reset_index(drop=True)

        log.info("After in-memory dedup: %d records to upsert", len(combined))

        # ── Upsert into PostgreSQL ─────────────────────────────────────────
        conn = None
        inserted = 0
        try:
            conn = self._connect()
            cur  = conn.cursor()

            self._ensure_table(cur)
            conn.commit()

            # Build list of tuples for batch insert
            rows = [
                (
                    row["job_title"]        or "",
                    row["company"]          or "",
                    row["city"]             or "",
                    row["sector"]           or "",
                    row["posted_date"]      or "",
                    row["experience"]       or "",
                    row["skills"]           or "",
                    int(row["ai_mention_count"] or 0),
                    row["url"]              or "",
                )
                for _, row in combined.iterrows()
            ]

            # execute_values is ~10x faster than executemany
            execute_values(cur, _UPSERT, rows, page_size=500)
            inserted = cur.rowcount
            conn.commit()
            cur.close()

            log.info("✅  Upserted %d new records into '%s' (duplicates skipped)",
                     inserted, DB_TABLE)

        except psycopg2.OperationalError as e:
            log.error("❌  Could not connect to PostgreSQL: %s", e)
            log.error("    Check DB_HOST/DB_PORT/DB_NAME/DB_USER/DB_PASSWORD in config/settings.py")
            raise
        except Exception as e:
            if conn:
                conn.rollback()
            log.error("❌  Database error: %s", e)
            raise
        finally:
            if conn:
                conn.close()

        return combined
