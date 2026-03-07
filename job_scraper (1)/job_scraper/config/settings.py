"""
config/settings.py
==================
All tunable constants for the job scraper pipeline.
Change values here — never hardcode in scraper files.
"""

from __future__ import annotations
import os

# ─── Concurrency ─────────────────────────────────────────────────────────────
# Lower = more polite; raise if you have residential proxies
LINKEDIN_CONCURRENCY   = 3
NAUKRI_CONCURRENCY     = 5
INTERNSHALA_CONCURRENCY = 8

# ─── Pagination ───────────────────────────────────────────────────────────────
MAX_PAGES_PER_KEYWORD  = 10   # pages to scrape per (keyword, city) pair
RESULTS_PER_PAGE       = 25   # site-default; used to build offset URLs

# ─── Retry / Back-off ─────────────────────────────────────────────────────────
MAX_RETRIES            = 3
RETRY_MIN_WAIT_SECS    = 2.0
RETRY_MAX_WAIT_SECS    = 15.0

# ─── Request Timing ───────────────────────────────────────────────────────────
REQUEST_TIMEOUT_SECS   = 20
INTER_REQUEST_DELAY    = (1.0, 3.5)   # random uniform range (seconds)

# ─── Proxy (optional) ─────────────────────────────────────────────────────────
PROXY_FILE             = os.path.join(os.path.dirname(__file__), "proxies.txt")
USE_PROXIES            = False   # flip to True and populate proxies.txt

# ─── PostgreSQL ───────────────────────────────────────────────────────────────
DB_HOST     = os.getenv("DB_HOST",     "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "job_market")
DB_USER     = os.getenv("DB_USER",     "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "Golu@1234")
DB_TABLE    = os.getenv("DB_TABLE",    "jobs")

# ─── Search Configuration ─────────────────────────────────────────────────────
# Keywords to search across all three platforms
SEARCH_KEYWORDS = [
    "software engineer",
    "data analyst",
    "data scientist",
    "machine learning engineer",
    "backend developer",
    "frontend developer",
    "full stack developer",
    "devops engineer",
    "product manager",
    "business analyst",
    "cloud engineer",
    "BPO",
    "customer support",
    "digital marketing",
    "content writer",
]

# Cities — mix of Tier 1, 2, 3 (as required by hackathon rubric)
TARGET_CITIES = [
    "Bangalore", "Mumbai", "Delhi", "Hyderabad", "Chennai",
    "Pune", "Kolkata", "Ahmedabad", "Jaipur", "Lucknow",
    "Indore", "Nagpur", "Bhopal", "Surat", "Coimbatore",
    "Kochi", "Chandigarh", "Patna", "Bhubaneswar", "Guwahati",
]

# ─── AI Keyword List ─────────────────────────────────────────────────────────
# Used to count ai_mentions in job descriptions
AI_KEYWORDS = [
    r"\bai\b",
    r"\bartificial intelligence\b",
    r"\bmachine learning\b",
    r"\bdeep learning\b",
    r"\bllm\b",
    r"\blarge language model\b",
    r"\bgenai\b",
    r"\bgenerative ai\b",
    r"\bchatgpt\b",
    r"\bgpt[\-\s]?\d*\b",
    r"\bgemini\b",
    r"\bclaude\b",
    r"\bllama\b",
    r"\bnlp\b",
    r"\bnatural language processing\b",
    r"\bcomputer vision\b",
    r"\bneural network\b",
    r"\btransformer\b",
    r"\bdiffusion model\b",
    r"\bprompt engineering\b",
    r"\brag\b",
    r"\bretrieval augmented\b",
    r"\bvector database\b",
    r"\bembedding\b",
    r"\bfine.?tun\b",
    r"\bautomation\b",
    r"\bai.?powered\b",
    r"\bai.?driven\b",
    r"\bintelligent automation\b",
    r"\bpredictive analytics\b",
    r"\bai tools\b",
    r"\bcopilot\b",
    r"\bhugging face\b",
    r"\blangchain\b",
    r"\bopenai\b",
    r"\banthropics?\b",
    r"\bmlops\b",
    r"\bai agent\b",
    r"\bagentic\b",
    r"\bmultimodal\b",
]
