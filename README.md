# AI Job Risk & Reskilling Platform 🚀

This project analyzes job market trends, evaluates AI automation risk, and provides reskilling pathways for workers.

It consists of:

1. 🔎 Job Scraper (Data Collection)
2. ⚡ FastAPI Backend (Risk & Analysis APIs)

---

## 📌 Features

- 📊 Hiring Trends by City & Sector
- 🧠 AI Vulnerability Index (0–100)
- 🛠 Skills Demand & Gap Analysis
- 📈 Worker Risk Assessment API
- 🔄 Reskilling Suggestions

---

# 🗂 Project Structure

.
├── job_scraper/
├── backend/
├── .gitignore
└── README.md

---

# 🔎 1️⃣ Job Scraper

Scrapes job listings and extracts:

- Job Title
- City
- Sector
- Skills
- AI Mentions

### ⚙️ Setup

```bash
cd job_scraper
python -m venv venv
source venv/bin/activate  # Linux
pip install -r requirements.txt
▶️ Run
python scraper.py
⚡ 2️⃣ FastAPI Backend

Provides APIs for:

Hiring Trends

AI Vulnerability Index

Skill Gap Analysis

Worker Risk Analysis

⚙️ Setup
cd backend
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
▶️ Run Server
uvicorn main:app --reload

API Docs:

http://127.0.0.1:8000/docs
📡 Example APIs
Hiring Trends
GET /api/hiring-trends
AI Vulnerability Index
GET /api/ai-risk
Worker Analysis
POST /api/analyze-worker
🧠 Tech Stack

Python

FastAPI

SQLAlchemy

SQLite / PostgreSQL

Uvicorn

Requests / BeautifulSoup (Scraper)

🔐 Environment Variables

Create a .env file inside backend:

DATABASE_URL=sqlite:///./jobs.db
🚀 Future Improvements

ML-based AI risk prediction

Real-time job scraping

Dashboard frontend

Cloud deployment (AWS / Azure)
```
