from sqlalchemy.orm import Session
from sqlalchemy import func
from app.models import Job
import random

def calculate_city_risk(db: Session):
    results = (
        db.query(Job.city, func.avg(Job.ai_mention_count))
        .group_by(Job.city)
        .all()
    )

    city_risk = {}
    for city, avg_ai in results:
        risk = min(int(avg_ai * 12), 100)
        city_risk[city] = risk

    return city_risk

def get_high_risk_jobs(db: Session):
    jobs = (
        db.query(Job)
        .order_by(Job.ai_mention_count.desc())
        .limit(10)
        .all()
    )

    return [
        {
            "role": job.job_title,
            "city": job.city,
            "risk": min(job.ai_mention_count * 10, 100)
        }
        for job in jobs
    ]

def analyze_worker(data):
    base_risk = random.randint(30, 70)

    if "bpo" in data.title.lower():
        base_risk += 15
    if "ai" in data.tasks.lower():
        base_risk -= 10

    return {
        "risk_score": min(base_risk, 100),
        "message": "Analysis complete"
    }

def job_impact(ai_level: int):
    return {
        "bpoRemaining": max(100 - ai_level, 0),
        "aiGrowth": ai_level * 2 + 10,
        "userRisk": ai_level // 2
    }

def course_recommendations():
    return [
        {
            "name": "AI for Customer Experience",
            "provider": "Coursera",
            "duration": "4 weeks",
            "skill": "AI Tools",
            "url": "https://coursera.org/"
        }
    ]
