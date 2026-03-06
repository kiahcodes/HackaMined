from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.services import (
    calculate_city_risk,
    get_high_risk_jobs,
    analyze_worker,
    job_impact,
    course_recommendations
)
from app.schemas import WorkerAnalysis, PredictWorker

router = APIRouter(prefix="/api")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/city-risk")
def city_risk(db: Session = Depends(get_db)):
    return calculate_city_risk(db)

@router.get("/high-risk-jobs")
def high_risk_jobs(db: Session = Depends(get_db)):
    return get_high_risk_jobs(db)

@router.post("/analyze-worker")
def analyze(data: WorkerAnalysis):
    return analyze_worker(data)

@router.post("/predict")
def predict_worker(data: PredictWorker):
    return {
        "risk_score": 0.72,
        "message": "Prediction generated successfully"
    }

@router.post("/course-recommendations")
def courses():
    return course_recommendations()

@router.get("/job-impact")
def impact(ai_level: int):
    return job_impact(ai_level)
