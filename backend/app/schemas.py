from pydantic import BaseModel

class WorkerAnalysis(BaseModel):
    title: str
    city: str
    experience: int
    tasks: str

class PredictWorker(BaseModel):
    name: str
    age: int
    gender: str
    department: str
    experience: int

# class CourseRecommendationRequest(BaseModel):
#     title: str
#     city: str
#     skills: str