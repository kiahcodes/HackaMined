from sqlalchemy import Column, Integer, Text, TIMESTAMP
from app.database import Base

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True, index=True)
    job_title = Column(Text)
    company = Column(Text)
    city = Column(Text)
    sector = Column(Text)
    posted_date = Column(Text)
    skills = Column(Text)
    ai_mention_count = Column(Integer)
    url = Column(Text)
    scraped_at = Column(TIMESTAMP)
