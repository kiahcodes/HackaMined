"""
test_pipeline.py — validates all 8 columns with mock data
"""
import sys, os, unittest
sys.path.insert(0, os.path.dirname(__file__))

from scrapers.base_scraper import Job
from pipeline.preprocessor import Preprocessor
from pipeline.aggregator import Aggregator

MOCK_JOBS = [
    Job(source="linkedin", job_title="Machine Learning Engineer",
        company="Google India", city="Bengaluru",
        posted_date="2026-03-04", skills_desc="LLM fine-tuning, RAG, GenAI tools",
        url="https://linkedin.com/jobs/view/123",
        full_text="LLM RAG GenAI ChatGPT AI-powered machine learning deep learning"),
    Job(source="naukri", job_title="Machine Learning Engineer",
        company="Google India", city="Bangalore",
        posted_date="2 days ago", skills=["Python", "TensorFlow", "AWS"],
        skills_desc="LLM, OpenAI, LangChain experience required",
        url="https://naukri.com/job/456",
        full_text="LLM OpenAI LangChain AI agents neural network"),
    Job(source="naukri", job_title="Data Analyst (2-5 yrs) | Remote",
        company="Infosys | India", city="Mumbai",
        posted_date="1 week ago", skills=["SQL", "Power BI", "Excel"],
        skills_desc="Strong SQL and data visualisation skills",
        url="https://naukri.com/job/789",
        full_text="SQL Power BI Excel data analytics no AI required"),
    Job(source="naukri", job_title="BPO Voice Executive",
        company="Concentrix", city="Pune",
        posted_date="3 days ago", skills=["Communication"],
        skills_desc="Handle inbound customer calls",
        url="https://naukri.com/job/101",
        full_text="customer calls BPO voice process"),
    Job(source="linkedin", job_title="Backend Developer - Urgent",
        company="Flipkart", city="Delhi NCR",
        posted_date="2026-03-01", skills_desc="Java Spring Boot microservices",
        url="https://linkedin.com/jobs/view/202",
        full_text="Java Spring Boot Docker Kubernetes AI automation tools"),
    Job(source="naukri", job_title="  ", company="Unknown",
        city="Chennai", full_text=""),  # garbage — should be dropped
]

for job in MOCK_JOBS:
    job.compute_ai_mentions()


class TestPreprocessor(unittest.TestCase):

    def _run(self):
        return Preprocessor().run(MOCK_JOBS)

    def test_columns_correct(self):
        df = self._run()
        self.assertEqual(list(df.columns),
            ["job_title", "company", "city", "sector",
             "posted_date", "experience", "skills", "ai_mention_count", "url"])

    def test_empty_titles_dropped(self):
        df = self._run()
        self.assertFalse(any(df["job_title"].str.strip() == ""))

    def test_city_normalised(self):
        df = self._run()
        cities = df["city"].tolist()
        self.assertIn("Bangalore", cities)
        self.assertIn("Delhi", cities)

    def test_company_noise_stripped(self):
        df = self._run()
        companies = df["company"].tolist()
        self.assertNotIn("Infosys | India", companies)

    def test_sector_inferred(self):
        df = self._run()
        ml_row = df[df["job_title"].str.contains("Machine Learning", na=False)]
        self.assertFalse(ml_row.empty)
        self.assertEqual(ml_row.iloc[0]["sector"], "AI / ML")

    def test_ai_mention_count_positive(self):
        df = self._run()
        ml_row = df[df["job_title"].str.contains("Machine Learning", na=False)]
        self.assertGreater(ml_row.iloc[0]["ai_mention_count"], 0)

    def test_url_present(self):
        df = self._run()
        non_empty_urls = df[df["url"] != ""]["url"]
        self.assertGreater(len(non_empty_urls), 0)

    def test_no_duplicate_title_city(self):
        df = self._run()
        dupes = df.duplicated(subset=["job_title", "city"])
        self.assertFalse(dupes.any())

    def test_title_noise_stripped(self):
        df = self._run()
        titles = df["job_title"].tolist()
        self.assertNotIn("Data Analyst (2-5 yrs) | Remote", titles)

    def test_skills_pipe_separated(self):
        df = self._run()
        skills_with_data = df[df["skills"] != ""]["skills"]
        if not skills_with_data.empty:
            sample = skills_with_data.iloc[0]
            self.assertIsInstance(sample, str)


class TestAggregator(unittest.TestCase):

    def _run(self):
        df = Preprocessor().run(MOCK_JOBS)
        return Aggregator().run([df])

    def test_csv_written(self):
        self._run()
        self.assertTrue(os.path.exists(os.path.join("output", "jobs_combined.csv")))

    def test_final_columns(self):
        df = self._run()
        self.assertEqual(list(df.columns),
            ["job_title", "company", "city", "sector",
             "posted_date", "experience", "skills", "ai_mention_count", "url"])

    def test_csv_readable(self):
        import pandas as pd
        self._run()
        loaded = pd.read_csv(os.path.join("output", "jobs_combined.csv"))
        self.assertGreater(len(loaded), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
    # Print sample
    df = Preprocessor().run(MOCK_JOBS)
    final = Aggregator().run([df])
    print("\n── Sample output ──")
    print(final.to_string(index=False))
