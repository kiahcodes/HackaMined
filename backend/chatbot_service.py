"""
Skills Mirage — AI Chatbot Service (Layer 2)
"""

import json
import httpx
import asyncpg
from typing import AsyncGenerator, Optional
from fastapi import HTTPException, APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from datetime import datetime

router = APIRouter(prefix="/chat", tags=["chatbot"])

OLLAMA_URL = "http://localhost:11434"
OLLAMA_MODEL = "qwen3:8b"
DB_URL = "postgresql://postgres:Golu%401234@localhost:5432/job_market"

db_pool: Optional[asyncpg.Pool] = None


async def init_chat_tables():
    """No-arg version — called from main.py startup."""
    global db_pool
    try:
        db_pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)
        async with db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS chat_sessions (
                    session_id TEXT PRIMARY KEY,
                    worker_profile JSONB,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id SERIAL PRIMARY KEY,
                    session_id TEXT REFERENCES chat_sessions(session_id) ON DELETE CASCADE,
                    role TEXT NOT NULL,
                    content TEXT NOT NULL,
                    language TEXT DEFAULT 'en',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
                CREATE INDEX IF NOT EXISTS idx_chat_messages_session
                    ON chat_messages(session_id, created_at);
                CREATE TABLE IF NOT EXISTS knowledge_chunks (
                    id SERIAL PRIMARY KEY,
                    source TEXT,
                    category TEXT,
                    content TEXT NOT NULL,
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)
        print("✅ Chat tables ready")
    except Exception as e:
        print(f"⚠️  DB init failed: {e}. Chatbot runs without persistence.")
        db_pool = None


class WorkerProfile(BaseModel):
    job_title: str
    city: str
    years_experience: int
    write_up: str
    risk_score: Optional[float] = None
    reskilling_path: Optional[list] = None


class ChatRequest(BaseModel):
    session_id: str
    message: str
    worker_profile: WorkerProfile
    language: str = "en"
    stream: bool = True


class SessionCreateRequest(BaseModel):
    session_id: str
    worker_profile: WorkerProfile


def _mock_rag_context(profile: WorkerProfile) -> str:
    return f"""
📍 Hiring trends in {profile.city} (last 30 days):
  • BPO/Voice: -18% decline. AI call-handling replacing manual roles.
  • Data Analytics: +34% rise. SQL, Python, Power BI highly sought.
  • Digital Marketing: +22% rise. SEO, Meta Ads in demand.

📈 Skills demand for '{profile.job_title}':
  • Python (Data): demand 88/100, rising ↑, avg ₹6.5L/yr
  • SQL: demand 82/100, stable →, avg ₹5.8L/yr
  • Excel/Sheets: demand 70/100, stable →, avg ₹4.2L/yr

⚠️ AI Vulnerability: {profile.risk_score or 72}/100 (High)
  Safe pivots: Junior Data Analyst, Operations Coordinator, Customer Success (SaaS).

🎓 Free resources:
  • [SWAYAM] Python for Data Science — 12 weeks — swayam.gov.in/course/python-ds
  • [NPTEL] Data Analytics with Excel & SQL — 8 weeks — nptel.ac.in/data-analytics
""".strip()


async def retrieve_rag_context(worker_profile: WorkerProfile, user_message: str, pool) -> str:
    if not pool:
        return _mock_rag_context(worker_profile)
    try:
        async with pool.acquire() as conn:
            city_trends = await conn.fetch("""
                SELECT sector, job_count, week_change_pct, top_skills
                FROM hiring_trends WHERE city ILIKE $1
                ORDER BY week_change_pct DESC LIMIT 5
            """, worker_profile.city)
            if city_trends:
                parts = [f"📍 Hiring trends in {worker_profile.city}:"]
                for row in city_trends:
                    parts.append(f"  • {row['sector']}: {row['job_count']} jobs, {row['week_change_pct']:+.1f}% wow. Skills: {row['top_skills']}")
                return "\n".join(parts)
    except Exception as e:
        print(f"RAG error: {e}")
    return _mock_rag_context(worker_profile)


async def get_chat_history(session_id: str, pool, limit: int = 10) -> list[dict]:
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT role, content FROM chat_messages
                WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2
            """, session_id, limit)
            return [{"role": r["role"], "content": r["content"]} for r in reversed(rows)]
    except Exception:
        return []


async def save_message(session_id: str, role: str, content: str, language: str, pool):
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO chat_messages (session_id, role, content, language)
                VALUES ($1, $2, $3, $4)
            """, session_id, role, content, language)
            await conn.execute("UPDATE chat_sessions SET updated_at = NOW() WHERE session_id = $1", session_id)
    except Exception as e:
        print(f"Save error: {e}")


def build_system_prompt(worker_profile: WorkerProfile, rag_context: str, language: str) -> str:
    lang_instruction = (
        "Respond ONLY in Hindi (Devanagari script). Use simple, clear Hindi."
        if language == "hi"
        else "Respond in clear, simple English."
    )
    risk_label = "Low 🟢"
    if worker_profile.risk_score:
        if worker_profile.risk_score >= 70: risk_label = "High 🔴"
        elif worker_profile.risk_score >= 40: risk_label = "Medium 🟡"

    return f"""You are Mirage Assistant — an AI career counsellor for Indian workers facing job displacement.

{lang_instruction}

## Worker Profile
- Role: {worker_profile.job_title} | City: {worker_profile.city} | Experience: {worker_profile.years_experience} yrs
- AI Risk Score: {worker_profile.risk_score or "N/A"}/100 ({risk_label})
- About them: {worker_profile.write_up}

## Live Market Data
{rag_context}

## Your job
Answer these 5 question types using the worker's profile and the data above:
1. Risk Explanation — why their score is what it is
2. Reskilling Advice — what to learn, free resources (SWAYAM, NPTEL)
3. Job Market Intel — trends in their city/sector
4. Career Pivot Guidance — roles to target, how to position experience
5. Motivation & Clarity — honest, empathetic guidance

Rules: Never give generic advice. Use only data above for statistics. Be concise (3-5 paragraphs).
"""


def detect_language(text: str) -> str:
    return "hi" if sum(1 for c in text if '\u0900' <= c <= '\u097F') > 2 else "en"


async def stream_ollama(messages: list[dict], system_prompt: str) -> AsyncGenerator[str, None]:
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
        "stream": True,
        "options": {"temperature": 0.7, "top_p": 0.9, "num_predict": 1024}
    }
    async with httpx.AsyncClient(timeout=120) as client:
        async with client.stream("POST", f"{OLLAMA_URL}/api/chat", json=payload) as resp:
            if resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Ollama model unavailable")
            async for line in resp.aiter_lines():
                if not line.strip():
                    continue
                try:
                    chunk = json.loads(line)
                    token = chunk.get("message", {}).get("content", "")
                    if token:
                        yield token
                    if chunk.get("done"):
                        break
                except json.JSONDecodeError:
                    continue


# ── Routes ────────────────────────────────────────────────────────────────────
# Router prefix is /chat, so these become /chat/session, /chat/message etc.

@router.post("/session")
async def create_session(req: SessionCreateRequest):
    pool = db_pool
    if pool:
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO chat_sessions (session_id, worker_profile)
                    VALUES ($1, $2)
                    ON CONFLICT (session_id) DO UPDATE
                    SET worker_profile = $2, updated_at = NOW()
                """, req.session_id, json.dumps(req.worker_profile.dict()))
        except Exception as e:
            print(f"Session error: {e}")
    return {"session_id": req.session_id, "status": "ready"}


@router.post("/message")
async def chat_message(req: ChatRequest):
    pool = db_pool
    language = detect_language(req.message) if detect_language(req.message) == "hi" else req.language
    rag_context = await retrieve_rag_context(req.worker_profile, req.message, pool)
    history = await get_chat_history(req.session_id, pool)
    messages = history + [{"role": "user", "content": req.message}]
    system_prompt = build_system_prompt(req.worker_profile, rag_context, language)
    await save_message(req.session_id, "user", req.message, language, pool)

    if req.stream:
        async def generate():
            full_response = ""
            try:
                async for token in stream_ollama(messages, system_prompt):
                    full_response += token
                    yield f"data: {json.dumps({'token': token, 'done': False})}\n\n"
            except Exception as e:
                yield f"data: {json.dumps({'error': str(e), 'done': True})}\n\n"
                return
            await save_message(req.session_id, "assistant", full_response, language, pool)
            yield f"data: {json.dumps({'token': '', 'done': True})}\n\n"
        return StreamingResponse(generate(), media_type="text/event-stream")
    else:
        full = ""
        async for token in stream_ollama(messages, system_prompt):
            full += token
        await save_message(req.session_id, "assistant", full, language, pool)
        return {"response": full, "language": language}


@router.get("/history/{session_id}")
async def get_history(session_id: str):
    if not db_pool:
        return {"messages": [], "note": "DB not connected"}
    history = await get_chat_history(session_id, db_pool, limit=50)
    return {"session_id": session_id, "messages": history}


@router.delete("/session/{session_id}")
async def clear_session(session_id: str):
    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM chat_sessions WHERE session_id = $1", session_id)
    return {"status": "cleared"}


@router.get("/health")
async def health():
    ollama_ok = False
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            r = await client.get(f"{OLLAMA_URL}/api/tags")
            models = [m["name"] for m in r.json().get("models", [])]
            ollama_ok = any(OLLAMA_MODEL in m for m in models)
    except Exception:
        pass
    return {
        "status": "ok" if (ollama_ok and db_pool is not None) else "degraded",
        "ollama": ollama_ok,
        "model": OLLAMA_MODEL,
        "db": db_pool is not None,
        "timestamp": datetime.utcnow().isoformat()
    }