from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes import router
from chatbot_service import router as chat_router
from chatbot_service import init_chat_tables

app = FastAPI(title="AI Job Risk API")

origins = [
    "http://localhost:3000",
    "http://127.0.0.1:3000",
    "http://10.21.7.11:3000",
    "http://10.21.13.44",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
app.include_router(chat_router)

@app.on_event("startup")
async def startup():               # ← must be async (init_chat_tables is async)
    await init_chat_tables()