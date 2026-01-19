from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from rag_agent.core.config import settings
from rag_agent.api.rag import router

app = FastAPI(title=settings.APP_NAME)

app.include_router(router)

