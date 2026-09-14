from __future__ import annotations

import logging

from fastapi import FastAPI

from core.config import Settings
from core.lifespan import lifespan
from routers.documents import router as documents_router
from routers.health import router as health_router
from routers.users import router as users_router

logger = logging.getLogger(__name__)
logging.basicConfig(level=getattr(logging, Settings.LOG_LEVEL.upper(), logging.INFO))


app = FastAPI(title="Document Insights API", version="0.1.0", lifespan=lifespan)
app.include_router(documents_router)
app.include_router(users_router)
app.include_router(health_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host=Settings.HOST,
        port=Settings.PORT,
        reload=Settings.APP_ENV == "development",
        workers=1,
        loop="uvloop",
        access_log=True,
    )
