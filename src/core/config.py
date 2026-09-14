from __future__ import annotations

from decouple import config


class Settings:
    HOST: str = config("HOST", default="0.0.0.0")
    PORT: int = config("PORT", default=8000, cast=int)

    MONGODB_URI: str = config("MONGODB_URI", default="mongodb://mongo:27017")
    MONGODB_DB: str = config("MONGODB_DB", default="document_insights")
    DOCUMENTS_COLLECTION: str = config("DOCUMENTS_COLLECTION", default="documents")

    REDIS_URL: str = config("REDIS_URL", default="redis://redis:6379/0")
    CACHE_TTL_SECONDS: int = config("CACHE_TTL_SECONDS", default=3600, cast=int)

    MAX_ACTIVE_PER_USER: int = config("MAX_ACTIVE_PER_USER", default=3, cast=int)

    SUBMIT_LOCK_TTL_MS: int = config("SUBMIT_LOCK_TTL_MS", default=10000, cast=int)

    JOB_MIN_SECONDS: int = config("JOB_MIN_SECONDS", default=10, cast=int)
    JOB_MAX_SECONDS: int = config("JOB_MAX_SECONDS", default=30, cast=int)
    JOB_FAILURE_RATE: float = config("JOB_FAILURE_RATE", default=0.1, cast=float)
    JOB_PERMANENT_FAILURE_ON_RETRY: bool = config(
        "JOB_PERMANENT_FAILURE_ON_RETRY", default=False, cast=bool
    )

    CELERY_BROKER_URL: str = config(
        "CELERY_BROKER_URL", default=config("REDIS_URL", default="redis://redis:6379/0")
    )
    CELERY_RESULT_BACKEND: str = config(
        "CELERY_RESULT_BACKEND",
        default=config("REDIS_URL", default="redis://redis:6379/0"),
    )
    WORKER_CONCURRENCY: int = config("WORKER_CONCURRENCY", default=4, cast=int)
    WORKER_MAX_TASKS_PER_CHILD: int = config(
        "WORKER_MAX_TASKS_PER_CHILD", default=200, cast=int
    )
    TASK_MAX_RETRIES: int = config("TASK_MAX_RETRIES", default=3, cast=int)
    LOG_LEVEL: str = config("LOG_LEVEL", default="INFO")
    APP_ENV: str = config("APP_ENV", default="development")
