from __future__ import annotations

from celery import Celery

from core.config import Settings

app = Celery("document_insights")

app.config_from_object(
    {
        "broker_url": Settings.CELERY_BROKER_URL,
        "result_backend": Settings.CELERY_RESULT_BACKEND,
        "broker_connection_retry_on_startup": True,
        "task_default_queue": "documents",
        "worker_prefetch_multiplier": 1,
        "task_acks_late": True,
        "task_track_started": True,
        "worker_max_tasks_per_child": Settings.WORKER_MAX_TASKS_PER_CHILD,
        "imports": ("workers.tasks",),
        "worker_concurrency": Settings.WORKER_CONCURRENCY,
    }
)

app.conf.timezone = "UTC"  # type: ignore[attr-defined]


def get_celery_app() -> Celery:
    """Return the configured Celery app instance."""
    return app


def check_celery_broker() -> str:
    """Ping the broker and workers to report Celery health."""
    with app.connection() as conn:
        conn.ensure_connection(max_retries=0, timeout=2)
    try:
        reply = app.control.inspect(timeout=1).ping()
        if reply:
            return f"ok ({len(reply)} worker(s))"
        return "ok - broker reachable, no workers"
    except Exception:  # noqa: BLE001  # inspect ping can raise various kombu/celery errors
        return "ok - broker reachable"
