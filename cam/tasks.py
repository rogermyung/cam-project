"""
Celery task definitions. Each ingestion/analysis module registers tasks here.
"""

from celery import Celery

from cam.config import get_settings


def make_celery() -> Celery:
    settings = get_settings()
    app = Celery(
        "cam",
        broker=settings.redis_url,
        backend=settings.redis_url,
    )
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        worker_prefetch_multiplier=1,
    )
    return app


celery_app = make_celery()


# ---------------------------------------------------------------------------
# Scheduled beat schedule (populated by individual modules)
# ---------------------------------------------------------------------------

celery_app.conf.beat_schedule = {
    # Placeholder — individual modules will add their own tasks
    # "ingest-edgar-daily": {
    #     "task": "cam.ingestion.edgar.ingest_all_10k",
    #     "schedule": crontab(hour=2, minute=0),
    # },
}
