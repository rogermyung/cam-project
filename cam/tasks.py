"""
Celery task definitions. Each ingestion/analysis module registers tasks here.
"""

import os

from celery import Celery


def make_celery() -> Celery:
    # Read the broker URL directly from the environment with a safe default.
    # Avoid calling get_settings() here: it requires DATABASE_URL and
    # EDGAR_USER_AGENT, which blocks worker/beat from starting if those vars
    # are not yet set.  Individual tasks call get_settings() themselves when
    # they actually need full configuration.
    redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    app = Celery("cam", broker=redis_url, backend=redis_url)
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
