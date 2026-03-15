FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Default entrypoint: run the CAM pipeline CLI.
#
# Each step is a separate sub-command so they can be scheduled independently:
#
#   docker run cam ingest --source all --since 2025-01-01
#   docker run cam score  --date today
#   docker run cam export --output-dir /out --digest
#
# Override ENTRYPOINT to run the Celery worker instead:
#   docker run --entrypoint celery cam -A cam.tasks:celery_app worker
ENTRYPOINT ["python", "-m", "cam.entrypoint"]
CMD ["--help"]
