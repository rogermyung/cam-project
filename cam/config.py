"""
Central configuration loaded from environment variables.
All credentials and thresholds live here; never hardcoded elsewhere.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Database
    database_url: str = Field(..., description="PostgreSQL connection string")

    # Object store
    s3_bucket: str = Field(default="cam-documents")
    s3_endpoint: str = Field(default="http://localhost:9000")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")

    # Redis / Celery
    redis_url: str = Field(default="redis://localhost:6379/0")

    # SEC EDGAR
    edgar_user_agent: str = Field(
        ..., description="Contact email for EDGAR User-Agent header"
    )

    # NLP
    nlp_model_dir: str = Field(default="./models")
    nlp_device: str = Field(default="cpu")

    # Alert thresholds
    alert_threshold_watch: float = Field(default=0.40)
    alert_threshold_elevated: float = Field(default=0.65)
    alert_threshold_critical: float = Field(default=0.80)

    # Entity resolution thresholds
    entity_fuzzy_threshold: float = Field(default=0.85)
    entity_review_threshold: float = Field(default=0.65)

    # Output
    api_auth_token: str = Field(default="change-me-in-production")
    digest_email_to: str = Field(default="alerts@example.org")
    smtp_host: str = Field(default="localhost")
    smtp_port: int = Field(default=587)


def get_settings() -> Settings:
    """Return a Settings instance. Raises ValidationError on missing required fields."""
    return Settings()  # type: ignore[call-arg]
