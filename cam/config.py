"""
Central configuration loaded from environment variables.
All credentials and thresholds live here; never hardcoded elsewhere.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


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
    edgar_user_agent: str = Field(..., description="Contact email for EDGAR User-Agent header")

    # NLP
    nlp_model_dir: str = Field(default="./models")
    nlp_device: str = Field(default="cpu")

    # Risk NLP (M7)
    risk_encoder_model: str = Field(default="sentence-transformers/all-MiniLM-L6-v2")
    risk_classifier_model: str = Field(default="facebook/bart-large-mnli")
    risk_similarity_threshold: float = Field(default=0.75)
    risk_min_sentence_words: int = Field(default=8)

    # Aggregation weights (M6)
    weight_osha_rate: float = Field(default=0.25)
    weight_epa_rate: float = Field(default=0.20)
    weight_cfpb_spike: float = Field(default=0.20)
    weight_agency_overlap: float = Field(default=0.35)

    # Analysis (M6)
    aggregation_lookback_days: int = Field(
        default=365,
        description=(
            "Look-back window in days for cross-agency event aggregation. "
            "Controls how far back compute_agency_summary() and "
            "write_cross_agency_signals() scan for events."
        ),
    )

    # Ingestion defaults
    ingest_default_since_days: int = Field(
        default=30,
        description="Default look-back window in days for ingestion when --since is not specified",
    )

    # CFPB ingestion (M5)
    cfpb_page_size: int = Field(
        default=1000,
        description=(
            "Records per page when paginating the CFPB complaints API. "
            "1 000 is a safe sweet spot (~10× fewer round-trips than 100). "
            "Reduce on memory-constrained runners; max accepted by API is 10 000."
        ),
    )

    # WARN Act ingestion (M11)
    warn_http_timeout: int = Field(
        default=60, description="HTTP timeout in seconds for WARN state fetches"
    )

    # EPA ECHO ingestion (M4)
    echo_bulk_zip_url: str = Field(
        default="https://echo.epa.gov/files/echodownloads/case_downloads.zip",
        description="URL for the ECHO bulk enforcement download zip (updated daily)",
    )

    # EDGAR ingestion (M2)
    edgar_full_index_base: str = Field(
        default="https://www.sec.gov/Archives/edgar/full-index",
        description="Base URL for EDGAR quarterly full-index master.zip files",
    )
    edgar_max_index_quarters: int = Field(
        default=4,
        description=(
            "Maximum quarterly index files to scan per ingest run. "
            "Increase for backfills (each extra quarter adds one HTTP call). "
            "Default 4 covers ~1 year and bounds daily/weekly runs."
        ),
    )

    # Alert thresholds
    alert_threshold_watch: float = Field(default=0.40)
    alert_threshold_elevated: float = Field(default=0.65)
    alert_threshold_critical: float = Field(default=0.80)

    # Composite score component weights (M13)
    weight_cross_agency_composite: float = Field(default=0.35)
    weight_risk_language_expansion: float = Field(default=0.20)
    weight_earnings_divergence: float = Field(default=0.15)
    weight_proxy_escalation: float = Field(default=0.15)
    weight_merger_vertical_risk: float = Field(default=0.10)
    weight_pe_warn_flag: float = Field(default=0.05)

    # Entity resolution thresholds
    entity_fuzzy_threshold: float = Field(default=0.85)
    entity_review_threshold: float = Field(default=0.65)

    # Output
    # Optional here so workers/Celery can start without an API credential.
    # The API layer must assert this is set before accepting requests.
    api_auth_token: str | None = Field(
        default=None, description="Bearer token for API authentication"
    )
    digest_email_to: str = Field(default="alerts@example.org")
    smtp_host: str = Field(default="localhost")
    smtp_port: int = Field(default=587)


def get_settings() -> Settings:
    """Return a Settings instance. Raises ValidationError on missing required fields."""
    return Settings()  # type: ignore[call-arg]
