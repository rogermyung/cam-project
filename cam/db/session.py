"""
Database session factory.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cam.config import get_settings


def get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


def get_session_factory():
    engine = get_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)


def get_db() -> Session:
    """Yield a database session. Use as a context manager or FastAPI dependency."""
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.close()
