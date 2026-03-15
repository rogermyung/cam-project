"""
Database session factory.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from cam.config import get_settings


def get_engine():
    settings = get_settings()
    return create_engine(settings.database_url, pool_pre_ping=True)


def get_session_factory():
    engine = get_engine()
    return sessionmaker(bind=engine, autocommit=False, autoflush=False)


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """Context manager that yields a database session.

    Rolls back on exception and always closes the session on exit::

        with get_session() as db:
            results = db.query(Entity).all()
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_db() -> Generator[Session, None, None]:
    """Yield a database session for use as a FastAPI dependency.

    For scripts and CLI usage prefer :func:`get_session`.
    """
    factory = get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.close()
