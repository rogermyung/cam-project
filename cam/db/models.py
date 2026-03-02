"""
SQLAlchemy ORM models for the CAM database schema.
"""

import uuid

from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB as _PGjsonb
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, relationship

# Use JSONB on PostgreSQL; fall back to JSON on SQLite (for unit tests)
JSONB = JSON().with_variant(_PGjsonb(), "postgresql")


class Base(DeclarativeBase):
    pass


class Entity(Base):
    """Canonical company entity."""

    __tablename__ = "entities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    canonical_name = Column(Text, nullable=False)
    ticker = Column(String(20), nullable=True)
    lei = Column(String(20), nullable=True)  # Legal Entity Identifier
    ein = Column(String(10), nullable=True)  # Employer Identification Number
    naics_code = Column(String(10), nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    aliases = relationship("EntityAlias", back_populates="entity")
    events = relationship("Event", back_populates="entity")
    signals = relationship("Signal", back_populates="entity")
    alert_scores = relationship("AlertScore", back_populates="entity")


class EntityAlias(Base):
    """Maps raw name strings to canonical entity IDs."""

    __tablename__ = "entity_aliases"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID(as_uuid=True), ForeignKey("entities.id"), nullable=False)
    raw_name = Column(Text, nullable=False)
    source = Column(String(50), nullable=False)  # e.g. 'osha', 'sec', 'manual'
    confidence = Column(Float, nullable=True)

    __table_args__ = (UniqueConstraint("raw_name", "source", name="uq_alias_name_source"),)

    entity = relationship("Entity", back_populates="aliases")


class Event(Base):
    """All violations/events from all sources."""

    __tablename__ = "events"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID(as_uuid=True), ForeignKey("entities.id"), nullable=True)
    source = Column(String(50), nullable=False)  # 'osha', 'epa', 'cfpb', etc.
    event_type = Column(String(50), nullable=False)  # 'violation', 'complaint', 'fine'
    event_date = Column(Date, nullable=True)
    penalty_usd = Column(Numeric(18, 2), nullable=True)
    description = Column(Text, nullable=True)
    raw_url = Column(Text, nullable=True)
    raw_json = Column(JSONB, nullable=True)
    ingested_at = Column(DateTime(timezone=True), server_default=func.now())

    entity = relationship("Entity", back_populates="events")


class Signal(Base):
    """NLP-derived signals from documents."""

    __tablename__ = "signals"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID(as_uuid=True), ForeignKey("entities.id"), nullable=True)
    source = Column(String(50), nullable=False)  # 'edgar_10k', 'earnings_call'
    signal_type = Column(String(100), nullable=False)  # 'risk_language_expansion', etc.
    signal_date = Column(Date, nullable=True)
    score = Column(Float, nullable=True)  # 0.0 to 1.0
    evidence = Column(Text, nullable=True)
    document_url = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    entity = relationship("Entity", back_populates="signals")


class AlertScore(Base):
    """Composite alert scores per entity per time period."""

    __tablename__ = "alert_scores"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    entity_id = Column(UUID(as_uuid=True), ForeignKey("entities.id"), nullable=False)
    score_date = Column(Date, nullable=False)
    composite_score = Column(Float, nullable=False)
    component_scores = Column(JSONB, nullable=True)
    alert_level = Column(String(20), nullable=True)  # 'watch', 'elevated', 'critical'
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    __table_args__ = (UniqueConstraint("entity_id", "score_date", name="uq_alert_entity_date"),)

    entity = relationship("Entity", back_populates="alert_scores")
