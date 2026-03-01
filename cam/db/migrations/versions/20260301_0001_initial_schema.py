"""Initial schema: entities, entity_aliases, events, signals, alert_scores

Revision ID: 0001
Revises:
Create Date: 2026-03-01 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID, JSONB
from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute('CREATE EXTENSION IF NOT EXISTS "pgcrypto"')

    op.create_table(
        "entities",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column("canonical_name", sa.Text, nullable=False),
        sa.Column("ticker", sa.String(20), nullable=True),
        sa.Column("lei", sa.String(20), nullable=True),
        sa.Column("ein", sa.String(10), nullable=True),
        sa.Column("naics_code", sa.String(10), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )

    op.create_table(
        "entity_aliases",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "entity_id",
            UUID(as_uuid=True),
            sa.ForeignKey("entities.id"),
            nullable=False,
        ),
        sa.Column("raw_name", sa.Text, nullable=False),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("confidence", sa.Float, nullable=True),
        sa.UniqueConstraint("raw_name", "source", name="uq_alias_name_source"),
    )
    op.create_index("ix_entity_aliases_entity_id", "entity_aliases", ["entity_id"])
    op.create_index("ix_entity_aliases_raw_name", "entity_aliases", ["raw_name"])

    op.create_table(
        "events",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "entity_id",
            UUID(as_uuid=True),
            sa.ForeignKey("entities.id"),
            nullable=True,
        ),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("event_type", sa.String(50), nullable=False),
        sa.Column("event_date", sa.Date, nullable=True),
        sa.Column("penalty_usd", sa.Numeric(18, 2), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("raw_url", sa.Text, nullable=True),
        sa.Column("raw_json", JSONB, nullable=True),
        sa.Column(
            "ingested_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_events_entity_id", "events", ["entity_id"])
    op.create_index("ix_events_source", "events", ["source"])
    op.create_index("ix_events_event_date", "events", ["event_date"])

    op.create_table(
        "signals",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "entity_id",
            UUID(as_uuid=True),
            sa.ForeignKey("entities.id"),
            nullable=True,
        ),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("signal_type", sa.String(100), nullable=False),
        sa.Column("signal_date", sa.Date, nullable=True),
        sa.Column("score", sa.Float, nullable=True),
        sa.Column("evidence", sa.Text, nullable=True),
        sa.Column("document_url", sa.Text, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_signals_entity_id", "signals", ["entity_id"])
    op.create_index("ix_signals_signal_type", "signals", ["signal_type"])

    op.create_table(
        "alert_scores",
        sa.Column(
            "id",
            UUID(as_uuid=True),
            primary_key=True,
            server_default=sa.text("gen_random_uuid()"),
        ),
        sa.Column(
            "entity_id",
            UUID(as_uuid=True),
            sa.ForeignKey("entities.id"),
            nullable=False,
        ),
        sa.Column("score_date", sa.Date, nullable=False),
        sa.Column("composite_score", sa.Float, nullable=False),
        sa.Column("component_scores", JSONB, nullable=True),
        sa.Column("alert_level", sa.String(20), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
        ),
        sa.UniqueConstraint("entity_id", "score_date", name="uq_alert_entity_date"),
    )
    op.create_index("ix_alert_scores_entity_id", "alert_scores", ["entity_id"])
    op.create_index("ix_alert_scores_score_date", "alert_scores", ["score_date"])
    op.create_index("ix_alert_scores_alert_level", "alert_scores", ["alert_level"])


def downgrade() -> None:
    op.drop_table("alert_scores")
    op.drop_table("signals")
    op.drop_table("events")
    op.drop_table("entity_aliases")
    op.drop_table("entities")
