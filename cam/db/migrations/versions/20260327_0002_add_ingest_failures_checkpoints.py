"""Add ingest_failures and ingest_checkpoints tables (M15 — Pipeline Resilience)

Revision ID: 0002
Revises: 0001
Create Date: 2026-03-27 00:00:00.000000
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "ingest_failures",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("raw_key", sa.Text, nullable=True),
        sa.Column("raw_json", JSONB, nullable=False),
        sa.Column("error_type", sa.String(50), nullable=False),
        sa.Column("error_msg", sa.Text, nullable=False),
        sa.Column("traceback", sa.Text, nullable=True),
        sa.Column("retry_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("last_retry", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("idx_ingest_failures_source", "ingest_failures", ["source"])
    op.create_index("idx_ingest_failures_run_id", "ingest_failures", ["run_id"])
    op.create_index("idx_ingest_failures_error_type", "ingest_failures", ["error_type"])
    # Partial index for fast open-failures queries (PostgreSQL only)
    op.create_index(
        "idx_ingest_failures_open",
        "ingest_failures",
        ["created_at"],
        postgresql_where=sa.text("resolved_at IS NULL"),
    )

    op.create_table(
        "ingest_checkpoints",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("source", sa.String(50), nullable=False),
        sa.Column("run_id", UUID(as_uuid=True), nullable=False),
        sa.Column("checkpoint", JSONB, nullable=False),
        sa.Column("records_ok", sa.Integer, nullable=False, server_default="0"),
        sa.Column("records_err", sa.Integer, nullable=False, server_default="0"),
        sa.Column(
            "started_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("source", "run_id", name="uq_checkpoint_source_run"),
    )
    op.create_index("idx_ingest_checkpoints_source", "ingest_checkpoints", ["source"])


def downgrade() -> None:
    op.drop_table("ingest_checkpoints")
    op.drop_index("idx_ingest_failures_open", table_name="ingest_failures")
    op.drop_index("idx_ingest_failures_error_type", table_name="ingest_failures")
    op.drop_index("idx_ingest_failures_run_id", table_name="ingest_failures")
    op.drop_index("idx_ingest_failures_source", table_name="ingest_failures")
    op.drop_table("ingest_failures")
