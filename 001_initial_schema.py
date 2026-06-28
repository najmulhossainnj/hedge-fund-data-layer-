"""Initial schema — dataset_records and ingestion_logs tables.

Revision ID: 001
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dataset_records",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("data_type", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column("timeframe", sa.String(16), nullable=True),
        sa.Column("series", sa.String(64), nullable=True),
        sa.Column("start_date", sa.String(32), nullable=True),
        sa.Column("end_date", sa.String(32), nullable=True),
        sa.Column("rows", sa.Integer(), nullable=True),
        sa.Column("columns", sa.Integer(), nullable=True),
        sa.Column("storage_uri", sa.Text(), nullable=False),
        sa.Column("hash", sa.String(64), unique=True, nullable=False),
        sa.Column("quality_passed", sa.Boolean(), default=True),
        sa.Column("quality_issues", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    op.create_index("ix_dataset_records_hash", "dataset_records", ["hash"])
    op.create_index("ix_dataset_records_symbol", "dataset_records", ["symbol"])
    op.create_index("ix_dataset_records_data_type", "dataset_records", ["data_type"])
    op.create_index(
        "ix_dataset_records_symbol_data_type",
        "dataset_records",
        ["symbol", "data_type"],
    )

    op.create_table(
        "ingestion_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "dataset_id",
            sa.Integer(),
            sa.ForeignKey("dataset_records.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("provider", sa.String(64), nullable=False),
        sa.Column("data_type", sa.String(32), nullable=False),
        sa.Column("symbol", sa.String(32), nullable=True),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("rows_fetched", sa.Integer(), nullable=True),
        sa.Column("rows_after_quality", sa.Integer(), nullable=True),
        sa.Column("issues", JSONB, nullable=True),
        sa.Column("error", sa.Text(), nullable=True),
    )

    op.create_index("ix_ingestion_logs_dataset_id", "ingestion_logs", ["dataset_id"])
    op.create_index("ix_ingestion_logs_symbol", "ingestion_logs", ["symbol"])
    op.create_index("ix_ingestion_logs_status", "ingestion_logs", ["status"])


def downgrade() -> None:
    op.drop_table("ingestion_logs")
    op.drop_table("dataset_records")
