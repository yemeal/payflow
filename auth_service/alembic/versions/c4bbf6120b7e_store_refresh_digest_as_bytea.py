"""store refresh digest as bytea

Revision ID: c4bbf6120b7e
Revises: fe83b855f0af
Create Date: 2026-07-24

"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "c4bbf6120b7e"
down_revision: str | None = "fe83b855f0af"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Перевести SHA-256 digest из hex-строки в его бинарное представление."""
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM refresh_tokens
                WHERE token_hash !~ '^[0-9A-Fa-f]{64}$'
            ) THEN
                RAISE EXCEPTION
                    'refresh_tokens.token_hash contains a non-SHA-256 hex value';
            END IF;
        END
        $$;
        """
    )
    op.alter_column(
        "refresh_tokens",
        "token_hash",
        existing_type=sa.String(length=255),
        type_=postgresql.BYTEA(),
        postgresql_using="decode(token_hash, 'hex')",
        existing_nullable=False,
    )
    op.create_check_constraint(
        "ck_refresh_tokens_token_hash_length",
        "refresh_tokens",
        "octet_length(token_hash) = 32",
    )


def downgrade() -> None:
    """Вернуть digest к 64-символьному hex-представлению."""
    op.drop_constraint(
        "ck_refresh_tokens_token_hash_length",
        "refresh_tokens",
        type_="check",
    )
    op.alter_column(
        "refresh_tokens",
        "token_hash",
        existing_type=postgresql.BYTEA(),
        type_=sa.String(length=64),
        postgresql_using="encode(token_hash, 'hex')",
        existing_nullable=False,
    )
