"""add command_correlations (transport-level saga correlation)

Revision ID: c3f1a7d24e05
Revises: b7e2d4a91c53
Create Date: 2026-07-14

Журнал транспортной корреляции входящих команд саги. Отдельная таблица, а не поле
в payments: корреляция - метадата сообщения, домену платежа она не принадлежит
(contracts/README, правило 1; docs/saga-design.md, итерация 4).
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c3f1a7d24e05"
down_revision: Union[str, Sequence[str], None] = "b7e2d4a91c53"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "command_correlations",
        sa.Column("command_id", sa.String(), nullable=False),
        sa.Column("correlation", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("command_id"),
    )


def downgrade() -> None:
    op.drop_table("command_correlations")
