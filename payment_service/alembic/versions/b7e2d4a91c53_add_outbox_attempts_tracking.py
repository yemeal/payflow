"""Add outbox attempts tracking (poison event handling)

Добавляет к outbox_events счётчик неудачных попыток публикации (attempts)
и текст последней ошибки (last_error). После OUTBOX_MAX_PUBLISH_ATTEMPTS
неудач relay помечает событие FAILED — "ядовитые" события не блокируют очередь.

Также добавляет индекс (status, created_at) под выборку relay:
WHERE status = 'PENDING' ORDER BY created_at.

Revision ID: b7e2d4a91c53
Revises: 6da30aa906f2
Create Date: 2026-07-10

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "b7e2d4a91c53"
down_revision: Union[str, Sequence[str], None] = "6da30aa906f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "outbox_events",
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "outbox_events",
        sa.Column("last_error", sa.Text(), nullable=True),
    )
    op.create_index(
        "ix_outbox_events_status_created_at",
        "outbox_events",
        ["status", "created_at"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_outbox_events_status_created_at", table_name="outbox_events")
    op.drop_column("outbox_events", "last_error")
    op.drop_column("outbox_events", "attempts")
