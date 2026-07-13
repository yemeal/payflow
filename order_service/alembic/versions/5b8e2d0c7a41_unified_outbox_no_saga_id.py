"""unified outbox, drop orders.saga_id, add currency, processed_events

ADR-006: заказ process-agnostic (saga_id уходит, корреляция по order_id как
business_key); единый outbox с kind/topic/key/type; processed_events для
дедупликации финальных событий саги. Плюс статус заказа сжимается до
PENDING/COMPLETED/CANCELLED: промежуточных состояний у заказа нет, шаги
процесса живут в оркестраторе.

Таблицы outbox_events/orders пусты по данным саги - пересоздание без переноса.

Revision ID: 5b8e2d0c7a41
Revises: c6f15ae45811
Create Date: 2026-07-14 21:45:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '5b8e2d0c7a41'
down_revision: Union[str, Sequence[str], None] = 'c6f15ae45811'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # --- заказ: без saga_id, с валютой ---
    op.drop_index(op.f('ix_orders_saga_id'), table_name='orders')
    op.drop_column('orders', 'saga_id')
    op.add_column(
        'orders',
        sa.Column('currency', sa.String(length=3), server_default='RUB', nullable=False),
    )

    # --- статус заказа: CONFIRMED уходит ---
    # Postgres не умеет удалять значение из enum, поэтому пересоздаём тип.
    # USING-каст упадёт, если в таблице остались строки со статусом CONFIRMED,
    # - это осознанно: молча потерять такой заказ хуже, чем упасть на миграции.
    op.execute("ALTER TYPE orderstatus RENAME TO orderstatus_old")
    op.execute("CREATE TYPE orderstatus AS ENUM ('PENDING', 'COMPLETED', 'CANCELLED')")
    op.execute(
        "ALTER TABLE orders ALTER COLUMN status TYPE orderstatus "
        "USING status::text::orderstatus"
    )
    op.execute("DROP TYPE orderstatus_old")

    # --- старый outbox_events уходит вместе со своим enum ---
    op.drop_index('ix_outbox_events_status_created_at', table_name='outbox_events')
    op.drop_table('outbox_events')
    op.execute("DROP TYPE outboxstatus")

    # --- единый outbox (ADR-006) ---
    # kind - класс сообщения (COMMAND/EVENT), type - тип из конверта ("order.created").
    # Схема совпадает с outbox оркестратора: relay generic и одинаков во всех сервисах.
    op.create_table(
        'outbox',
        sa.Column('kind', sa.Enum('COMMAND', 'EVENT', name='outboxkind'), nullable=False),
        sa.Column('topic', sa.String(length=255), nullable=False),
        sa.Column('key', sa.String(length=255), nullable=False),
        sa.Column('type', sa.String(length=100), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            'status',
            sa.Enum('PENDING', 'SUCCESS', 'FAILED', name='outboxstatus'),
            nullable=False,
        ),
        sa.Column('attempts', sa.Integer(), server_default='0', nullable=False),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    # покрывает выборку relay: WHERE status = 'PENDING' ORDER BY created_at
    op.create_index(
        'ix_outbox_status_created_at', 'outbox', ['status', 'created_at'], unique=False
    )

    # --- дедупликация финальных событий саги (Idempotent Consumer) ---
    op.create_table(
        'processed_events',
        sa.Column('event_id', sa.Uuid(), nullable=False),
        sa.Column('saga_id', sa.Uuid(), nullable=True),
        sa.Column('event_type', sa.String(length=255), nullable=False),
        sa.Column('processed_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('event_id'),
    )
    op.create_index(
        op.f('ix_processed_events_saga_id'), 'processed_events', ['saga_id'], unique=False
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_processed_events_saga_id'), table_name='processed_events')
    op.drop_table('processed_events')
    op.drop_index('ix_outbox_status_created_at', table_name='outbox')
    op.drop_table('outbox')
    op.execute("DROP TYPE outboxstatus")
    op.execute("DROP TYPE outboxkind")

    # статус заказа: возвращаем CONFIRMED в enum
    op.execute("ALTER TYPE orderstatus RENAME TO orderstatus_old")
    op.execute(
        "CREATE TYPE orderstatus AS ENUM "
        "('PENDING', 'CONFIRMED', 'COMPLETED', 'CANCELLED')"
    )
    op.execute(
        "ALTER TABLE orders ALTER COLUMN status TYPE orderstatus "
        "USING status::text::orderstatus"
    )
    op.execute("DROP TYPE orderstatus_old")

    op.create_table(
        'outbox_events',
        sa.Column('event_type', sa.String(), nullable=False),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            'status',
            sa.Enum('PENDING', 'IN_PROGRESS', 'SUCCESS', 'FAILED', name='outboxstatus'),
            nullable=False,
        ),
        sa.Column('reserved_to', sa.DateTime(), nullable=True),
        sa.Column('attempts', sa.Integer(), server_default='0', nullable=False),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'ix_outbox_events_status_created_at', 'outbox_events',
        ['status', 'created_at'], unique=False,
    )
    op.drop_column('orders', 'currency')
    op.add_column('orders', sa.Column('saga_id', sa.Uuid(), nullable=False))
    op.create_index(op.f('ix_orders_saga_id'), 'orders', ['saga_id'], unique=True)
