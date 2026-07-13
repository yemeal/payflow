"""generic saga core: saga_type+business_key, saga_transitions, unified outbox

ADR-006 (docs/saga-design.md): generic-ядро вместо саги, зашитой в заказ.
Старая таблица sagas пуста (скелетная фаза) - пересоздаём без переноса данных.

Revision ID: 9c4d1e7a2f50
Revises: 48bf76f32db7
Create Date: 2026-07-14 21:40:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '9c4d1e7a2f50'
down_revision: Union[str, Sequence[str], None] = '48bf76f32db7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # --- старая сага (order_id + бизнес-специфичный enum) уходит целиком ---
    op.drop_index(op.f('ix_sagas_status'), table_name='sagas')
    op.drop_index(op.f('ix_sagas_retry_after'), table_name='sagas')
    op.drop_index(op.f('ix_sagas_order_id'), table_name='sagas')
    op.drop_table('sagas')
    # enum пересоздаётся с generic-значениями под тем же именем
    op.execute("DROP TYPE sagastatus")

    # --- generic-ядро ---
    op.create_table(
        'sagas',
        sa.Column('saga_type', sa.String(length=100), nullable=False),
        sa.Column('business_key', sa.String(length=255), nullable=False),
        sa.Column(
            'status',
            sa.Enum('RUNNING', 'COMPENSATING', 'COMPLETED', 'CANCELLED', 'FAILED',
                    name='sagastatus'),
            nullable=False,
        ),
        sa.Column('current_step', sa.String(length=100), nullable=True),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('retry_count', sa.Integer(), server_default='0', nullable=False),
        sa.Column('retry_after', sa.DateTime(), nullable=True),
        sa.Column('deadline_at', sa.DateTime(), nullable=True),
        sa.Column('active_command_id', sa.Uuid(), nullable=True),
        sa.Column('last_error', sa.Text(), nullable=True),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        # идемпотентное создание саги: INSERT ... ON CONFLICT DO NOTHING
        sa.UniqueConstraint('saga_type', 'business_key', name='uq_sagas_type_business_key'),
    )
    op.create_index(op.f('ix_sagas_saga_type'), 'sagas', ['saga_type'], unique=False)
    op.create_index(op.f('ix_sagas_status'), 'sagas', ['status'], unique=False)
    # выборки фонового поллера (retry / deadline)
    op.create_index(op.f('ix_sagas_retry_after'), 'sagas', ['retry_after'], unique=False)
    op.create_index(op.f('ix_sagas_deadline_at'), 'sagas', ['deadline_at'], unique=False)

    # --- append-only история переходов (Admin API, аудит) ---
    op.create_table(
        'saga_transitions',
        sa.Column('saga_id', sa.Uuid(), nullable=False),
        sa.Column('from_status', sa.String(length=50), nullable=True),
        sa.Column('from_step', sa.String(length=100), nullable=True),
        sa.Column('to_status', sa.String(length=50), nullable=False),
        sa.Column('to_step', sa.String(length=100), nullable=True),
        sa.Column('event_id', sa.Uuid(), nullable=True),
        sa.Column('event_type', sa.String(length=100), nullable=True),
        sa.Column('detail', sa.Text(), nullable=True),
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        # CASCADE: retention-скрипты удаляют сагу вместе с историей
        sa.ForeignKeyConstraint(['saga_id'], ['sagas.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_saga_transitions_saga_id'), 'saga_transitions', ['saga_id'], unique=False
    )

    # --- единый outbox команд и событий (топик и ключ - атрибуты записи) ---
    op.create_table(
        'outbox',
        # kind - класс сообщения (COMMAND / EVENT), type - тип из конверта
        # ("inventory.reserve", "saga.completed"): разные вещи, разные колонки
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
    # разбор инцидентов: все сообщения по одному бизнес-ключу
    op.create_index(op.f('ix_outbox_key'), 'outbox', ['key'], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(op.f('ix_outbox_key'), table_name='outbox')
    op.drop_index('ix_outbox_status_created_at', table_name='outbox')
    op.drop_table('outbox')
    op.execute("DROP TYPE outboxstatus")
    op.execute("DROP TYPE outboxkind")
    op.drop_index(op.f('ix_saga_transitions_saga_id'), table_name='saga_transitions')
    op.drop_table('saga_transitions')
    op.drop_index(op.f('ix_sagas_deadline_at'), table_name='sagas')
    op.drop_index(op.f('ix_sagas_retry_after'), table_name='sagas')
    op.drop_index(op.f('ix_sagas_status'), table_name='sagas')
    op.drop_index(op.f('ix_sagas_saga_type'), table_name='sagas')
    op.drop_table('sagas')
    op.execute("DROP TYPE sagastatus")
    op.create_table(
        'sagas',
        sa.Column('id', sa.Uuid(), nullable=False),
        sa.Column('order_id', sa.Uuid(), nullable=False),
        sa.Column(
            'status',
            sa.Enum('CREATED', 'INVENTORY_RESERVING', 'INVENTORY_RESERVED',
                    'PAYMENT_CHARGING', 'COMPENSATING_INVENTORY', 'COMPLETED',
                    'CANCELLED', 'FAILED', name='sagastatus'),
            nullable=False,
        ),
        sa.Column('payload', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('retry_count', sa.Integer(), server_default='0', nullable=False),
        sa.Column('retry_after', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(op.f('ix_sagas_order_id'), 'sagas', ['order_id'], unique=True)
    op.create_index(op.f('ix_sagas_retry_after'), 'sagas', ['retry_after'], unique=False)
    op.create_index(op.f('ix_sagas_status'), 'sagas', ['status'], unique=False)
