"""init inventory schema: stock_items, reservations, processed_commands, outbox

Склад перестаёт быть stateless-заглушкой: остатки, резервы с TTL, журнал
идемпотентности участника (contracts/README, правило 2) и единый outbox
(ADR-006, docs/saga-design.md).

Revision ID: c1a4e9d07b31
Revises:
Create Date: 2026-07-15 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c1a4e9d07b31"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # --- остатки склада ---
    op.create_table(
        "stock_items",
        sa.Column("product_id", sa.String(length=255), nullable=False),
        sa.Column("available", sa.Integer(), server_default="0", nullable=False),
        sa.Column("reserved", sa.Integer(), server_default="0", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        # последний рубеж целостности: даже при ошибке в арифметике сервиса
        # отрицательный остаток не запишется - транзакция упадёт
        sa.CheckConstraint("available >= 0", name="check_available_non_negative"),
        sa.CheckConstraint("reserved >= 0", name="check_reserved_non_negative"),
        sa.PrimaryKeyConstraint("product_id"),
    )

    # --- резервы с TTL ---
    op.create_table(
        "reservations",
        sa.Column("order_id", sa.Uuid(), nullable=False),
        sa.Column(
            "status",
            sa.Enum(
                "ACTIVE",
                "COMMITTED",
                "CANCELLED",
                "EXPIRED",
                name="reservationstatus",
            ),
            nullable=False,
        ),
        sa.Column("items", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    # на заказ - максимум один резерв: защита от двойного резерва повторной
    # командой с новым commandId (дедуп по commandId такую не поймает)
    op.create_index(
        op.f("ix_reservations_order_id"), "reservations", ["order_id"], unique=True
    )
    op.create_index(op.f("ix_reservations_status"), "reservations", ["status"])
    # выборка поллера автоистечения: expires_at <= now
    op.create_index(op.f("ix_reservations_expires_at"), "reservations", ["expires_at"])

    # --- журнал идемпотентности участника: commandId -> сохранённый ответ ---
    op.create_table(
        "processed_commands",
        sa.Column("command_id", sa.String(length=255), nullable=False),
        sa.Column("result", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        # PK по command_id: INSERT ... ON CONFLICT DO NOTHING по этому ключу и
        # есть атомарная проверка "команда уже обработана"
        sa.PrimaryKeyConstraint("command_id"),
    )

    # --- единый outbox (у склада - только события) ---
    op.create_table(
        "outbox",
        sa.Column("kind", sa.Enum("COMMAND", "EVENT", name="outboxkind"), nullable=False),
        sa.Column("topic", sa.String(length=255), nullable=False),
        sa.Column("key", sa.String(length=255), nullable=False),
        sa.Column("type", sa.String(length=100), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "status",
            sa.Enum("PENDING", "SUCCESS", "FAILED", name="outboxstatus"),
            nullable=False,
        ),
        sa.Column("attempts", sa.Integer(), server_default="0", nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.text("now()"), nullable=False
        ),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    # покрывает выборку relay: WHERE status = 'PENDING' ORDER BY created_at
    op.create_index("ix_outbox_status_created_at", "outbox", ["status", "created_at"])


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_outbox_status_created_at", table_name="outbox")
    op.drop_table("outbox")
    op.execute("DROP TYPE outboxstatus")
    op.execute("DROP TYPE outboxkind")

    op.drop_table("processed_commands")

    op.drop_index(op.f("ix_reservations_expires_at"), table_name="reservations")
    op.drop_index(op.f("ix_reservations_status"), table_name="reservations")
    op.drop_index(op.f("ix_reservations_order_id"), table_name="reservations")
    op.drop_table("reservations")
    op.execute("DROP TYPE reservationstatus")

    op.drop_table("stock_items")
