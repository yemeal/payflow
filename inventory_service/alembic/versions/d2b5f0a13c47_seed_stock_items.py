"""seed stock: sku-1..sku-5 по 100 штук

Сид для MVP: без товаров сагу заказа нельзя прогнать end-to-end.
Идемпотентен (ON CONFLICT DO NOTHING) - повторный upgrade на непустой базе
не перетирает реальные остатки.

Revision ID: d2b5f0a13c47
Revises: c1a4e9d07b31
Create Date: 2026-07-15 10:05:00.000000

"""
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d2b5f0a13c47"
down_revision: Union[str, Sequence[str], None] = "c1a4e9d07b31"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SEED_PRODUCT_IDS: tuple[str, ...] = ("sku-1", "sku-2", "sku-3", "sku-4", "sku-5")
SEED_AVAILABLE = 100


def upgrade() -> None:
    """Upgrade schema."""
    values = ", ".join(
        f"('{product_id}', {SEED_AVAILABLE}, 0)" for product_id in SEED_PRODUCT_IDS
    )
    op.execute(
        f"INSERT INTO stock_items (product_id, available, reserved) "
        f"VALUES {values} "
        f"ON CONFLICT (product_id) DO NOTHING"
    )


def downgrade() -> None:
    """Downgrade schema."""
    ids = ", ".join(f"'{product_id}'" for product_id in SEED_PRODUCT_IDS)
    op.execute(f"DELETE FROM stock_items WHERE product_id IN ({ids})")
