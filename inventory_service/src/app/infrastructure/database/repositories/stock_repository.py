from typing import Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.stock import StockItem
from app.infrastructure.database.models.stock import StockItemORM


class StockRepository:
    """
    Остатки склада. PK - product_id, поэтому базовый репозиторий (он ходит
    по .id) здесь не подходит: пишем запросы явно.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_for_update(self, product_ids: Sequence[str]) -> list[StockItem]:
        if not product_ids:
            return []

        # ORDER BY product_id + FOR UPDATE: все транзакции захватывают строки
        # в одном и том же порядке. Без этого два параллельных заказа с
        # пересекающимися товарами могут взять блокировки крест-накрест
        # и словить дедлок (Postgres убьёт одну из транзакций).
        stmt = (
            select(StockItemORM)
            .where(StockItemORM.product_id.in_(product_ids))
            .order_by(StockItemORM.product_id.asc())
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        return [
            StockItem.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]

    async def update(self, item: StockItem) -> StockItem:
        orm_model = await self._session.get(StockItemORM, item.product_id)
        if orm_model is None:
            raise ValueError(f"stock item {item.product_id} not found for update")

        orm_model.available = item.available
        orm_model.reserved = item.reserved
        await self._session.flush()
        return StockItem.model_validate(orm_model, from_attributes=True)
