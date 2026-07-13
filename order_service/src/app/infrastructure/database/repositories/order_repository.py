import uuid

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.orders import Order
from app.infrastructure.database.models.orders import OrderORM
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)


def _to_orm_payload(entity: Order) -> dict:
    """Order -> kwargs для OrderORM.

    items содержат Decimal, а JSONB принимает только json-совместимые типы,
    поэтому позиции сериализуем через mode="json" (Decimal -> str).
    Остальные поля оставляем нативными (total_amount должен остаться Decimal
    для колонки Numeric).
    """
    data = entity.model_dump()
    data["items"] = entity.model_dump(mode="json")["items"]
    return data


class OrderRepository(SQLAlchemyAsyncRepository[Order, OrderORM]):
    """Специализированный репозиторий для Order"""

    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Order, OrderORM)

    async def create(self, entity: Order) -> Order:
        orm_model = OrderORM(**_to_orm_payload(entity))
        self._session.add(orm_model)
        await self._session.flush()
        return Order.model_validate(orm_model, from_attributes=True)

    async def update(self, entity: Order) -> Order:
        # как базовый update (SELECT FOR UPDATE), но с json-сериализацией items
        stmt = (
            select(OrderORM)
            .where(OrderORM.id == entity.id)
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if not orm_model:
            raise ValueError(f"Order with id {entity.id} not found for update")

        for key, value in _to_orm_payload(entity).items():
            setattr(orm_model, key, value)

        await self._session.flush()
        return Order.model_validate(orm_model, from_attributes=True)

    async def get_for_user(
        self, order_id: uuid.UUID, user_id: uuid.UUID
    ) -> Order | None:
        # владелец проверяется в самом запросе, а не в коде:
        # чужой заказ неотличим от несуществующего
        stmt = select(OrderORM).where(
            OrderORM.id == order_id,
            OrderORM.user_id == user_id,
        )
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if orm_model is None:
            return None
        return Order.model_validate(orm_model, from_attributes=True)

    async def list_for_user(
        self, user_id: uuid.UUID, limit: int = 50, offset: int = 0
    ) -> list[Order]:
        stmt = (
            select(OrderORM)
            .where(OrderORM.user_id == user_id)
            .order_by(OrderORM.created_at.desc(), OrderORM.id.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return [
            Order.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]

    async def list_all(self, limit: int = 50, offset: int = 0) -> list[Order]:
        stmt = (
            select(OrderORM)
            .order_by(OrderORM.created_at.desc(), OrderORM.id.desc())
            .limit(limit)
            .offset(offset)
        )
        result = await self._session.execute(stmt)
        return [
            Order.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]

    async def get_for_update(self, order_id: uuid.UUID) -> Order | None:
        stmt = (
            select(OrderORM)
            .where(OrderORM.id == order_id)
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if orm_model is None:
            return None
        return Order.model_validate(orm_model, from_attributes=True)
