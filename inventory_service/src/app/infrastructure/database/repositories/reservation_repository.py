from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.reservations import Reservation, ReservationStatus
from app.infrastructure.database.models.reservations import ReservationORM
from app.infrastructure.database.repositories.base_repository import (
    SQLAlchemyAsyncRepository,
)


class ReservationRepository(SQLAlchemyAsyncRepository[Reservation, ReservationORM]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, Reservation, ReservationORM)

    async def add(self, reservation: Reservation) -> Reservation:
        return await self.create(reservation)

    async def get_by_order_id(self, order_id: UUID) -> Reservation | None:
        stmt = select(ReservationORM).where(ReservationORM.order_id == order_id)
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if orm_model is None:
            return None
        return Reservation.model_validate(orm_model, from_attributes=True)

    async def get_by_order_id_for_update(self, order_id: UUID) -> Reservation | None:
        # блокировка резерва на всю транзакцию: commit/cancel/expiry не должны
        # пересечься на одном заказе и дважды подвинуть остатки
        stmt = (
            select(ReservationORM)
            .where(ReservationORM.order_id == order_id)
            .with_for_update()
        )
        result = await self._session.execute(stmt)
        orm_model = result.scalar_one_or_none()
        if orm_model is None:
            return None
        return Reservation.model_validate(orm_model, from_attributes=True)

    async def find_expired_active(
        self, now: datetime, limit: int
    ) -> list[Reservation]:
        # SKIP LOCKED: строки, уже взятые другим инстансом поллера (или командой
        # commit/cancel), молча пропускаем - на следующем тике разберёмся
        stmt = (
            select(ReservationORM)
            .where(
                ReservationORM.status == ReservationStatus.ACTIVE,
                ReservationORM.expires_at <= now,
            )
            .order_by(ReservationORM.expires_at.asc())
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        result = await self._session.execute(stmt)
        return [
            Reservation.model_validate(orm_model, from_attributes=True)
            for orm_model in result.scalars().all()
        ]
