from typing import Any

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.infrastructure.database.models.correlation import CommandCorrelationORM
from app.infrastructure.database.models.payments import PaymentORM


class SQLAlchemyCommandCorrelationStore:
    """
    Адаптер CommandCorrelationStoreProtocol.

    Работает на СОБСТВЕННОЙ короткой сессии (не на request-сессии): запись
    корреляции не должна участвовать в транзакциях платежа, а чтение нужно
    relay-процессу, у которого своей request-сессии нет вовсе.
    """

    def __init__(self, sessionmaker: async_sessionmaker[AsyncSession]) -> None:
        self._sessionmaker = sessionmaker

    async def remember(self, command_id: str, correlation: dict[str, Any]) -> None:
        # ON CONFLICT DO NOTHING: переигранная (дублирующая) команда не перезаписывает
        # correlation и не падает - идемпотентность на уровне вставки, а не проверок
        stmt = (
            insert(CommandCorrelationORM)
            .values(command_id=command_id, correlation=correlation)
            .on_conflict_do_nothing(index_elements=[CommandCorrelationORM.command_id])
        )
        async with self._sessionmaker() as session:
            await session.execute(stmt)
            await session.commit()

    async def resolve_for_payment(self, payment_id: str) -> dict[str, Any] | None:
        stmt = (
            select(CommandCorrelationORM.correlation)
            .join(
                PaymentORM,
                PaymentORM.idempotency_key == CommandCorrelationORM.command_id,
            )
            .where(PaymentORM.id == payment_id)
        )
        async with self._sessionmaker() as session:
            result = await session.execute(stmt)
            return result.scalar_one_or_none()
