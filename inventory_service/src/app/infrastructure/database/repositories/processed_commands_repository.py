from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.processed_commands import ProcessedCommand
from app.infrastructure.database.models.processed_commands import ProcessedCommandORM


class ProcessedCommandRepository:
    """Журнал идемпотентности участника (contracts/README, правило 2)"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, command_id: str) -> ProcessedCommand | None:
        orm_model = await self._session.get(ProcessedCommandORM, command_id)
        if orm_model is None:
            return None
        return ProcessedCommand.model_validate(orm_model, from_attributes=True)

    async def add_if_absent(self, command: ProcessedCommand) -> bool:
        # INSERT ... ON CONFLICT DO NOTHING - атомарно. SELECT + INSERT двумя
        # запросами - классическая ошибка идемпотентности: два консюмера успевают
        # оба пройти SELECT и оба применить бизнес-эффект
        stmt = (
            pg_insert(ProcessedCommandORM)
            .values(command_id=command.command_id, result=command.result)
            .on_conflict_do_nothing(index_elements=["command_id"])
        )
        result = await self._session.execute(stmt)
        # rowcount == 0 -> сработал ON CONFLICT: команду уже записал кто-то другой
        return bool(result.rowcount)
