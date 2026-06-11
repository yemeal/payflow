from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession


class AsyncRepositoryProtocol[EntityT](Protocol):
    async def create(self, entity: EntityT) -> EntityT: ...

    async def get(self, entity_id: str | int) -> EntityT | None: ...


class SQLAlchemyAsyncRepository[EntityT]:
    def __init__(
        self,
        session: AsyncSession,
        model: type[EntityT],
    ) -> None:
        self._session = session
        self._model = model

    async def create(self, entity: EntityT) -> EntityT:
        self._session.add(entity)
        await self._session.flush()
        return entity

    async def get(self, entity_id: str | int) -> EntityT | None:
        return await self._session.get(entity=self._model, ident=entity_id)
