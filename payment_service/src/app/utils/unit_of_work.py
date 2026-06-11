from types import TracebackType
from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

logger = structlog.get_logger()


class AsyncUOWProtocol(Protocol):
    async def __aenter__(self) -> "AsyncUOWProtocol": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...


class SQLAlchemyAsyncUOW:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def __aenter__(self) -> "SQLAlchemyAsyncUOW":
        logger.debug("uow_enter")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        try:
            if exc_type is None:
                await self.session.commit()
            else:
                await self.session.rollback()
        except Exception:
            await self.session.rollback()
            raise
        finally:
            logger.debug("uow_exit")
