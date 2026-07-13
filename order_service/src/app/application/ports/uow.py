from types import TracebackType
from typing import Protocol


# Порт живёт в application-слое: сервисы управляют границами транзакций,
# не зная о конкретной реализации.
class AsyncUOWProtocol(Protocol):
    async def __aenter__(self) -> "AsyncUOWProtocol": ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None: ...
