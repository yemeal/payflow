from typing import Protocol

from app.application.ports.dto import (
    ProviderTransactionRequest,
    ProviderTransactionInitiated,
    ProviderTransactionStatus,
)


class PaymentProviderProtocol(Protocol):
    async def initiate_transaction(
        self, request: ProviderTransactionRequest
    ) -> ProviderTransactionInitiated:
        """Инициирует платеж. Возвращает transaction_id и первичный статус."""
        ...

    async def get_transaction_status(
        self, transaction_id: str
    ) -> ProviderTransactionStatus:
        """Запрашивает актуальный статус транзакции у провайдера."""
        ...
