"""
Тесты командного консьюмера payment_service (топик payments.commands).

Здесь проверяем маршрутизацию команд (CommandRouter) и обработчик payment.process:
корректный разбор конверта команды, применение Two-Level Idempotency вокруг create()
и поведение на кэш-хите и неизвестной команде.

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from decimal import Decimal
from uuid import uuid4
from datetime import datetime, timezone

from pydantic import ValidationError

import app.entrypoints.messaging.consumer as consumer_module
from app.entrypoints.messaging.consumer import (
    router,
    process_command_message,
    send_command_to_dlq,
    NON_RETRIABLE_ERRORS,
)
from app.entrypoints.messaging.exceptions import UnknownCommandError
from app.application.exceptions.idempotency import (
    IdempotencyKeyPayloadMismatchError,
    IdempotencyStateInconsistencyError,
)
from app.infrastructure.exceptions.redis import RedisUnavailableError
from app.domain.payments import Payment, PaymentStatus

# ---------------------------------------------------------------------------
# Хелперы
# ---------------------------------------------------------------------------


def make_command(amount="150.00", currency="USD", command_id=None):
    """Валидный конверт команды payment.process."""
    return {
        "metadata": {
            "commandId": command_id or "12345678-1234-5678-1234-567812345678",
            "commandType": "payment.process",
            "timestamp": "2026-07-08T04:29:50Z",
            "source": "orchestrator",
        },
        "data": {
            "amount": amount,
            "currency": currency,
            "customerId": "cust_12345",
            "description": "Integration Test",
        },
    }


def make_idempotency_service(guard):
    """
    Фабрика заглушки IdempotencyService: вызов возвращает async-контекст,
    отдающий переданный guard (повторяет реальный интерфейс менеджера).
    """
    service = MagicMock()
    async_context = MagicMock()
    async_context.__aenter__.return_value = guard
    async_context.__aexit__.return_value = None
    service.return_value = async_context
    return service


def make_fresh_guard():
    """Guard в состоянии 'кэша нет, обрабатываем впервые'."""
    guard = MagicMock()
    guard.has_cached_result = False
    guard.cached_status_code = None
    return guard


def make_kafka_message(offset=42, partition=0):
    """Заглушка FastStream-сообщения: нужен только raw_message с offset/partition."""
    raw = SimpleNamespace(offset=offset, partition=partition)
    return SimpleNamespace(raw_message=raw)


def make_raising_idempotency_service(exc: Exception):
    """
    IdempotencyService, чей async-контекст падает на входе заданным исключением.
    Используем для проверки, что временный сбой (например, Redis) всплывает наружу.
    """
    service = MagicMock()
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(side_effect=exc)
    ctx.__aexit__ = AsyncMock(return_value=None)
    service.return_value = ctx
    return service


def make_payment_service():
    """
    AsyncMock платежного сервиса. build_idempotency_db_lookup в реальном протоколе
    синхронный (возвращает callable), поэтому подменяем его MagicMock -
    иначе AsyncMock вернул бы неожиданную неожидаемую корутину.
    """
    service = AsyncMock()
    service.build_idempotency_db_lookup = MagicMock(
        return_value=AsyncMock(return_value=None)
    )
    return service


# ---------------------------------------------------------------------------
# Маршрутизация и обработка payment.process
# ---------------------------------------------------------------------------


class TestProcessPaymentCommand:
    @pytest.mark.asyncio
    async def test_success_creates_payment_and_caches_result(self):
        """
        Проверяем: обработка валидной команды payment.process без кэша.
        Успех: create() вызван c распарсенным payload и command_id как ключом
               идемпотентности; результат сохранён в guard.set_result.
        Нежелательное поведение: неверный ключ идемпотентности, потеря полей payload,
               отсутствие кэширования результата (повтор создаст дубль платежа).
        """
        payment_service = make_payment_service()
        guard = make_fresh_guard()
        idempotency_service = make_idempotency_service(guard)

        created = Payment(
            id=uuid4(),
            idempotency_key="12345678-1234-5678-1234-567812345678",
            amount=Decimal("150.00"),
            currency="USD",
            status=PaymentStatus.PROCESSING,
            customer_id="cust_12345",
            description="Integration Test",
            created_at=datetime.now(timezone.utc),
        )
        payment_service.create.return_value = created

        await router.handle(
            command_type="payment.process",
            msg=make_command(),
            payment_service=payment_service,
            idempotency_service=idempotency_service,
        )

        payment_service.create.assert_called_once()
        payload, key = payment_service.create.call_args[0]
        assert key == "12345678-1234-5678-1234-567812345678"
        assert payload.amount == Decimal("150.00")
        assert payload.currency == "USD"
        assert payload.customer_id == "cust_12345"
        assert payload.description == "Integration Test"
        guard.set_result.assert_called_once()

    @pytest.mark.asyncio
    async def test_idempotency_hit_skips_create(self):
        """
        Проверяем: команда с ключом, для которого уже есть кэшированный результат.
        Успех: возвращается кэшированный ответ, create() НЕ вызывается,
               set_result не трогается.
        Нежелательное поведение: повторное создание платежа по дублю команды
               (нарушение идемпотентности при redelivery из Kafka).
        """
        payment_service = make_payment_service()
        guard = MagicMock()
        guard.has_cached_result = True
        guard.cached_status_code = 201
        guard.cached_response = {"id": "payment-123", "status": "PROCESSING"}
        idempotency_service = make_idempotency_service(guard)

        result = await router.handle(
            command_type="payment.process",
            msg=make_command(),
            payment_service=payment_service,
            idempotency_service=idempotency_service,
        )

        assert result == {"id": "payment-123", "status": "PROCESSING"}
        payment_service.create.assert_not_called()
        guard.set_result.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_payload_raises_validation_error(self):
        """
        Проверяем: команда с некорректными данными (отрицательная сумма).
        Успех: обработчик поднимает ошибку валидации Pydantic, до create() не доходит.
               Исключение приводит к NACK и переигрыванию/разбору, а не к тихому созданию.
        Нежелательное поведение: молчаливое проглатывание невалидной команды.
        """
        payment_service = make_payment_service()
        guard = make_fresh_guard()
        idempotency_service = make_idempotency_service(guard)

        bad = make_command(amount="-10.00")

        with pytest.raises(ValidationError):
            await router.handle(
                command_type="payment.process",
                msg=bad,
                payment_service=payment_service,
                idempotency_service=idempotency_service,
            )
        payment_service.create.assert_not_called()


# ---------------------------------------------------------------------------
# Неизвестные команды
# ---------------------------------------------------------------------------


class TestUnknownCommand:
    """Маршрутизация команды, для которой не зарегистрирован обработчик."""

    @pytest.mark.asyncio
    async def test_unknown_command_raises(self):
        """
        Проверяем: тип команды, для которого не зарегистрирован обработчик.
        Успех: handle() поднимает UnknownCommandError (невосстановимо), платежи не трогаются.
        Нежелательное поведение: молчаливый возврат None и ACK -> команда теряется бесследно.
        """
        payment_service = make_payment_service()
        idempotency_service = MagicMock()

        with pytest.raises(UnknownCommandError):
            await router.handle(
                command_type="unknown.command",
                msg={},
                payment_service=payment_service,
                idempotency_service=idempotency_service,
            )

        payment_service.create.assert_not_called()


# ---------------------------------------------------------------------------
# Политика ошибок и DLQ (process_command_message)
# ---------------------------------------------------------------------------


class TestErrorClassification:
    """
    Классификация ошибок консьюмера: невосстановимые (битые данные, неизвестная
    команда) уводятся в DLQ, восстановимые (временная недоступность инфраструктуры)
    переигрываются через NACK.
    """

    def test_non_retriable_set_contents(self):
        """
        Проверяем: набор невосстановимых ошибок.
        Успех: битые данные и неизвестная команда классифицированы как невосстановимые,
               а недоступность Redis (временный сбой) - нет.
        Нежелательное поведение: временный сбой уходит в DLQ вместо ретрая,
               либо poison-сообщение бесконечно ретраится вместо DLQ.
        """
        for exc_type in (
            ValidationError,
            UnknownCommandError,
            IdempotencyKeyPayloadMismatchError,
            IdempotencyStateInconsistencyError,
        ):
            assert exc_type in NON_RETRIABLE_ERRORS

        assert RedisUnavailableError not in NON_RETRIABLE_ERRORS


class TestSendCommandToDlq:
    """Публикация невосстановимой команды в DLQ и поведение при сбое самой публикации."""

    @pytest.mark.asyncio
    async def test_publishes_to_dlq_with_diagnostic_headers(self, monkeypatch):
        """
        Проверяем: публикацию невосстановимой команды в DLQ.
        Успех: сообщение уходит в DLQ-топик с заголовками об ошибке и исходном offset.
        Нежелательное поведение: потеря команды или отсутствие диагностики для разбора.
        """
        publish = AsyncMock()
        monkeypatch.setattr(consumer_module.broker, "publish", publish)

        msg = make_command()
        error = UnknownCommandError("foo.bar")

        await send_command_to_dlq(msg, error, make_kafka_message(offset=7, partition=3))

        publish.assert_awaited_once()
        assert publish.await_args.args[0] is msg
        assert (
            publish.await_args.kwargs["topic"]
            == consumer_module.settings.KAFKA_COMMANDS_DLQ_TOPIC
        )
        headers = publish.await_args.kwargs["headers"]
        assert headers["x-error-type"] == "UnknownCommandError"
        assert headers["x-original-offset"] == "7"
        assert headers["x-original-partition"] == "3"

    @pytest.mark.asyncio
    async def test_reraises_when_dlq_publish_fails(self, monkeypatch):
        """
        Проверяем: сбой публикации в DLQ.
        Успех: исключение пробрасывается (сработает NACK), команда не теряется.
        Нежелательное поведение: тихое проглатывание -> команда потеряна и не в DLQ.
        """
        publish = AsyncMock(side_effect=RuntimeError("kafka down"))
        monkeypatch.setattr(consumer_module.broker, "publish", publish)

        with pytest.raises(RuntimeError):
            await send_command_to_dlq(make_command(), UnknownCommandError("x"), None)


class TestProcessCommandMessage:
    """
    Политика ошибок граничной функции process_command_message:
    невосстановимое -> DLQ и ACK; временный сбой -> проброс исключения -> NACK.
    """

    @pytest.mark.asyncio
    async def test_invalid_payload_routed_to_dlq(self, monkeypatch):
        """
        Проверяем: команда с невалидным payload (отрицательная сумма).
        Успех: НЕ поднимает исключение (иначе poison pill -> бесконечный NACK),
               а уводит команду в DLQ; create() не вызывается.
        Нежелательное поведение: тихая потеря или бесконечное переигрывание.
        """
        publish = AsyncMock()
        monkeypatch.setattr(consumer_module.broker, "publish", publish)

        payment_service = make_payment_service()
        idempotency_service = make_idempotency_service(make_fresh_guard())

        await process_command_message(
            make_command(amount="-10.00"),
            payment_service,
            idempotency_service,
            make_kafka_message(),
        )

        publish.assert_awaited_once()
        assert (
            publish.await_args.kwargs["topic"]
            == consumer_module.settings.KAFKA_COMMANDS_DLQ_TOPIC
        )
        assert publish.await_args.kwargs["headers"]["x-error-type"] == "ValidationError"
        payment_service.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_unknown_command_routed_to_dlq(self, monkeypatch):
        """
        Проверяем: команда неизвестного типа.
        Успех: уходит в DLQ с типом ошибки UnknownCommandError, create() не вызывается.
        Нежелательное поведение: тихая потеря (старое поведение return None + ACK).
        """
        publish = AsyncMock()
        monkeypatch.setattr(consumer_module.broker, "publish", publish)

        payment_service = make_payment_service()
        msg = make_command()
        msg["metadata"]["commandType"] = "totally.unknown"

        await process_command_message(
            msg, payment_service, MagicMock(), make_kafka_message()
        )

        publish.assert_awaited_once()
        assert (
            publish.await_args.kwargs["headers"]["x-error-type"] == "UnknownCommandError"
        )
        payment_service.create.assert_not_called()

    @pytest.mark.asyncio
    async def test_invalid_metadata_routed_to_dlq(self, monkeypatch):
        """
        Проверяем: битые метаданные (нельзя даже определить тип команды).
        Успех: команда уходит в DLQ (ValidationError), а не теряется молча.
        Нежелательное поведение: прежний тихий return -> потеря сообщения.
        """
        publish = AsyncMock()
        monkeypatch.setattr(consumer_module.broker, "publish", publish)

        # нет commandId/timestamp/source -> метаданные не валидируются
        msg = {"metadata": {"commandType": "payment.process"}, "data": {}}

        await process_command_message(
            msg, make_payment_service(), MagicMock(), make_kafka_message()
        )

        publish.assert_awaited_once()
        assert publish.await_args.kwargs["headers"]["x-error-type"] == "ValidationError"

    @pytest.mark.asyncio
    async def test_transient_error_reraised_not_dlq(self, monkeypatch):
        """
        Проверяем: временный сбой инфраструктуры (Redis недоступен) при обработке.
        Успех: исключение всплывает (сработает NACK -> переигрывание), в DLQ НЕ уходит.
        Нежелательное поведение: восстановимая команда уводится в DLQ и не переигрывается.
        """
        publish = AsyncMock()
        monkeypatch.setattr(consumer_module.broker, "publish", publish)

        payment_service = make_payment_service()
        idempotency_service = make_raising_idempotency_service(RedisUnavailableError())

        with pytest.raises(RedisUnavailableError):
            await process_command_message(
                make_command(),
                payment_service,
                idempotency_service,
                make_kafka_message(),
            )

        publish.assert_not_awaited()
