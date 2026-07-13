"""
Контракт-тесты: конверты payment_service против JSON Schema из contracts/.

contracts/ - единственный источник истины по формату сообщений. Юнит-тесты выше
проверяют НАШУ трактовку контракта; здесь мы сверяемся с самим контрактом, чтобы
дрейф схемы (её правят и другие сервисы) ловился до продакшена, а не оркестратором
в рантайме.

Проверяются:
  - payment.completed / payment.failed -> payments/payment-result.v1.schema.json;
  - команда payment.process            -> payments/process.v1.schema.json.

Схемы читаются с диска по относительному пути от корня репозитория и связываются
через referencing (внутри контрактов $ref идут на ../envelope/*).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

import json
from pathlib import Path

import pytest

# jsonschema - dev-зависимость (см. pyproject.toml). Если .venv собран без неё
# (poetry lock/install ещё не прогоняли) - контракт-тесты пропускаем, но не
# маскируем: остальные тесты падают честно, а здесь просто нечем валидировать.
jsonschema = pytest.importorskip(
    "jsonschema",
    reason="jsonschema не установлен в .venv: poetry install после добавления dev-зависимости",
)

from jsonschema import Draft202012Validator  # noqa: E402
from referencing import Registry, Resource  # noqa: E402
from referencing.exceptions import NoSuchResource  # noqa: E402
from referencing.jsonschema import DRAFT202012  # noqa: E402

from unittest.mock import AsyncMock  # noqa: E402
from uuid import uuid4  # noqa: E402
from decimal import Decimal  # noqa: E402
from datetime import datetime, timezone  # noqa: E402

from app.domain.outbox import OutboxEvent  # noqa: E402
from app.domain.payments import Payment, PaymentStatus  # noqa: E402
from app.application.ports.dto.events import EventEnvelope  # noqa: E402
from app.application.services.payment_service import PaymentService  # noqa: E402
from app.entrypoints.http.schemas.payments import PaymentResponse  # noqa: E402
from app.entrypoints.messaging.schemas.commands import ProcessPaymentCommand  # noqa: E402
from app.infrastructure.brokers.adapters import (  # noqa: E402
    CorrelationEnrichingPublisher,
    KafkaOutboxPublisher,
)

# src/tests/test_contracts.py -> tests -> src -> payment_service -> корень репозитория
REPO_ROOT = Path(__file__).resolve().parents[3]
CONTRACTS_ROOT = REPO_ROOT / "contracts"

# в образе сервиса contracts/ рядом нет: тесты контрактов гоняются из монорепы
pytestmark = pytest.mark.skipif(
    not CONTRACTS_ROOT.is_dir(),
    reason=f"каталог контрактов не найден: {CONTRACTS_ROOT}",
)


# ---------------------------------------------------------------------------
# Загрузка схем
# ---------------------------------------------------------------------------


def _load_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _retrieve(uri: str) -> Resource:
    """
    Резолвер $ref для схем контрактов.

    $id в контрактах - не URI ("payflow.contracts.payments.payment-result.v1"),
    поэтому относительный $ref ("../envelope/failure.v1.schema.json") не даёт
    полноценного абсолютного адреса. Ищем схему по имени файла внутри contracts/ -
    имена уникальны и от способа склейки базового URI это не зависит.
    """
    name = uri.rsplit("/", 1)[-1]
    matches = sorted(CONTRACTS_ROOT.rglob(name))
    if not matches:
        raise NoSuchResource(ref=uri)
    return Resource.from_contents(
        _load_schema(matches[0]), default_specification=DRAFT202012
    )


def validator_for(relative_path: str) -> Draft202012Validator:
    """Валидатор конкретной схемы контракта (путь от contracts/)."""
    return Draft202012Validator(
        _load_schema(CONTRACTS_ROOT / relative_path),
        registry=Registry(retrieve=_retrieve),
        format_checker=Draft202012Validator.FORMAT_CHECKER,
    )


def errors(validator: Draft202012Validator, instance: dict) -> list[str]:
    """Человекочитаемый список нарушений контракта (пустой - конверт валиден)."""
    return [
        f"{'/'.join(str(p) for p in e.absolute_path)}: {e.message}"
        for e in validator.iter_errors(instance)
    ]


@pytest.fixture(scope="module")
def result_validator() -> Draft202012Validator:
    return validator_for("payments/payment-result.v1.schema.json")


@pytest.fixture(scope="module")
def command_validator() -> Draft202012Validator:
    return validator_for("payments/process.v1.schema.json")


# ---------------------------------------------------------------------------
# Примеры конвертов
# ---------------------------------------------------------------------------

SAGA_ID = "11111111-2222-3333-4444-555555555555"
BUSINESS_KEY = "order-42"
COMMAND_ID = "66666666-7777-8888-9999-000000000000"

CORRELATION = {
    "sagaId": SAGA_ID,
    "businessKey": BUSINESS_KEY,
    "commandId": COMMAND_ID,
}


def event_example(event_type: str, failure: dict | None = None) -> dict:
    """Эталонный конверт события платежа (metadata snake_case, data camelCase)."""
    data = {
        "id": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        "customerId": "cust_12345",
        "amount": "150.00",
        "currency": "USD",
        "status": event_type.split(".")[1].upper(),
    }
    if failure is not None:
        data["failure"] = failure
    return {
        "metadata": {
            "event_id": "019f62a4-6580-773b-9d62-a6665285ff73",
            "event_type": event_type,
            "version": "1.0",
            "timestamp": "2026-07-15T10:00:00Z",
            "source": "payment-service",
            "correlation": CORRELATION,
        },
        "data": data,
    }


def command_example() -> dict:
    """Эталонный конверт команды payment.process (metadata и data camelCase)."""
    return {
        "metadata": {
            "commandId": COMMAND_ID,
            "commandType": "payment.process",
            "version": "1.0",
            "timestamp": "2026-07-15T10:00:00Z",
            "source": "orchestrator-service",
            "sagaId": SAGA_ID,
            "businessKey": BUSINESS_KEY,
        },
        "data": {
            "amount": "150.00",
            "currency": "USD",
            "customerId": "cust_12345",
            "description": "Order payment",
        },
    }


# ---------------------------------------------------------------------------
# События payment.completed / payment.failed
# ---------------------------------------------------------------------------


class TestPaymentResultContract:
    """payments.events: то, по чему оркестратор двигает сагу."""

    def test_completed_example_is_valid(self, result_validator):
        """
        Проверяем: эталонный payment.completed против payment-result.v1.
        Успех: нарушений схемы нет.
        Нежелательное поведение: наш эталон разошёлся с контрактом - значит,
               и реальные события разойдутся.
        """
        assert errors(result_validator, event_example("payment.completed")) == []

    def test_failed_with_failure_is_valid(self, result_validator):
        """
        Проверяем: payment.failed с обязательным блоком failure.
        Успех: нарушений схемы нет, retriable - булев.
        Нежелательное поведение: контракт не принимает наши коды отказа.
        """
        envelope = event_example(
            "payment.failed",
            failure={
                "code": "provider_unavailable",
                "message": "circuit breaker is open",
                "retriable": True,
            },
        )
        assert errors(result_validator, envelope) == []

    def test_failed_without_failure_is_rejected(self, result_validator):
        """
        Проверяем: payment.failed без блока failure.
        Успех: схема ОТКЛОНЯЕТ такой конверт (негативный тест: доказывает, что
               валидатор действительно подключён и требование failure работает).
        Нежелательное поведение: тест зелёный на любом мусоре - контракт не проверяется.
        """
        found = errors(result_validator, event_example("payment.failed"))
        assert any("failure" in message for message in found), found

    def test_failure_requires_all_three_fields(self, result_validator):
        """
        Проверяем: неполный блок failure (нет retriable).
        Успех: схема отклоняет конверт - оркестратору не по чему принимать решение.
        Нежелательное поведение: частичный failure проезжает и ломает потребителя.
        """
        envelope = event_example(
            "payment.failed",
            failure={"code": "payment_declined", "message": "declined"},
        )
        found = errors(result_validator, envelope)
        assert any("retriable" in message for message in found), found

    def test_event_without_correlation_is_valid(self, result_validator):
        """
        Проверяем: событие платежа вне саги (HTTP API) - без metadata.correlation.
        Успех: конверт валиден (correlation опционален, оркестратор такое игнорирует).
        Нежелательное поведение: платежи из HTTP API перестают публиковаться
               или не проходят валидацию у потребителей.
        """
        envelope = event_example("payment.completed")
        envelope["metadata"].pop("correlation")
        assert errors(result_validator, envelope) == []


# ---------------------------------------------------------------------------
# Команда payment.process
# ---------------------------------------------------------------------------


class TestProcessCommandContract:
    """payments.commands: то, что присылает оркестратор."""

    def test_command_example_is_valid(self, command_validator):
        """
        Проверяем: эталонная команда payment.process против process.v1.
        Успех: нарушений схемы нет.
        Нежелательное поведение: расхождение эталона с контрактом оркестратора.
        """
        assert errors(command_validator, command_example()) == []

    def test_command_requires_saga_correlation(self, command_validator):
        """
        Проверяем: команда без sagaId/businessKey.
        Успех: схема отклоняет - для команды саги корреляция обязательна.
        Нежелательное поведение: команда без корреляции считается валидной,
               ответное событие уходит без echo и сага висит до таймаута.
        """
        command = command_example()
        command["metadata"].pop("sagaId")
        command["metadata"].pop("businessKey")

        found = errors(command_validator, command)
        assert any("sagaId" in message for message in found), found

    def test_our_model_parses_contract_valid_command(self, command_validator):
        """
        Проверяем: схема-валидную команду принимает НАША Pydantic-модель.
        Успех: ProcessPaymentCommand разбирает конверт и извлекает корреляцию
               и данные платежа (camelCase-алиасы совпадают с контрактом).
        Нежелательное поведение: контракт валиден, а консьюмер уводит команду
               в DLQ по ValidationError - расхождение имён/типов полей.
        """
        raw = command_example()
        assert errors(command_validator, raw) == []

        command = ProcessPaymentCommand.model_validate(raw)

        assert str(command.metadata.command_id) == COMMAND_ID
        assert str(command.metadata.saga_id) == SAGA_ID
        assert command.metadata.business_key == BUSINESS_KEY
        assert command.data.amount == Decimal("150.00")
        assert command.data.currency == "USD"
        assert command.data.customer_id == "cust_12345"


# ---------------------------------------------------------------------------
# Реальный конверт, собранный production-кодом
# ---------------------------------------------------------------------------


async def publish_real_envelope(payment: Payment, event_type: str) -> dict:
    """
    Прогоняет платёж по реальному пути публикации и возвращает JSON, ушедший в Kafka:
    PaymentService._create_status_event -> OutboxEvent -> EventEnvelope ->
    CorrelationEnrichingPublisher -> KafkaOutboxPublisher -> producer.

    Payload собирает САМ _create_status_event (никаких копий его логики в тесте:
    копия уже однажды разъехалась с продом и маскировала snake_case-баг).
    """
    outbox_repo = AsyncMock()
    service = PaymentService(
        payment_repository=AsyncMock(),
        uow=AsyncMock(),
        payment_provider=AsyncMock(),
        outbox_repository=outbox_repo,
    )
    await service._create_status_event(payment)
    event: OutboxEvent = outbox_repo.create.call_args.args[0]
    # тип события в тесте задаётся явно (статус платежа мог бы дать другой суффикс)
    event = event.model_copy(update={"event_type": event_type})

    store = AsyncMock()
    store.resolve_for_payment.return_value = CORRELATION
    producer = AsyncMock()
    publisher = CorrelationEnrichingPublisher(
        KafkaOutboxPublisher(producer, topic="payments.events"), store
    )

    await publisher.publish(EventEnvelope.from_outbox_event(event))

    _, kwargs = producer.send_and_wait.call_args
    return json.loads(kwargs["value"].decode("utf-8"))


class TestRealEnvelopeAgainstContract:
    """
    Самое ценное здесь: валидируем не выдуманный пример, а конверт, который
    production-код реально кладёт в Kafka.
    """

    async def test_real_completed_envelope_metadata_matches_contract(
        self, result_validator
    ):
        """
        Проверяем: metadata реального payment.completed (snake_case + correlation).
        Успех: обязательные поля метаданных и echo-блок корреляции на месте.
        Нежелательное поведение: событие без event_id/version/source - потребитель
               не может дедуплицировать и версионировать сообщения.
        """
        payment = Payment(
            idempotency_key=COMMAND_ID,
            amount=Decimal("150.00"),
            currency="USD",
            status=PaymentStatus.COMPLETED,
            customer_id="cust_12345",
        )

        raw = await publish_real_envelope(payment, "payment.completed")

        metadata = raw["metadata"]
        assert {"event_id", "event_type", "version", "timestamp", "source"} <= set(
            metadata
        )
        assert metadata["event_type"] == "payment.completed"
        assert metadata["correlation"] == CORRELATION

    async def test_real_completed_envelope_data_matches_contract(self, result_validator):
        """
        Проверяем: data реального payment.completed против payment-result.v1.
        Успех: конверт валиден по схеме (в частности, есть обязательный customerId).
        Нежелательное поведение: события уезжают в Kafka в snake_case (регресс
               by_alias=True в _create_status_event, исправлен 2026-07-15).
        """
        payment = Payment(
            idempotency_key=COMMAND_ID,
            amount=Decimal("150.00"),
            currency="USD",
            status=PaymentStatus.COMPLETED,
            customer_id="cust_12345",
        )

        raw = await publish_real_envelope(payment, "payment.completed")

        assert errors(result_validator, raw) == []
