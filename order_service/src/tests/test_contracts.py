"""
Контракт-тесты order_service: конверты против JSON Schema из contracts/.

contracts/ - единственный источник истины по формату сообщений (ADR-007). Юнит-тесты
выше проверяют НАШУ трактовку; здесь мы сверяемся с самим контрактом, чтобы дрейф
схемы (её правят и другие сервисы) ловился до продакшена, а не оркестратором в рантайме.

Проверяются:
  - order.created  (что мы публикуем) -> orders/order-created.v1.schema.json;
  - saga.* финализация (что мы потребляем) -> orders/saga-finished.v1.schema.json.

Схемы читаются с диска и связываются через referencing (внутри контрактов $ref
идут на ../envelope/*).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

# jsonschema - dev-зависимость. Если .venv собран без неё (poetry install ещё не
# прогоняли) - контракт-тесты пропускаем, но не маскируем: остальные тесты падают
# честно, а здесь просто нечем валидировать.
jsonschema = pytest.importorskip(
    "jsonschema",
    reason="jsonschema не установлен в .venv: poetry install после добавления dev-зависимости",
)

from jsonschema import Draft202012Validator  # noqa: E402
from referencing import Registry, Resource  # noqa: E402
from referencing.exceptions import NoSuchResource  # noqa: E402
from referencing.jsonschema import DRAFT202012  # noqa: E402

from tests.conftest import make_order_create, make_saga_event  # noqa: E402

# src/tests/test_contracts.py -> tests -> src -> order_service -> корень репозитория
REPO_ROOT = Path(__file__).resolve().parents[3]
CONTRACTS_ROOT = REPO_ROOT / "contracts"

# в образе сервиса contracts/ рядом нет: контракт-тесты гоняются из монорепы
pytestmark = pytest.mark.skipif(
    not CONTRACTS_ROOT.is_dir(),
    reason=f"каталог контрактов не найден: {CONTRACTS_ROOT}",
)


def _load_schema(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _retrieve(uri: str) -> Resource:
    """
    Резолвер $ref для схем контрактов.

    $id в контрактах - не URI ("payflow.contracts.orders.order-created.v1"),
    поэтому относительный $ref ("../envelope/event-metadata.v1.schema.json") не даёт
    абсолютного адреса. Ищем схему по имени файла внутри contracts/ - имена уникальны.
    """
    name = uri.rsplit("/", 1)[-1]
    matches = sorted(CONTRACTS_ROOT.rglob(name))
    if not matches:
        raise NoSuchResource(ref=uri)
    return Resource.from_contents(
        _load_schema(matches[0]), default_specification=DRAFT202012
    )


def validator_for(relative_path: str) -> Draft202012Validator:
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
def created_validator() -> Draft202012Validator:
    return validator_for("orders/order-created.v1.schema.json")


@pytest.fixture(scope="module")
def finished_validator() -> Draft202012Validator:
    return validator_for("orders/saga-finished.v1.schema.json")


# ---------------------------------------------------------------------------
# order.created: то, что мы публикуем (реальный конверт из OrderService)
# ---------------------------------------------------------------------------


class TestOrderCreatedContract:
    async def test_real_created_envelope_matches_contract(
        self, order_service, session, created_validator
    ):
        """
        Проверяем: конверт order.created, собранный production-кодом OrderService,
            против orders/order-created.v1.
        Успех: нарушений схемы нет (metadata + data.orderId/userId/items/totalAmount/currency).
        Нежелательное поведение: реальное событие расходится с контрактом - сага
            стартует на мусоре или оркестратор роняет его в DLQ.
        """
        await order_service.create_order(uuid.uuid4(), make_order_create())

        raw = session.outbox[0].payload
        assert errors(created_validator, raw) == []

    def test_created_without_total_amount_is_rejected(self, created_validator):
        """
        Проверяем: order.created без обязательного totalAmount.
        Успех: схема ОТКЛОНЯЕТ конверт (негативный тест: доказывает, что валидатор
            подключён, а не зеленит любой вход).
        Нежелательное поведение: тест зелёный на любом мусоре - контракт не проверяется.
        """
        raw = {
            "metadata": {
                "event_id": str(uuid.uuid4()),
                "event_type": "order.created",
                "version": "1.0",
                "timestamp": "2026-07-15T10:00:00Z",
                "source": "order-service",
            },
            "data": {
                "orderId": str(uuid.uuid4()),
                "userId": str(uuid.uuid4()),
                "items": [{"productId": "sku-1", "quantity": 1, "price": "10.00"}],
                "currency": "RUB",
            },
        }
        found = errors(created_validator, raw)
        assert any("totalAmount" in message for message in found), found


# ---------------------------------------------------------------------------
# saga.completed / saga.cancelled / saga.failed: то, что мы потребляем
# ---------------------------------------------------------------------------


class TestSagaFinishedContract:
    @pytest.mark.parametrize(
        "event_type, status",
        [
            ("saga.completed", "COMPLETED"),
            ("saga.cancelled", "CANCELLED"),
            ("saga.failed", "FAILED"),
        ],
    )
    def test_finalization_fixtures_match_contract(
        self, finished_validator, event_type, status
    ):
        """
        Проверяем: фикстуры финализации (по которым тестируется консюмер) валидны
            по orders/saga-finished.v1.
        Успех: нарушений схемы нет - значит, тесты консюмера гоняются на конверте,
            который реально пришлёт оркестратор.
        Нежелательное поведение: консюмер зелёный на фикстуре, которой не бывает в шине.
        """
        raw = make_saga_event(event_type, uuid.uuid4(), status=status)
        assert errors(finished_validator, raw) == []

    def test_finalization_without_status_is_rejected(self, finished_validator):
        """
        Проверяем: событие финализации без обязательного data.status.
        Успех: схема отклоняет конверт.
        Нежелательное поведение: неполное событие проезжает валидацию.
        """
        raw = make_saga_event("saga.completed", uuid.uuid4())
        raw["data"].pop("status")
        found = errors(finished_validator, raw)
        assert any("status" in message for message in found), found
