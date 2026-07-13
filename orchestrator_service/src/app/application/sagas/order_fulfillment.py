"""
Определение саги оформления заказа (order-fulfillment).

Флоу v2 (docs/saga-design.md, 9.8):
  reserve (compensatable, TTL) -> charge (pivot, ожидание оплаты пользователем
  с бизнес-дедлайном) -> commit_reservation (retriable: списать товар и снять резерв).

Это ДАННЫЕ для generic-исполнителя, а не код с ветвлениями. Никаких глобалей:
реестр собирает create_saga_registry(settings) и отдаёт DI (APP scope).
"""

from typing import Any

from app.core.settings import Settings
from app.domain.definitions import (
    CompensationSpec,
    EventBinding,
    SagaDefinition,
    SagaRegistry,
    SagaStep,
    StepOutcome,
    TimeoutPolicy,
)

ORDER_FULFILLMENT = "order-fulfillment"


def _data(message: dict[str, Any]) -> dict[str, Any]:
    data = message.get("data")
    return data if isinstance(data, dict) else {}


def _business_key_from_order_created(message: dict[str, Any]) -> str | None:
    order_id = _data(message).get("orderId")
    return str(order_id) if order_id else None


def _business_key_from_correlation(message: dict[str, Any]) -> str | None:
    """Echo-корреляция участников: metadata.correlation.businessKey"""
    metadata = message.get("metadata")
    if not isinstance(metadata, dict):
        return None
    correlation = metadata.get("correlation")
    if not isinstance(correlation, dict):
        return None
    key = correlation.get("businessKey")
    return str(key) if key else None


def _build_payload(message: dict[str, Any]) -> dict[str, Any]:
    """Минимальный снапшот для команд и компенсаций (решение итерации 3, п.4):
    без email, токенов и платёжных данных."""
    data = _data(message)
    return {
        "orderId": data.get("orderId"),
        "userId": data.get("userId"),
        "items": data.get("items", []),
        "totalAmount": data.get("totalAmount"),
        "currency": data.get("currency"),
    }


def _reserve_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"productId": item.get("productId"), "quantity": item.get("quantity")}
        for item in payload.get("items", [])
        if isinstance(item, dict)
    ]


def _finished_data(
    business_key: str,
    payload: dict[str, Any],
    status: str,
    reason: str | None,
) -> dict[str, Any]:
    data: dict[str, Any] = {"orderId": business_key, "status": status}
    if reason:
        data["reason"] = reason
    return data


def build_order_fulfillment_definition(settings: Settings) -> SagaDefinition:
    ttl = settings.RESERVATION_TTL_SECONDS
    payment_wait = settings.PAYMENT_WAIT_TIMEOUT_SECONDS
    buffer = settings.RESERVATION_TTL_BUFFER_SECONDS
    # fail-fast на старте: гонка "оплата успела, резерв истёк" исключается
    # конфигурацией (решение итерации 3, п.1)
    if ttl < payment_wait + buffer:
        raise ValueError(
            "нарушен инвариант резерва: RESERVATION_TTL_SECONDS "
            f"({ttl}) < PAYMENT_WAIT_TIMEOUT_SECONDS ({payment_wait}) "
            f"+ RESERVATION_TTL_BUFFER_SECONDS ({buffer})"
        )

    def _reserve_data(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "orderId": payload.get("orderId"),
            "items": _reserve_items(payload),
            "ttlSeconds": ttl,
        }

    def _charge_data(payload: dict[str, Any]) -> dict[str, Any]:
        return {
            "amount": payload.get("totalAmount"),
            "currency": payload.get("currency"),
            # customerId - это плательщик (пользователь), а не заказ;
            # связь платежа с сагой идёт ТОЛЬКО через metadata.correlation (echo)
            "customerId": payload.get("userId"),
            "description": f"OrderFlow: оплата заказа {payload.get('orderId')}",
        }

    def _commit_data(payload: dict[str, Any]) -> dict[str, Any]:
        return {"orderId": payload.get("orderId")}

    def _cancel_data(payload: dict[str, Any]) -> dict[str, Any]:
        return {"orderId": payload.get("orderId"), "reason": "saga compensation"}

    steps = (
        SagaStep(
            name="reserve",
            command_type="inventory.reserve",
            command_topic=settings.KAFKA_INVENTORY_COMMANDS_TOPIC,
            build_command_data=_reserve_data,
            timeout_seconds=settings.SAGA_DEFAULT_STEP_TIMEOUT_SECONDS,
            max_attempts=settings.SAGA_MAX_STEP_ATTEMPTS,
            compensation=CompensationSpec(
                command_type="inventory.cancel_reservation",
                command_topic=settings.KAFKA_INVENTORY_COMMANDS_TOPIC,
                build_command_data=_cancel_data,
            ),
        ),
        SagaStep(
            name="charge",
            command_type="payment.process",
            command_topic=settings.KAFKA_PAYMENTS_COMMANDS_TOPIC,
            build_command_data=_charge_data,
            # human-in-the-loop: ждём оплату пользователем; молчание дольше
            # дедлайна - бизнес-исход (не оплатил), а не технический сбой
            timeout_seconds=float(payment_wait),
            on_timeout=TimeoutPolicy.BUSINESS_FAIL,
            max_attempts=settings.SAGA_MAX_STEP_ATTEMPTS,
            pivot=True,
        ),
        SagaStep(
            name="commit_reservation",
            command_type="inventory.commit_reservation",
            command_topic=settings.KAFKA_INVENTORY_COMMANDS_TOPIC,
            build_command_data=_commit_data,
            timeout_seconds=settings.SAGA_DEFAULT_STEP_TIMEOUT_SECONDS,
            max_attempts=settings.SAGA_MAX_STEP_ATTEMPTS,
        ),
    )

    event_bindings = {
        "inventory.reserved": EventBinding(
            "reserve", StepOutcome.SUCCESS, _business_key_from_correlation
        ),
        "inventory.reserve-failed": EventBinding(
            "reserve", StepOutcome.FAILED, _business_key_from_correlation
        ),
        "payment.completed": EventBinding(
            "charge", StepOutcome.SUCCESS, _business_key_from_correlation
        ),
        "payment.failed": EventBinding(
            "charge", StepOutcome.FAILED, _business_key_from_correlation
        ),
        "inventory.reservation-committed": EventBinding(
            "commit_reservation", StepOutcome.SUCCESS, _business_key_from_correlation
        ),
        "inventory.commit-failed": EventBinding(
            "commit_reservation", StepOutcome.FAILED, _business_key_from_correlation
        ),
        "inventory.reservation-cancelled": EventBinding(
            "reserve", StepOutcome.COMPENSATED, _business_key_from_correlation
        ),
    }

    return SagaDefinition(
        saga_type=ORDER_FULFILLMENT,
        start_event_type="order.created",
        business_key_from_start=_business_key_from_order_created,
        build_payload=_build_payload,
        steps=steps,
        event_bindings=event_bindings,
        events_topic=settings.KAFKA_ORDERS_EVENTS_TOPIC,
        build_finished_data=_finished_data,
    )


def create_saga_registry(settings: Settings) -> SagaRegistry:
    """Все известные оркестратору саги. Новая бизнес-сага = новый build_* + строка здесь."""
    return SagaRegistry((build_order_fulfillment_definition(settings),))
