"""
Тесты InventoryService - ядро трёх команд саги (reserve/commit/cancel).

Формат документации у каждого теста единый:
    Проверяем: какое поведение под контролем.
    Успех: что должно произойти, чтобы тест был зелёным.
    Нежелательное поведение: что мы этим тестом ловим (ради чего он существует).

Инварианты под контролем:
  1) успех/бизнес-отказ резерва идут в outbox готовым конвертом с echo-корреляцией;
  2) дубль команды НЕ применяет эффект дважды и переиздаёт тот же ответ;
  3) commit на неактивном резерве - commit-failed(reservation_expired, retriable=false);
  4) cancel идемпотентен и коммутативен: отмена отсутствующего резерва - успех.
"""

import uuid
from datetime import timedelta

import pytest

from app.application.ports.dto.commands import CommandCorrelation, ReserveCommand
from app.domain.reservations import ReservationItem, ReservationStatus, utc_now
from tests.conftest import (
    make_cancel,
    make_commit,
    make_reserve,
    make_reservation,
)


# --- reserve: успех ---


async def test_reserve_success_moves_available_to_reserved(service, stock, outbox):
    """
    Проверяем: успешный резерв блокирует товар (available -> reserved) и шлёт событие.
    Успех: available 100->97, reserved 0->3, в outbox один inventory.reserved.
    Нежелательное поведение: резерв "списывает" товар или не публикует событие.
    """
    order_id = uuid.uuid7()

    await service.reserve(make_reserve(order_id, {"sku-1": 3}))

    assert stock.available("sku-1") == 97
    assert stock.reserved("sku-1") == 3
    assert outbox.event_types == ["inventory.reserved"]
    assert outbox.last_data["orderId"] == str(order_id)
    assert "expiresAt" in outbox.last_data


async def test_reserve_success_echoes_correlation(service, outbox):
    """
    Проверяем: в metadata.correlation ответа возвращаются значения команды (echo).
    Успех: correlation.commandId и businessKey совпадают с командой, sagaId присутствует.
    Нежелательное поведение: участник теряет или переписывает корреляцию саги.
    """
    order_id = uuid.uuid7()
    command_id = str(uuid.uuid7())

    await service.reserve(make_reserve(order_id, {"sku-1": 1}, command_id=command_id))

    metadata = outbox.last_payload["metadata"]
    assert metadata["event_type"] == "inventory.reserved"
    assert metadata["source"] == "inventory-service"
    assert metadata["correlation"]["commandId"] == command_id
    assert metadata["correlation"]["businessKey"] == str(order_id)
    assert metadata["correlation"]["sagaId"]


async def test_reserve_uses_default_ttl_when_absent(service, outbox, settings):
    """
    Проверяем: отсутствие ttlSeconds в команде подменяется дефолтом из настроек.
    Успех: expiresAt лежит около now + RESERVATION_DEFAULT_TTL_SECONDS.
    Нежелательное поведение: None-ttl роняет резерв или даёт бессрочную блокировку.
    """
    order_id = uuid.uuid7()

    await service.reserve(make_reserve(order_id, {"sku-1": 1}, ttl_seconds=None))

    expires_at = outbox.last_data["expiresAt"]
    from datetime import datetime

    parsed = datetime.fromisoformat(expires_at)
    delta = parsed.replace(tzinfo=None) - utc_now()
    assert abs(delta.total_seconds() - settings.RESERVATION_DEFAULT_TTL_SECONDS) < 30


async def test_reserve_aggregates_repeated_product_lines(service, stock):
    """
    Проверяем: две строки одного товара в команде складываются, а не перетирают.
    Успех: sku-1 списывается на суммарные 5, available 100->95.
    Нежелательное поведение: вторая строка затирает первую, сток разъезжается.
    """
    order_id = uuid.uuid7()
    command = ReserveCommand(
        correlation=CommandCorrelation(
            saga_id=str(uuid.uuid7()),
            business_key=str(order_id),
            command_id=str(uuid.uuid7()),
        ),
        order_id=order_id,
        items=[
            ReservationItem(product_id="sku-1", quantity=2),
            ReservationItem(product_id="sku-1", quantity=3),
        ],
        ttl_seconds=60,
    )

    await service.reserve(command)

    assert stock.available("sku-1") == 95
    assert stock.reserved("sku-1") == 5


# --- reserve: бизнес-отказы ---


async def test_reserve_insufficient_stock_is_business_failure(service, stock, outbox):
    """
    Проверяем: нехватка товара - бизнес-отказ (retriable=false), сток не тронут.
    Успех: inventory.reserve-failed, failure.code=insufficient_stock, retriable false.
    Нежелательное поведение: частичный резерв или пометка отказа как временного.
    """
    order_id = uuid.uuid7()

    await service.reserve(make_reserve(order_id, {"sku-2": 10}))  # доступно 5

    assert stock.available("sku-2") == 5
    assert stock.reserved("sku-2") == 0
    assert outbox.event_types == ["inventory.reserve-failed"]
    failure = outbox.last_data["failure"]
    assert failure["code"] == "insufficient_stock"
    assert failure["retriable"] is False


async def test_reserve_unknown_product_is_business_failure(service, outbox):
    """
    Проверяем: товар вне каталога склада - бизнес-отказ, а не 500/зависание.
    Успех: inventory.reserve-failed, failure.code=unknown_product, retriable false.
    Нежелательное поведение: неизвестный sku валит обработчик в NACK-цикл.
    """
    order_id = uuid.uuid7()

    await service.reserve(make_reserve(order_id, {"sku-404": 1}))

    assert outbox.event_types == ["inventory.reserve-failed"]
    failure = outbox.last_data["failure"]
    assert failure["code"] == "unknown_product"
    assert failure["retriable"] is False


# --- идемпотентность по commandId ---


async def test_duplicate_command_replays_same_answer_without_double_effect(
    service, stock, outbox, processed_commands, reservations
):
    """
    Проверяем: дубль команды (тот же commandId) не двигает сток дважды.
    Успех: available списан один раз (97), обе outbox-записи идентичны, журнал=1.
    Нежелательное поведение: повторная команда резервирует товар второй раз.
    """
    order_id = uuid.uuid7()
    command_id = str(uuid.uuid7())
    command = make_reserve(order_id, {"sku-1": 3}, command_id=command_id)

    await service.reserve(command)
    await service.reserve(command)  # тот же commandId - дубль

    assert stock.available("sku-1") == 97  # списано ровно один раз
    assert len(reservations.by_order) == 1
    assert len(processed_commands.by_command) == 1
    # переиздан тот же самый конверт (тот же event_id) - дубль погасит дедуп саги
    assert len(outbox.messages) == 2
    assert outbox.messages[0].payload == outbox.messages[1].payload


# --- commit ---


async def test_commit_active_reservation_ships_goods(service, stock, reservations):
    """
    Проверяем: commit списывает товар (reserved уходит) и переводит резерв в COMMITTED.
    Успех: reserved 3->0, available остаётся 97, статус COMMITTED, событие committed.
    Нежелательное поведение: commit возвращает товар в available или трогает его дважды.
    """
    order_id = uuid.uuid7()
    await service.reserve(make_reserve(order_id, {"sku-1": 3}))

    await service.commit_reservation(make_commit(order_id))

    assert stock.reserved("sku-1") == 0
    assert stock.available("sku-1") == 97  # товар уехал покупателю, не вернулся
    assert reservations.status_of(order_id) is ReservationStatus.COMMITTED


async def test_commit_on_expired_reservation_fails(service, stock, reservations, outbox):
    """
    Проверяем: commit на истёкшем резерве - нарушение инварианта TTL (сага в FAILED).
    Успех: inventory.commit-failed, failure.code=reservation_expired, retriable false.
    Нежелательное поведение: молчаливый успех commit по резерву, чей сток уже возвращён.
    """
    order_id = uuid.uuid7()
    await reservations.add(
        make_reservation(order_id, {"sku-1": 3}, status=ReservationStatus.EXPIRED)
    )
    available_before = stock.available("sku-1")

    await service.commit_reservation(make_commit(order_id))

    assert stock.available("sku-1") == available_before  # сток не тронут
    assert outbox.event_types == ["inventory.commit-failed"]
    failure = outbox.last_data["failure"]
    assert failure["code"] == "reservation_expired"
    assert failure["retriable"] is False


async def test_commit_without_reservation_fails(service, outbox):
    """
    Проверяем: commit по заказу без резерва - отказ (рассинхрон саги и склада).
    Успех: inventory.commit-failed, failure.code=reservation_not_found, retriable false.
    Нежелательное поведение: commit несуществующего резерва проходит как успех.
    """
    order_id = uuid.uuid7()

    await service.commit_reservation(make_commit(order_id))

    assert outbox.event_types == ["inventory.commit-failed"]
    assert outbox.last_data["failure"]["code"] == "reservation_not_found"
    assert outbox.last_data["failure"]["retriable"] is False


# --- cancel: идемпотентность и коммутативность ---


async def test_cancel_nonexistent_reservation_is_success(service, outbox):
    """
    Проверяем: отмена несуществующего резерва - успех, а не ошибка (компенсация саги).
    Успех: публикуется inventory.reservation-cancelled без failure-блока.
    Нежелательное поведение: сага не может завершить компенсацию и зависает навсегда.
    """
    order_id = uuid.uuid7()

    await service.cancel_reservation(make_cancel(order_id))

    assert outbox.event_types == ["inventory.reservation-cancelled"]
    assert "failure" not in outbox.last_data


async def test_cancel_active_reservation_returns_stock(service, stock, reservations):
    """
    Проверяем: отмена ACTIVE-резерва возвращает товар (reserved -> available).
    Успех: available 97->100, reserved 3->0, статус CANCELLED.
    Нежелательное поведение: компенсация не возвращает заблокированный товар.
    """
    order_id = uuid.uuid7()
    await service.reserve(make_reserve(order_id, {"sku-1": 3}))
    assert stock.available("sku-1") == 97

    await service.cancel_reservation(make_cancel(order_id))

    assert stock.available("sku-1") == 100
    assert stock.reserved("sku-1") == 0
    assert reservations.status_of(order_id) is ReservationStatus.CANCELLED


async def test_cancel_is_commutative_and_returns_stock_once(service, stock, reservations):
    """
    Проверяем: повторная отмена (новый commandId) не возвращает сток дважды.
    Успех: после двух cancel available ровно 100, статус остаётся CANCELLED.
    Нежелательное поведение: второй cancel накручивает available сверх исходного.
    """
    order_id = uuid.uuid7()
    await service.reserve(make_reserve(order_id, {"sku-1": 3}))

    await service.cancel_reservation(make_cancel(order_id))
    await service.cancel_reservation(make_cancel(order_id))  # уже CANCELLED

    assert stock.available("sku-1") == 100  # возвращено один раз
    assert stock.reserved("sku-1") == 0
    assert reservations.status_of(order_id) is ReservationStatus.CANCELLED


async def test_cancel_already_expired_is_success_without_stock_change(
    service, stock, reservations, outbox
):
    """
    Проверяем: отмена уже истёкшего резерва - успех, сток не трогается повторно.
    Успех: inventory.reservation-cancelled, available не меняется (поллер уже вернул).
    Нежелательное поведение: cancel возвращает сток, который expiry уже вернул.
    """
    order_id = uuid.uuid7()
    await reservations.add(
        make_reservation(order_id, {"sku-1": 3}, status=ReservationStatus.EXPIRED)
    )
    available_before = stock.available("sku-1")

    await service.cancel_reservation(make_cancel(order_id))

    assert stock.available("sku-1") == available_before
    assert outbox.event_types == ["inventory.reservation-cancelled"]
    assert reservations.status_of(order_id) is ReservationStatus.EXPIRED
