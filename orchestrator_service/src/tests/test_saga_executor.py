"""
Юнит-тесты generic-исполнителя саг: happy path, бизнес- и технические отказы,
компенсации, guard'ы, идемпотентность, поллеры retry/deadline.
Вся грязь (Kafka, Postgres) заменена фейками из conftest.
"""

import uuid
from datetime import timedelta

from app.application.services.saga_executor import SagaExecutorService
from app.domain.outbox import OutboxKind
from app.domain.saga import Saga, SagaStatus, utc_now
from tests.conftest import (
    FakeOutboxRepository,
    FakeSagaRepository,
    order_created_event,
    participant_event,
)

ORDERS = "orders.events"
PAYMENTS = "payments.events"


async def start_saga(
    executor: SagaExecutorService, saga_repo: FakeSagaRepository, order_id: str
) -> Saga:
    report = await executor.handle_event(ORDERS, order_created_event(order_id))
    assert report.action == "processed"
    return saga_repo.single()


async def pass_reserve(
    executor: SagaExecutorService, saga_repo: FakeSagaRepository
) -> Saga:
    saga = saga_repo.single()
    report = await executor.handle_event(
        ORDERS,
        participant_event(
            "inventory.reserved", saga.id, saga.business_key, saga.active_command_id
        ),
    )
    assert report.action == "processed"
    return saga_repo.single()


class TestSagaStart:
    async def test_start_creates_saga_and_emits_reserve_command(
        self, executor, saga_repo, outbox_repo, transitions_repo, settings
    ):
        """Проверяем: order.created создаёт сагу и кладёт команду резерва в outbox.
        Успех: RUNNING/reserve, команда inventory.reserve с ttlSeconds и корреляцией.
        Нежелательное поведение: команда уходит мимо outbox или без метаданных саги."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        assert saga.status is SagaStatus.RUNNING
        assert saga.current_step == "reserve"
        assert saga.business_key == order_id
        assert saga.active_command_id is not None
        assert saga.deadline_at is not None

        commands = outbox_repo.by_type("inventory.reserve")
        assert len(commands) == 1
        command = commands[0]
        assert command.kind is OutboxKind.COMMAND
        assert command.topic == settings.KAFKA_INVENTORY_COMMANDS_TOPIC
        assert command.key == order_id
        assert command.payload["metadata"]["sagaId"] == str(saga.id)
        assert command.payload["metadata"]["businessKey"] == order_id
        assert command.payload["data"]["ttlSeconds"] == settings.RESERVATION_TTL_SECONDS
        assert len(transitions_repo.items) == 1

    async def test_duplicate_start_event_creates_single_saga(
        self, executor, saga_repo
    ):
        """Проверяем: повторный order.created (новый event_id, тот же заказ) не плодит саги.
        Успех: вторая обработка отвечает duplicate, сага одна.
        Нежелательное поведение: две саги на один business_key."""
        order_id = str(uuid.uuid7())
        await start_saga(executor, saga_repo, order_id)
        report = await executor.handle_event(ORDERS, order_created_event(order_id))

        assert report.action == "duplicate"
        assert len(saga_repo.by_id) == 1

    async def test_same_event_id_is_deduplicated(self, executor, saga_repo, outbox_repo):
        """Проверяем: одно и то же событие (тот же event_id) обрабатывается один раз.
        Успех: повтор отвечает duplicate, состояние и outbox не меняются.
        Нежелательное поведение: повторная обработка двигает сагу второй раз."""
        order_id = str(uuid.uuid7())
        event = order_created_event(order_id)
        await executor.handle_event(ORDERS, event)
        commands_before = len(outbox_repo.messages)

        report = await executor.handle_event(ORDERS, event)

        assert report.action == "duplicate"
        assert len(saga_repo.by_id) == 1
        assert len(outbox_repo.messages) == commands_before


class TestHappyPath:
    async def test_full_flow_reaches_completed(
        self, executor, saga_repo, outbox_repo, settings
    ):
        """Проверяем: полный happy path reserve -> charge -> commit_reservation.
        Успех: сага COMPLETED, три команды и событие saga.completed в outbox.
        Нежелательное поведение: пропуск шага или финал без saga.completed."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        saga = await pass_reserve(executor, saga_repo)
        assert saga.status is SagaStatus.RUNNING
        assert saga.current_step == "charge"
        assert saga.retry_count == 0
        charge = outbox_repo.by_type("payment.process")
        assert len(charge) == 1
        assert charge[0].topic == settings.KAFKA_PAYMENTS_COMMANDS_TOPIC

        await executor.handle_event(
            PAYMENTS,
            participant_event(
                "payment.completed", saga.id, order_id, saga.active_command_id
            ),
        )
        saga = saga_repo.single()
        assert saga.current_step == "commit_reservation"
        assert len(outbox_repo.by_type("inventory.commit_reservation")) == 1

        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reservation-committed",
                saga.id,
                order_id,
                saga.active_command_id,
            ),
        )
        saga = saga_repo.single()
        assert saga.status is SagaStatus.COMPLETED
        assert saga.current_step is None
        assert saga.active_command_id is None

        finished = outbox_repo.by_type("saga.completed")
        assert len(finished) == 1
        assert finished[0].kind is OutboxKind.EVENT
        assert finished[0].topic == settings.KAFKA_ORDERS_EVENTS_TOPIC
        assert finished[0].payload["data"]["status"] == "COMPLETED"


class TestBusinessFailures:
    async def test_reserve_failure_cancels_without_compensation(
        self, executor, saga_repo, outbox_repo
    ):
        """Проверяем: бизнес-отказ первого шага отменяет сагу без компенсаций.
        Успех: CANCELLED, saga.cancelled опубликовано, оплата не командовалась.
        Нежелательное поведение: компенсация несделанного резерва или вызов оплаты."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserve-failed",
                saga.id,
                order_id,
                saga.active_command_id,
                failure={"code": "insufficient_stock", "message": "нет товара", "retriable": False},
            ),
        )
        saga = saga_repo.single()
        assert saga.status is SagaStatus.CANCELLED
        assert outbox_repo.by_type("inventory.cancel_reservation") == []
        assert outbox_repo.by_type("payment.process") == []
        assert len(outbox_repo.by_type("saga.cancelled")) == 1

    async def test_payment_failure_compensates_reserve(
        self, executor, saga_repo, outbox_repo
    ):
        """Проверяем: бизнес-отказ оплаты запускает компенсацию резерва до CANCELLED.
        Успех: COMPENSATING + cancel_reservation, после ответа склада - CANCELLED.
        Нежелательное поведение: отмена заказа без снятия резерва."""
        order_id = str(uuid.uuid7())
        await start_saga(executor, saga_repo, order_id)
        saga = await pass_reserve(executor, saga_repo)

        await executor.handle_event(
            PAYMENTS,
            participant_event(
                "payment.failed",
                saga.id,
                order_id,
                saga.active_command_id,
                failure={"code": "card_declined", "message": "отказ банка", "retriable": False},
            ),
        )
        saga = saga_repo.single()
        assert saga.status is SagaStatus.COMPENSATING
        assert saga.current_step == "reserve"
        assert len(outbox_repo.by_type("inventory.cancel_reservation")) == 1

        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reservation-cancelled",
                saga.id,
                order_id,
                saga.active_command_id,
            ),
        )
        saga = saga_repo.single()
        assert saga.status is SagaStatus.CANCELLED
        assert len(outbox_repo.by_type("saga.cancelled")) == 1

    async def test_commit_failure_past_pivot_fails_saga(
        self, executor, saga_repo, outbox_repo
    ):
        """Проверяем: отказ после pivot (гонка TTL) не компенсируется, а падает в FAILED.
        Успех: FAILED + saga.failed - ручной разбор (решение итерации 3, п.1).
        Нежелательное поведение: попытка компенсировать уже оплаченный заказ."""
        order_id = str(uuid.uuid7())
        await start_saga(executor, saga_repo, order_id)
        saga = await pass_reserve(executor, saga_repo)
        await executor.handle_event(
            PAYMENTS,
            participant_event(
                "payment.completed", saga.id, order_id, saga.active_command_id
            ),
        )
        saga = saga_repo.single()

        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.commit-failed",
                saga.id,
                order_id,
                saga.active_command_id,
                failure={"code": "reservation_expired", "message": "резерв истёк", "retriable": False},
            ),
        )
        saga = saga_repo.single()
        assert saga.status is SagaStatus.FAILED
        assert outbox_repo.by_type("inventory.cancel_reservation") == []
        assert len(outbox_repo.by_type("saga.failed")) == 1


class TestTechnicalFailures:
    async def test_retriable_failure_schedules_retry(self, executor, saga_repo, outbox_repo):
        """Проверяем: retriable-отказ не двигает сагу, а планирует ретрай с backoff.
        Успех: retry_count=1, retry_after в будущем, дедлайн снят, команд не прибавилось.
        Нежелательное поведение: немедленная компенсация или мгновенная переотправка."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)
        commands_before = len(outbox_repo.messages)

        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserve-failed",
                saga.id,
                order_id,
                saga.active_command_id,
                failure={"code": "timeout", "message": "склад молчит", "retriable": True},
            ),
        )
        saga = saga_repo.single()
        assert saga.status is SagaStatus.RUNNING
        assert saga.current_step == "reserve"
        assert saga.retry_count == 1
        assert saga.retry_after is not None and saga.retry_after > utc_now()
        assert saga.deadline_at is None
        assert len(outbox_repo.messages) == commands_before

    async def test_reserve_retries_exhausted_goes_dlq_and_cancels(
        self, executor, saga_repo, outbox_repo, settings
    ):
        """Проверяем: исчерпание ретраев первого шага - DLQ команды + отмена саги.
        Успех: конверт в inventory.commands.dlq, сага CANCELLED (компенсировать нечего).
        Нежелательное поведение: бесконечные ретраи или зависание вне терминала."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        for attempt in range(settings.SAGA_MAX_STEP_ATTEMPTS):
            await executor.handle_event(
                ORDERS,
                participant_event(
                    "inventory.reserve-failed",
                    saga.id,
                    order_id,
                    saga.active_command_id,
                    failure={"code": "timeout", "message": f"попытка {attempt}", "retriable": True},
                ),
            )
            stored = saga_repo.by_id[saga.id]
            # между отказами "переотправляем" команду: сбрасываем retry_after,
            # как это сделал бы поллер (стейт-машина от этого не зависит)
            stored.retry_after = None

        saga = saga_repo.single()
        assert saga.status is SagaStatus.CANCELLED
        dlq = outbox_repo.by_type("inventory.reserve.dlq")
        assert len(dlq) == 1
        assert dlq[0].topic == f"{settings.KAFKA_INVENTORY_COMMANDS_TOPIC}.dlq"
        assert dlq[0].payload["dlqMeta"]["errorClass"] == "StepRetriesExhausted"

    async def test_charge_retries_exhausted_compensates(
        self, executor, saga_repo, outbox_repo, settings
    ):
        """Проверяем: исчерпание ретраев оплаты - DLQ + компенсация резерва.
        Успех: конверт в payments.commands.dlq, сага в COMPENSATING с cancel-командой.
        Нежелательное поведение: FAILED без попытки снять резерв."""
        order_id = str(uuid.uuid7())
        await start_saga(executor, saga_repo, order_id)
        saga = await pass_reserve(executor, saga_repo)

        for _ in range(settings.SAGA_MAX_STEP_ATTEMPTS):
            await executor.handle_event(
                PAYMENTS,
                participant_event(
                    "payment.failed",
                    saga.id,
                    order_id,
                    saga.active_command_id,
                    failure={"code": "provider_5xx", "message": "провайдер лежит", "retriable": True},
                ),
            )
            saga_repo.by_id[saga.id].retry_after = None

        saga = saga_repo.single()
        assert saga.status is SagaStatus.COMPENSATING
        assert saga.current_step == "reserve"
        assert len(outbox_repo.by_type("payment.process.dlq")) == 1
        assert len(outbox_repo.by_type("inventory.cancel_reservation")) == 1


class TestGuards:
    async def test_unexpected_event_is_ignored(self, executor, saga_repo):
        """Проверяем: событие не к текущему шагу логируется и игнорируется.
        Успех: payment.completed на шаге reserve не меняет сагу (ответ ignored).
        Нежелательное поведение: перескок через шаг или падение обработчика."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        report = await executor.handle_event(
            PAYMENTS,
            participant_event(
                "payment.completed", saga.id, order_id, saga.active_command_id
            ),
        )
        assert report.action == "ignored"
        saga = saga_repo.single()
        assert saga.status is SagaStatus.RUNNING
        assert saga.current_step == "reserve"

    async def test_event_for_terminal_saga_is_ignored(self, executor, saga_repo):
        """Проверяем: терминальная сага абсорбирует любые события.
        Успех: inventory.reserved после CANCELLED отвечает ignored, статус не меняется.
        Нежелательное поведение: "воскрешение" завершённой саги."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)
        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserve-failed",
                saga.id,
                order_id,
                saga.active_command_id,
                failure={"code": "insufficient_stock", "message": "-", "retriable": False},
            ),
        )
        assert saga_repo.single().status is SagaStatus.CANCELLED

        report = await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserved", saga.id, order_id, saga.active_command_id
            ),
        )
        assert report.action == "ignored"
        assert saga_repo.single().status is SagaStatus.CANCELLED

    async def test_stale_command_response_is_ignored(self, executor, saga_repo):
        """Проверяем: ответ на устаревший commandId отбрасывается.
        Успех: событие с чужим commandId в correlation отвечает ignored.
        Нежелательное поведение: старый ответ двигает сагу после переотправки."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        report = await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserved", saga.id, order_id, uuid.uuid7()
            ),
        )
        assert report.action == "ignored"
        assert saga_repo.single().current_step == "reserve"

    async def test_event_without_correlation_is_ignored(self, executor, saga_repo):
        """Проверяем: событие без correlation к сагам не относится.
        Успех: payment.completed без блока correlation отвечает ignored.
        Нежелательное поведение: poison/DLQ для легитимного платежа вне саги."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        report = await executor.handle_event(
            PAYMENTS,
            participant_event(
                "payment.completed", saga.id, order_id, saga.active_command_id,
                correlation=False,
            ),
        )
        assert report.action == "ignored"
        assert saga_repo.single().current_step == "reserve"

    async def test_failed_event_without_failure_block_is_poison(
        self, executor, saga_repo
    ):
        """Проверяем: *.failed без обязательного failure-блока - контрактный брак.
        Успех: ответ poison (консюмер отправит в DLQ), сага не изменилась.
        Нежелательное поведение: трактовка брака как бизнес-отказа с отменой заказа."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)

        report = await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserve-failed", saga.id, order_id, saga.active_command_id
            ),
        )
        assert report.action == "poison"
        saga = saga_repo.single()
        assert saga.status is SagaStatus.RUNNING
        assert saga.retry_count == 0


class TestPollers:
    async def test_retry_due_resends_same_command_id(
        self, executor, saga_repo, outbox_repo
    ):
        """Проверяем: поллер переотправляет команду с ТЕМ ЖЕ commandId.
        Успех: новая outbox-запись с прежним commandId, retry_after снят, дедлайн выставлен.
        Нежелательное поведение: новый commandId (двойное списание у неидемпотентного участника)."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)
        await executor.handle_event(
            ORDERS,
            participant_event(
                "inventory.reserve-failed",
                saga.id,
                order_id,
                saga.active_command_id,
                failure={"code": "timeout", "message": "-", "retriable": True},
            ),
        )
        stored = saga_repo.by_id[saga.id]
        original_command_id = stored.active_command_id
        stored.retry_after = utc_now() - timedelta(seconds=1)

        resent = await executor.process_due_retries()

        assert resent == 1
        saga = saga_repo.single()
        assert saga.retry_after is None
        assert saga.deadline_at is not None
        commands = outbox_repo.by_type("inventory.reserve")
        assert len(commands) == 2
        assert commands[1].payload["metadata"]["commandId"] == str(original_command_id)

    async def test_deadline_retry_policy_schedules_retry(self, executor, saga_repo):
        """Проверяем: молчание участника дольше дедлайна = технический сбой.
        Успех: retry_count=1 и запланирован backoff-ретрай (политика RETRY).
        Нежелательное поведение: немедленная отмена саги из-за таймаута."""
        order_id = str(uuid.uuid7())
        saga = await start_saga(executor, saga_repo, order_id)
        saga_repo.by_id[saga.id].deadline_at = utc_now() - timedelta(seconds=1)

        timed_out = await executor.process_due_deadlines()

        assert timed_out == 1
        saga = saga_repo.single()
        assert saga.status is SagaStatus.RUNNING
        assert saga.retry_count == 1
        assert saga.retry_after is not None

    async def test_charge_deadline_is_business_fail(
        self, executor, saga_repo, outbox_repo
    ):
        """Проверяем: истечение окна оплаты - бизнес-исход, а не технический ретрай.
        Успех: сага уходит в COMPENSATING с командой снятия резерва (политика BUSINESS_FAIL).
        Нежелательное поведение: ретраи "неоплаты" или зависание в RUNNING."""
        order_id = str(uuid.uuid7())
        await start_saga(executor, saga_repo, order_id)
        saga = await pass_reserve(executor, saga_repo)
        saga_repo.by_id[saga.id].deadline_at = utc_now() - timedelta(seconds=1)

        timed_out = await executor.process_due_deadlines()

        assert timed_out == 1
        saga = saga_repo.single()
        assert saga.status is SagaStatus.COMPENSATING
        assert saga.current_step == "reserve"
        assert len(outbox_repo.by_type("inventory.cancel_reservation")) == 1
