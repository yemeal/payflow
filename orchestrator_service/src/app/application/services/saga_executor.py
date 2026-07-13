"""
Generic-исполнитель саг (ADR-006).

Интерпретирует декларативные SagaDefinition: сам не знает ни одного бизнес-правила
шагов - только механику процесса (дедуп, guard, переходы, ретраи, таймауты,
компенсации, DLQ). Каждый переход - одна транзакция БД: дедуп-отметка,
блокировка строки саги (FOR UPDATE), мутация, команды/события в outbox,
запись в историю переходов.
"""

import random
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Literal
from uuid import UUID

import structlog

from app.application.ports.repositories import (
    OutboxRepositoryProtocol,
    ProcessedEventRepositoryProtocol,
    SagaRepositoryProtocol,
    SagaTransitionRepositoryProtocol,
)
from app.application.ports.uow import AsyncUOWProtocol
from app.core.settings import Settings
from app.domain.definitions import (
    SagaDefinition,
    SagaRegistry,
    SagaStep,
    StepOutcome,
    TimeoutPolicy,
)
from app.domain.outbox import OutboxKind, OutboxMessage
from app.domain.processed_events import ProcessedEvent
from app.domain.saga import Saga, SagaStatus, SagaTransition, utc_now

logger = structlog.get_logger()

_LAST_ERROR_MAX_LEN = 1000

HandleAction = Literal["processed", "duplicate", "ignored", "poison"]


@dataclass(frozen=True, slots=True)
class HandleReport:
    """Итог обработки события; poison консюмер публикует в <топик>.dlq"""

    action: HandleAction
    detail: str | None = None


class SagaExecutorService:
    def __init__(
        self,
        registry: SagaRegistry,
        sagas: SagaRepositoryProtocol,
        transitions: SagaTransitionRepositoryProtocol,
        processed_events: ProcessedEventRepositoryProtocol,
        outbox: OutboxRepositoryProtocol,
        uow: AsyncUOWProtocol,
        settings: Settings,
    ) -> None:
        self._registry = registry
        self._sagas = sagas
        self._transitions = transitions
        self._processed_events = processed_events
        self._outbox = outbox
        self._uow = uow
        self._settings = settings

    # ------------------------------------------------------------------
    # обработка события из Kafka (вызывается консюмером, транзакция своя)
    # ------------------------------------------------------------------

    async def handle_event(self, source_topic: str, message: dict[str, Any]) -> HandleReport:
        metadata = message.get("metadata")
        if not isinstance(metadata, dict):
            return HandleReport("poison", "message without metadata")
        event_type = metadata.get("event_type")
        try:
            event_id = UUID(str(metadata.get("event_id")))
        except (ValueError, TypeError):
            return HandleReport("poison", "invalid or missing event_id")
        if not event_type or not isinstance(event_type, str):
            return HandleReport("poison", "missing event_type")

        log = logger.bind(event_id=str(event_id), event_type=event_type, topic=source_topic)

        async with self._uow:
            start_definition = self._registry.find_by_start_event(event_type)
            if start_definition is not None:
                return await self._handle_start_event(start_definition, event_id, event_type, message, log)

            found = self._registry.find_binding(event_type)
            if found is None:
                # чужие события общей шины (saga.*, payment.pending, ...) - не наши
                return HandleReport("ignored", "no binding")
            definition, binding = found

            business_key = binding.business_key_from(message)
            if business_key is None:
                # событие без корреляции к сагам не относится (например, платёж,
                # созданный через HTTP API мимо саги) - легитимно игнорируем;
                # если это нарушение контракта участника, шаг дожмёт дедлайн-поллер
                await self._mark_processed(event_id, event_type, saga_id=None)
                log.info("event_without_correlation_ignored")
                return HandleReport("ignored", "no correlation / business key")

            saga = await self._sagas.get_by_business_key_for_update(
                definition.saga_type, business_key
            )
            if saga is None:
                # легитимно: например, платежи, созданные мимо саги (HTTP API)
                return HandleReport("ignored", "saga not found")

            log = log.bind(saga_id=str(saga.id), business_key=business_key)

            if saga.is_terminal:
                await self._mark_processed(event_id, event_type, saga.id)
                log.info("event_for_terminal_saga_ignored", status=saga.status.value)
                return HandleReport("ignored", "terminal saga")

            step = definition.step_by_name(binding.step)
            if not self._is_expected(saga, step, binding.outcome):
                await self._mark_processed(event_id, event_type, saga.id)
                log.warning(
                    "unexpected_event_ignored",
                    saga_status=saga.status.value,
                    current_step=saga.current_step,
                    bound_step=step.name,
                )
                return HandleReport("ignored", "unexpected for current state")

            stale = self._is_stale_command_response(saga, message)
            if stale:
                await self._mark_processed(event_id, event_type, saga.id)
                log.info("stale_command_response_ignored")
                return HandleReport("ignored", "stale command response")

            fresh = await self._mark_processed(event_id, event_type, saga.id)
            if not fresh:
                log.info("duplicate_event_skipped")
                return HandleReport("duplicate")

            report = await self._apply_outcome(
                definition, saga, step, binding.outcome, message, event_id, event_type, log
            )
            await self._sagas.update(saga)
            return report

    async def _handle_start_event(
        self,
        definition: SagaDefinition,
        event_id: UUID,
        event_type: str,
        message: dict[str, Any],
        log: Any,
    ) -> HandleReport:
        business_key = definition.business_key_from_start(message)
        if business_key is None:
            await self._mark_processed(event_id, event_type, saga_id=None)
            log.warning("start_event_without_business_key")
            return HandleReport("poison", "missing business key in start event")

        saga = Saga(
            saga_type=definition.saga_type,
            business_key=business_key,
            status=SagaStatus.RUNNING,
            current_step=definition.steps[0].name,
            payload=definition.build_payload(message),
        )
        created = await self._sagas.create_if_absent(saga)
        if not created:
            # дубль order.created: сага уже есть (UNIQUE saga_type+business_key)
            await self._mark_processed(event_id, event_type, saga_id=None)
            log.info("duplicate_start_event_skipped", business_key=business_key)
            return HandleReport("duplicate", "saga already exists")

        await self._mark_processed(event_id, event_type, saga.id)
        first_step = definition.steps[0]
        await self._emit_step_command(saga, first_step, command_id=uuid.uuid7())
        await self._record(
            saga,
            from_status=None,
            from_step=None,
            event_id=event_id,
            event_type=event_type,
            detail=f"saga started, command '{first_step.command_type}' emitted",
        )
        await self._sagas.update(saga)
        log.info(
            "saga_started",
            saga_id=str(saga.id),
            saga_type=definition.saga_type,
            business_key=business_key,
            first_step=first_step.name,
        )
        return HandleReport("processed", "saga started")

    # ------------------------------------------------------------------
    # применение исхода шага
    # ------------------------------------------------------------------

    async def _apply_outcome(
        self,
        definition: SagaDefinition,
        saga: Saga,
        step: SagaStep,
        outcome: StepOutcome,
        message: dict[str, Any],
        event_id: UUID,
        event_type: str,
        log: Any,
    ) -> HandleReport:
        if outcome is StepOutcome.SUCCESS:
            await self._on_step_success(definition, saga, step, event_id, event_type, log)
            return HandleReport("processed")

        if outcome is StepOutcome.COMPENSATED:
            await self._on_compensated(definition, saga, step, event_id, event_type, log)
            return HandleReport("processed")

        # StepOutcome.FAILED: retriable решает контрактный блок data.failure
        data = message.get("data")
        failure = data.get("failure") if isinstance(data, dict) else None
        if not isinstance(failure, dict) or not isinstance(failure.get("retriable"), bool):
            # контракт нарушен: событие уже дедуплицировано, состояние не трогаем,
            # шаг доведёт до исхода дедлайн-поллер; событие уедет в DLQ
            log.error("failure_block_missing_or_invalid")
            return HandleReport("poison", "missing mandatory failure block in *.failed")

        error = f"{failure.get('code', 'unknown')}: {failure.get('message', '')}"
        if failure["retriable"]:
            await self._register_technical_failure(
                definition, saga, step, error, event_id, event_type, log
            )
        else:
            await self._on_business_failure(
                definition, saga, step, error, event_id, event_type, log
            )
        return HandleReport("processed")

    async def _on_step_success(
        self,
        definition: SagaDefinition,
        saga: Saga,
        step: SagaStep,
        event_id: UUID | None,
        event_type: str | None,
        log: Any,
    ) -> None:
        prev = self._snapshot(saga)
        next_step = definition.next_step(step.name)
        if next_step is None:
            self._close(saga, SagaStatus.COMPLETED)
            await self._emit_finished(definition, saga, "saga.completed", "COMPLETED", None)
            await self._record(saga, *prev, event_id, event_type, "saga completed")
            log.info("saga_completed")
            return

        saga.current_step = next_step.name
        saga.retry_count = 0
        saga.last_error = None
        await self._emit_step_command(saga, next_step, command_id=uuid.uuid7())
        await self._record(
            saga, *prev, event_id, event_type,
            f"step '{step.name}' succeeded, command '{next_step.command_type}' emitted",
        )
        log.info("saga_step_advanced", next_step=next_step.name)

    async def _on_business_failure(
        self,
        definition: SagaDefinition,
        saga: Saga,
        step: SagaStep,
        reason: str,
        event_id: UUID | None,
        event_type: str | None,
        log: Any,
    ) -> None:
        saga.last_error = reason[:_LAST_ERROR_MAX_LEN]
        if saga.status is SagaStatus.COMPENSATING or definition.is_past_pivot(step.name):
            # отказ компенсации или отказ после точки невозврата - только ручной разбор
            prev = self._snapshot(saga)
            self._close(saga, SagaStatus.FAILED)
            await self._emit_finished(definition, saga, "saga.failed", "FAILED", reason)
            await self._record(saga, *prev, event_id, event_type, f"business failure: {reason}")
            log.error("saga_failed", reason=reason)
            return
        await self._start_compensation(definition, saga, step, reason, event_id, event_type, log)

    async def _start_compensation(
        self,
        definition: SagaDefinition,
        saga: Saga,
        failed_step: SagaStep,
        reason: str,
        event_id: UUID | None,
        event_type: str | None,
        log: Any,
    ) -> None:
        prev = self._snapshot(saga)
        targets = definition.compensation_targets_before(failed_step.name)
        if not targets:
            # компенсировать нечего (упал первый шаг) - заказ просто отменяется
            self._close(saga, SagaStatus.CANCELLED)
            await self._emit_finished(definition, saga, "saga.cancelled", "CANCELLED", reason)
            await self._record(saga, *prev, event_id, event_type, f"cancelled, nothing to compensate: {reason}")
            log.info("saga_cancelled", reason=reason)
            return

        target = targets[0]
        saga.status = SagaStatus.COMPENSATING
        saga.current_step = target.name
        saga.retry_count = 0
        await self._emit_compensation_command(saga, target, command_id=uuid.uuid7())
        await self._record(
            saga, *prev, event_id, event_type,
            f"compensation started ({reason}), command '{target.compensation.command_type}' emitted",  # type: ignore[union-attr]
        )
        log.info("saga_compensation_started", target_step=target.name, reason=reason)

    async def _on_compensated(
        self,
        definition: SagaDefinition,
        saga: Saga,
        step: SagaStep,
        event_id: UUID | None,
        event_type: str | None,
        log: Any,
    ) -> None:
        prev = self._snapshot(saga)
        remaining = definition.compensation_targets_before(step.name)
        if not remaining:
            self._close(saga, SagaStatus.CANCELLED)
            await self._emit_finished(
                definition, saga, "saga.cancelled", "CANCELLED", saga.last_error
            )
            await self._record(saga, *prev, event_id, event_type, "compensation finished, saga cancelled")
            log.info("saga_cancelled_after_compensation")
            return

        target = remaining[0]
        saga.current_step = target.name
        saga.retry_count = 0
        await self._emit_compensation_command(saga, target, command_id=uuid.uuid7())
        await self._record(
            saga, *prev, event_id, event_type,
            f"step '{step.name}' compensated, next compensation '{target.name}'",
        )

    async def _register_technical_failure(
        self,
        definition: SagaDefinition,
        saga: Saga,
        step: SagaStep,
        error: str,
        event_id: UUID | None,
        event_type: str | None,
        log: Any,
    ) -> None:
        prev = self._snapshot(saga)
        saga.retry_count += 1
        saga.last_error = error[:_LAST_ERROR_MAX_LEN]

        if saga.retry_count < step.max_attempts:
            delay = self._backoff_delay(saga.retry_count)
            saga.retry_after = utc_now() + timedelta(seconds=delay)
            saga.deadline_at = None
            await self._record(
                saga, *prev, event_id, event_type,
                f"technical failure ({error}), retry {saga.retry_count}/{step.max_attempts - 1} in {delay:.1f}s",
            )
            log.warning(
                "saga_step_retry_scheduled",
                attempt=saga.retry_count,
                max_attempts=step.max_attempts,
                delay_seconds=round(delay, 1),
                error=error,
            )
            return

        # попытки исчерпаны: команда шага уходит в DLQ, сага - в компенсацию или FAILED
        await self._publish_command_dlq(saga, step, error)
        log.error(
            "saga_step_retries_exhausted",
            step=saga.current_step,
            attempts=saga.retry_count,
            error=error,
        )
        reason = f"step '{step.name}' retries exhausted: {error}"
        if saga.status is SagaStatus.COMPENSATING or definition.is_past_pivot(step.name):
            self._close(saga, SagaStatus.FAILED)
            await self._emit_finished(definition, saga, "saga.failed", "FAILED", reason)
            await self._record(saga, *prev, event_id, event_type, reason)
            log.error("saga_failed", reason=reason)
            return
        await self._start_compensation(definition, saga, step, reason, event_id, event_type, log)

    # ------------------------------------------------------------------
    # фоновые циклы (вызывает поллер; транзакция на каждый вызов)
    # ------------------------------------------------------------------

    async def process_due_retries(self) -> int:
        """Переотправляет команды саг с наступившим retry_after.
        Тот же commandId - дедуп участника вернёт сохранённый результат."""
        processed = 0
        async with self._uow:
            due = await self._sagas.find_retry_due(
                utc_now(), self._settings.SAGA_POLLER_BATCH_SIZE
            )
            for saga in due:
                definition = self._registry.by_type(saga.saga_type)
                step = self._current_step_of(definition, saga)
                if definition is None or step is None:
                    logger.error("saga_retry_without_definition", saga_id=str(saga.id))
                    continue
                prev = self._snapshot(saga)
                command_id = saga.active_command_id or uuid.uuid7()
                if saga.status is SagaStatus.COMPENSATING:
                    await self._emit_compensation_command(saga, step, command_id)
                else:
                    await self._emit_step_command(saga, step, command_id)
                await self._record(
                    saga, *prev, None, None,
                    f"retry {saga.retry_count}: command re-emitted",
                )
                await self._sagas.update(saga)
                processed += 1
                logger.info(
                    "saga_command_resent",
                    saga_id=str(saga.id),
                    step=saga.current_step,
                    attempt=saga.retry_count,
                )
        return processed

    async def process_due_deadlines(self) -> int:
        """Обрабатывает саги, чей участник молчит дольше дедлайна шага"""
        processed = 0
        async with self._uow:
            due = await self._sagas.find_deadline_due(
                utc_now(), self._settings.SAGA_POLLER_BATCH_SIZE
            )
            for saga in due:
                definition = self._registry.by_type(saga.saga_type)
                step = self._current_step_of(definition, saga)
                if definition is None or step is None:
                    logger.error("saga_deadline_without_definition", saga_id=str(saga.id))
                    continue
                log = logger.bind(saga_id=str(saga.id), step=saga.current_step)
                log.warning("saga_step_deadline_expired")
                if (
                    saga.status is SagaStatus.RUNNING
                    and step.on_timeout is TimeoutPolicy.BUSINESS_FAIL
                ):
                    # бизнес-таймаут (пользователь не оплатил) - не ретраим
                    await self._on_business_failure(
                        definition, saga, step,
                        f"step '{step.name}' business deadline expired",
                        None, None, log,
                    )
                else:
                    await self._register_technical_failure(
                        definition, saga, step,
                        f"step '{step.name}' response deadline expired",
                        None, None, log,
                    )
                await self._sagas.update(saga)
                processed += 1
        return processed

    # ------------------------------------------------------------------
    # приватная механика
    # ------------------------------------------------------------------

    @staticmethod
    def _snapshot(saga: Saga) -> tuple[str, str | None]:
        return saga.status.value, saga.current_step

    @staticmethod
    def _is_expected(saga: Saga, step: SagaStep, outcome: StepOutcome) -> bool:
        if outcome is StepOutcome.COMPENSATED:
            return saga.status is SagaStatus.COMPENSATING and saga.current_step == step.name
        return saga.status is SagaStatus.RUNNING and saga.current_step == step.name

    @staticmethod
    def _is_stale_command_response(saga: Saga, message: dict[str, Any]) -> bool:
        """Ответ на устаревшую (переотправленную ранее) команду отбрасывается.
        Работает только при наличии echo-блока correlation (у payments его нет)."""
        metadata = message.get("metadata")
        if not isinstance(metadata, dict):
            return False
        correlation = metadata.get("correlation")
        if not isinstance(correlation, dict):
            return False
        raw = correlation.get("commandId")
        if raw is None or saga.active_command_id is None:
            return False
        try:
            return UUID(str(raw)) != saga.active_command_id
        except (ValueError, TypeError):
            return False

    def _current_step_of(
        self, definition: SagaDefinition | None, saga: Saga
    ) -> SagaStep | None:
        if definition is None or saga.current_step is None:
            return None
        try:
            return definition.step_by_name(saga.current_step)
        except KeyError:
            return None

    async def _mark_processed(
        self, event_id: UUID, event_type: str, saga_id: UUID | None
    ) -> bool:
        return await self._processed_events.try_mark_processed(
            ProcessedEvent(event_id=event_id, saga_id=saga_id, event_type=event_type)
        )

    def _backoff_delay(self, attempt: int) -> float:
        base = self._settings.SAGA_RETRY_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
        jitter = self._settings.SAGA_RETRY_BACKOFF_JITTER
        return base * random.uniform(1 - jitter, 1 + jitter)

    def _close(self, saga: Saga, status: SagaStatus) -> None:
        saga.status = status
        saga.current_step = None
        saga.retry_after = None
        saga.deadline_at = None
        saga.active_command_id = None

    def _command_envelope(
        self,
        saga: Saga,
        command_id: UUID,
        command_type: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            "metadata": {
                "commandId": str(command_id),
                "commandType": command_type,
                "version": "1.0",
                "timestamp": utc_now().isoformat(),
                "source": "orchestrator",
                "sagaId": str(saga.id),
                "businessKey": saga.business_key,
            },
            "data": data,
        }

    async def _emit_step_command(self, saga: Saga, step: SagaStep, command_id: UUID) -> None:
        envelope = self._command_envelope(
            saga, command_id, step.command_type, step.build_command_data(saga.payload)
        )
        await self._outbox.add(
            OutboxMessage(
                kind=OutboxKind.COMMAND,
                topic=step.command_topic,
                key=saga.business_key,
                type=step.command_type,
                payload=envelope,
            )
        )
        saga.active_command_id = command_id
        saga.deadline_at = utc_now() + timedelta(seconds=step.timeout_seconds)
        saga.retry_after = None

    async def _emit_compensation_command(
        self, saga: Saga, step: SagaStep, command_id: UUID
    ) -> None:
        compensation = step.compensation
        if compensation is None:  # защита от некорректного определения
            raise ValueError(f"step '{step.name}' has no compensation")
        envelope = self._command_envelope(
            saga, command_id, compensation.command_type,
            compensation.build_command_data(saga.payload),
        )
        await self._outbox.add(
            OutboxMessage(
                kind=OutboxKind.COMMAND,
                topic=compensation.command_topic,
                key=saga.business_key,
                type=compensation.command_type,
                payload=envelope,
            )
        )
        saga.active_command_id = command_id
        saga.deadline_at = utc_now() + timedelta(seconds=step.timeout_seconds)
        saga.retry_after = None

    async def _emit_finished(
        self,
        definition: SagaDefinition,
        saga: Saga,
        event_type: str,
        status: str,
        reason: str | None,
    ) -> None:
        event_id = uuid.uuid7()
        envelope = {
            "metadata": {
                "event_id": str(event_id),
                "event_type": event_type,
                "version": "1.0",
                "timestamp": utc_now().isoformat(),
                "source": "orchestrator",
            },
            "data": definition.build_finished_data(
                saga.business_key, saga.payload, status, reason
            ),
        }
        await self._outbox.add(
            OutboxMessage(
                kind=OutboxKind.EVENT,
                topic=definition.events_topic,
                key=saga.business_key,
                type=event_type,
                payload=envelope,
            )
        )

    async def _publish_command_dlq(self, saga: Saga, step: SagaStep, error: str) -> None:
        """Команда исчерпала попытки: конверт в <топик команды>.dlq (contracts/envelope/dlq-envelope).
        Публикация через outbox - транзакционно с переходом саги."""
        if saga.status is SagaStatus.COMPENSATING and step.compensation is not None:
            command_type = step.compensation.command_type
            command_topic = step.compensation.command_topic
            data = step.compensation.build_command_data(saga.payload)
        else:
            command_type = step.command_type
            command_topic = step.command_topic
            data = step.build_command_data(saga.payload)
        original = self._command_envelope(
            saga, saga.active_command_id or uuid.uuid7(), command_type, data
        )
        dlq_envelope = {
            "original": original,
            "dlqMeta": {
                "sourceTopic": command_topic,
                "consumerGroup": self._settings.KAFKA_CONSUMER_GROUP,
                "errorClass": "StepRetriesExhausted",
                "errorMessage": error[:_LAST_ERROR_MAX_LEN],
                "retryCount": saga.retry_count,
                "redriveCount": 0,
                "failedAt": utc_now().isoformat(),
            },
        }
        await self._outbox.add(
            OutboxMessage(
                kind=OutboxKind.EVENT,
                topic=f"{command_topic}.dlq",
                key=saga.business_key,
                type=f"{command_type}.dlq",
                payload=dlq_envelope,
            )
        )

    async def _record(
        self,
        saga: Saga,
        from_status: str | None,
        from_step: str | None,
        event_id: UUID | None,
        event_type: str | None,
        detail: str,
    ) -> None:
        await self._transitions.add(
            SagaTransition(
                saga_id=saga.id,
                from_status=from_status,
                from_step=from_step,
                to_status=saga.status.value,
                to_step=saga.current_step,
                event_id=event_id,
                event_type=event_type,
                detail=detail[:_LAST_ERROR_MAX_LEN],
            )
        )
