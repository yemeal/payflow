"""
Декларативные определения саг (ADR-006).

SagaDefinition - immutable-ДАННЫЕ, а не код с ветвлениями: упорядоченные шаги,
их команды, компенсации, таймауты и маппинг событий на исходы. Одна generic-машина
исполнения (SagaExecutorService) интерпретирует любые определения.

Определения НЕ живут глобальными переменными уровня модуля: их собирают
фабрики (app.application.sagas) и отдаёт DI как SagaRegistry (APP scope).
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

# извлекает значение из сырого сообщения {"metadata": ..., "data": ...};
# None означает "в сообщении нет обязательного поля" (poison)
MessageGetter = Callable[[dict[str, Any]], str | None]
# собирает data-часть команды из payload саги (чистая функция)
CommandDataBuilder = Callable[[dict[str, Any]], dict[str, Any]]


class StepOutcome(Enum):
    """Исход шага, на который маппится входящее событие"""

    SUCCESS = "SUCCESS"
    # провал прямой команды шага; retriable решается по data.failure.retriable
    FAILED = "FAILED"
    # успешный ответ на компенсирующую команду
    COMPENSATED = "COMPENSATED"


class TimeoutPolicy(Enum):
    """Что означает молчание участника дольше дедлайна шага"""

    # технический сбой: ретраим команду (backoff), после лимита - DLQ + компенсация
    RETRY = "RETRY"
    # бизнес-исход: например, пользователь не оплатил за отведённое время
    BUSINESS_FAIL = "BUSINESS_FAIL"


@dataclass(frozen=True, slots=True)
class CompensationSpec:
    """Обратное действие шага: отдельная команда, retriable по семантике"""

    command_type: str
    command_topic: str
    build_command_data: CommandDataBuilder


@dataclass(frozen=True, slots=True)
class SagaStep:
    name: str
    command_type: str
    command_topic: str
    build_command_data: CommandDataBuilder
    timeout_seconds: float
    on_timeout: TimeoutPolicy = TimeoutPolicy.RETRY
    max_attempts: int = 3
    # pivot - точка невозврата: после её успеха откат невозможен,
    # все последующие шаги обязаны быть retriable
    pivot: bool = False
    compensation: CompensationSpec | None = None


@dataclass(frozen=True, slots=True)
class EventBinding:
    """Маппинг типа события на (шаг, исход) + способ извлечь business_key"""

    step: str
    outcome: StepOutcome
    business_key_from: MessageGetter


@dataclass(frozen=True, slots=True)
class SagaDefinition:
    saga_type: str
    # событие, стартующее сагу (у заказа - order.created)
    start_event_type: str
    business_key_from_start: MessageGetter
    # снапшот payload саги из стартового события
    build_payload: Callable[[dict[str, Any]], dict[str, Any]]
    steps: tuple[SagaStep, ...]
    event_bindings: Mapping[str, EventBinding]
    # куда публиковать финальные saga.completed / saga.cancelled / saga.failed
    events_topic: str
    # data-часть финального события (generic-ядро не знает про orderId)
    build_finished_data: Callable[[str, dict[str, Any], str, str | None], dict[str, Any]]
    _steps_by_name: Mapping[str, SagaStep] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        by_name = {step.name: step for step in self.steps}
        object.__setattr__(self, "_steps_by_name", by_name)
        self._validate()

    def _validate(self) -> None:
        if not self.steps:
            raise ValueError(f"saga '{self.saga_type}': нет шагов")
        if len(self._steps_by_name) != len(self.steps):
            raise ValueError(f"saga '{self.saga_type}': имена шагов не уникальны")
        pivots = [s for s in self.steps if s.pivot]
        if len(pivots) > 1:
            raise ValueError(f"saga '{self.saga_type}': pivot-шаг может быть только один")
        if pivots:
            pivot_index = self.step_index(pivots[0].name)
            for step in self.steps[pivot_index + 1:]:
                if step.compensation is not None:
                    raise ValueError(
                        f"saga '{self.saga_type}': шаг '{step.name}' после pivot "
                        "не может иметь компенсацию (после точки невозврата - только вперёд)"
                    )
        for event_type, binding in self.event_bindings.items():
            if binding.step not in self._steps_by_name:
                raise ValueError(
                    f"saga '{self.saga_type}': событие '{event_type}' ссылается "
                    f"на несуществующий шаг '{binding.step}'"
                )
        for step in self.steps:
            if step.timeout_seconds <= 0:
                raise ValueError(
                    f"saga '{self.saga_type}': шаг '{step.name}' с неположительным таймаутом"
                )

    def step_by_name(self, name: str) -> SagaStep:
        return self._steps_by_name[name]

    def step_index(self, name: str) -> int:
        for i, step in enumerate(self.steps):
            if step.name == name:
                return i
        raise KeyError(name)

    def next_step(self, after: str) -> SagaStep | None:
        index = self.step_index(after)
        if index + 1 < len(self.steps):
            return self.steps[index + 1]
        return None

    def is_past_pivot(self, step_name: str) -> bool:
        """True, если шаг находится строго после pivot (откат уже невозможен)"""
        index = self.step_index(step_name)
        for i, step in enumerate(self.steps):
            if step.pivot:
                return index > i
        return False

    def compensation_targets_before(self, failed_step: str) -> tuple[SagaStep, ...]:
        """Шаги с компенсацией ДО упавшего, в обратном порядке выполнения.
        Сам упавший шаг не компенсируется - его прямое действие не совершилось."""
        bound = self.step_index(failed_step)
        return tuple(
            step for step in reversed(self.steps[:bound]) if step.compensation is not None
        )


class SagaRegistry:
    """
    Реестр определений. Собирается фабрикой в DI (APP scope) - не глобаль.
    Диспетчеризация событий - методы реестра, а не словарь уровня модуля.
    """

    def __init__(self, definitions: tuple[SagaDefinition, ...]) -> None:
        if len({d.saga_type for d in definitions}) != len(definitions):
            raise ValueError("saga_type определений не уникальны")
        self._by_type = {d.saga_type: d for d in definitions}
        self._by_start_event: dict[str, SagaDefinition] = {}
        self._bindings: dict[str, tuple[SagaDefinition, EventBinding]] = {}
        for definition in definitions:
            if definition.start_event_type in self._by_start_event:
                raise ValueError(
                    f"стартовое событие '{definition.start_event_type}' "
                    "закреплено за двумя определениями"
                )
            self._by_start_event[definition.start_event_type] = definition
            for event_type, binding in definition.event_bindings.items():
                if event_type in self._bindings:
                    raise ValueError(f"событие '{event_type}' замаплено дважды")
                self._bindings[event_type] = (definition, binding)

    def by_type(self, saga_type: str) -> SagaDefinition | None:
        return self._by_type.get(saga_type)

    def find_by_start_event(self, event_type: str) -> SagaDefinition | None:
        return self._by_start_event.get(event_type)

    def find_binding(self, event_type: str) -> tuple[SagaDefinition, EventBinding] | None:
        return self._bindings.get(event_type)
