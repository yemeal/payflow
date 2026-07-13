"""
Юнит-тесты декларативных определений саг: инварианты конфигурации,
валидация структуры, порядок компенсаций, реестр.
"""

import pytest

from app.application.sagas.order_fulfillment import (
    ORDER_FULFILLMENT,
    build_order_fulfillment_definition,
    create_saga_registry,
)
from app.domain.definitions import (
    CompensationSpec,
    EventBinding,
    SagaDefinition,
    SagaRegistry,
    SagaStep,
    StepOutcome,
)


def _noop_data(payload: dict) -> dict:
    return {}


def _noop_key(message: dict) -> str | None:
    return "key"


def _noop_finished(business_key: str, payload: dict, status: str, reason: str | None) -> dict:
    return {"status": status}


def _make_definition(steps: tuple[SagaStep, ...]) -> SagaDefinition:
    return SagaDefinition(
        saga_type="test-saga",
        start_event_type="test.started",
        business_key_from_start=_noop_key,
        build_payload=lambda m: {},
        steps=steps,
        event_bindings={},
        events_topic="test.events",
        build_finished_data=_noop_finished,
    )


class TestTtlInvariant:
    def test_ttl_smaller_than_payment_window_fails_fast(self, settings):
        """Проверяем: инвариант TTL резерва >= дедлайн оплаты + буфер (итерация 3).
        Успех: сборка определения падает ValueError ещё на старте приложения.
        Нежелательное поведение: молчаливый запуск с гонкой 'оплата успела, резерв истёк'."""
        broken = settings.model_copy(update={"RESERVATION_TTL_SECONDS": 100})
        with pytest.raises(ValueError, match="инвариант резерва"):
            build_order_fulfillment_definition(broken)

    def test_default_settings_satisfy_invariant(self, settings):
        """Проверяем: дефолтная конфигурация проходит инвариант.
        Успех: определение собирается, шаги в согласованном порядке.
        Нежелательное поведение: дефолты из Settings противоречат сами себе."""
        definition = build_order_fulfillment_definition(settings)
        assert [s.name for s in definition.steps] == [
            "reserve", "charge", "commit_reservation",
        ]


class TestDefinitionValidation:
    def test_duplicate_step_names_are_rejected(self):
        """Проверяем: два шага с одним именем не проходят валидацию определения.
        Успех: ValueError при сборке (имена шагов - ключи маршрутизации событий).
        Нежелательное поведение: второй шаг молча затирает первый в _steps_by_name."""
        steps = (
            SagaStep(
                name="same", command_type="a.do", command_topic="a.commands",
                build_command_data=_noop_data, timeout_seconds=1.0,
            ),
            SagaStep(
                name="same", command_type="b.do", command_topic="b.commands",
                build_command_data=_noop_data, timeout_seconds=1.0,
            ),
        )
        with pytest.raises(ValueError, match="не уникальны"):
            _make_definition(steps)

    def test_two_pivots_are_rejected(self):
        """Проверяем: точка невозврата в саге может быть только одна.
        Успех: ValueError при сборке определения.
        Нежелательное поведение: неоднозначная граница компенсируемой фазы."""
        steps = (
            SagaStep(
                name="first", command_type="a.do", command_topic="a.commands",
                build_command_data=_noop_data, timeout_seconds=1.0, pivot=True,
            ),
            SagaStep(
                name="second", command_type="b.do", command_topic="b.commands",
                build_command_data=_noop_data, timeout_seconds=1.0, pivot=True,
            ),
        )
        with pytest.raises(ValueError, match="pivot"):
            _make_definition(steps)

    def test_empty_steps_are_rejected(self):
        """Проверяем: сага без шагов бессмысленна и не собирается.
        Успех: ValueError при сборке определения.
        Нежелательное поведение: старт саги, которая сразу падает по IndexError."""
        with pytest.raises(ValueError, match="нет шагов"):
            _make_definition(())

    def test_non_positive_timeout_is_rejected(self):
        """Проверяем: шаг с неположительным таймаутом ловится при сборке.
        Успех: ValueError (иначе дедлайн наступает раньше отправки команды).
        Нежелательное поведение: сага, вечно таймаутящаяся на первом же шаге."""
        steps = (
            SagaStep(
                name="broken", command_type="a.do", command_topic="a.commands",
                build_command_data=_noop_data, timeout_seconds=0.0,
            ),
        )
        with pytest.raises(ValueError, match="таймаут"):
            _make_definition(steps)

    def test_compensation_after_pivot_is_rejected(self):
        """Проверяем: шаг после pivot не может иметь компенсацию.
        Успех: ValueError при сборке определения (после точки невозврата - только вперёд).
        Нежелательное поведение: молчаливое принятие некомпенсируемой конфигурации."""
        steps = (
            SagaStep(
                name="pivot-step", command_type="a.do", command_topic="a.commands",
                build_command_data=_noop_data, timeout_seconds=1.0, pivot=True,
            ),
            SagaStep(
                name="after-pivot", command_type="b.do", command_topic="b.commands",
                build_command_data=_noop_data, timeout_seconds=1.0,
                compensation=CompensationSpec("b.undo", "b.commands", _noop_data),
            ),
        )
        with pytest.raises(ValueError, match="после pivot"):
            _make_definition(steps)

    def test_binding_to_unknown_step_is_rejected(self):
        """Проверяем: маппинг события на несуществующий шаг ловится при сборке.
        Успех: ValueError с именем события.
        Нежелательное поведение: KeyError в рантайме на живом событии."""
        step = SagaStep(
            name="only", command_type="a.do", command_topic="a.commands",
            build_command_data=_noop_data, timeout_seconds=1.0,
        )
        with pytest.raises(ValueError, match="несуществующий шаг"):
            SagaDefinition(
                saga_type="broken",
                start_event_type="broken.started",
                business_key_from_start=_noop_key,
                build_payload=lambda m: {},
                steps=(step,),
                event_bindings={
                    "a.done": EventBinding("ghost", StepOutcome.SUCCESS, _noop_key)
                },
                events_topic="t",
                build_finished_data=_noop_finished,
            )


class TestStepNavigation:
    def test_step_index_and_next_step_follow_declared_order(self, settings):
        """Проверяем: навигация по шагам идёт в объявленном порядке определения.
        Успех: reserve -> charge -> commit_reservation, после последнего шага - None.
        Нежелательное поведение: перескок шага или зацикливание на последнем."""
        definition = build_order_fulfillment_definition(settings)

        assert definition.step_index("reserve") == 0
        assert definition.step_index("commit_reservation") == 2
        assert definition.next_step("reserve").name == "charge"
        assert definition.next_step("charge").name == "commit_reservation"
        # None у последнего шага - сигнал исполнителю закрыть сагу в COMPLETED
        assert definition.next_step("commit_reservation") is None

    def test_unknown_step_raises_key_error(self, settings):
        """Проверяем: обращение к несуществующему шагу не молчит.
        Успех: KeyError с именем шага.
        Нежелательное поведение: None вместо шага и падение глубже по стеку."""
        definition = build_order_fulfillment_definition(settings)
        with pytest.raises(KeyError):
            definition.step_index("ghost")


class TestCompensationOrder:
    def test_targets_before_charge_is_reserve_only(self, settings):
        """Проверяем: компенсируются только совершённые шаги до упавшего, в обратном порядке.
        Успех: перед charge - (reserve,), перед reserve - пусто.
        Нежелательное поведение: компенсация самого упавшего шага."""
        definition = build_order_fulfillment_definition(settings)
        assert [s.name for s in definition.compensation_targets_before("charge")] == ["reserve"]
        assert definition.compensation_targets_before("reserve") == ()

    def test_pivot_boundary(self, settings):
        """Проверяем: is_past_pivot отделяет compensatable-фазу от retriable.
        Успех: charge (сам pivot) - не 'за pivot', commit_reservation - за ним.
        Нежелательное поведение: FAILED вместо компенсации при отказе самого pivot."""
        definition = build_order_fulfillment_definition(settings)
        assert definition.is_past_pivot("charge") is False
        assert definition.is_past_pivot("commit_reservation") is True


class TestRegistry:
    def test_registry_resolves_start_and_bindings(self, settings):
        """Проверяем: реестр находит определение по стартовому событию и биндингам.
        Успех: order.created и inventory.reserved резолвятся в order-fulfillment.
        Нежелательное поведение: событие саги остаётся без обработчика."""
        registry = create_saga_registry(settings)
        assert registry.find_by_start_event("order.created") is not None
        found = registry.find_binding("inventory.reserved")
        assert found is not None
        assert found[0].saga_type == ORDER_FULFILLMENT

    def test_duplicate_saga_type_is_rejected(self, settings):
        """Проверяем: два определения с одним saga_type не регистрируются.
        Успех: ValueError при сборке реестра.
        Нежелательное поведение: молчаливая перезапись определения."""
        definition = build_order_fulfillment_definition(settings)
        with pytest.raises(ValueError):
            SagaRegistry((definition, definition))
