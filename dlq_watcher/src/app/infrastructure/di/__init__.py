from dishka import AsyncContainer, make_async_container

from app.infrastructure.di.provider import (
    ObservabilityProvider,
    ServiceProvider,
    SettingsProvider,
)


def create_container() -> AsyncContainer:
    """Контейнер собирается фабрикой и живёт внутри create_app: глобалей уровня модуля нет."""
    return make_async_container(
        SettingsProvider(),
        ObservabilityProvider(),
        ServiceProvider(),
    )


__all__ = (
    "ObservabilityProvider",
    "ServiceProvider",
    "SettingsProvider",
    "create_container",
)
