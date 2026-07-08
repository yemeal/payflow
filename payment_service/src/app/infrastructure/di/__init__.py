from dishka import make_async_container, AsyncContainer
from app.infrastructure.di.provider import (
    SettingsProvider,
    DatabaseProvider,
    RedisProvider,
    ServiceProvider,
    IntegrationsProvider,
    KafkaProvider,
)


def create_container() -> AsyncContainer:
    return make_async_container(
        SettingsProvider(),
        DatabaseProvider(),
        RedisProvider(),
        ServiceProvider(),
        IntegrationsProvider(),
        KafkaProvider(),
    )


from .provider import (
    logger,
    SettingsProvider,
    DatabaseProvider,
    RedisProvider,
    ServiceProvider,
    IntegrationsProvider,
    KafkaProvider,
)
from .outbox_scope import DishkaOutboxScopeFactory, DishkaOutboxScope

__all__ = (
    "logger",
    "SettingsProvider",
    "DatabaseProvider",
    "RedisProvider",
    "ServiceProvider",
    "IntegrationsProvider",
    "KafkaProvider",
    "DishkaOutboxScopeFactory",
    "DishkaOutboxScope",
    "create_container",
)
