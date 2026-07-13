from dishka import AsyncContainer, make_async_container

from app.core.settings import Settings
from app.infrastructure.di.provider import (
    DatabaseProvider,
    KafkaProvider,
    OutboxRelayProvider,
    RedisProvider,
    ServiceProvider,
    SettingsProvider,
)


def create_container(settings: Settings) -> AsyncContainer:
    """Фабрика контейнера: настройки передаются параметром, глобалей нет"""
    return make_async_container(
        SettingsProvider(settings),
        DatabaseProvider(),
        RedisProvider(),
        KafkaProvider(),
        OutboxRelayProvider(),
        ServiceProvider(),
    )
