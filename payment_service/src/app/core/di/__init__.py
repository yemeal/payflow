from dishka import make_async_container, AsyncContainer
from app.core.di.provider import (
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
