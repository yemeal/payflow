import structlog
from dishka import AsyncContainer
from dishka.integrations.taskiq import setup_dishka
from taskiq import TaskiqScheduler, TaskiqEvents, TaskiqState
from taskiq.schedule_sources import LabelScheduleSource
from taskiq_redis import RedisStreamBroker, RedisScheduleSource

from app.infrastructure.di import create_container
from app.core.logging import setup_logging
from app.core.settings import get_settings

setup_logging()
logger = structlog.get_logger(__name__)

settings = get_settings()

TASKIQ_QUEUE_NAME = "payments.tasks"
TASKIQ_CONSUMER_GROUP = "payment-service-workers"
TASKIQ_XREAD_BLOCK_MS = 1000 * 5
TASKIQ_IDLE_TIMEOUT_MS = 1000 * 60 * 5
TASKIQ_UNACK_TIMEOUT_SEC = 60 * 5
TASKIQ_XREAD_COUNT = 100

# Инициализация брокера
broker = RedisStreamBroker(
    url=settings.REDIS_URL,
    # имя очереди из Redis Stream
    queue_name=TASKIQ_QUEUE_NAME,
    # название группы консьюмеров, которым брокер будет равномерно распределять задачи,
    # важно, чтобы у воркеров было одно и то же имя группы
    consumer_group_name=TASKIQ_CONSUMER_GROUP,
    # сколько воркер будет ждать новые сообщения,
    # если за 5 секунд ничего нет, снова делает запрос
    # тем самым не перегружает сеть холостыми запросами
    xread_block=TASKIQ_XREAD_BLOCK_MS,
    # через сколько миллисекунд считать Worker умершим
    # (если воркер не ackнул задачу)
    idle_timeout=TASKIQ_IDLE_TIMEOUT_MS,
    # через сколько секунд освобождаем блокировку для неподтвержденной задачи
    # если воркер упадет посреди обработки сверки платежей,
    # другая нода воркера подхватит зависшую задачу ровно через 5 минут
    unacknowledged_lock_timeout=TASKIQ_UNACK_TIMEOUT_SEC,
    # за один запрос читает сразу 100 сообщений
    # снижает количество сетевых запросов к Redis при высоких нагрузках
    xread_count=TASKIQ_XREAD_COUNT,
    # если Stream нет, создаем его
    mkstream=True,
)


# создаем источник расписания в Redis
schedule_source = RedisScheduleSource(
    url=settings.REDIS_URL,
    # префикс для ключей в Redis, чтобы отделить расписания этого сервиса
    prefix="payments.schedule",
)
# создаем шедулер, передав ему брокер и наш источник расписаний
scheduler = TaskiqScheduler(
    broker,
    sources=[LabelScheduleSource(broker), schedule_source],
)


# ленивая инициализация при старте воркера
container: AsyncContainer | None = None


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def worker_startup(state: TaskiqState):
    global container
    logger.info("TaskIQ worker startup initiated")

    # Создаем контейнер только тогда, когда воркер реально запустился
    container = create_container()
    assert container is not None
    # подключаем дишку к TaskIQ
    # эта функция научит TaskIQ понимать аннотации FromDishka и
    # будет автоматически открывать REQUEST-scope на каждую задачу
    setup_dishka(container, broker)

    logger.info("DI container initialized")


@broker.on_event(TaskiqEvents.WORKER_SHUTDOWN)
async def worker_shutdown(state: TaskiqState) -> None:
    logger.info("TaskIQ worker is shutting down")
    if container is not None:
        await container.close()
        logger.info("DI container closed successfully")


# taskiq worker app.core.taskiq:broker
from app.entrypoints.workers.tasks import *
