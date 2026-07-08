import asyncio
import structlog
from faststream import FastStream
from faststream.kafka import KafkaBroker
from dishka_faststream import setup_dishka

from app.infrastructure.di import create_container
from app.core.logging import setup_logging
from app.core.settings import get_settings
from app.application.services.outbox_relay import OutboxRelayService

setup_logging()
logger = structlog.get_logger(__name__)

settings = get_settings()

broker = KafkaBroker(settings.KAFKA_BOOTSTRAP_SERVERS)
app = FastStream(broker)

# Setup Dishka container
container = create_container()
setup_dishka(container=container, broker=broker, auto_inject=True)

relay_task: asyncio.Task | None = None


@app.on_startup
async def start_outbox_relay():
    global relay_task
    relay = await container.get(OutboxRelayService)
    logger.info("producer_worker_starting")
    relay_task = asyncio.create_task(relay.run())


@app.after_shutdown
async def stop_outbox_relay():
    logger.info("producer_worker_stopping")
    relay = await container.get(OutboxRelayService)
    relay.stop()
    if relay_task and not relay_task.done():
        try:
            await asyncio.wait_for(relay_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("producer_relay_task_timeout")
            relay_task.cancel()


if __name__ == "__main__":
    asyncio.run(app.run())
