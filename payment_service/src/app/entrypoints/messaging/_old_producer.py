# import asyncio
# import signal
# import structlog
#
# from app.infrastructure.di import create_container
# from app.core.logging import setup_logging
# from app.application.services.outbox_relay import OutboxRelayService
#
# logger = structlog.get_logger()
#
# async def main() -> None:
#     setup_logging()
#     logger.info("worker_starting")
#
#     container = create_container()
#
#     relay = await container.get(OutboxRelayService)
#
#     # Graceful shutdown handling
#     loop = asyncio.get_running_loop()
#     stop_event = asyncio.Event()
#
#     def handle_stop_signal() -> None:
#         logger.info("worker_stop_signal_received")
#         relay.stop()
#         stop_event.set()
#
#     for sig in (signal.SIGINT, signal.SIGTERM):
#         loop.add_signal_handler(sig, handle_stop_signal)
#
#     # Run relay
#     relay_task = asyncio.create_task(relay.run())
#
#     # Wait for either signal or task completion
#     await stop_event.wait()
#
#     try:
#         await asyncio.wait_for(relay_task, timeout=10.0)
#     except asyncio.TimeoutError:
#         logger.warning("worker_relay_task_timeout")
#         relay_task.cancel()
#
#     await container.close()
#     logger.info("worker_stopped")
#
# if __name__ == "__main__":
#     asyncio.run(main())
