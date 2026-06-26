import structlog
import logging
import logging.config

from app.core.settings import get_settings

settings = get_settings()

# общие процессоры, обеспечивают только "обогащение" данных без рендера и форматтинга
# цепочка, через которую проходит каждый лог
_SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
]


def _get_log_level() -> int:
    return getattr(logging, settings.LOG_LEVEL)


def _get_renderer() -> structlog.types.Processor:
    if settings.DEV_LOGS:
        return structlog.dev.ConsoleRenderer(colors=True)
    return structlog.processors.JSONRenderer()


def setup_logging() -> None:
    structlog.configure(
        processors=[
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.make_filtering_bound_logger(_get_log_level()),
        cache_logger_on_first_use=True,
    )

    mute_level = logging.WARNING if logging.WARNING > _get_log_level() else _get_log_level()
    loggers_config = {}
    if hasattr(settings, "MUTE_LOGGERS"):
        for muted in settings.MUTE_LOGGERS:
            loggers_config[muted] = {
                "level": mute_level,
                "handlers": [],
                "propagate": True
            }

    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "structlog": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    "foreign_pre_chain": _SHARED_PROCESSORS,
                    "processors": [
                        structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                        _get_renderer(),
                    ],
                },
            },
            "handlers": {
                "default": {
                    "class": "logging.StreamHandler",
                    "formatter": "structlog",
                },
            },
            "loggers": loggers_config,
            "root": {
                "handlers": ["default"],
                "level": _get_log_level(),
            },
        }
    )

    for logger_name in logging.root.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
        logger.propagate = True

        if hasattr(settings, "MUTE_LOGGERS") and logger_name in settings.MUTE_LOGGERS:
            logger.setLevel(
                logging.WARNING
                if logging.WARNING > _get_log_level()
                else _get_log_level()
            )
        else:
            logger.setLevel(_get_log_level())


def clear_contextvars() -> None:
    """вызывать в middleware при начале обработки нового запроса/таски"""
    structlog.contextvars.clear_contextvars()


def bind_contextvars(**kwargs) -> None:
    """вызывать в любом месте кода для добавления контекста (event_id, payment_id)"""
    structlog.contextvars.bind_contextvars(**kwargs)
