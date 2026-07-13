import structlog
import logging
import logging.config

from app.core.settings import get_settings

settings = get_settings()

# общие процессоры, обеспечивают только "обогащение" данных без рендера и форматтинга
# цепочка, через которую проходит каждый лог
_SHARED_PROCESSORS: list[structlog.types.Processor] = [
    structlog.contextvars.merge_contextvars,  # добавляет в лог contextvars, привязанные в миддлваре или таске.
    structlog.stdlib.add_log_level,  # добавляем уровень (INFO, WARNING и тд)
    structlog.stdlib.add_logger_name,  # полезно знать какой именно логгер сработал
    structlog.processors.TimeStamper(fmt="iso", utc=True),  # добавляем таймстемп к логу
    # записывает стек вызовов (только если явно передаем stack_info=True)
    # разворачивает цепочку всех функций и методов, которые привели к этой строчке кода
    # критически важно при отладке, если варн возникает глубоко в утилитарной функции из десятка мест в аппе
    # и нам нужно понять, кто именно ее сейчас вызвал
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,  # правильно обрабатываем исключения, показываем стек, когда апп упал
]


def _get_log_level() -> int:
    return getattr(logging, settings.LOG_LEVEL)


def _get_renderer() -> structlog.types.Processor:
    if settings.DEV_LOGS:
        return structlog.dev.ConsoleRenderer(colors=True)
    return structlog.processors.JSONRenderer()


def setup_logging() -> None:
    # настраивает логгеры, созданные через structlog.get_logger()
    structlog.configure(
        processors=[
            *_SHARED_PROCESSORS,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,  # подготавливает event_dict для stdlib
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

    # перехватывает стандартный logging для единого формата
    logging.config.dictConfig(
        {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "structlog": {
                    "()": structlog.stdlib.ProcessorFormatter,
                    # цепочка для логов извне (uvicorn, celery, sqlalchemy и т.д.)
                    "foreign_pre_chain": _SHARED_PROCESSORS,
                    # финальный рендеринг (выполняется непосредственно перед выводом)
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

    # перехватываем логгеры из сторонних либ
    for logger_name in logging.root.manager.loggerDict:
        # как и почему это работает:
        # logging.getLogger() НЕ СОЗДАЕТ изолированный объект, он отправляет запрос к глобальному менеджеру.
        # менеджер заглядывает в свой loggerDict и если логгера с таким именем нет,
        # то он его создает, записывает в словарь и отдает ссылку. Если есть - просто отдает ссылку
        # поэтому logging.root.manager.loggerDict - это буквально РЕЕСТР ВСЕХ ЛОГГЕРОВ, которые когда либо
        # были запрошены с момента запуска интерпретатора.
        # мы просто ПРОХОДИМСЯ циклом ПО РЕЕСТРУ и подменяем каждого из них

        logger = logging.getLogger(logger_name)

        # сам по себе логгер не умеет писать, для этого к нему крепятся хендлеры.
        # поэтому отрываем их собственные хендлеры (чтобы не было дублей)
        logger.handlers.clear()

        # заставляем всплывать до нашего root-логгера
        # буквально означает "если ты получил сообщение, но тебе некуда его вывести, передай его своему родителю"
        # а так как ранее мы оторвали хэндлер, то сторонняя либа вынуждена отдать его вверх по цепочке до рута,
        # который настроен в dictConfig выше, где оно красиво оборачивается через structlog
        logger.propagate = True

        # применяем единый уровень логирования. дабы не тратить лишние ресурсы на формирование логов
        # либо берем базовый уровень, либо глушим сильно болтливые либы через settings
        # не забыть написать в .env те логгеры, которые хотим заглушить, если это необходимо
        # например uvicorn.access,sqlalchemy.engine.Engine
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
    """вызывать в любом месте кода для добавления контекста (user_id, order_id)"""
    structlog.contextvars.bind_contextvars(**kwargs)
