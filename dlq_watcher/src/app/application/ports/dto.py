from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class DlqRecord:
    """
    Разобранный конверт из <топик>.dlq (contracts/envelope/dlq-envelope.v1.schema.json).

    Плоская проекция того, что нужно дежурному: где умерло, почему умерло и к какой
    саге относится. Само тело original сюда НЕ кладём: watcher его не обрабатывает,
    а тащить мегабайтный payload через лог и алерт незачем.
    """

    # топик, из которого прочитали (сам .dlq), и топик, куда переигрывать
    dlq_topic: str
    source_topic: str

    error_class: str
    error_message: str

    # сколько раз уже пытались обработать и сколько раз переигрывали (лимит redrive - 2)
    retry_count: int
    redrive_count: int

    failed_at: str | None = None

    # координаты исходного сообщения: нужны, чтобы найти его в source_topic руками
    partition: int | None = None
    offset: int | None = None
    consumer_group: str | None = None

    # корреляция саги, если исходное сообщение её несло (у мусора её может не быть)
    saga_id: str | None = None
    business_key: str | None = None
