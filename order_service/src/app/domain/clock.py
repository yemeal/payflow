from datetime import datetime, timezone


def utc_now() -> datetime:
    """Naive-UTC, как во всех таблицах проекта (колонки без таймзоны)"""
    return datetime.now(timezone.utc).replace(tzinfo=None)
