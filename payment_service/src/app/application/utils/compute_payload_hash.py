import json
import hashlib


def compute_payload_hash(payload: dict) -> str:
    """Используется, чтобы посчитать хеш пейлоада"""
    # один и тот же ключ идемпотетности должен быть привязан к одному пейлоаду
    # если клиент отправит тот же ключ с другими данными - ошибка.
    canonical = json.dumps(
        payload,
        sort_keys=True,  # критично, т.к. {"a":1,"b":2} и {"b":2,"a":1} дадут разные хеши
        default=str,  # корректная обработка нестандартных типов данных по типу Decimal()
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
