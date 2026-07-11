"""
Тесты compute_payload_hash - привязка ключа идемпотентности к содержимому payload.

Хеш нужен, чтобы поймать переиспользование одного ключа с разными данными
(payload mismatch -> 409). Требования к функции: детерминизм, независимость
от порядка ключей и поддержка нестандартных типов (Decimal).

Формат документации: Проверяем / Успех / Нежелательное поведение.
"""

from decimal import Decimal

from app.application.utils.compute_payload_hash import compute_payload_hash


def test_deterministic():
    """
    Проверяем: один и тот же payload считается одинаково.
    Успех: два вызова дают идентичный хеш.
    Нежелательное поведение: недетерминированный хеш - ложные срабатывания mismatch.
    """
    payload = {"amount": "100.00", "currency": "RUB"}
    assert compute_payload_hash(payload) == compute_payload_hash(payload)


def test_key_order_independent():
    """
    Проверяем: порядок ключей в словаре не влияет на результат.
    Успех: {"a":1,"b":2} и {"b":2,"a":1} дают одинаковый хеш (sort_keys=True).
    Нежелательное поведение: разный хеш из-за порядка - клиент с тем же телом получит 409.
    """
    a = {"amount": "100.00", "currency": "RUB"}
    b = {"currency": "RUB", "amount": "100.00"}
    assert compute_payload_hash(a) == compute_payload_hash(b)


def test_different_payload_different_hash():
    """
    Проверяем: разные данные дают разный хеш.
    Успех: изменение суммы меняет хеш.
    Нежелательное поведение: коллизия на разных payload - двойная обработка пройдёт как дубль.
    """
    a = {"amount": "100.00", "currency": "RUB"}
    b = {"amount": "200.00", "currency": "RUB"}
    assert compute_payload_hash(a) != compute_payload_hash(b)


def test_supports_decimal():
    """
    Проверяем: payload с Decimal сериализуется без ошибок (default=str).
    Успех: возвращается корректный sha256-хекс длиной 64 символа.
    Нежелательное поведение: TypeError на несериализуемом типе.
    """
    payload = {"amount": Decimal("100.00"), "currency": "RUB"}
    result = compute_payload_hash(payload)
    assert isinstance(result, str)
    assert len(result) == 64
