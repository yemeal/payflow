import hashlib
import json
from typing import Any


def compute_payload_hash(payload: dict[str, Any]) -> str:
    """
    Привязываем idempotency key к конкретному payload.

    В Redis попадает только SHA-256, поэтому сырой refresh-токен там не хранится
    как часть ключа или lock-записи.
    """
    canonical_payload = json.dumps(
        payload,
        sort_keys=True,
        default=str,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()
