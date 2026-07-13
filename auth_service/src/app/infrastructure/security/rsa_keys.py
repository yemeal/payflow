from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import (
    RSAPrivateKey,
    RSAPublicKey,
)

from app.domain.exceptions import DomainErrors


MIN_RSA_KEY_SIZE_BITS = 2048


@dataclass(frozen=True, slots=True)
class RSAKeyPair:
    private_key: RSAPrivateKey
    public_key: RSAPublicKey


def load_rsa_private_key(path: Path) -> RSAPrivateKey:
    """
    Прочитать закрытый RSA-ключ из PEM-файла.

    Функция вызывается при старте приложения. Любая проблема с файлом,
    форматом или размером ключа останавливает запуск: без рабочего ключа
    auth_service не может корректно выпускать access-токены.
    """
    try:
        pem = path.read_bytes()
    except OSError as error:
        raise DomainErrors.Token.INVALID_SIGNING_KEY() from error

    try:
        private_key = serialization.load_pem_private_key(
            pem,
            password=None,
        )
    except (TypeError, ValueError) as error:
        raise DomainErrors.Token.INVALID_SIGNING_KEY() from error

    if not isinstance(private_key, RSAPrivateKey):
        raise DomainErrors.Token.INVALID_SIGNING_KEY()
    if private_key.key_size < MIN_RSA_KEY_SIZE_BITS:
        raise DomainErrors.Token.INVALID_SIGNING_KEY()

    return private_key


def load_rsa_public_key(path: Path) -> RSAPublicKey:
    """
    Прочитать публичный RSA-ключ из PEM-файла.

    Публичный ключ не является секретом, но его источник должен быть доверенным:
    подмена файла позволит атакующему подписывать принимаемые сервисом токены.
    """
    try:
        pem = path.read_bytes()
    except OSError as error:
        raise DomainErrors.Token.INVALID_SIGNING_KEY() from error

    try:
        public_key = serialization.load_pem_public_key(pem)
    except (TypeError, ValueError) as error:
        raise DomainErrors.Token.INVALID_SIGNING_KEY() from error

    if not isinstance(public_key, RSAPublicKey):
        raise DomainErrors.Token.INVALID_SIGNING_KEY()
    if public_key.key_size < MIN_RSA_KEY_SIZE_BITS:
        raise DomainErrors.Token.INVALID_SIGNING_KEY()

    return public_key


def load_rsa_key_pair(
    private_key_path: Path,
    public_key_path: Path,
) -> RSAKeyPair:
    """
    Загрузить пару и проверить, что публичный ключ получен из закрытого.

    Проверка выполняется при старте auth_service и защищает от ситуации, когда
    сервис подписывает одним ключом, а потребителям случайно отдали другой.
    """
    private_key = load_rsa_private_key(private_key_path)
    public_key = load_rsa_public_key(public_key_path)

    if (
        private_key.public_key().public_numbers()
        != public_key.public_numbers()
    ):
        raise DomainErrors.Token.INVALID_SIGNING_KEY()

    return RSAKeyPair(
        private_key=private_key,
        public_key=public_key,
    )
