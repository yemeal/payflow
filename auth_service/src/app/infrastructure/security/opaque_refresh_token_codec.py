import hashlib
import secrets

from app.application.ports.dto import IssuedRefreshToken


class SHA256OpaqueRefreshTokenCodec:
    TOKEN_BYTES = 32

    def issue(self) -> IssuedRefreshToken:
        token = secrets.token_urlsafe(self.TOKEN_BYTES)

        return IssuedRefreshToken(value=token, digest=self.digest(token))

    def digest(self, token: str) -> bytes:
        # Наши токены ASCII, но вход от клиента может быть любым Unicode.
        # Хэшируем его как обычную строку: чужое значение просто не найдётся в БД,
        # а наружу уйдёт доменный INVALID_REFRESH вместо UnicodeEncodeError.
        return hashlib.sha256(token.encode("utf-8")).digest()
