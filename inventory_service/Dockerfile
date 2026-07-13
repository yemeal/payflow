# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.14

# --- builder --
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

# переменные poetry
ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# устанавливаем poetry
# --no-cache-dir экономит ~30MB
RUN pip install --no-cache-dir poetry==2.4.1

# создаем рабочую директорию
WORKDIR /app

# зависимости пересобираются только тогда, когда меняется poetry.lock,
# а не при каждом изменении кода + создаем персистентный кэш между сборками
COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/tmp/poetry_cache \
    poetry install --only main --no-root --no-ansi

# копируем код
COPY src/ ./src/


# --- runtime ---
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

ARG APP_VERSION=develop
ENV APP_VERSION=${APP_VERSION}

# не генерить .pyc файлы (экономия места, не нужны в контейнере)
# и отключить буферизацию stdout/stderr (логи видны сразу)
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# создаем non-root юзера для безопасности
RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --no-create-home --shell /bin/false appuser

# создаем рабочую директорию
WORKDIR /app

# копируем зависимости
COPY --from=builder /app/.venv ./.venv

# копируем код и миграции
COPY src/ ./src/
COPY alembic.ini ./
COPY alembic/ ./alembic/

COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

# активируем venv и указываем Python где искать пакеты
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src"

# HTTP API у склада нет - порты не документируем

# обязательно переключаемся на ранее созданного non-root юзера
USER appuser

ENTRYPOINT ["/app/docker-entrypoint.sh"]
# по умолчанию - консьюмер команд; той же сборкой запускаются relay и поллер
# автоистечения (см. docker-compose.yml):
#   python -m app.entrypoints.messaging.relay
#   python -m app.entrypoints.messaging.expiry
CMD ["python", "-m", "app.entrypoints.messaging.consumer"]
