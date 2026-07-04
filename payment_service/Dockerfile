# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.14

# --- builder --
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

# переменные poetry
ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

# системные зависимости для сборки (asyncpg и другие пакеты с C-расширениями).
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends gcc libpq-dev && \
#     rm -rf /var/lib/apt/lists/*

# устанавливаем poetry
# --no-cache-dir экономит ~30MB
RUN pip install --no-cache-dir poetry==2.4.1

# создаем рабочую директорию
WORKDIR /app

# зависимости пересобираются только тогда, когда меняется poetry.lock,
# а не при каждом изменении кода + создаем персистентный кэш между сборками
# poetry не будет скачивать пакеты заново, если они уже есть в кэше!!!
COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/tmp/poetry_cache \
    poetry install --only main --no-root --no-ansi

# копируем код и устанавливаем сам проект в уже готовый venv
COPY src/ ./src/
#RUN --mount=type=cache,target=/tmp/poetry_cache \
#    poetry install --only main --no-ansi


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

# runtime-зависимости (libpq5 нужна asyncpg для работы с PostgreSQL).
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends libpq5 && \
#     rm -rf /var/lib/apt/lists/*

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

# документируем порт (но не открываем)
EXPOSE 8000

# LABEL ...

# обязательно переключаемся на ранее созданного non-root юзера
USER appuser

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
