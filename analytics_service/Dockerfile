# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.14

# --- builder ---
FROM python:${PYTHON_VERSION}-slim-bookworm AS builder

ENV POETRY_VIRTUALENVS_IN_PROJECT=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=1 \
    POETRY_CACHE_DIR=/tmp/poetry_cache

RUN pip install --no-cache-dir poetry==2.4.1

WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN --mount=type=cache,target=/tmp/poetry_cache \
    poetry install --only main --no-root --no-ansi

COPY src/ ./src/

# --- runtime ---
FROM python:${PYTHON_VERSION}-slim-bookworm AS runtime

ARG APP_VERSION=develop
ENV APP_VERSION=${APP_VERSION}

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 1000 appuser && \
    useradd --uid 1000 --gid 1000 --no-create-home --shell /bin/false appuser

WORKDIR /app

COPY --from=builder /app/.venv ./.venv

COPY src/ ./src/
COPY alembic.ini ./
COPY alembic/ ./alembic/

COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONPATH="/app/src"

EXPOSE 8001

USER appuser

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]