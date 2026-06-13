#!/bin/sh
set -e

echo "=== PayFlow Payment Service ==="
echo "Version: ${APP_VERSION:-unknown}"

if [ "$RUN_MIGRATIONS" = "true" ]; then
  echo "Running Alembic migrations..."
  alembic upgrade head
  echo "Migrations applied successfully."
fi

exec "$@"
