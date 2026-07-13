#!/bin/sh
set -e

echo "=== OrderFlow DLQ Watcher ==="

# БД у сервиса нет - миграции не нужны

exec "$@"
