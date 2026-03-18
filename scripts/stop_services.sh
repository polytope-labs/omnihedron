#!/usr/bin/env bash
# stop_services.sh — Stop query services (and optionally postgres)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

WITH_DB=false
for arg in "$@"; do
  case "$arg" in
    --with-db) WITH_DB=true ;;
    *) echo "Unknown flag: $arg"; exit 1 ;;
  esac
done

echo "==> Stopping query services..."
docker compose -f docker/docker-compose.yml stop omnihedron subql-query-ts 2>/dev/null || true
docker compose -f docker/docker-compose.yml rm -f omnihedron subql-query-ts 2>/dev/null || true

if [ "$WITH_DB" = true ]; then
  echo "==> Stopping PostgreSQL..."
  docker compose -f docker/docker-compose.yml down
fi

echo "==> Done."
