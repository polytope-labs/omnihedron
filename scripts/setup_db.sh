#!/usr/bin/env bash
# setup_db.sh — Start PostgreSQL 17, load fixture data, write .env.test
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "==> Starting PostgreSQL..."
docker compose -f docker/docker-compose.yml up -d postgres

echo "==> Waiting for PostgreSQL to be healthy..."
until docker compose -f docker/docker-compose.yml exec -T postgres pg_isready -U postgres -d indexer -q 2>/dev/null; do
  sleep 2
done
echo "==> PostgreSQL is ready."

echo "==> Checking if fixture is already loaded..."
TABLE_COUNT=$(docker compose -f docker/docker-compose.yml exec -T postgres psql -U postgres -d indexer -t -c \
  "SELECT COUNT(*) FROM information_schema.tables
   WHERE table_schema NOT IN ('public','information_schema','pg_catalog')
   AND table_schema NOT LIKE 'pg_%';" 2>/dev/null | tr -d '[:space:]')

if [ "${TABLE_COUNT:-0}" -gt "0" ]; then
  echo "==> Fixture already loaded ($TABLE_COUNT tables). Skipping."
else
  echo "==> Loading fixture data..."
  docker compose -f docker/docker-compose.yml exec -T postgres \
    psql -U postgres -d indexer < "$PROJECT_ROOT/tests/fixtures/test_db.sql" || true
  echo "==> Fixture loaded."
fi

echo "==> Discovering schema name..."
SCHEMA_NAME=$(docker compose -f docker/docker-compose.yml exec -T postgres psql -U postgres -d indexer -t -c \
  "SELECT nspname FROM pg_namespace
   WHERE nspname NOT IN ('public','information_schema')
   AND nspname NOT LIKE 'pg_%'
   ORDER BY oid LIMIT 1;" 2>/dev/null | tr -d '[:space:]')

if [ -z "$SCHEMA_NAME" ]; then
  echo "ERROR: Could not discover schema name."
  exit 1
fi

echo "==> Schema: $SCHEMA_NAME"
cat > "$PROJECT_ROOT/.env.test" <<EOF
SCHEMA_NAME=$SCHEMA_NAME
DB_HOST=localhost
DB_PORT=5433
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=indexer
EOF
echo "==> Written .env.test"
