#!/usr/bin/env bash
# create_fixture.sh — Extract a minimal SQL fixture from the live database for CI use.
#
# Produces tests/fixtures/test_db.sql containing:
#   - Full schema DDL (all tables, types, indexes, constraints)
#   - All rows from _metadata* and _global tables (small key-value stores)
#   - Up to ENTITY_ROWS rows from each entity table
#
# Usage:
#   bash scripts/create_fixture.sh
#
# Prerequisites: the PostgreSQL container must be running (docker compose up -d postgres).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

FIXTURE_DIR="$PROJECT_ROOT/tests/fixtures"
FIXTURE_FILE="$FIXTURE_DIR/test_db.sql"
ENTITY_ROWS=20

# --- connection -----------------------------------------------------------------
# Run pg_dump/psql inside the Docker container to avoid client/server version
# mismatch. Falls back to host binaries when OMNIHEDRON_NO_DOCKER=1 is set
# (e.g. when this script itself runs inside CI's service container).
PG_HOST="${DB_HOST:-localhost}"
PG_PORT="${DB_PORT:-5432}"       # inside-container port
PG_USER="${DB_USER:-postgres}"
PG_PASS="${DB_PASS:-postgres}"
PG_DB="${DB_DATABASE:-indexer}"

# Detect whether we should exec into the container or talk to a host DB.
# When running on a CI runner that already has PostgreSQL as a service, set
# OMNIHEDRON_NO_DOCKER=1 so we skip docker exec.
USE_DOCKER=1
if [ "${OMNIHEDRON_NO_DOCKER:-0}" = "1" ]; then
  USE_DOCKER=0
fi

# Detect the compose container name (allows override)
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-subql-postgres}"

psql_cmd() {
  if [ "$USE_DOCKER" = "1" ]; then
    docker exec -e PGPASSWORD="$PG_PASS" "$POSTGRES_CONTAINER" \
      psql -U "$PG_USER" -d "$PG_DB" "$@"
  else
    PGPASSWORD="$PG_PASS" psql -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" "$@"
  fi
}

pg_dump_cmd() {
  if [ "$USE_DOCKER" = "1" ]; then
    docker exec -e PGPASSWORD="$PG_PASS" "$POSTGRES_CONTAINER" \
      pg_dump -U "$PG_USER" -d "$PG_DB" "$@"
  else
    PGPASSWORD="$PG_PASS" pg_dump -h "$PG_HOST" -p "$PG_PORT" -U "$PG_USER" -d "$PG_DB" "$@"
  fi
}

# --- discover schema ------------------------------------------------------------
SCHEMA=$(psql_cmd -t -c \
  "SELECT nspname FROM pg_namespace
   WHERE nspname NOT IN ('public','information_schema')
     AND nspname NOT LIKE 'pg_%'
   ORDER BY oid LIMIT 1;" | tr -d '[:space:]')

if [ -z "$SCHEMA" ]; then
  echo "ERROR: No user schema found in database '$PG_DB'." >&2
  exit 1
fi

echo "==> Schema: $SCHEMA"

# --- get table lists ------------------------------------------------------------
ALL_TABLES=$(psql_cmd -t -c \
  "SELECT table_name FROM information_schema.tables
   WHERE table_schema = '$SCHEMA'
   ORDER BY table_name;" | tr -d ' ' | grep -v '^$')

META_TABLES=$(echo "$ALL_TABLES" | grep -E '^_metadata|^_global' || true)
ENTITY_TABLES=$(echo "$ALL_TABLES" | grep -Ev '^_metadata|^_global' || true)

echo "==> Found $(echo "$ALL_TABLES" | wc -l | tr -d ' ') tables total"
echo "    $(echo "$META_TABLES" | wc -l | tr -d ' ') metadata/global tables (full dump)"
echo "    $(echo "$ENTITY_TABLES" | wc -l | tr -d ' ') entity tables (${ENTITY_ROWS} rows each)"

# --- create output dir ----------------------------------------------------------
mkdir -p "$FIXTURE_DIR"

# --- 1. schema DDL --------------------------------------------------------------
echo "==> Dumping schema DDL..."
pg_dump_cmd --schema-only --schema="$SCHEMA" \
  --no-owner --no-privileges \
  --no-comments \
  > "$FIXTURE_FILE"

# --- 2. metadata / global tables (all rows) ------------------------------------
echo "==> Dumping metadata/global tables..."
for TABLE in $META_TABLES; do
  ROW_COUNT=$(psql_cmd -t -c \
    "SELECT COUNT(*) FROM \"$SCHEMA\".\"$TABLE\";" | tr -d '[:space:]')

  if [ "${ROW_COUNT:-0}" -gt 0 ]; then
    pg_dump_cmd --data-only \
      --no-owner --no-privileges \
      --table="\"$SCHEMA\".\"$TABLE\"" \
      >> "$FIXTURE_FILE"
    echo "    $TABLE: $ROW_COUNT rows"
  else
    echo "    $TABLE: empty — skipping"
  fi
done

# --- 3. entity tables (limited rows) -------------------------------------------
# pg_dump --where is not universally available; use psql \COPY to stdout instead.
# The output format is PostgreSQL COPY text, wrapped in COPY...FROM stdin blocks.
echo "==> Dumping entity tables (up to ${ENTITY_ROWS} rows each)..."
for TABLE in $ENTITY_TABLES; do
  ROW_COUNT=$(psql_cmd -t -c \
    "SELECT COUNT(*) FROM \"$SCHEMA\".\"$TABLE\";" | tr -d '[:space:]')

  if [ "${ROW_COUNT:-0}" -eq 0 ]; then
    echo "    $TABLE: empty — skipping"
    continue
  fi

  # Get column list to build the COPY header
  COLS=$(psql_cmd -t -c \
    "SELECT string_agg('\"' || column_name || '\"', ', ' ORDER BY ordinal_position)
     FROM information_schema.columns
     WHERE table_schema = '$SCHEMA' AND table_name = '$TABLE';" | tr -d '[:space:]')

  # Emit a COPY block using psql \COPY to stdout
  {
    printf '\nCOPY "%s"."%s" (%s) FROM stdin;\n' "$SCHEMA" "$TABLE" "$COLS"
    psql_cmd -t -c \
      "\COPY (SELECT * FROM \"$SCHEMA\".\"$TABLE\" LIMIT ${ENTITY_ROWS}) TO STDOUT"
    printf '\\.\n'
  } >> "$FIXTURE_FILE"

  ACTUAL=$(psql_cmd -t -c \
    "SELECT COUNT(*) FROM (SELECT 1 FROM \"$SCHEMA\".\"$TABLE\" LIMIT ${ENTITY_ROWS}) t;" \
    | tr -d '[:space:]')
  echo "    $TABLE: $ACTUAL / $ROW_COUNT rows"
done

# --- summary --------------------------------------------------------------------
FIXTURE_SIZE=$(du -sh "$FIXTURE_FILE" | cut -f1)
echo ""
echo "==> Fixture written to: $FIXTURE_FILE"
echo "    Size: $FIXTURE_SIZE"
echo ""
echo "To use this fixture in CI:"
echo "  psql -U postgres -d indexer -f tests/fixtures/test_db.sql"
