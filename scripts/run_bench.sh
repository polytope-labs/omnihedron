#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

ENV_FILE="$PROJECT_ROOT/.env.test"
if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run scripts/setup_db.sh first."
  exit 1
fi

# shellcheck source=/dev/null
source "$ENV_FILE"

TS_PORT=3001
RUST_PORT=3000
TOTAL_REQUESTS=1000

# The metadata query works on any schema
METADATA_QUERY='{"query":"{ _metadata { lastProcessedHeight chain specName } }"}'

# -----------------------------------------------------------------
# Check if oha is available via docker
# -----------------------------------------------------------------
OHA_AVAILABLE=false
if docker image inspect ghcr.io/hatoo/oha:latest &>/dev/null 2>&1; then
  OHA_AVAILABLE=true
fi

run_oha() {
  local label="$1"
  local port="$2"
  local concurrency="$3"
  local query_body="$4"
  local url="http://localhost:${port}/graphql"

  echo ""
  echo "--- $label | port=$port | concurrency=$concurrency | n=$TOTAL_REQUESTS ---"

  if [ "$OHA_AVAILABLE" = true ]; then
    docker run --rm \
      --add-host=host.docker.internal:host-gateway \
      ghcr.io/hatoo/oha:latest \
      -n "$TOTAL_REQUESTS" \
      -c "$concurrency" \
      -m POST \
      -H "Content-Type: application/json" \
      -d "$query_body" \
      "http://host.docker.internal:${port}/graphql"
  else
    # Fallback: curl loop (sequential, not concurrent — approximation only)
    echo "(oha not available; running $TOTAL_REQUESTS sequential curl requests as fallback)"
    local start
    start=$(date +%s%3N)
    for i in $(seq 1 "$TOTAL_REQUESTS"); do
      curl -sf -X POST \
        -H "Content-Type: application/json" \
        -d "$query_body" \
        "$url" \
        -o /dev/null
    done
    local end
    end=$(date +%s%3N)
    local elapsed_ms=$(( end - start ))
    local rps=$(( TOTAL_REQUESTS * 1000 / elapsed_ms ))
    echo "Completed $TOTAL_REQUESTS requests in ${elapsed_ms}ms (~${rps} req/s sequential)"
  fi
}

echo "========================================"
echo " SubQL Benchmark"
echo " Schema: $SCHEMA_NAME"
echo " Query:  _metadata"
echo "========================================"

for CONCURRENCY in 10 50 100; do
  echo ""
  echo "###################################"
  echo "# Concurrency = $CONCURRENCY"
  echo "###################################"

  run_oha "TypeScript service" "$TS_PORT" "$CONCURRENCY" "$METADATA_QUERY"
  run_oha "Rust service"       "$RUST_PORT" "$CONCURRENCY" "$METADATA_QUERY"
done

echo ""
echo "========================================"
echo " Benchmark complete"
echo "========================================"
