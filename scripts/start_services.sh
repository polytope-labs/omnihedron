#!/usr/bin/env bash
# start_services.sh — Build the Rust binary on the host, package into Docker,
#                     then start both query services via Docker Compose.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

ENV_FILE="$PROJECT_ROOT/.env.test"
if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: $ENV_FILE not found. Run scripts/setup_db.sh first."
  exit 1
fi
# shellcheck source=/dev/null
source "$ENV_FILE"
export SCHEMA_NAME

echo "==> Schema: $SCHEMA_NAME"

# 1. Build the release binary on the host
echo "==> Building Rust binary (cargo build --release)..."
cargo build --release --bin omnihedron

# 2. Build the Docker image (just copies the pre-built binary)
echo "==> Building Rust Docker image..."
docker compose -f docker/docker-compose.yml build omnihedron

# 3. Start both services
echo "==> Starting omnihedron (3000) and subql-query-ts (3001)..."
docker compose -f docker/docker-compose.yml up -d omnihedron subql-query-ts

# 4. Wait for readiness
wait_for() {
  local name="$1" url="$2" svc="$3" max=120 elapsed=0
  echo "==> Waiting for $name..."
  while ! curl -sf "$url" -o /dev/null 2>/dev/null; do
    if [ "$elapsed" -ge "$max" ]; then
      echo "ERROR: $name not ready after ${max}s."
      docker compose -f docker/docker-compose.yml logs "$svc" --tail=40
      return 1
    fi
    sleep 2; elapsed=$((elapsed + 2))
  done
  echo "==> $name is ready."
}

wait_for "Rust service"       "http://localhost:3000/health" "omnihedron"
wait_for "TypeScript service" "http://localhost:3001/health" "subql-query-ts"

echo ""
echo "==> Both services running."
echo "    Rust : http://localhost:3000/graphql"
echo "    TS   : http://localhost:3001/graphql"
