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

CONCURRENCY_LEVELS=(10 50 100)
TOTAL_REQUESTS=1000

echo "# SubQL Benchmark Comparison"
echo ""
echo "| Service     | Concurrency | Requests | RPS (approx) | Notes |"
echo "|-------------|-------------|----------|--------------|-------|"

run_bench_binary() {
  local label="$1"
  local url="$2"
  local concurrency="$3"
  local requests="$4"

  "$PROJECT_ROOT/target/release/bench" \
    --url "$url" \
    --concurrency "$concurrency" \
    --requests "$requests" \
    --label "$label" \
    2>&1
}

# Build bench binary if needed
echo "Building bench binary..." >&2
cd "$PROJECT_ROOT"
cargo build --release --bin bench 2>&1 >&2

for CONCURRENCY in "${CONCURRENCY_LEVELS[@]}"; do
  for SERVICE in "Rust:http://localhost:3000" "TypeScript:http://localhost:3001"; do
    NAME="${SERVICE%%:*}"
    URL="${SERVICE#*:}"
    run_bench_binary "$NAME" "$URL" "$CONCURRENCY" "$TOTAL_REQUESTS"
  done
done
