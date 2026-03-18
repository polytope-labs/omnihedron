#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

echo "========================================"
echo " SubQL Integration Test Runner"
echo "========================================"

# Step 1: Set up the database
echo ""
echo "[1/4] Setting up database..."
bash "$SCRIPT_DIR/setup_db.sh"

# Step 2: Start services
echo ""
echo "[2/4] Starting services..."
bash "$SCRIPT_DIR/start_services.sh"

# Step 3: Source env and run tests
echo ""
echo "[3/4] Running integration tests..."

# shellcheck source=/dev/null
source "$PROJECT_ROOT/.env.test"

TEST_EXIT_CODE=0
RUST_SERVICE_URL=http://localhost:3000 \
TS_SERVICE_URL=http://localhost:3001 \
SCHEMA_NAME="$SCHEMA_NAME" \
  cargo test --test integration_test -- --nocapture 2>&1 || TEST_EXIT_CODE=$?

# Step 4: Stop services
echo ""
echo "[4/4] Stopping services..."
bash "$SCRIPT_DIR/stop_services.sh"

echo ""
echo "========================================"
if [ "$TEST_EXIT_CODE" -eq 0 ]; then
  echo " All integration tests PASSED"
else
  echo " Some integration tests FAILED (exit code: $TEST_EXIT_CODE)"
fi
echo "========================================"

exit "$TEST_EXIT_CODE"
