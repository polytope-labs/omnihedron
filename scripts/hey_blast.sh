#!/usr/bin/env bash
# hey_blast.sh — Sustained parallel load using hey, all query types at once
set -euo pipefail

URL="${1:-http://localhost:3000}"
CONCURRENCY="${2:-20}"
DURATION="${3:-1800}"  # total seconds
HEY="$HOME/go/bin/hey"

declare -A QUERIES
QUERIES[connection]='{"query":"{ assetTeleporteds(first: 20, orderBy: [BLOCK_NUMBER_ASC, ID_ASC]) { totalCount nodes { id chain blockNumber amount } pageInfo { hasNextPage endCursor } } }"}'
QUERIES[single]='{"query":"{ assetTeleported(id: \"0x2c5edd0c6b1b3152b324e09e70e3e8ec1a3898ec6\") { id chain blockNumber amount } }"}'
QUERIES[aggregate]='{"query":"{ assetTeleporteds { aggregates { sum { blockNumber } count distinctCount { id } average { blockNumber } } } }"}'
QUERIES[metadata]='{"query":"{ _metadata(chainId: \"11155111\") { lastProcessedHeight chain targetHeight } }"}'
QUERIES[filter]='{"query":"{ assetTeleporteds(filter: { blockNumber: { greaterThan: 2156000 }, chain: { equalTo: \"KUSAMA-4009\" } }, first: 10) { totalCount nodes { id blockNumber chain } } }"}'
QUERIES[pagination]='{"query":"{ assetTeleporteds(first: 5) { nodes { id } pageInfo { endCursor hasNextPage } } }"}'
QUERIES[relation_bw]='{"query":"{ testAuthors { nodes { id name books { totalCount nodes { id title } } } } }"}'
QUERIES[relation_nested]='{"query":"{ testAuthor(id: \"author-alice\") { id name books(first: 5, orderBy: TITLE_ASC) { totalCount nodes { id title creator { id name } } pageInfo { hasNextPage } } } }"}'
QUERIES[relation_filter]='{"query":"{ testAuthors(filter: { books: { some: { title: { startsWith: \"Book\" } } } }) { totalCount nodes { id name books { totalCount nodes { id title } } } } }"}'
QUERIES[dl_all]='{"query":"{ testAuthors(first: 100) { nodes { id books(first: 100) { nodes { id title creator { id name } } } } } }"}'

NAMES=(connection single aggregate metadata filter pagination relation_bw relation_nested relation_filter dl_all)
NUM_TYPES=${#NAMES[@]}

# Distribute concurrency across query types (minimum 1 per type)
PER_TYPE=$((CONCURRENCY / NUM_TYPES))
if [ "$PER_TYPE" -lt 1 ]; then
  PER_TYPE=1
fi
REMAINDER=$((CONCURRENCY - PER_TYPE * NUM_TYPES))

echo "==> hey blast: ${CONCURRENCY} total concurrency, ${DURATION}s, ALL query types in parallel"
echo "==> ${PER_TYPE} workers per type (${NUM_TYPES} types), ${REMAINDER} extra distributed to first types"
echo "==> Query types: ${NAMES[*]}"
echo ""

PIDS=()
for i in "${!NAMES[@]}"; do
  NAME="${NAMES[$i]}"
  BODY="${QUERIES[$NAME]}"
  C=$PER_TYPE
  if [ "$i" -lt "$REMAINDER" ] 2>/dev/null; then
    C=$((C + 1))
  fi

  echo "==> Starting $NAME (c=$C)"
  $HEY -z "${DURATION}s" -c "$C" -m POST \
    -H "Content-Type: application/json" \
    -d "$BODY" \
    "$URL" > "/tmp/hey_${NAME}.out" 2>&1 &
  PIDS+=($!)
done

echo ""
echo "==> All ${NUM_TYPES} query types launched. Running for ${DURATION}s..."
echo ""

# Wait for all to finish
for i in "${!NAMES[@]}"; do
  wait "${PIDS[$i]}" || true
  NAME="${NAMES[$i]}"
  echo "=== $NAME ==="
  grep -E "Requests/sec|Average|Fastest|Slowest|Status code" "/tmp/hey_${NAME}.out" || true
  echo ""
done

echo "==> hey blast complete."
