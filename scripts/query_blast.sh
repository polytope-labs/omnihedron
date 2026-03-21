#!/usr/bin/env bash
# query_blast.sh — Sustained mixed-query load against omnihedron
set -euo pipefail

URL="${1:-http://localhost:3000}"
CONCURRENCY="${2:-10}"
DURATION="${3:-300}"  # seconds

# Query templates
CONNECTION='{"query":"{ assetTeleporteds(first: 20, orderBy: [BLOCK_NUMBER_ASC, ID_ASC]) { totalCount nodes { id chain blockNumber amount } pageInfo { hasNextPage endCursor } } }"}'
SINGLE='{"query":"{ assetTeleported(id: \"0x2c5edd0c6b1b3152b324e09e70e3e8ec1a3898ec6\") { id chain blockNumber amount } }"}'
AGGREGATE='{"query":"{ assetTeleporteds { aggregates { sum { blockNumber } count distinctCount { id } average { blockNumber } } } }"}'
METADATA='{"query":"{ _metadata(chainId: \"11155111\") { lastProcessedHeight chain targetHeight } }"}'
FILTER='{"query":"{ assetTeleporteds(filter: { blockNumber: { greaterThan: 2156000 }, chain: { equalTo: \"KUSAMA-4009\" } }, first: 10) { totalCount nodes { id blockNumber chain } } }"}'
BATCH="[${CONNECTION},${SINGLE}]"
PAGINATION='{"query":"{ assetTeleporteds(first: 5) { nodes { id } pageInfo { endCursor hasNextPage } } }"}'

# Relation queries — backward relations reuse the same SQL shapes within a request,
# triggering statement cache hits (count + select on child table).
RELATION_BACKWARD='{"query":"{ testAuthors { nodes { id name books { totalCount nodes { id title } } } } }"}'
RELATION_NESTED='{"query":"{ testAuthor(id: \"author-alice\") { id name books(first: 5, orderBy: TITLE_ASC) { totalCount nodes { id title creator { id name } } pageInfo { hasNextPage } } } }"}'
RELATION_FILTER='{"query":"{ testAuthors(filter: { books: { some: { title: { startsWith: \"Book\" } } } }) { totalCount nodes { id name books { totalCount nodes { id title } } } } }"}'

# DataLoader queries — forward relations across multiple distinct FK targets.
# 10 authors × 20 books × 10 distinct creators = batches of ~3 keys each.
DATALOADER_ALL='{"query":"{ testAuthors(first: 100) { nodes { id books(first: 100) { nodes { id title creator { id name } } } } } }"}'
DATALOADER_FILTERED='{"query":"{ testAuthors(first: 5) { nodes { id books(first: 100) { nodes { id creator { id name } } } } } }"}'

QUERIES=("$CONNECTION" "$SINGLE" "$AGGREGATE" "$METADATA" "$FILTER" "$BATCH" "$PAGINATION" "$RELATION_BACKWARD" "$RELATION_NESTED" "$RELATION_FILTER" "$DATALOADER_ALL" "$DATALOADER_FILTERED")
NAMES=("connection" "single" "aggregate" "metadata" "filter" "batch" "pagination" "relation_bw" "relation_nested" "relation_filter" "dl_all" "dl_filtered")

echo "==> Query blast: ${CONCURRENCY} workers, ${DURATION}s duration against ${URL}"
echo "==> Query types: ${NAMES[*]}"
echo ""

blast_worker() {
  local id=$1
  local end=$((SECONDS + DURATION))
  local count=0
  while [ $SECONDS -lt $end ]; do
    local idx=$((RANDOM % ${#QUERIES[@]}))
    local query="${QUERIES[$idx]}"
    local name="${NAMES[$idx]}"
    local status
    status=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$URL" \
      -H "Content-Type: application/json" \
      -d "$query" --max-time 10 2>/dev/null || echo "ERR")
    count=$((count + 1))
    if [ $((count % 50)) -eq 0 ]; then
      echo "[worker-$id] $count queries sent (last: $name=$status)"
    fi
  done
  echo "[worker-$id] DONE: $count total queries"
}

for i in $(seq 1 "$CONCURRENCY"); do
  blast_worker "$i" &
done

echo "==> All $CONCURRENCY workers launched. Running for ${DURATION}s..."
wait
echo "==> Query blast complete."
