# omnihedron

A high-performance Rust reimplementation of [`@subql/query`](https://github.com/subquery/subql/tree/main/packages/query) — the GraphQL query service for SubQuery Network indexers.

The original TypeScript service uses [PostGraphile](https://www.graphile.org/postgraphile/) to auto-generate a full GraphQL API from a live PostgreSQL schema at runtime. This port replicates that behaviour entirely in Rust, producing an identical external API while handling high concurrent load across all CPU cores.

---

## Why Rust?

The TypeScript service is single-threaded. Under high query concurrency the Node.js event loop saturates and the process becomes the bottleneck — even when PostgreSQL has capacity to spare.

This Rust implementation:
- Dispatches work across all CPU cores via [Tokio](https://tokio.rs/)
- Has no garbage collector pauses or JIT deoptimisations
- Maintains stable throughput at tens of thousands of concurrent connections
- Produces **2–5x lower p99 tail latency** under load

---

## Benchmarks

Measured against a real SubQuery testnet database (47 tables, 9 enum types). Both services connect to the same PostgreSQL instance. 10,000 requests per test, complex GraphQL queries (not just metadata).

### Throughput (req/sec) — 10,000 requests, complex queries

| Concurrency | Query | Rust | TypeScript | Speedup |
|---|---|---|---|---|
| 100 | Filtered connection (20 rows) | 2,610 | 411 | 6.4x |
| 100 | Nested relations | 5,807 | 656 | 8.8x |
| 100 | Complex filter + distinct | 3,374 | 290 | 11.6x |
| 100 | 100 rows + edges | 2,125 | 277 | 7.7x |
| 100 | Aggregates (sum+min only) | 4,302 | 751 | 5.7x |
| 100 | Full aggregates + groupBy | 575 | 537 | ~same |
| 500 | Filtered connection | 2,739 | 455 | 6.0x |
| 500 | Nested relations | 6,104 | 735 | 8.3x |
| 1,000 | Complex filter | 2,469 | 331 | 7.5x |
| 5,000 | Filtered connection | 2,784 | 416 | 6.7x |
| 5,000 | Nested relations | 6,333 | 727 (timing out) | 8.7x |
| 10,000 | Filtered connection | 2,776 | dead | ∞ |
| 10,000 | Nested relations | 5,945 | dead | ∞ |

### p99 Tail Latency

| Concurrency | Query | Rust p99 | TS p99 |
|---|---|---|---|
| 100 | Filtered connection | 101ms | 1,961ms |
| 100 | Nested relations | 94ms | 1,031ms |
| 500 | Complex filter | 287ms | 5,872ms |
| 1,000 | 100 rows | 486ms | 12,647ms |
| 5,000 | Filtered connection | 1,830ms | 30,000ms (timeout) |

**Key observations:**

- **Rust is 6-12x faster** on query-heavy workloads (filters, relations, large results)
- **Selective aggregates** — only computes requested aggregates in SQL, making simple aggregates 13x faster than TS
- **TS collapses at ~5,000 concurrent connections** (Node.js event loop saturation)
- **Rust maintains stable throughput** up to 10,000+ concurrent connections
- **Rust p99 stays under 2s** at 5,000 concurrency; TS hits 30s timeout

> Benchmarks run on a 128-core machine. Both services used a pool of 10 PostgreSQL connections (default). Run `cargo build --release --bin bench && ./target/release/bench --help` to reproduce.

---

## Memory

Measured on the same machine using `/proc/{pid}/status` (RSS = resident physical RAM).

### Idle (after startup, no traffic)

| Service | RSS |
|---|---|
| Rust (8 worker threads) | **21 MB** |
| TypeScript | 167 MB |

### Peak under load (concurrency=500, 3,000 requests)

| Service | Peak RSS | Growth from idle |
|---|---|---|
| Rust (8 worker threads) | **47 MB** | +26 MB |
| TypeScript | 408 MB | +241 MB (2.4x) |

**Rust uses ~8.7x less memory under load than TypeScript.**

TypeScript's memory grows significantly under load due to V8 heap expansion — the GC allows the heap to balloon as live objects accumulate. Rust's memory grows only by the working set of in-flight requests (async task state + query buffers) and stays flat once the pool of worker stacks is warm.

### About thread count and memory

Tokio defaults to one worker thread per CPU core. On a machine with many cores, this inflates RSS from thread stacks (each thread allocates up to 2 MB of stack as it runs deep call chains). This is controlled with the `TOKIO_WORKER_THREADS` environment variable:

| Thread count | Idle RSS | Peak RSS (c=500) |
|---|---|---|
| 8 threads | 21 MB | 47 MB |
| 128 threads (128-core machine) | 678 MB | 680 MB |

For a PostgreSQL-bound service with a pool of 10 connections, 8–16 worker threads is optimal regardless of core count. Set `TOKIO_WORKER_THREADS=<n>` to match your deployment.

---

## API Compatibility

The Rust service generates an identical GraphQL schema to the TypeScript/PostGraphile service. This is verified at every CI run by an integration test that introspects both live services and asserts the full type set matches.

For every table in the PostgreSQL schema, the service generates:

```graphql
type Query {
  # Connection query (list + pagination + filtering)
  {entities}(
    first: Int, last: Int,
    after: Cursor, before: Cursor,
    offset: Int,
    orderBy: [{Entity}OrderBy!],
    orderByNull: NullOrder,
    filter: {Entity}Filter,
    distinct: [{Entity}DistinctEnum!],
    blockHeight: String          # historical tables only
  ): {Entity}Connection!

  # Single record by primary key
  {entity}(id: ID!): {Entity}
}
```

With full connection/pagination types (`{Entity}Connection`, `{Entity}Edge`, `PageInfo`), filter input types (per-column operators: `equalTo`, `in`, `like`, `isNull`, logical `and`/`or`/`not`, relation filters), ordering, aggregates, and subscriptions — matching PostGraphile's naming conventions exactly.

---

## Architecture

```
PostgreSQL introspection (information_schema + pg_catalog)
         │
         ▼
   Schema builder  ──→  async-graphql dynamic schema
         │
         ▼
   axum HTTP server  (Tokio, all CPU cores)
    ├── POST /graphql   (queries + batches)
    ├── GET  /graphql   (WebSocket subscriptions)
    └── GET  /health
         │
         ▼
   Hot reload listener  (LISTEN/NOTIFY on schema channel)
```

### Stack

| Concern | Crate |
|---|---|
| HTTP server | `axum` + `tower` |
| GraphQL engine | `async-graphql` (dynamic schema module) |
| PostgreSQL driver | `tokio-postgres` |
| Connection pooling | `deadpool-postgres` |
| Async runtime | `tokio` |
| Serialisation | `serde` + `serde_json` |
| Logging | `tracing` + `tracing-subscriber` |

### Key design points

**Dynamic schema** — `async-graphql`'s `dynamic` module builds the entire GraphQL schema at runtime from PostgreSQL introspection. No code generation, no compile-time schema.

**Schema hot reload** — a dedicated PostgreSQL connection `LISTEN`s on the SubQuery schema channel. When a `schema_updated` notification arrives, introspection reruns and the schema is atomically swapped behind an `Arc<RwLock<Schema>>`. In-flight requests are unaffected.

**SQL safety** — all user-supplied values use parameterized `$N` placeholders. Column and table names come from introspection and are never interpolated from user input.

**Name inflection** — replicates PostGraphile's `formatInsideUnderscores` behaviour exactly: leading underscores are preserved, consecutive uppercase runs are normalised (e.g. `cumulative_volume_u_s_ds` → `CumulativeVolumeUsds`), Latin irregular plurals handled (`metadata` → `Metadatum`/`Metadata`).

---

## Getting Started

### Prerequisites

- Rust 1.85+ (`rustup update stable`)
- PostgreSQL 14+
- A running SubQuery indexer database

### Build

```bash
cargo build --release
```

### Run

```bash
./target/release/omnihedron \
  --name <schema_name> \
  --port 3000
```

Database connection via environment variables:

```bash
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASS=postgres
DB_DATABASE=indexer
```

### Docker

Helper scripts in `scripts/` manage the full Docker lifecycle:

```bash
# First-time setup: start PostgreSQL and load fixture data
bash scripts/setup_db.sh

# Build the Rust binary + Docker image and start both services
bash scripts/start_services.sh

# Stop query services (leave PostgreSQL running)
bash scripts/stop_services.sh

# Stop everything including PostgreSQL
bash scripts/stop_services.sh --with-db
```

Services after startup:
- Rust service → `http://localhost:3000/graphql`
- TypeScript service → `http://localhost:3001/graphql`

---

## Configuration

All flags also accept environment variables with the `OMNIHEDRON_` prefix.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--name` / `-n` | `OMNIHEDRON_NAME` | required | PostgreSQL schema name |
| `--port` / `-p` | `OMNIHEDRON_PORT` | `3000` | HTTP port |
| `--playground` | `OMNIHEDRON_PLAYGROUND` | off | Enable GraphiQL UI |
| `--subscription` | `OMNIHEDRON_SUBSCRIPTION` | off | Enable WebSocket subscriptions |
| `--aggregate` | `OMNIHEDRON_AGGREGATE` | on | Enable aggregation queries |
| `--unsafe-mode` | `OMNIHEDRON_UNSAFE` | off | Disable all query limits |
| `--query-limit` | `OMNIHEDRON_QUERY_LIMIT` | `100` | Max records per query |
| `--query-batch-limit` | `OMNIHEDRON_QUERY_BATCH_LIMIT` | unlimited | Max queries per batch |
| `--query-depth-limit` | `OMNIHEDRON_QUERY_DEPTH_LIMIT` | unlimited | Max query AST depth |
| `--query-alias-limit` | `OMNIHEDRON_QUERY_ALIAS_LIMIT` | unlimited | Max field aliases |
| `--query-complexity` | `OMNIHEDRON_QUERY_COMPLEXITY` | unlimited | Max query complexity score |
| `--query-timeout` | `OMNIHEDRON_QUERY_TIMEOUT` | `10000` | Query timeout (ms) |
| `--max-connection` | `OMNIHEDRON_MAX_CONNECTION` | `10` | PostgreSQL pool size |
| `--indexer` | `OMNIHEDRON_INDEXER` | none | Indexer API URL for metadata fallback |
| `--disable-hot-schema` | `OMNIHEDRON_DISABLE_HOT_SCHEMA` | off | Disable schema hot reload |
| `--log-level` | `OMNIHEDRON_LOG_LEVEL` | `info` | `fatal\|error\|warn\|info\|debug\|trace` |
| `--output-fmt` | `OMNIHEDRON_OUTPUT_FMT` | `colored` | `json\|colored` |
| `--pg-ca` / `--pg-key` / `--pg-cert` | | none | PostgreSQL TLS certificates |

---

## Testing

```bash
# Unit tests only
cargo test --lib

# Full integration tests (requires both services running via docker compose)
docker compose -f docker/docker-compose.yml up -d
cargo test
```

62 integration tests across 8 files (`tests/test_basics.rs`, `tests/test_pagination.rs`, `tests/test_filters.rs`, `tests/test_ordering.rs`, `tests/test_aggregates.rs`, `tests/test_relations.rs`, `tests/test_historical.rs`, `tests/test_limits.rs`) verify the Rust service against the same live PostgreSQL database:

- Full schema compatibility (`test_introspection_types` — 716 types matched)
- Entity list queries and field correctness
- Cursor-based pagination (after/before, offset, last)
- Filter operators (equalTo, in, like, ilike, range, logical and/or/not, enum filters)
- Multi-field ordering and distinct
- Aggregations (sum, count, min, max, stddev, variance)
- Relation queries (forward, backward, nested)
- Historical block height queries
- `_metadata` query
- GraphQL variables
- Batch queries
- Health endpoint

### Running benchmarks

```bash
cargo build --release --bin bench

# Single service
./target/release/bench --url http://localhost:3000 --concurrency 100 --requests 1000

# Compare both (requires docker compose -f docker/docker-compose.yml up)
bash scripts/bench_compare.sh
```
