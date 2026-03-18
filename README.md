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

Measured against a real SubQuery testnet database (47 tables, 9 enum types). Both services connect to the same PostgreSQL instance. Query: `_metadata { lastProcessedHeight chain specName }`.

### Throughput (req/sec) — 5,000 requests

| Concurrency | Rust | TypeScript | Rust vs TS |
|---|---|---|---|
| 10 | 1,315 | 1,177 | +12% |
| 50 | 1,326 | 1,294 | +2% |
| 100 | 1,275 | 1,290 | ~same |
| 500 | 1,233 | 1,182 | +4% |
| 1,000 | 1,367 | 1,167 | +17% |
| 2,000 | 1,328 | 1,342 | ~same |
| **5,000** | **1,409** | **315** | **+4.5x** |
| **10,000** | **1,411** | **318** | **+4.4x** |
| 20,000 | 1,418 | 642 | +2.2x |
| 50,000 | 1,466 | 657 | +2.2x |
| 64,000 | 1,410 | 665 | +2.1x |

### p99 Tail Latency

| Concurrency | Rust p99 | TypeScript p99 | Improvement |
|---|---|---|---|
| 10 | 13ms | 18ms | 1.3x |
| 50 | 112ms | 255ms | 2.3x |
| 200 | 200ms | 1,130ms | 5.6x |
| 1,000 | 950ms | 4,113ms | 4.3x |
| 5,000 | 3,479ms | 15,730ms | 4.5x |
| 10,000 | 3,427ms | 8,178ms | 2.4x |
| 64,000 | 3,453ms | 7,427ms | 2.2x |

**p99** = the slowest 1% of requests. This is what users actually experience as "slow" and what triggers timeouts in production.

**The cliff at 5,000 concurrency** is the Node.js event loop saturating — TypeScript throughput collapses to ~315 RPS and p99 hits 15 seconds. Rust holds flat at ~1,400 RPS all the way to 64,000 concurrent connections (near the OS file descriptor limit). Above that, the ceiling is entirely the PostgreSQL connection pool, not the application layer.

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
# First-time setup: start PostgreSQL and restore the dump
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

Integration tests spin up the TypeScript and Rust services against the same live PostgreSQL database and verify:

- Full schema compatibility (`test_introspection_types` — 716 types matched)
- Entity list queries and field correctness
- Cursor-based pagination
- Null/non-null filters
- Multi-field ordering
- Aggregations (sum, count, min, max)
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
