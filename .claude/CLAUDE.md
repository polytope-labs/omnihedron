# omnihedron — Claude Context

## What this project is

**omnihedron** is a high-performance Rust rewrite of `@subql/query` (the GraphQL query service for SubQuery Network indexers). The TypeScript original uses PostGraphile to auto-generate a full GraphQL API from a live PostgreSQL schema at runtime. This port replicates that behaviour entirely in Rust, producing an identical external API while handling high concurrent load across all CPU cores.

GitHub: https://github.com/polytope-labs/omnihedron
Docker Hub: `polytopelabs/omnihedron`

---

## Stack

| Concern | Crate |
|---|---|
| HTTP server | `axum` |
| GraphQL engine | `async-graphql` (dynamic schema module) |
| PostgreSQL driver | `tokio-postgres` |
| Connection pooling | `deadpool-postgres` |
| CLI | `clap` |
| Logging | `tracing` + `tracing-subscriber` |
| Serialisation | `serde` + `serde_json` |

---

## Key architectural facts

- **Dynamic schema** — `async-graphql`'s `dynamic` module builds the entire GraphQL schema at runtime from PostgreSQL introspection. No code generation, no compile-time schema.
- **Schema hot reload** — a dedicated PostgreSQL connection `LISTEN`s on the SubQuery schema channel. When a `schema_updated` notification arrives, introspection reruns and the schema is atomically swapped behind an `Arc<RwLock<Schema>>`. **NOT YET IMPLEMENTED.**
- **SQL safety** — all user-supplied values use parameterised `$N` placeholders. Column and table names come from introspection and are never interpolated from user input.
- **PostGraphile compatibility** — naming conventions, cursor encoding (base64 JSON), `nodeId` field, `{entity}ByNodeId` root queries, `Node` interface all match PostGraphile exactly.

---

## Implementation status

### Done
- PostgreSQL introspection (tables, columns, types, PKs, FKs, unique constraints, enums)
- Full dynamic GraphQL schema generation per entity:
  - Object types with all columns as fields
  - `nodeId` computed field (base64(`["TypeName", pkValue]`)) on every entity
  - `{entity}(id: ID!)` single-record root query
  - `{entity}ByNodeId(nodeId: ID!)` root query
  - `{entities}(first, last, after, before, offset, orderBy, orderByNull, filter, distinct, blockHeight)` connection root query
  - `{Entity}Connection`, `{Entity}Edge`, `PageInfo` types
  - `{Entity}Filter` input with per-column operators + logical `and`/`or`/`not`
  - `{Entity}OrderBy` enum
  - `{Entity}DistinctEnum`
  - Enum scalar types with `EnumFilter`
  - Forward relation fields (FK → single record of related type)
  - Backward relation fields (reverse FK → `{Related}Connection` with full pagination)
  - Aggregates types + `aggregates` field on connection (conditional on `--aggregate`)
  - `_metadata(chainId)` query (multi-chain)
  - `_metadatas` query (all chains, paginated)
  - `node(nodeId: ID!)` root query (Node interface)
  - `NullOrder` enum for `NULLS FIRST` / `NULLS LAST`
  - `query` root field (PostGraphile compatibility alias)
- Historical table support (`_block_range` detection, `blockHeight` argument)
- Cursor-based and offset pagination
- Filter SQL generation (all scalar filter operators)
- Relation filters (`some`/`none`/`every` for list relations)
- `distinct` parameter with `DISTINCT ON`
- `orderBy` (multiple fields, `ID_ASC`/`ID_DESC`/etc.)
- Batch query support (POST with JSON array)
- GraphQL variables
- `/health` endpoint
- `cargo +nightly fmt` formatting enforced in CI

### NOT YET IMPLEMENTED
- Hot schema reload (LISTEN/NOTIFY + schema swap)
- GraphQL subscriptions (schema registers the type, no resolver)
- Query validation middleware (complexity, depth, alias, batch limits — flags exist in config but no enforcement)
- Query timeout enforcement
- `--query-explain` SQL EXPLAIN logging
- GraphiQL playground UI (flag exists, no HTML served)
- Response compression
- Schema listener keep-alive (`SELECT 1` every 180s)

---

## Project structure

```
src/
  main.rs                  # Entry point, CLI, server startup
  config.rs                # Config struct (clap + OMNIHEDRON_* env vars)
  db/                      # Pool setup, schema discovery
  introspection/
    model.rs               # TableInfo, ColumnInfo, ForeignKey structs
    queries.rs             # PostgreSQL information_schema queries
    types.rs               # PG type → GraphQL scalar type mapping
  schema/
    builder.rs             # Core: introspection → async-graphql dynamic schema
    inflector.rs           # snake_case↔camelCase, singularize/pluralize
    filters.rs             # Filter input type generation
    aggregates.rs          # Aggregate type generation
    cursor.rs              # Cursor + nodeId encode/decode (base64 JSON)
    metadata.rs            # _metadata/_metadatas schema registration
  resolvers/
    connection.rs          # List query SQL + pagination
    single.rs              # Single record by PK / nodeId
    relations.rs           # Forward/backward relation resolution
    aggregates.rs          # Aggregate query execution
    metadata.rs            # _metadata resolver
  sql/                     # Dynamic SQL construction helpers
  validation/              # Stubs: complexity, depth, alias, batch (not wired up)
  hot_reload/              # Stub: LISTEN/NOTIFY schema reload (not wired up)
  server.rs                # axum router
docker/
  Dockerfile               # Copies pre-built binary; build on host first
  docker-compose.yml       # Full dev stack (needs testnet-indexer-db.dump)
  docker-compose.ci.yml    # CI stack (uses tests/fixtures/test_db.sql)
scripts/
  setup_db.sh              # Start postgres + restore 5.2GB dump (one-time)
  start_services.sh        # Build binary + start Rust + TypeScript services
  stop_services.sh         # Stop services (--with-db to also stop postgres)
  create_fixture.sh        # Regenerate tests/fixtures/test_db.sql from live DB
  bench_compare.sh         # Throughput benchmarks vs TypeScript
tests/
  integration_test.rs      # Compare Rust vs TypeScript service responses
  fixtures/test_db.sql     # Minimal 1.6MB SQL fixture for CI (replaces 5.2GB dump)
```

---

## Configuration

All CLI flags accept env vars with the `OMNIHEDRON_` prefix.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--name` / `-n` | `OMNIHEDRON_NAME` | required | PostgreSQL schema name |
| `--port` / `-p` | `OMNIHEDRON_PORT` | 3000 | HTTP port |
| `--playground` | `OMNIHEDRON_PLAYGROUND` | off | Enable GraphiQL UI (not implemented) |
| `--subscription` | `OMNIHEDRON_SUBSCRIPTION` | off | Enable subscriptions (not implemented) |
| `--aggregate` | `OMNIHEDRON_AGGREGATE` | on | Enable aggregation queries |
| `--unsafe-mode` | `OMNIHEDRON_UNSAFE` | off | Disable all query limits |
| `--query-limit` | `OMNIHEDRON_QUERY_LIMIT` | 100 | Max records per query |
| `--query-timeout` | `OMNIHEDRON_QUERY_TIMEOUT` | 10000 | Query timeout ms (not enforced yet) |
| `--max-connection` | `OMNIHEDRON_MAX_CONNECTION` | 10 | PostgreSQL pool size |
| `--disable-hot-schema` | `OMNIHEDRON_DISABLE_HOT_SCHEMA` | off | Disable hot reload (moot, not implemented) |
| `--log-level` | `OMNIHEDRON_LOG_LEVEL` | info | fatal\|error\|warn\|info\|debug\|trace |
| `--output-fmt` | `OMNIHEDRON_OUTPUT_FMT` | colored | json\|colored |

Database env vars: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_DATABASE`

Memory tip: set `TOKIO_WORKER_THREADS=8` on many-core machines — Tokio defaults to one thread per CPU core, inflating RSS from idle thread stacks (128 cores × 2MB = 256MB idle). For a PostgreSQL-bound service with a pool of 10 connections, 8–16 threads is optimal.

---

## PostgreSQL type → GraphQL scalar mapping

| PostgreSQL type | GraphQL scalar |
|---|---|
| `text`, `varchar`, `char`, `name`, `citext` | `String` |
| `int2`, `int4` | `Int` |
| `int8` | `BigInt` (serialised as JSON **string** to preserve precision) |
| `numeric`, `decimal` | `BigFloat` (serialised as JSON **string**) |
| `float4`, `float8` | `Float` |
| `bool` | `Boolean` |
| `timestamp`, `timestamptz` | `Datetime` (RFC3339 string) |
| `date` | `Date` (ISO date string) |
| `json`, `jsonb` | `JSON` |
| `uuid` | `String` |
| `bytea` | `String` (hex-encoded) |
| enum types | Custom scalar with `EnumFilter` |
| arrays | `JSON` |
| unknown | `String` (fallback) |

`BigInt`, `BigFloat`, `Cursor`, `Date`, `Datetime`, `JSON` are registered as custom scalars.

---

## Naming conventions (PostGraphile-compatible)

All naming logic lives in `src/schema/inflector.rs`. Must match PostGraphile's `formatInsideUnderscores` exactly.

**snake_case → camelCase:** leading underscores preserved; consecutive uppercase runs normalised (e.g. `cumulative_volume_u_s_ds` → `cumulativeVolumeUsds`).

**snake_case → PascalCase:** same rules, initial char uppercased.

**Pluralisation rules (non-obvious cases):**
- Latin neuter: `metadatum` → `metadata`, `*_metadatum` → `*_metadata`
- Vowel-before-S: `responses` → `response` (not `respons`)
- Digit suffix: `V2` → `V2s` (lowercase `s`, not `V2S`)
- Consecutive uppercase: `CumulativeVolumeUSDS` → `CumulativeVolumeUsds`
- Last uppercase in PascalCase determines where `s`/`S` suffix goes

**Table → field names:**
- `transfers` → type `Transfer`, connection query `transfers`, single `transfer`
- OrderBy enum: `COLUMN_NAME_ASC` / `COLUMN_NAME_DESC`
- Filter: `{Entity}Filter`
- Connection: `{Entity}Connection`
- Edge: `{Entity}Edge`
- Backward relation: `{relatedEntities}By{FkColumn}` (e.g., `transfersByAccountId`)

---

## Cursor & nodeId format

Both use base64-encoded JSON, matching PostGraphile exactly.

**Cursor:** `base64({"id": "<pk_value>"})` — currently encodes only the primary key. Multi-column ordering pagination has a known gap: cursors should include all order-by fields as tiebreakers but currently only include `id`. This means cursor pagination may return duplicate rows when ordering by a non-unique column.

**nodeId:** `base64(["TypeName", pkValue])` — e.g., `base64(["Transfer", "abc123"])`.
`{entity}ByNodeId` queries decode this and look up `WHERE t.id = $1`.

---

## SQL patterns

**List query (connection resolver):**
```sql
SELECT * FROM "{schema}"."{table}" AS t
[WHERE upper_inf(t._block_range)]      -- historical tables, no blockHeight arg
[WHERE t._block_range @> $N::bigint]   -- historical tables, with blockHeight arg
[WHERE {filter_clauses}]
[ORDER BY t.id ASC]                    -- default; replaced by orderBy arg
LIMIT $N OFFSET $N
```

**Distinct:**
```sql
SELECT DISTINCT ON (t.{col}) t.* FROM ...
ORDER BY t.{col} {dir}                 -- must match DISTINCT ON column
```

**Forward relation:**
```sql
SELECT * FROM "{schema}"."{related_table}" AS t
WHERE t.id = $1 LIMIT 1
```

**Backward relation (connection):**
```sql
SELECT * FROM "{schema}"."{child_table}" AS t
WHERE t.{fk_column} = $1
[filter / order / limit]
```

**Aggregate context:** the connection resolver embeds `_agg_ctx` in a sidecar JSON field containing `{schema, table, where_clause, params}` so the aggregate resolver can reuse the same filter without re-parsing.

---

## Multi-chain `_metadata`

SubQuery multi-chain projects store metadata in per-chain tables named `_metadata_<genesisHash>` (e.g., `_metadata_0x718cc0c53ec26c63`). The `_metadata(chainId: String)` query maps `chainId` to the correct table by reading the `chain` key from each `_metadata_*` table at schema build time.

The global `_global` table exists but is typically empty.

---

## Integration tests

Tests live in `tests/integration_test.rs` and compare Rust vs TypeScript service responses.

**What's tested:**
- `test_health` — `/health` returns 2xx; TypeScript `/graphql` responds to `{ __typename }`
- `test_metadata` — `_metadata(chainId: "11155111")` returns matching data from both services
- `test_introspection_types` — full schema type set matches (716 types); excludes `__*`, `_Global*`, `_Metadata*`, `_Multi*`, `Having*`, `*AggregatesFilter`, `*GroupBy`, `*DistinctCountAggregates`, `*AggregateFilter`, `*ToMany*`
- `test_first_entity_list` — first connection field discovered via introspection; `first: 5` returns matching nodes
- `test_pagination` — page 1 → page 2 cursor, verifies no overlap between pages
- `test_order_by` — `orderBy: ID_ASC` returns lexicographically sorted results on both services
- `test_filter_null` — `filter: { id: { isNull: false } }` returns matching results
- `test_aggregates` — aggregates field responds without error (values not strictly compared)
- `test_batch_query` — POST with JSON array of 2 queries returns array of 2 results
- `test_query_with_variables` — GraphQL variable `$count: Int!` correctly applied

**Test infrastructure:**
- `sort_nodes()` — recursively sorts arrays of objects by `id` field for deterministic comparison
- Stripped before comparison: `cursor`, `startCursor`, `endCursor`, `queryNodeVersion`, `indexerNodeVersion` (implementation-specific)
- Tests skip (with `eprintln!("SKIP:")`) if services not reachable — they don't fail CI when services are down

**Fixture:** `tests/fixtures/test_db.sql` (1.6MB) — full schema DDL + all `_metadata_*` rows + 20 rows per entity table. Regenerate with `bash scripts/create_fixture.sh` when the real schema changes (requires the full DB running).

---

## CI

| Workflow | Trigger | What it does |
|---|---|---|
| `fmt.yml` | PR to main | `cargo +nightly fmt --all -- --check` |
| `integration-tests.yml` | PR to main | `docker/docker-compose.ci.yml` up → build Rust → run tests |
| `docker-publish.yml` | Push `v*` tag | Build binary → push `polytopelabs/omnihedron` with semver + SHA tags |

All workflows: `cancel-in-progress: true`.

**Important:** always use `cargo +nightly fmt`. The `rust-toolchain.toml` pins the build toolchain but rustfmt must be run with `+nightly`. The CI workflow uses `dtolnay/rust-toolchain@nightly` + `cargo +nightly fmt`.

---

## Development workflow

```bash
# First-time setup (restores 5.2GB dump — takes ~10 min)
bash scripts/setup_db.sh

# Start both services
bash scripts/start_services.sh    # Rust on :3000, TypeScript on :3001

# Run tests
cargo test --lib                  # unit tests
cargo test --test integration_test # requires both services running

# Format (nightly required)
cargo +nightly fmt --all

# Benchmarks
bash scripts/bench_compare.sh

# Regenerate CI fixture after schema changes
bash scripts/create_fixture.sh

# Stop services
bash scripts/stop_services.sh
bash scripts/stop_services.sh --with-db   # also stops postgres
```

---

## Reference: TypeScript @subql/query files

| File | Purpose |
|---|---|
| `packages/query/src/yargs.ts` | All config options and defaults |
| `packages/query/src/graphql/graphql.module.ts` | Apollo + hot reload |
| `packages/query/src/graphql/project.service.ts` | Schema discovery SQL |
| `packages/query/src/graphql/plugins/GetMetadataPlugin.ts` | Metadata SQL |
| `packages/query/src/graphql/plugins/PgSubscriptionPlugin.ts` | LISTEN/NOTIFY |
| `packages/query/src/graphql/plugins/historical/PgBlockHeightPlugin.ts` | `_block_range` SQL |
| `packages/query/src/graphql/plugins/PgAggregateSpecsPlugin.ts` | Aggregation specs |
| `packages/query/src/graphql/plugins/PgDistinctPlugin.ts` | DISTINCT ON |
| `packages/query/src/graphql/plugins/QueryComplexityPlugin.ts` | Complexity algorithm |
