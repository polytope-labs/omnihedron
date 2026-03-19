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
- **Schema hot reload** — a dedicated PostgreSQL connection `LISTEN`s on the SubQuery schema channel. When a `schema_updated` notification arrives, introspection reruns and the schema is atomically swapped behind an `Arc<RwLock<Schema>>`. In-flight requests are unaffected.
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
- Selective `SELECT` — only columns referenced in the query are fetched (`filter_columns_by_request`). Uses `ctx.field().selection_set()` to iterate the direct children of the connection field, then drills into `nodes { … }` and `edges { node { … } }` to collect the entity field names. **Important:** the correct async-graphql 7.x API is `ctx.field().selection_set()` (not `ctx.look_ahead().field("nodes").selection_fields()` — `.selection_fields()` returns the matched node itself, not its children; use `.exists()` for boolean presence checks).
- Count-only fast-path — queries with no `nodes`/`edges` selection skip the row fetch entirely. `has_node_selection` uses `ctx.look_ahead().field("nodes").exists()` which correctly traverses the current field's selection set in async-graphql 7.x dynamic schema.
- Window function `COUNT(*) OVER()` — total count and rows fetched in a single SQL round-trip (non-DISTINCT queries)
- `TextParam` wrapper in `json_to_pg_params` — sends all numeric and array JSON values as PostgreSQL text-format parameters (accepts any server type, uses `Format::Text`). This avoids OID mismatch errors when filtering on INT4/NUMERIC/JSONB columns with parameterised queries.
- Hex enum type naming — `pg_enum_type_to_gql_name` replicates PostGraphile's `coerceToGraphQLName` + lodash `upperCamelCase` with digit→letter word boundaries (for SubQuery's blake2 10-char hash enum names)
- `_MetadatasEdge` type registered with `cursor` + `node` fields; `_Metadatas` exposes `edges` field
- Cursor-based pagination (`after`/`before`) with explicit `orderBy` — `parse_orderby` and `parse_distinct` accept both `List` and bare `Enum`/`String` values since async-graphql dynamic schema does not auto-coerce single enum values into lists for these args
- Schema hot reload (LISTEN/NOTIFY with atomic schema swap)
- Backward relation `filter`/`orderBy`/`orderByNull`/`distinct` args
- Forward relation field naming (PostGraphile simplify inflector: strip `_id` from FK column)
- Backward relation field naming (`{childPlural}By{FkCol}`)
- Relation filters: `{relation}Exists` boolean, forward relation EXISTS subquery, backward `some`/`none`/`every`
- `FilterContext` struct for relation-aware filter SQL
- Many-to-many junction table detection and resolver
- Aggregate orderBy enum values with correlated subqueries
- Historical `timestamp` mode (reads `historicalStateEnabled` from metadata)
- `_block_range @> MAX_INT64::bigint` (matches PostGraphile default, not `upper_inf`)
- `_id ASC` tiebreaker in ORDER BY for historical tables
- Cache-Control headers (`public, max-age=5`)
- HTTP request tracing with request_id (UUID prefix)
- `query-complexity` and `max-query-complexity` response headers
- `--indexer` metadata HTTP fallback (fetches /meta + /health)
- `DB_HOST_READ` read replica support
- `--query-explain` SQL EXPLAIN logging
- Backward relation one-to-one detection (unique FK → single record)
- BlockHeight in relation filter subqueries (_block_range @> in EXISTS)
- Fulltext search sanitization (`sanitize_tsquery`)
- Forward-relation scalar ordering (`pg-order-by-related` parity): enum `{SINGULAR_TABLE}_BY_{FK_COL}__{TARGET_COL}_ASC/DESC`, generates correlated subqueries in ORDER BY
- Metadata default chain caching
- `first+last` and `offset+last` argument rejection
- Negative first/last/offset clamping
- `first:0` treated as unset (matches TS JS-truthiness)
- DISTINCT ON columns prepended to ORDER BY
- Selective aggregate computation (only requested aggregates in SQL)
- Apache 2.0 license headers on all files
- Trimmed tokio features (macros, rt-multi-thread, net, sync, time)
- RUST_LOG env filter support
- GitHub Release with auto-generated changelog

### Notes
- Query timeout is enforced via PostgreSQL `statement_timeout` set on every pool connection (`src/db/pool.rs`: `.options(&format!("-c statement_timeout={}", cfg.query_timeout))`). The DB kills any query exceeding the limit.

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
  validation/              # Query validation: complexity, depth, alias, batch limits
  hot_reload/              # LISTEN/NOTIFY schema reload with atomic schema swap
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
  common/mod.rs            # Shared test infrastructure
  test_basics.rs           # 12 tests (health, metadata, introspection, etc.)
  test_pagination.rs       # 8 tests (cursor, offset, last pagination)
  test_filters.rs          # 14 tests (including enum filter)
  test_ordering.rs         # 7 tests (multi-column, non-id, forward-relation scalar ordering)
  test_aggregates.rs       # 7 tests (sum, count, min, max, stddev, variance)
  test_relations.rs        # 10 tests (forward, backward, nested relations)
  test_historical.rs       # 3 tests (block height queries)
  test_limits.rs           # 2 tests (1 ignored)
  fixtures/test_db.sql     # Minimal 1.6MB SQL fixture for CI (replaces 5.2GB dump)
```

---

## Configuration

All CLI flags accept env vars with the `OMNIHEDRON_` prefix.

| Flag | Env var | Default | Description |
|---|---|---|---|
| `--name` / `-n` | `OMNIHEDRON_NAME` | required | PostgreSQL schema name |
| `--port` / `-p` | `OMNIHEDRON_PORT` | 3000 | HTTP port |
| `--playground` | `OMNIHEDRON_PLAYGROUND` | off | Enable GraphiQL UI |
| `--subscription` | `OMNIHEDRON_SUBSCRIPTION` | off | Enable WebSocket subscriptions |
| `--aggregate` | `OMNIHEDRON_AGGREGATE` | on | Enable aggregation queries |
| `--unsafe-mode` | `OMNIHEDRON_UNSAFE` | off | Disable all query limits |
| `--query-limit` | `OMNIHEDRON_QUERY_LIMIT` | 100 | Max records per query |
| `--query-timeout` | `OMNIHEDRON_QUERY_TIMEOUT` | 10000 | Query timeout ms (not enforced yet) |
| `--max-connection` | `OMNIHEDRON_MAX_CONNECTION` | 10 | PostgreSQL pool size |
| `--disable-hot-schema` | `OMNIHEDRON_DISABLE_HOT_SCHEMA` | off | Disable schema hot reload |
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

**Enum type names (PostgreSQL → GraphQL):** `pg_enum_type_to_gql_name` in `inflector.rs` replicates PostGraphile's exact chain: prepend `_` if name starts with a digit (`coerceToGraphQLName`), then apply lodash-style `upperCamelCase` with digit→letter transitions as word boundaries. This correctly handles SubQuery's blake2 10-char hex hash names (e.g. `869e90c211` → `_869E90C211`). Note: this logic is **separate** from `to_camel_case` — the digit→letter split only applies to the enum fallback path, not to regular column/table name inflection.

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

**Cursor:** `base64({"id": "<pk_value>"})` — encodes only the primary key. Cursor-based pagination works for `id`-ordered queries. Multi-column ordering (ordering by non-PK columns) may return duplicate rows at page boundaries since the cursor only includes `id`; this is a known limitation but sufficient for the primary use case.

**nodeId:** `base64(["TypeName", pkValue])` — e.g., `base64(["Transfer", "abc123"])`.
`{entity}ByNodeId` queries decode this and look up `WHERE t.id = $1`.

---

## SQL patterns

**List query (connection resolver):**
```sql
SELECT t."id", t."col1", ..., COUNT(*) OVER() AS __total_count
FROM "{schema}"."{table}" AS t
[WHERE upper_inf(t._block_range)]      -- historical tables, no blockHeight arg
[WHERE t._block_range @> $N::bigint]   -- historical tables, with blockHeight arg
[WHERE {filter_clauses}]
[ORDER BY t.id ASC]                    -- default; replaced by orderBy arg
LIMIT $N OFFSET $N
-- Only columns referenced in nodes/edges selection are fetched (ctx.field().selection_set() drill-down).
-- COUNT(*) OVER() fetches total count in one round-trip (omitted for DISTINCT queries).
-- If query has no nodes/edges selection, row fetch is skipped entirely (count-only fast-path).
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

Tests are split across 8 files in `tests/` with shared infrastructure in `tests/common/mod.rs`. They compare Rust vs TypeScript service responses.

**What's tested (63 tests total, 62 + 1 ignored):**

**Rust + TS comparison tests (both services must be running):**
- `test_health` — `/health` returns 2xx; TypeScript `/graphql` responds to `{ __typename }`
- `test_metadata` — `_metadata(chainId: "11155111")` returns matching data from both services
- `test_metadatas` — `_metadatas` query returns all chain metadata edges; TS/Rust compared
- `test_introspection_types` — full schema type set matches; excludes PostGraphile aggregate helper types: `__*`, `Having*`, `*AggregatesFilter`, `*GroupBy`, `*DistinctCountAggregates`, `*AggregateFilter`, `*ToMany*`
- `test_first_entity_list` — first connection field discovered via introspection; `first: 5` returns matching nodes
- `test_pagination` — page 1 → page 2 cursor, verifies no overlap between pages
- `test_order_by` — `orderBy: ID_ASC` returns lexicographically sorted results on both services
- `test_filter_null` — `filter: { id: { isNull: false } }` returns matching results
- `test_aggregates` — aggregates field responds without error (values not strictly compared)
- `test_batch_query` — POST with JSON array of 2 queries returns array of 2 results
- `test_query_with_variables` — GraphQL variable `$count: Int!` correctly applied
- `test_single_record` — `assetTeleported(id: "0x2c5edd...")` returns exact record on both services
- `test_offset_pagination` — `offset: 5, first: 5` skips first 5 rows; verifies no overlap
- `test_last_pagination` — `last: 3` returns last 3 rows with `hasPreviousPage: true`; no overlap with `first: 3`
- `test_orderby_multi_column` — `[BLOCK_NUMBER_ASC, ID_ASC]` multi-field ordering; verifies ascending order
- `test_filter_equalto` — `filter: { chain: { equalTo: "KUSAMA-4009" } }` returns 20 matching rows
- `test_filter_comparison` — `filter: { blockNumber: { greaterThan: 2156000 } }` on INT4 column; exercises `TextParam` fix
- `test_filter_in` — `filter: { id: { in: [...] } }` returns exactly 3 named fixture rows
- `test_filter_string_ops` — `filter: { id: { startsWith: "0x2c5edd" } }` returns 1 row; LIKE operator
- `test_filter_logical` — `and`/`or` logical filter operators combining chain and blockNumber conditions
- `test_filter_not_equal` — `notEqualTo: "POLKADOT"` matches all 20 rows
- `test_filter_not_in` — `notIn: [3 ids]` from 20 rows leaves exactly 17
- `test_filter_contains` — `contains: "2c5edd"` matches exactly 1 row
- `test_filter_ends_with` — `endsWith: "8ec6"` matches exactly 1 row
- `test_filter_ilike` — `ilike: "kusama%"` matches all 20 rows (case-insensitive)
- `test_filter_range` — `greaterThanOrEqualTo`/`lessThanOrEqualTo` on block_number; full range → 20 rows, min value → 1 row
- `test_filter_not` — logical `not: { chain: { equalTo: "POLKADOT" } }` matches all 20 rows
- `test_orderby_non_id` — `orderBy: BLOCK_NUMBER_ASC` returns ascending block number order on both services
- `test_orderby_related_scalar` — `orderBy: TEST_AUTHOR_BY_CREATOR_ID__NAME_ASC/DESC` orders books by related author name via correlated subquery
- `test_distinct` — `distinct: [CHAIN]` collapses 20 same-chain rows to 1; both services return chain="KUSAMA-4009"
- `test_enum_field` — `orders { nodes { status } }` returns valid enum values (PLACED/FILLED/REDEEMED/REFUNDED)

**Rust-only tests (probe only the Rust service):**
- `test_count_only` — query with only `totalCount` (no `nodes`/`edges`) returns valid count
- `test_total_count_accuracy` — `totalCount` with `first: 1000` equals actual `nodes.length` (verifies window function COUNT)
- `test_numeric_aggregates` — `sum`/`min`/`max`/`average` on `blockNumber` and `amount` columns; all results are strings
- `test_blockheight` — `blockHeight: "9999999999999"` returns all 20 rows; `blockHeight: "1729590000000"` returns exactly 1 row (Rust-only feature, TS rejects blockHeight)
- `test_by_node_id` — fetches `nodeId` dynamically then looks up via `assetTeleportedByNodeId(nodeId: ...)`, verifies correct entity returned
- `test_node_interface` — `node(nodeId: "...")` with inline fragment `... on AssetTeleported { id }`, verifies entity resolved via Node interface
- `test_stddev_variance_aggregates` — `stddevSample`/`stddevPopulation`/`varianceSample`/`variancePopulation` on `blockNumber`; all returned as parseable non-negative f64 strings
- `test_bigint_serialization` — `amount` field (int8/BigInt) serialised as JSON string `"9950040000"` not a JSON number
- `test_bigfloat_serialization` — `orders.blockTimestamp` (numeric/BigFloat) serialised as a JSON string parseable as f64

**Test infrastructure:**
- `sort_nodes()` — recursively sorts arrays of objects by `id` field for deterministic comparison
- Stripped before comparison: `cursor`, `startCursor`, `endCursor`, `queryNodeVersion`, `indexerNodeVersion` (implementation-specific)
- Tests skip (with `eprintln!("SKIP:")`) if services not reachable — they don't fail CI when services are down

**Fixture:** `tests/fixtures/test_db.sql` (1.6MB) — full schema DDL + all `_metadata_*` rows + 20 rows per entity table. Regenerate with `bash scripts/create_fixture.sh` when the real schema changes (requires the full DB running).

---

## Known divergences from PostGraphile

These are intentional or accepted differences from the TypeScript `@subql/query` behaviour.

| Area | PostGraphile behaviour | omnihedron behaviour |
|---|---|---|
| `aggregates { count }` | `count` does **not** exist on `{Entity}Aggregates` — PostGraphile's pg-aggregates plugin omits it | `count: BigInt!` **is** present on `{Entity}Aggregates`. Use `aggregates { count }` to get a filtered row count alongside other aggregate fields. |
| Counting entities | Use `connection { totalCount }` | Same — `{ assetTeleporteds { totalCount } }` works on both and hits the count-only fast-path (no rows fetched). |

**Consequence for tests:** `test_aggregates` uses `distinctCount { id }` instead of `count` so the query is valid on both services. If you add a Rust-only aggregates test, `aggregates { count }` is safe to use.

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

## Git workflow

**Never commit directly to `main`.** Always create a new branch, push it, and open a pull request. Wait for CI to pass before merging. Sign all commits with GPG.

---

## Development workflow

```bash
# First-time setup (restores 5.2GB dump — takes ~10 min)
bash scripts/setup_db.sh

# Start both services
bash scripts/start_services.sh    # Rust on :3000, TypeScript on :3001

# Run tests
cargo test --lib                  # unit tests
cargo test                        # requires both services running (62 tests across 8 files)

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
