# omnihedron — Claude Context

## What this project is

**omnihedron** is a high-performance Rust rewrite of `@subql/query` (the GraphQL query service for SubQuery Network indexers). The TypeScript original uses PostGraphile to auto-generate a full GraphQL API from a live PostgreSQL schema at runtime. This port replicates that behaviour entirely in Rust, producing an identical external API.

GitHub: https://github.com/polytope-labs/omnihedron

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
- **Schema hot reload** — a dedicated PostgreSQL connection `LISTEN`s on the SubQuery schema channel. When a `schema_updated` notification arrives, introspection reruns and the schema is atomically swapped behind an `Arc<RwLock<Schema>>`.
- **SQL safety** — all user-supplied values use parameterised `$N` placeholders. Column and table names come from introspection and are never interpolated from user input.
- **PostGraphile compatibility** — naming conventions, cursor encoding (base64 JSON), `nodeId` field, `{entity}ByNodeId` root queries all match PostGraphile exactly.

---

## Project structure

```
src/
  main.rs                  # Entry point, CLI, server startup
  config.rs                # Config struct (clap + OMNIHEDRON_* env vars)
  db/                      # Pool setup, schema discovery
  introspection/           # PostgreSQL information_schema queries → TableInfo structs
  schema/
    builder.rs             # Core: introspection → async-graphql dynamic schema
    inflector.rs           # snake_case↔camelCase, singularize/pluralize
    filters.rs             # Filter input type generation
    aggregates.rs          # Aggregate type generation
    cursor.rs              # Cursor + nodeId encode/decode (base64 JSON)
    metadata.rs            # _metadata query
  resolvers/
    connection.rs          # List query SQL + pagination
    single.rs              # Single record by PK / nodeId
    relations.rs           # Forward/backward relation resolution
    aggregates.rs          # Aggregate query execution
    metadata.rs            # _metadata resolver
  sql/                     # Dynamic SQL construction
  validation/              # Complexity, depth, alias, batch limits
  hot_reload/              # LISTEN/NOTIFY schema reload
  server.rs                # axum router
docker/
  Dockerfile
  docker-compose.yml       # Full dev stack (needs testnet-indexer-db.dump)
  docker-compose.ci.yml    # CI stack (uses tests/fixtures/test_db.sql)
scripts/
  setup_db.sh              # Start postgres + restore 5.2GB dump
  start_services.sh        # Build binary + start both services
  stop_services.sh         # Stop services (--with-db to also stop postgres)
  create_fixture.sh        # Regenerate tests/fixtures/test_db.sql from live DB
  bench_compare.sh         # Run throughput benchmarks vs TypeScript
tests/
  integration_test.rs      # Compare Rust vs TypeScript service responses
  fixtures/test_db.sql     # Minimal 1.6MB SQL fixture for CI
```

---

## Configuration

All CLI flags also accept env vars with the `OMNIHEDRON_` prefix.

Key flags:
- `--name` / `OMNIHEDRON_NAME` — PostgreSQL schema name (required)
- `--port` / `OMNIHEDRON_PORT` — HTTP port (default: 3000)
- `--unsafe-mode` — disable all query limits
- `--aggregate` — enable aggregation queries (default: on)
- `--playground` — enable GraphiQL UI

Database env vars: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_DATABASE`

---

## Development workflow

```bash
# Local dev (full stack with 5.2GB dump)
bash scripts/setup_db.sh          # start postgres + restore dump (one-time)
bash scripts/start_services.sh    # build + start Rust + TypeScript services
bash scripts/stop_services.sh     # stop services

# Run tests
cargo test --lib                  # unit tests only
cargo test --test integration_test # requires both services running

# Format (must use nightly)
cargo +nightly fmt --all

# Benchmarks
bash scripts/bench_compare.sh
```

---

## CI (GitHub Actions)

- **`fmt.yml`** — `cargo +nightly fmt --all -- --check` on PRs to main
- **`integration-tests.yml`** — spins up `docker/docker-compose.ci.yml` (postgres + TS service via fixture), runs Rust natively, runs integration tests
- **`docker-publish.yml`** — builds and pushes `polytopelabs/omnihedron` to Docker Hub on `v*` tags

All workflows: PR-to-main only, `cancel-in-progress: true`.

---

## Important conventions

- **Formatting**: always use `cargo +nightly fmt`. The `rust-toolchain.toml` pins the build toolchain but rustfmt must be run with `+nightly`.
- **Fixture regeneration**: when the real schema changes, run `bash scripts/create_fixture.sh` to regenerate `tests/fixtures/test_db.sql` and commit it.
- **Naming**: PostGraphile naming must be matched exactly — see `src/schema/inflector.rs`. Latin irregulars (`metadatum → metadata`), digit-suffix plurals (`V2 → V2s`), and consecutive-uppercase normalisation are all handled there.
- **No SQL injection**: column/table names from introspection are whitelisted; user values always go through `$N` parameters.

---

## Known non-trivial details

- `nodeId` — computed field on every entity: `base64(["TypeName", pkValue])`. `{entity}ByNodeId(nodeId: ID!)` root queries decode this and look up by PK.
- `_metadata` — multi-chain: tables named `_metadata_<genesisHash>`. Queried by `chainId` arg which maps to the `chain` key in the metadata table.
- Enum display names — fall back to `to_pascal_case(pg_type_name)` when no `@enumName` comment is present.
- Memory on many-core machines — Tokio defaults to one thread per CPU core. Set `TOKIO_WORKER_THREADS=8` for a PostgreSQL-bound service to avoid inflated RSS from idle thread stacks.
