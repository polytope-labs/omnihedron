//! PostgreSQL connection pool and schema discovery.
//!
//! - [`pool::create_pool`]              — builds a `deadpool-postgres` connection pool, optionally
//!   with TLS using the certificates from [`Config`].
//! - [`schema_discovery::discover_schema`] — resolves the target PostgreSQL schema name from the
//!   project name supplied via `--name`.

pub mod pool;
pub mod schema_discovery;

pub use pool::create_pool;
pub use schema_discovery::discover_schema;
