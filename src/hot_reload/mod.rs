//! Hot schema reload via PostgreSQL `LISTEN`/`NOTIFY`.
//!
//! [`start_schema_listener`] spawns a background task that holds a dedicated
//! PostgreSQL connection and listens on the SubQuery schema-change channel.
//! When a `schema_updated` notification arrives the schema is re-introspected
//! and atomically swapped behind the [`server::SharedSchema`] `RwLock`.
//!
//! A keep-alive `SELECT 1` is sent every `--sl-keep-alive-interval` ms
//! (default 180 s) to prevent the idle connection from being terminated.

pub mod listener;
pub use listener::start_schema_listener;
