//! PostgreSQL schema introspection.
//!
//! This module queries `information_schema` and `pg_catalog` at startup (and on
//! every hot-reload cycle) to discover the full structure of the target schema:
//! tables, columns, types, primary keys, foreign keys, unique constraints, and
//! enum types.
//!
//! Entry points:
//! - [`queries::introspect_schema`] — returns a [`Vec<model::TableInfo>`]
//! - [`queries::introspect_enums`]  — returns a [`Vec<model::EnumInfo>`]

pub mod model;
pub mod queries;
pub mod types;

pub use model::TableInfo;
#[allow(unused_imports)]
pub use model::{ColumnInfo, EnumInfo, ForeignKey};
pub use queries::{introspect_enums, introspect_schema};
