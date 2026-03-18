//! Data model for PostgreSQL schema introspection results.
//!
//! These structs are populated by [`super::queries`] at startup and on every
//! hot-reload cycle.  They are intentionally kept as plain data — no async
//! logic, no database dependencies.

/// A fully introspected table (or view) in the target schema.
#[derive(Debug, Clone)]
pub struct TableInfo {
	/// The unquoted table name as it appears in `information_schema`.
	pub name: String,
	pub columns: Vec<ColumnInfo>,
	pub primary_keys: Vec<String>,
	pub foreign_keys: Vec<ForeignKey>,
	/// Unique constraints, each represented as the list of column names that
	/// form the constraint.  Available for future `queryBy{UniqueField}` support.
	#[allow(dead_code)]
	pub unique_constraints: Vec<Vec<String>>,
	/// `true` when the table has a `_block_range` column (historical indexing).
	pub is_historical: bool,
}

impl TableInfo {
	/// Returns the column with the given name, if it exists.
	#[allow(dead_code)]
	pub fn column(&self, name: &str) -> Option<&ColumnInfo> {
		self.columns.iter().find(|c| c.name == name)
	}

	/// Returns only the columns that should be exposed in the GraphQL schema
	/// (excludes internal SubQuery columns such as `_block_range` and `_id`).
	pub fn public_columns(&self) -> impl Iterator<Item = &ColumnInfo> {
		self.columns.iter().filter(|c| !is_internal_column(&c.name))
	}
}

/// A single column in a table.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
	/// The unquoted column name.
	pub name: String,
	/// PostgreSQL `data_type` from `information_schema.columns` (e.g. `"integer"`, `"text"`).
	pub pg_type: String,
	/// PostgreSQL `udt_name` (e.g. `int4`, `text`, `_text` for arrays).
	pub udt_name: String,
	pub is_nullable: bool,
	/// Position of this column within the table (1-based), as reported by
	/// `information_schema.columns`.  Preserved for future use.
	#[allow(dead_code)]
	pub ordinal_position: i32,
	/// For `USER-DEFINED` columns (enums), the resolved GraphQL type display
	/// name (e.g. `"EnumGlobalStatus"`).  `None` for all other column types.
	pub enum_display_name: Option<String>,
}

/// Information about a PostgreSQL enum type in the target schema.
#[derive(Debug, Clone)]
pub struct EnumInfo {
	/// The internal pg type name (often a hash, e.g. `167d3578e1`).
	pub pg_type_name: String,
	/// The display name extracted from the `@enumName` comment, or the
	/// PascalCase conversion of `pg_type_name` if no comment was present.
	pub display_name: String,
	/// The enum values in sort order.
	pub values: Vec<String>,
}

/// A foreign-key relationship from this table to another table.
#[derive(Debug, Clone)]
pub struct ForeignKey {
	/// The PostgreSQL constraint name; preserved for diagnostics.
	#[allow(dead_code)]
	pub constraint_name: String,
	/// The FK column in this (source) table.
	pub column: String,
	/// The referenced table.
	pub foreign_table: String,
	/// The referenced column (usually `id`).  Preserved for future join-condition use.
	#[allow(dead_code)]
	pub foreign_column: String,
}

/// Names of internal SubQuery columns that must be hidden from the GraphQL schema.
const INTERNAL_COLUMNS: &[&str] = &["_block_range", "_id"];

/// Returns `true` if `name` is an internal SubQuery column that should not
/// appear in the generated GraphQL schema.
pub fn is_internal_column(name: &str) -> bool {
	INTERNAL_COLUMNS.contains(&name)
}

/// Returns `true` if `name` is an internal SubQuery table that should be
/// excluded from schema introspection.
///
/// Excluded patterns:
/// - `_metadata` / `_metadata_0x*` — exposed via the bespoke `_metadata` / `_metadatas` query
///   fields instead.
/// - `_multi_*` — internal multi-chain plumbing tables.
///
/// Single-leading-underscore tables such as `_global` are **not** excluded and
/// are exposed as regular entity types.
pub fn is_internal_table(name: &str) -> bool {
	name.starts_with("_metadata") || name.starts_with("_multi_")
}
