// Copyright (C) 2026 Polytope Labs.
// SPDX-License-Identifier: Apache-2.0

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	/// Returns `true` if the given column is covered by a single-column unique constraint,
	/// indicating a one-to-one relationship when used as a foreign key.
	pub fn is_column_unique(&self, col: &str) -> bool {
		self.unique_constraints.iter().any(|uc| uc.len() == 1 && uc[0] == col)
	}

	/// Returns `true` if this table is a junction table (many-to-many link):
	/// exactly 2 foreign keys, and no public columns other than `id` and the FK columns.
	pub fn is_junction_table(&self) -> bool {
		if self.foreign_keys.len() != 2 {
			return false;
		}
		let fk_cols: std::collections::HashSet<&str> =
			self.foreign_keys.iter().map(|fk| fk.column.as_str()).collect();
		self.public_columns()
			.filter(|c| c.name != "id" && !fk_cols.contains(c.name.as_str()))
			.count() == 0
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
	/// PostGraphile smart tags parsed from the FK constraint's `COMMENT ON CONSTRAINT`.
	/// SubQuery stores `@foreignFieldName` (hasMany) and `@singleForeignFieldName`
	/// (hasOne) tags to override backward relation field names (from `@derivedFrom`).
	pub smart_tags: SmartTags,
}

/// PostGraphile smart tags parsed from a constraint comment.
#[derive(Debug, Clone, Default)]
pub struct SmartTags {
	/// `@foreignFieldName <name>` — overrides the backward relation field name
	/// for hasMany (one-to-many) relations.
	pub foreign_field_name: Option<String>,
	/// `@singleForeignFieldName <name>` — overrides the backward relation field name
	/// for hasOne (one-to-one) relations.
	pub single_foreign_field_name: Option<String>,
}

impl SmartTags {
	/// Parse smart tags from a PostgreSQL constraint comment string.
	///
	/// Format: `@tagName value` separated by newlines or `|` (pipe).
	/// SubQuery uses `|` as separator within a single tag set for historical tables
	/// (stored as table comments), and newlines for constraint comments.
	pub fn from_comment(comment: &str) -> Self {
		let mut tags = SmartTags::default();
		// Split on both newlines and pipes to handle both formats.
		for segment in comment.split(|c| c == '\n' || c == '|') {
			let segment = segment.trim();
			let parts: Vec<&str> = segment.splitn(2, ' ').collect();
			if parts.len() == 2 {
				match parts[0] {
					"@foreignFieldName" => {
						tags.foreign_field_name = Some(parts[1].trim().to_string());
					},
					"@singleForeignFieldName" => {
						tags.single_foreign_field_name = Some(parts[1].trim().to_string());
					},
					_ => {},
				}
			}
		}
		tags
	}

	/// Parse all smart tag groups from a table comment (historical mode).
	///
	/// In historical mode, SubQuery stores all FK smart tags as a single table
	/// comment with newline-separated groups. Each group contains `|`-separated
	/// tags for one FK, including a `@foreignKey (col) REFERENCES "table" (id)`
	/// that identifies which FK the tags belong to.
	///
	/// Returns `Vec<(fk_column, SmartTags)>` where `fk_column` is extracted from
	/// the `@foreignKey` tag.
	pub fn from_table_comment(comment: &str) -> Vec<(String, SmartTags)> {
		let mut results = Vec::new();
		// Each line represents one FK's tag set.
		for line in comment.lines() {
			let line = line.trim();
			if line.is_empty() {
				continue;
			}
			let mut tags = SmartTags::default();
			let mut fk_column: Option<String> = None;

			for segment in line.split('|') {
				let segment = segment.trim();
				let parts: Vec<&str> = segment.splitn(2, ' ').collect();
				if parts.len() < 2 {
					continue;
				}
				match parts[0] {
					"@foreignFieldName" => {
						tags.foreign_field_name = Some(parts[1].trim().to_string());
					},
					"@singleForeignFieldName" => {
						tags.single_foreign_field_name = Some(parts[1].trim().to_string());
					},
					"@foreignKey" => {
						// Format: "(col_name) REFERENCES ..."
						// Extract column name from parentheses.
						if let Some(start) = parts[1].find('(') {
							if let Some(end) = parts[1][start..].find(')') {
								fk_column =
									Some(parts[1][start + 1..start + end].trim().to_string());
							}
						}
					},
					_ => {},
				}
			}

			if let Some(col) = fk_column {
				if tags.foreign_field_name.is_some() || tags.single_foreign_field_name.is_some() {
					results.push((col, tags));
				}
			}
		}
		results
	}
}

/// A PostgreSQL function created by SubQuery's `@fullText` directive.
///
/// Pattern: `search_{hash}(search text) RETURNS SETOF table`
/// with a comment `@name search_{table}` to set the GraphQL field name.
#[derive(Debug, Clone)]
pub struct SearchFunction {
	/// The raw PostgreSQL function name (often a hash like `search_abc123`).
	pub pg_name: String,
	/// The GraphQL field name extracted from the `@name search_{table}` comment.
	pub graphql_name: String,
	/// The table that this function returns rows from (`RETURNS SETOF table`).
	pub returns_table: String,
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
