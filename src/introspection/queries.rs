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

use std::collections::HashMap;

use super::{
	model::{ColumnInfo, EnumInfo, ForeignKey, TableInfo, is_internal_table},
	types::pg_type_to_graphql,
};
use crate::schema::inflector::pg_enum_type_to_gql_name;
use anyhow::Result;
use deadpool_postgres::Pool;
use tracing::debug;

/// Introspect every user-defined table in `schema` and return their full
/// metadata: columns, primary keys, foreign keys, and unique constraints.
pub async fn introspect_schema(pool: &Pool, schema: &str) -> Result<Vec<TableInfo>> {
	let client = pool.get().await?;

	// ── 0. Build enum map (pg_type_name → display_name) ─────────────────────
	let enums = introspect_enums(pool, schema).await?;
	let enum_map: HashMap<String, String> =
		enums.into_iter().map(|e| (e.pg_type_name, e.display_name)).collect();

	// ── 1. List tables (with table comments for historical smart tags) ───────
	let table_rows = client
		.query(
			r#"
            SELECT t.table_name,
                   obj_description(c.oid, 'pg_class') AS table_comment
            FROM information_schema.tables t
            LEFT JOIN pg_catalog.pg_class c
              ON c.relname = t.table_name
              AND c.relnamespace = (
                SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = t.table_schema
              )
            WHERE t.table_schema = $1
              AND t.table_type IN ('BASE TABLE', 'VIEW')
            ORDER BY t.table_name
            "#,
			&[&schema],
		)
		.await?;

	let table_names: Vec<String> = table_rows
		.iter()
		.map(|r| r.get::<_, String>("table_name"))
		.filter(|n| !is_internal_table(n))
		.collect();

	// Build a map of table_name → table_comment for smart tag extraction.
	let table_comments: HashMap<String, String> = table_rows
		.iter()
		.filter_map(|r| {
			let name: String = r.get("table_name");
			let comment: Option<String> = r.get("table_comment");
			comment.map(|c| (name, c))
		})
		.collect();

	debug!(schema, table_count = table_names.len(), "Introspecting tables");

	// Log tables with smart tag comments for debugging.
	for (table, comment) in &table_comments {
		tracing::info!(table = %table, comment = %comment, "Table comment with smart tags");
	}
	if table_comments.is_empty() {
		tracing::info!("No table comments found with smart tags");
	}

	let mut tables = Vec::with_capacity(table_names.len());

	for table_name in &table_names {
		let columns = fetch_columns(&client, schema, table_name, &enum_map).await?;
		let primary_keys = fetch_primary_keys(&client, schema, table_name).await?;
		let mut foreign_keys = fetch_foreign_keys(&client, schema, table_name).await?;
		let unique_constraints = fetch_unique_constraints(&client, schema, table_name).await?;
		let is_historical = columns.iter().any(|c| c.name == "_block_range");

		// For historical tables, SubQuery stores smart tags in table comments
		// (not constraint comments). Parse them and apply to matching FKs.
		if let Some(comment) = table_comments.get(table_name) {
			let table_tags = super::model::SmartTags::from_table_comment(comment);
			for (fk_col, tags) in &table_tags {
				debug!(
					table = %table_name,
					fk_col = %fk_col,
					foreign_field_name = ?tags.foreign_field_name,
					single_foreign_field_name = ?tags.single_foreign_field_name,
					"Parsed smart tag from table comment"
				);
			}
			for (fk_col, tags) in table_tags {
				if let Some(fk) = foreign_keys.iter_mut().find(|fk| fk.column == fk_col) {
					if fk.smart_tags.foreign_field_name.is_none() {
						fk.smart_tags.foreign_field_name = tags.foreign_field_name;
					}
					if fk.smart_tags.single_foreign_field_name.is_none() {
						fk.smart_tags.single_foreign_field_name = tags.single_foreign_field_name;
					}
				} else {
					debug!(
						table = %table_name,
						fk_col = %fk_col,
						"Smart tag FK column not found in table's foreign keys"
					);
				}
			}
		}

		// Also log constraint-level smart tags
		for fk in &foreign_keys {
			if fk.smart_tags.foreign_field_name.is_some() ||
				fk.smart_tags.single_foreign_field_name.is_some()
			{
				debug!(
					table = %table_name,
					constraint = %fk.constraint_name,
					fk_col = %fk.column,
					foreign_field_name = ?fk.smart_tags.foreign_field_name,
					single_foreign_field_name = ?fk.smart_tags.single_foreign_field_name,
					"FK with smart tags"
				);
			}
		}

		tables.push(TableInfo {
			name: table_name.clone(),
			columns,
			primary_keys,
			foreign_keys,
			unique_constraints,
			is_historical,
		});
	}

	Ok(tables)
}

/// Discover fulltext search functions created by SubQuery's `@fullText` directive.
///
/// These are PostgreSQL functions matching the pattern:
/// `search_{hash}(search text) RETURNS SETOF table`
/// with a `@name search_{table}` comment.
pub async fn introspect_search_functions(
	pool: &Pool,
	schema: &str,
) -> Result<Vec<super::model::SearchFunction>> {
	let client = pool.get().await?;

	let rows = client
		.query(
			r#"
            SELECT
                p.proname AS func_name,
                obj_description(p.oid, 'pg_proc') AS comment,
                c.relname AS return_table
            FROM pg_proc p
            JOIN pg_namespace n ON p.pronamespace = n.oid
            JOIN pg_type rt ON p.prorettype = rt.oid
            LEFT JOIN pg_class c ON rt.typrelid = c.oid
            WHERE n.nspname = $1
              AND p.proretset = true
              AND c.relname IS NOT NULL
              AND obj_description(p.oid, 'pg_proc') LIKE '@name search_%'
            "#,
			&[&schema],
		)
		.await?;

	let functions = rows
		.iter()
		.filter_map(|r| {
			let pg_name: String = r.get("func_name");
			let comment: Option<String> = r.get("comment");
			let returns_table: String = r.get("return_table");

			let graphql_name = comment.as_deref().and_then(|c| {
				c.lines().find_map(|line| {
					line.trim().strip_prefix("@name ").map(|n| n.trim().to_string())
				})
			})?;

			Some(super::model::SearchFunction { pg_name, graphql_name, returns_table })
		})
		.collect();

	Ok(functions)
}

/// Introspect all enum types defined in `schema`.
///
/// The display name is extracted from the `@enumName <Name>` tag stored in
/// `pg_description` for the type. If no such tag exists, the raw pg type name
/// is used as the display name.
pub async fn introspect_enums(pool: &Pool, schema: &str) -> Result<Vec<EnumInfo>> {
	let client = pool.get().await?;

	let rows = client
		.query(
			r#"
            SELECT
                t.typname AS type_name,
                obj_description(t.oid, 'pg_type') AS comment,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
            FROM pg_type t
            JOIN pg_namespace n ON t.typnamespace = n.oid
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = $1
              AND t.typtype = 'e'
            GROUP BY t.typname, t.oid
            "#,
			&[&schema],
		)
		.await?;

	let enums = rows
		.iter()
		.map(|r| {
			let pg_type_name: String = r.get("type_name");
			let comment: Option<String> = r.get("comment");
			let values: Vec<String> = r.get("values");

			let display_name = comment
				.as_deref()
				.and_then(extract_enum_name)
				.unwrap_or_else(|| pg_enum_type_to_gql_name(&pg_type_name));

			EnumInfo { pg_type_name, display_name, values }
		})
		.collect();

	Ok(enums)
}

/// Extract the display name from a `@enumName <Name>` tag in a pg comment string.
fn extract_enum_name(comment: &str) -> Option<String> {
	for part in comment.split_whitespace().collect::<Vec<_>>().windows(2) {
		if part[0] == "@enumName" {
			return Some(part[1].to_string());
		}
	}
	None
}

// ── Private helpers ──────────────────────────────────────────────────────────

async fn fetch_columns(
	client: &deadpool_postgres::Object,
	schema: &str,
	table: &str,
	enum_map: &HashMap<String, String>,
) -> Result<Vec<ColumnInfo>> {
	let rows = client
		.query(
			r#"
            SELECT column_name, data_type, udt_name, is_nullable, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            ORDER BY ordinal_position
            "#,
			&[&schema, &table],
		)
		.await?;

	Ok(rows
		.iter()
		.map(|r| {
			let pg_type: String = r.get("data_type");
			let udt_name: String = r.get("udt_name");

			// For USER-DEFINED columns (enums), resolve the display name.
			let enum_display_name =
				if pg_type == "USER-DEFINED" { enum_map.get(&udt_name).cloned() } else { None };

			let (_, _) = pg_type_to_graphql(&pg_type, &udt_name); // validate mapping exists
			ColumnInfo {
				name: r.get("column_name"),
				pg_type,
				udt_name,
				is_nullable: r.get::<_, &str>("is_nullable") == "YES",
				ordinal_position: r.get("ordinal_position"),
				enum_display_name,
			}
		})
		.collect())
}

async fn fetch_primary_keys(
	client: &deadpool_postgres::Object,
	schema: &str,
	table: &str,
) -> Result<Vec<String>> {
	let rows = client
		.query(
			r#"
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            "#,
			&[&schema, &table],
		)
		.await?;

	Ok(rows.iter().map(|r| r.get::<_, String>("column_name")).collect())
}

async fn fetch_foreign_keys(
	client: &deadpool_postgres::Object,
	schema: &str,
	table: &str,
) -> Result<Vec<ForeignKey>> {
	use super::model::SmartTags;
	let rows = client
		.query(
			r#"
            SELECT
              tc.constraint_name,
              kcu.column_name,
              ccu.table_name  AS foreign_table,
              ccu.column_name AS foreign_column,
              pg_catalog.obj_description(pgc.oid, 'pg_constraint') AS comment
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
            LEFT JOIN pg_catalog.pg_constraint pgc
              ON pgc.conname = tc.constraint_name
              AND pgc.connamespace = (
                SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = tc.table_schema
              )
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = $1
              AND tc.table_name = $2
            "#,
			&[&schema, &table],
		)
		.await?;

	Ok(rows
		.iter()
		.map(|r| {
			let comment: Option<String> = r.get("comment");
			let smart_tags = comment.as_deref().map(SmartTags::from_comment).unwrap_or_default();
			ForeignKey {
				constraint_name: r.get("constraint_name"),
				column: r.get("column_name"),
				foreign_table: r.get("foreign_table"),
				foreign_column: r.get("foreign_column"),
				smart_tags,
			}
		})
		.collect())
}

async fn fetch_unique_constraints(
	client: &deadpool_postgres::Object,
	schema: &str,
	table: &str,
) -> Result<Vec<Vec<String>>> {
	let rows = client
		.query(
			r#"
            SELECT tc.constraint_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
              AND tc.constraint_type = 'UNIQUE'
            ORDER BY tc.constraint_name, kcu.ordinal_position
            "#,
			&[&schema, &table],
		)
		.await?;

	// Group columns by constraint name
	let mut map: std::collections::BTreeMap<String, Vec<String>> =
		std::collections::BTreeMap::new();
	for r in &rows {
		map.entry(r.get::<_, String>("constraint_name"))
			.or_default()
			.push(r.get::<_, String>("column_name"));
	}

	Ok(map.into_values().collect())
}
