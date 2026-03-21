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

//! Dataloader for batching forward relation queries.
//!
//! When resolving forward FK relations (e.g., `transfer.account`), individual
//! `SELECT ... WHERE id = $1` queries are batched into a single
//! `SELECT ... WHERE id IN ($1, $2, ...)` query, eliminating the N+1 problem.

use std::collections::HashMap;

use async_graphql::dataloader::Loader;
use deadpool_postgres::Pool;
use serde_json::Value;
use tracing::trace;

use crate::resolvers::connection::row_to_json;

/// Key for the relation dataloader — identifies which table and optional
/// block height to use for the batched query.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RelationKey {
	/// The ID value to look up
	pub id: String,
	/// Schema name
	pub schema: String,
	/// Target table name
	pub table: String,
	/// Columns to fetch (sorted for deterministic hashing). Must always include `"id"`.
	pub columns: Vec<String>,
	/// Whether the target table is historical
	pub is_historical: bool,
	/// Inherited block height (for historical tables)
	pub block_height: Option<String>,
}

/// Batches forward relation lookups into a single IN query.
pub struct RelationLoader {
	pool: Pool,
}

impl RelationLoader {
	pub fn new(pool: Pool) -> Self {
		Self { pool }
	}
}

impl Loader<RelationKey> for RelationLoader {
	type Value = Value;
	type Error = async_graphql::Error;

	fn load(
		&self,
		keys: &[RelationKey],
	) -> impl std::future::Future<Output = Result<HashMap<RelationKey, Self::Value>, Self::Error>> + Send
	{
		let keys = keys.to_vec();
		let pool = self.pool.clone();

		async move {
			if keys.is_empty() {
				return Ok(HashMap::new());
			}

			crate::metrics::record_dataloader_batch_size(keys.len());

			// Group keys by (schema, table, columns, is_historical, block_height) since
			// different tables or column sets need separate queries.
			let mut groups: HashMap<
				(String, String, Vec<String>, bool, Option<String>),
				Vec<&RelationKey>,
			> = HashMap::new();
			for key in &keys {
				let group_key = (
					key.schema.clone(),
					key.table.clone(),
					key.columns.clone(),
					key.is_historical,
					key.block_height.clone(),
				);
				groups.entry(group_key).or_default().push(key);
			}

			let client = crate::db::checkout(&pool).await?;
			let mut results = HashMap::new();

			for ((schema, table, columns, is_historical, block_height), group_keys) in &groups {
				let ids: Vec<&str> = group_keys.iter().map(|k| k.id.as_str()).collect();

				// Build selective column list from the key's requested columns
				let select_cols =
					columns.iter().map(|c| format!(r#"t."{}""#, c)).collect::<Vec<_>>().join(", ");

				// Build IN clause with parameterized placeholders
				let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("${i}")).collect();
				let in_clause = placeholders.join(", ");

				let mut where_parts = vec![format!("t.id IN ({in_clause})")];
				let mut param_count = ids.len();

				if *is_historical {
					param_count += 1;
					where_parts.push(format!("t._block_range @> ${param_count}::bigint"));
				}

				let where_clause = format!("WHERE {}", where_parts.join(" AND "));
				let sql = format!(
					r#"SELECT {select_cols} FROM "{schema}"."{table}" AS t {where_clause} ORDER BY t._id ASC"#
				);

				trace!(sql = %sql, count = ids.len(), "Executing batched relation query");

				// Build params
				let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> =
					Vec::new();
				for id in &ids {
					params.push(Box::new(id.to_string()));
				}
				if *is_historical {
					let bh: i64 =
						block_height.as_ref().and_then(|s| s.parse().ok()).unwrap_or(i64::MAX);
					params.push(Box::new(bh));
				}

				let pg_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
					.iter()
					.map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
					.collect();

				let sql_start = std::time::Instant::now();
				let rows = client.query(&sql, &pg_refs).await?;
				crate::metrics::record_sql_query("select", sql_start.elapsed().as_secs_f64());

				// Map results back to keys by id
				for row in &rows {
					let mut node = row_to_json(row);
					// Propagate block height for nested relations
					if *is_historical {
						if let (Value::Object(map), Some(bh)) = (&mut node, block_height) {
							map.insert("_block_height".to_string(), serde_json::json!(bh));
						}
					}
					if let Some(id_val) =
						node.get("id").and_then(|v| v.as_str()).map(str::to_string)
					{
						// Find the matching key(s) and insert results
						for key in group_keys.iter() {
							if key.id == id_val {
								results.insert((*key).clone(), node.clone());
							}
						}
					}
				}
			}

			Ok(results)
		}
	}
}
