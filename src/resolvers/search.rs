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

//! Fulltext search resolver.
//!
//! SubQuery's `@fullText` directive creates PostgreSQL functions of the form:
//! `search_{hash}(search text) RETURNS SETOF table`
//!
//! This resolver calls the function and wraps the results in a connection
//! response (nodes, edges, pageInfo, totalCount).

use async_graphql::dynamic::ResolverContext;
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tokio_postgres::types::ToSql;
use tracing::trace;

use crate::{
	config::Config, resolvers::connection::row_to_json, schema::cursor::encode_cursor,
	sql::search::sanitize_tsquery,
};

/// Resolve a fulltext search query by calling the PostgreSQL search function.
///
/// Returns a connection response with nodes, edges, pageInfo, and totalCount.
pub async fn resolve_search(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	pg_function_name: &str,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let schema = &cfg.name;

	let search_raw: String = ctx
		.args
		.get("search")
		.and_then(|v| v.string().ok())
		.map(str::to_string)
		.unwrap_or_default();

	let search_query = match sanitize_tsquery(&search_raw) {
		Some(q) => q,
		None => {
			return Ok(Some(json!({
				"nodes": [],
				"edges": [],
				"pageInfo": { "hasNextPage": false, "hasPreviousPage": false },
				"totalCount": 0,
			})));
		},
	};

	let first: usize = ctx
		.args
		.get("first")
		.and_then(|v| v.i64().ok())
		.map(|n| n.max(0) as usize)
		.filter(|&n| n > 0)
		.unwrap_or(cfg.query_limit)
		.min(cfg.query_limit);

	let offset: usize = ctx
		.args
		.get("offset")
		.and_then(|v| v.i64().ok())
		.map(|n| n.max(0) as usize)
		.unwrap_or(0);

	// Call the search function and get paginated results.
	let sql = format!(
		r#"SELECT * FROM "{schema}"."{pg_function_name}"($1) LIMIT {first} OFFSET {offset}"#
	);
	let count_sql = format!(r#"SELECT COUNT(*) AS total FROM "{schema}"."{pg_function_name}"($1)"#);

	trace!(sql = %sql, search = %search_query, "Executing fulltext search query");

	let client = pool.get().await?;
	let params: Vec<&(dyn ToSql + Sync)> = vec![&search_query];

	let rows = client.query(&sql, &params).await?;
	let count_row = client.query_one(&count_sql, &params).await?;
	let total: i64 = count_row.get("total");

	let mut nodes = vec![];
	let mut edges = vec![];
	for row in &rows {
		let node = row_to_json(row);
		let cursor = encode_cursor(&[("id", node.get("id").cloned().unwrap_or(json!(null)))]);
		edges.push(json!({ "cursor": cursor, "node": node.clone() }));
		nodes.push(node);
	}

	let has_next = (offset + first) < total as usize;
	let has_prev = offset > 0;

	Ok(Some(json!({
		"nodes": nodes,
		"edges": edges,
		"pageInfo": {
			"hasNextPage": has_next,
			"hasPreviousPage": has_prev,
			"startCursor": edges.first().and_then(|e| e.get("cursor")),
			"endCursor": edges.last().and_then(|e| e.get("cursor")),
		},
		"totalCount": total,
	})))
}
