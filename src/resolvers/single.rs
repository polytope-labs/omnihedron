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

//! Single-record resolver — lookup by primary key or nodeId.
//!
//! - [`resolve_single`] — handles `{entity}(id: ID!)` root query fields.
//! - [`resolve_by_node_id`] — handles `{entity}ByNodeId(nodeId: ID!)` root query fields, decoding
//!   the PostGraphile-compatible base64 nodeId before performing the lookup.

use async_graphql::dynamic::ResolverContext;
use deadpool_postgres::Pool;
use serde_json::Value;
use tokio_postgres::types::ToSql;
use tracing::trace;

use crate::{config::Config, resolvers::connection::row_to_json, schema::cursor::decode_node_id};

/// Resolve a single-record query by primary key (the `id` argument).
/// Returns a plain `serde_json::Value` so nested field resolvers can use
/// `ctx.parent_value.try_downcast_ref::<serde_json::Value>()`.
pub async fn resolve_single(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	table: &str,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let id: String = ctx
		.args
		.get("id")
		.and_then(|v| v.string().ok())
		.map(str::to_string)
		.ok_or_else(|| async_graphql::Error::new("Missing required argument: id"))?;

	let schema = &cfg.name;
	let sql = format!(r#"SELECT * FROM "{schema}"."{table}" AS t WHERE t.id = $1 LIMIT 1"#);

	trace!(sql = %sql, "Executing single query");

	let client = pool.get().await?;
	let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(id)];
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = client.query(&sql, &pg_refs).await?;
	if rows.is_empty() {
		return Ok(None);
	}

	Ok(Some(row_to_json(&rows[0])))
}

/// Resolve a `{entity}ByNodeId(nodeId: ID!)` query.
///
/// Decodes the PostGraphile-compatible nodeId (base64 `[table_name, _id_uuid]`)
/// and performs a lookup by the internal `_id` UUID column.
pub async fn resolve_by_node_id(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	table: &str,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let node_id: String = ctx
		.args
		.get("nodeId")
		.and_then(|v| v.string().ok())
		.map(str::to_string)
		.ok_or_else(|| async_graphql::Error::new("Missing required argument: nodeId"))?;

	let (_table_name, pk_value) = decode_node_id(&node_id)
		.map_err(|e| async_graphql::Error::new(format!("Invalid nodeId: {e}")))?;

	let pk_str = match &pk_value {
		Value::String(s) => s.clone(),
		Value::Number(n) => n.to_string(),
		other => other.to_string(),
	};

	let schema = &cfg.name;
	// Look up by the internal _id UUID column (PostGraphile-compatible nodeId encoding).
	let sql =
		format!(r#"SELECT * FROM "{schema}"."{table}" AS t WHERE t."_id"::text = $1 LIMIT 1"#);

	trace!(sql = %sql, "Executing byNodeId query");

	let client = pool.get().await?;
	let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(pk_str)];
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = client.query(&sql, &pg_refs).await?;
	if rows.is_empty() {
		return Ok(None);
	}

	Ok(Some(row_to_json(&rows[0])))
}
