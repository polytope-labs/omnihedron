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
//!
//! Historical tables store one row per version of an entity, discriminated by the
//! `_block_range` column. `resolve_single` therefore *must* constrain the lookup to a
//! single version — without it, `WHERE id = $1 LIMIT 1` returns whichever version
//! PostgreSQL happens to reach first in heap order, which is arbitrary and unstable.
//! `resolve_by_node_id` needs no such predicate: a nodeId encodes the internal `_id`
//! column, which is unique *per version row*, so it already identifies exactly one row.

use async_graphql::dynamic::ResolverContext;
use serde_json::Value;
use tokio_postgres::types::ToSql;
use tracing::trace;

use crate::{config::Config, resolvers::connection::row_to_json, schema::cursor::decode_node_id};

/// Resolve a single-record query by primary key (the `id` argument).
/// Returns a plain `serde_json::Value` so nested field resolvers can use
/// `ctx.parent_value.try_downcast_ref::<serde_json::Value>()`.
pub async fn resolve_single(
	ctx: &ResolverContext<'_>,
	table: &str,
	cfg: &Config,
	is_historical: bool,
) -> async_graphql::Result<Option<Value>> {
	let id: String = ctx
		.args
		.get("id")
		.and_then(|v| v.string().ok())
		.map(str::to_string)
		.ok_or_else(|| async_graphql::Error::new("Missing required argument: id"))?;

	// The historical argument is named `blockHeight` or `timestamp` depending on the
	// project's `historicalStateEnabled` mode; accept either, as the connection resolver does.
	let block_height: Option<String> = ctx
		.args
		.get("blockHeight")
		.or_else(|| ctx.args.get("timestamp"))
		.and_then(|v| v.string().ok())
		.map(str::to_string);

	let schema = &cfg.name;
	let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(id)];

	// Historical tables keep every version of an entity. Select the single version whose
	// `_block_range` contains the requested point, defaulting to MAX_INT64 so that only
	// the currently-open version matches — the same rule the connection resolver applies.
	let sql = if is_historical {
		let bh = block_height.as_ref().and_then(|s| s.parse::<i64>().ok()).unwrap_or(i64::MAX);
		params.push(Box::new(bh));
		format!(
			r#"SELECT * FROM "{schema}"."{table}" AS t WHERE t.id = $1 AND t._block_range @> $2::bigint ORDER BY lower(t._block_range) DESC LIMIT 1"#
		)
	} else {
		format!(r#"SELECT * FROM "{schema}"."{table}" AS t WHERE t.id = $1 LIMIT 1"#)
	};

	trace!(sql = %sql, "Executing single query");

	let req_client = ctx
		.data::<std::sync::Arc<crate::db::RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = req_client.query(&sql, &pg_refs).await.map_err(super::pg_to_gql_error)?;
	if rows.is_empty() {
		return Ok(None);
	}

	let mut node = row_to_json(&rows[0]);
	// Propagate the requested blockHeight so nested relation resolvers filter consistently
	// (they read `_block_height` off the parent entity JSON).
	if let (Value::Object(map), Some(bh)) = (&mut node, block_height) {
		map.insert("_block_height".to_string(), Value::String(bh));
	}

	Ok(Some(node))
}

/// Resolve a `{entity}ByNodeId(nodeId: ID!)` query.
///
/// Decodes the PostGraphile-compatible nodeId (base64 `[table_name, _id_uuid]`)
/// and performs a lookup by the internal `_id` UUID column.
pub async fn resolve_by_node_id(
	ctx: &ResolverContext<'_>,
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

	let req_client = ctx
		.data::<std::sync::Arc<crate::db::RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;
	let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(pk_str)];
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = req_client.query(&sql, &pg_refs).await.map_err(super::pg_to_gql_error)?;
	if rows.is_empty() {
		return Ok(None);
	}

	Ok(Some(row_to_json(&rows[0])))
}
