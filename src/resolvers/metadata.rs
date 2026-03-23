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

//! Metadata resolver — `_metadata` and `_metadatas` query fields.
//!
//! Reads from the `_metadata` key/value table present in every SubQuery
//! indexed schema and assembles the [`_Metadata`] GraphQL type.  When an
//! `--indexer` URL is configured, additional fields are fetched from the
//! indexer's HTTP API and merged into the response.

use std::sync::Arc;

use async_graphql::dynamic::ResolverContext;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::{Value, json};
use tracing::{debug, warn};

use crate::{config::Config, db::RequestClient};

fn base64_cursor(idx: usize) -> String {
	BASE64.encode(format!("[{idx}]"))
}

/// If `v` is a JSON string whose content is a JSON object or array,
/// re-parse it and return the inner value. Otherwise return `v` unchanged.
///
/// Some `_metadata` keys (e.g. `deployments`) are stored in PostgreSQL as a
/// JSONB *string* that contains a serialised JSON object — i.e. the DB value
/// is `""{\"k\":\"v\"}"` at the JSONB level.  PostGraphile's GetMetadataPlugin
/// explicitly parses these back to objects; we replicate that behaviour here.
fn try_reparse_json_string(v: Value) -> Value {
	if let Value::String(ref s) = v {
		let t = s.trim();
		if (t.starts_with('{') && t.ends_with('}')) || (t.starts_with('[') && t.ends_with(']')) {
			if let Ok(parsed) = serde_json::from_str::<Value>(t) {
				return parsed;
			}
		}
	}
	v
}

/// Cached default metadata table name (no chainId).
static DEFAULT_METADATA_TABLE: std::sync::OnceLock<tokio::sync::Mutex<Option<String>>> =
	std::sync::OnceLock::new();

fn default_cache() -> &'static tokio::sync::Mutex<Option<String>> {
	DEFAULT_METADATA_TABLE.get_or_init(|| tokio::sync::Mutex::new(None))
}

/// Find the most appropriate metadata table in the given schema.
/// Priority:
///   1. Plain `_metadata` (single-chain projects)
///   2. `_metadata_0x<hash>` matching the requested chainId
///   3. First `_metadata_0x<hash>` table found (fallback)
///
/// Uses `pg_class`/`pg_namespace` directly instead of `information_schema.tables`
/// which is a slow multi-join view.
async fn find_metadata_table(
	client: &RequestClient,
	schema: &str,
	chain_id: Option<&str>,
) -> Option<String> {
	// Return cached default table if no chainId requested
	if chain_id.is_none() {
		let cache = default_cache().lock().await;
		if let Some(ref cached) = *cache {
			return Some(cached.clone());
		}
	}

	let sql = "SELECT c.relname::text AS table_name \
               FROM pg_class c \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               WHERE n.nspname = $1 \
                 AND c.relkind = 'r' \
                 AND (c.relname = '_metadata' OR c.relname LIKE '_metadata_0x%') \
               ORDER BY c.relname";
	let rows = client.query(sql, &[&schema]).await.ok()?;
	let tables: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

	if tables.is_empty() {
		return None;
	}

	// Prefer plain _metadata
	if tables.iter().any(|t| t == "_metadata") {
		let result = "_metadata".to_string();
		if chain_id.is_none() {
			*default_cache().lock().await = Some(result.clone());
		}
		return Some(result);
	}

	// If chainId requested, try to find matching table by querying 'chain' key
	if let Some(cid) = chain_id {
		for table in &tables {
			let check_sql =
				format!(r#"SELECT value FROM "{schema}"."{table}" WHERE key = 'chain' LIMIT 1"#);
			if let Ok(row) = client.query_opt(&check_sql, &[]).await {
				if let Some(row) = row {
					let val: Option<Value> = row.try_get::<_, Value>(0).ok();
					let chain_val = val.as_ref().and_then(|v| v.as_str()).unwrap_or("");
					if chain_val == cid {
						return Some(table.clone());
					}
				}
			}
		}
	}

	// Fallback: first table
	let result = tables.into_iter().next().unwrap();
	if chain_id.is_none() {
		*default_cache().lock().await = Some(result.clone());
	}
	Some(result)
}

pub async fn resolve_metadata(
	ctx: &ResolverContext<'_>,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let schema = &cfg.name;
	let req_client = ctx
		.data::<Arc<RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;

	// Extract optional chainId argument
	let chain_id_owned: Option<String> = ctx
		.args
		.get("chainId")
		.and_then(|v| v.string().ok().map(|s| s.to_string()))
		.filter(|s| !s.is_empty());
	let chain_id = chain_id_owned.as_deref();

	let table = find_metadata_table(req_client, schema, chain_id).await;

	let Some(table) = table else {
		// No metadata table found – return an object with just the node version
		let mut meta = serde_json::Map::new();
		meta.insert("queryNodeVersion".to_string(), json!(env!("CARGO_PKG_VERSION")));
		return Ok(Some(Value::Object(meta)));
	};

	debug!(table = %table, "Fetching metadata from table");

	// Run the key/value scan, row count estimates, and db size concurrently.
	// Uses pg_class directly instead of information_schema for row estimates.
	let kv_sql = format!(r#"SELECT key, value FROM "{schema}"."{table}""#);
	let estimate_sql = "SELECT c.relname AS \"table\", c.reltuples::bigint AS estimate \
                        FROM pg_class c \
                        JOIN pg_namespace n ON n.oid = c.relnamespace \
                        WHERE n.nspname = $1 \
                          AND c.relkind = 'r' \
                          AND c.relname NOT LIKE '\\_metadata%'";
	let size_sql = "SELECT pg_database_size(current_database()) AS db_size";

	let schema_str: &str = schema.as_str();
	let est_params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![&schema_str];
	let (kv_result, est_result, size_result) = tokio::join!(
		req_client.query(&kv_sql, &[]),
		req_client.query(estimate_sql, &est_params),
		req_client.query_one(size_sql, &[]),
	);

	let rows = kv_result.map_err(|e| {
		warn!(error = %e, table = %table, "Failed to query metadata table");
		e
	})?;

	let mut meta = serde_json::Map::new();
	for row in &rows {
		let key: String = row.get(0);
		// value is jsonb — read natively as serde_json::Value.
		// Some keys (e.g. `deployments`) are stored as a JSON string whose content
		// is itself a JSON object/array (double-encoded). Re-parse those strings so
		// the GraphQL JSON scalar returns a proper object, matching PostGraphile.
		let val: Option<Value> = row.try_get::<_, Value>(1).ok();
		if let Some(v) = val {
			let v = try_reparse_json_string(v);
			meta.insert(key, v);
		}
	}

	let estimates: Vec<Value> = est_result
		.unwrap_or_else(|e| {
			warn!(error = %e, "Failed to query row count estimates");
			vec![]
		})
		.iter()
		.map(|r| {
			let tbl: String = r.get("table");
			let estimate: i64 = r.try_get("estimate").unwrap_or(0);
			json!({ "table": tbl, "estimate": estimate })
		})
		.collect();

	let db_size: i64 = match size_result {
		Ok(row) => row.try_get("db_size").unwrap_or(0),
		Err(e) => {
			warn!(error = %e, "Failed to query database size");
			0
		},
	};

	meta.insert("rowCountEstimate".to_string(), Value::Array(estimates));
	meta.insert("dbSize".to_string(), json!(db_size.to_string()));
	meta.insert("queryNodeVersion".to_string(), json!(env!("CARGO_PKG_VERSION")));

	// Merge fields from --indexer HTTP API (if configured)
	if let Some(ref indexer_url) = cfg.indexer {
		merge_indexer_metadata(&mut meta, indexer_url).await;
	}

	Ok(Some(Value::Object(meta)))
}

pub async fn resolve_metadatas(
	ctx: &ResolverContext<'_>,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let schema = &cfg.name;
	let req_client = ctx
		.data::<Arc<RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;

	// Find all metadata tables via pg_class (avoids slow information_schema view).
	let sql = "SELECT c.relname::text AS table_name \
               FROM pg_class c \
               JOIN pg_namespace n ON n.oid = c.relnamespace \
               WHERE n.nspname = $1 \
                 AND c.relkind = 'r' \
                 AND (c.relname = '_metadata' OR c.relname LIKE '_metadata_0x%' \
                      OR c.relname LIKE '_multi_metadata_%') \
               ORDER BY c.relname";
	let table_rows = req_client.query(sql, &[&schema.as_str()]).await.map_err(|e| {
		warn!(error = %e, "Failed to list metadata tables");
		e
	})?;
	let metadata_tables: Vec<String> = table_rows.iter().map(|r| r.get::<_, String>(0)).collect();
	let total_count = metadata_tables.len();

	// Resolve each metadata table as a separate node
	let mut nodes: Vec<Value> = Vec::new();
	for table in &metadata_tables {
		let q = format!(r#"SELECT key, value FROM "{schema}"."{table}""#);
		let rows = req_client.query(&q, &[]).await.unwrap_or_else(|e| {
			warn!(error = %e, table = %table, "Failed to query metadata table");
			vec![]
		});
		let mut meta = serde_json::Map::new();
		for row in &rows {
			let key: String = row.get(0);
			let val: Option<Value> = row.try_get::<_, Value>(1).ok();
			if let Some(v) = val {
				meta.insert(key, try_reparse_json_string(v));
			}
		}
		meta.insert("queryNodeVersion".to_string(), json!(env!("CARGO_PKG_VERSION")));
		nodes.push(Value::Object(meta));
	}

	if nodes.is_empty() {
		// Fallback: single resolved metadata
		let meta = resolve_metadata(ctx, cfg).await?;
		if let Some(m) = meta {
			nodes.push(m);
		}
	}

	let edges: Vec<Value> = nodes
		.iter()
		.enumerate()
		.map(|(i, node)| json!({ "cursor": base64_cursor(i), "node": node }))
		.collect();

	Ok(Some(json!({
		"totalCount": total_count,
		"nodes": nodes,
		"edges": edges,
	})))
}

/// Fetch metadata from the indexer HTTP API and merge into the existing metadata map.
/// Matches TS `GetMetadataPlugin.ts` behaviour: GET `{indexer_url}/meta` and `/health`.
/// Both requests run concurrently.
async fn merge_indexer_metadata(meta: &mut serde_json::Map<String, Value>, indexer_url: &str) {
	let client = match reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build()
	{
		Ok(c) => c,
		Err(_) => return,
	};

	let base = indexer_url.trim_end_matches('/');
	let meta_url = format!("{base}/meta");
	let health_url = format!("{base}/health");

	let (meta_resp, health_resp) =
		tokio::join!(client.get(&meta_url).send(), client.get(&health_url).send(),);

	// Merge /meta fields
	if let Ok(resp) = meta_resp {
		if let Ok(body) = resp.json::<Value>().await {
			if let Some(obj) = body.as_object() {
				for (k, v) in obj {
					// Only merge fields not already present from DB
					if !meta.contains_key(k) {
						meta.insert(k.clone(), v.clone());
					}
				}
			}
		}
	}

	// Merge /health status
	match health_resp {
		Ok(resp) => {
			meta.insert("indexerHealthy".to_string(), json!(resp.status().is_success()));
		},
		Err(_) => {
			meta.insert("indexerHealthy".to_string(), json!(false));
		},
	}
}
