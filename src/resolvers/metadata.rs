//! Metadata resolver — `_metadata` and `_metadatas` query fields.
//!
//! Reads from the `_metadata` key/value table present in every SubQuery
//! indexed schema and assembles the [`_Metadata`] GraphQL type.  When an
//! `--indexer` URL is configured, additional fields are fetched from the
//! indexer's HTTP API and merged into the response.

use async_graphql::dynamic::ResolverContext;
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tracing::debug;

use crate::config::Config;

/// Find the most appropriate metadata table in the given schema.
/// Priority:
///   1. Plain `_metadata` (single-chain projects)
///   2. `_metadata_0x<hash>` matching the requested chainId
///   3. First `_metadata_0x<hash>` table found (fallback)
async fn find_metadata_table(
	client: &deadpool_postgres::Object,
	schema: &str,
	chain_id: Option<&str>,
) -> Option<String> {
	let sql = "SELECT table_name FROM information_schema.tables \
               WHERE table_schema = $1 \
                 AND (table_name = '_metadata' OR table_name LIKE '_metadata_0x%') \
               ORDER BY table_name";
	let rows = client.query(sql, &[&schema]).await.ok()?;
	let tables: Vec<String> = rows.iter().map(|r| r.get::<_, String>(0)).collect();

	if tables.is_empty() {
		return None;
	}

	// Prefer plain _metadata
	if tables.iter().any(|t| t == "_metadata") {
		return Some("_metadata".to_string());
	}

	// If chainId requested, try to find matching table by querying 'chain' key
	if let Some(cid) = chain_id {
		for table in &tables {
			let check_sql =
				format!(r#"SELECT value FROM "{schema}"."{table}" WHERE key = 'chain' LIMIT 1"#);
			if let Ok(row) = client.query_opt(&check_sql, &[]).await {
				if let Some(row) = row {
					// value column is jsonb
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
	Some(tables.into_iter().next().unwrap())
}

pub async fn resolve_metadata(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let schema = &cfg.name;
	let client = pool.get().await?;

	// Extract optional chainId argument
	let chain_id_owned: Option<String> = ctx
		.args
		.get("chainId")
		.and_then(|v| v.string().ok().map(|s| s.to_string()))
		.filter(|s| !s.is_empty());
	let chain_id = chain_id_owned.as_deref();

	let table = find_metadata_table(&client, schema, chain_id).await;

	let Some(table) = table else {
		// No metadata table found – return an object with just the node version
		let mut meta = serde_json::Map::new();
		meta.insert("queryNodeVersion".to_string(), json!(env!("CARGO_PKG_VERSION")));
		return Ok(Some(Value::Object(meta)));
	};

	debug!(table = %table, "Fetching metadata from table");

	let sql = format!(r#"SELECT key, value FROM "{schema}"."{table}""#);
	let rows = client.query(&sql, &[]).await.unwrap_or_default();

	let mut meta = serde_json::Map::new();
	for row in &rows {
		let key: String = row.get(0);
		// value is jsonb — read natively as serde_json::Value
		let val: Option<Value> = row.try_get::<_, Value>(1).ok();
		if let Some(v) = val {
			meta.insert(key, v);
		}
	}

	// Row count estimates
	let estimate_sql = r#"
        SELECT relname AS "table", reltuples::bigint AS estimate
        FROM pg_class
        WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = $1)
          AND relname IN (
            SELECT table_name FROM information_schema.tables WHERE table_schema = $1
          )
    "#;
	let est_rows = client.query(estimate_sql, &[&schema.as_str()]).await.unwrap_or_default();
	let estimates: Vec<Value> = est_rows
		.iter()
		.map(|r| {
			let tbl: String = r.get("table");
			let estimate: i64 = r.try_get("estimate").unwrap_or(0);
			json!({ "table": tbl, "estimate": estimate })
		})
		.collect();

	// DB size
	let size_sql = "SELECT pg_database_size(current_database()) AS db_size";
	let size_row = client.query_one(size_sql, &[]).await.ok();
	let db_size: i64 = size_row.as_ref().and_then(|r| r.try_get("db_size").ok()).unwrap_or(0);

	meta.insert("rowCountEstimate".to_string(), Value::Array(estimates));
	meta.insert("dbSize".to_string(), json!(db_size.to_string()));
	meta.insert("queryNodeVersion".to_string(), json!(env!("CARGO_PKG_VERSION")));

	Ok(Some(Value::Object(meta)))
}

pub async fn resolve_metadatas(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let schema = &cfg.name;
	let client = pool.get().await?;

	// Find all metadata tables
	let sql = "SELECT table_name FROM information_schema.tables \
               WHERE table_schema = $1 \
                 AND (table_name = '_metadata' OR table_name LIKE '_metadata_0x%' \
                      OR table_name LIKE '_multi_metadata_%') \
               ORDER BY table_name";
	let table_rows = client.query(sql, &[&schema.as_str()]).await.unwrap_or_default();
	let metadata_tables: Vec<String> = table_rows.iter().map(|r| r.get::<_, String>(0)).collect();
	let total_count = metadata_tables.len();

	// Resolve each metadata table as a separate node
	let mut nodes: Vec<Value> = Vec::new();
	for table in &metadata_tables {
		let q = format!(r#"SELECT key, value FROM "{schema}"."{table}""#);
		let rows = client.query(&q, &[]).await.unwrap_or_default();
		let mut meta = serde_json::Map::new();
		for row in &rows {
			let key: String = row.get(0);
			let val: Option<Value> = row.try_get::<_, Value>(1).ok();
			if let Some(v) = val {
				meta.insert(key, v);
			}
		}
		meta.insert("queryNodeVersion".to_string(), json!(env!("CARGO_PKG_VERSION")));
		nodes.push(Value::Object(meta));
	}

	if nodes.is_empty() {
		// Fallback: single resolved metadata
		let meta = resolve_metadata(ctx, pool, cfg).await?;
		if let Some(m) = meta {
			nodes.push(m);
		}
	}

	Ok(Some(json!({
		"totalCount": total_count,
		"nodes": nodes,
	})))
}
