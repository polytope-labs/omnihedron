//! Relation resolvers — forward and backward FK traversal.
//!
//! - [`resolve_forward_relation`] — resolves a field added to an entity type for each outgoing
//!   foreign key (FK column on this table → single record on the referenced table).
//! - [`resolve_backward_relation`] — resolves a field added to an entity type for each incoming
//!   foreign key (records in another table that reference this one), returned as a
//!   `{Child}Connection`.

use async_graphql::dynamic::ResolverContext;
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::resolvers::connection::row_to_json;

/// Resolve a forward relation (FK → single parent record).
/// Returns a plain `serde_json::Value` for nested field resolution.
///
/// When `foreign_is_historical` is true the related record is filtered by
/// `_block_range`, using the `_block_height` embedded in the parent entity JSON
/// (set by the connection resolver when a `blockHeight` argument is provided).
/// If no inherited blockHeight is present, only the latest version is returned
/// (`upper_inf(_block_range)`).
pub async fn resolve_forward_relation(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	foreign_table: &str,
	fk_column: &str,
	foreign_is_historical: bool,
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
	let fk_val = match parent.get(fk_column) {
		Some(v) if !v.is_null() => v.clone(),
		_ => return Ok(None),
	};

	// Inherit blockHeight from the parent entity (set by the connection resolver).
	let inherited_height: Option<String> =
		parent.get("_block_height").and_then(|v| v.as_str()).map(str::to_string);

	let schema = ctx
		.data::<String>()
		.map_err(|_| async_graphql::Error::new("Missing schema name in context"))?
		.clone();

	let id_str = match &fk_val {
		Value::String(s) => s.clone(),
		v => v.to_string(),
	};

	let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(id_str)];
	let mut where_parts = vec!["t.id = $1".to_string()];

	if foreign_is_historical {
		if let Some(ref bh) = inherited_height {
			let bh_int: i64 = bh.parse().unwrap_or(i64::MAX);
			params.push(Box::new(bh_int));
			where_parts.push(format!("t._block_range @> ${}::bigint", params.len()));
		} else {
			where_parts.push("upper_inf(t._block_range)".to_string());
		}
	}

	let where_clause = format!("WHERE {}", where_parts.join(" AND "));
	let sql = format!(r#"SELECT * FROM "{schema}"."{foreign_table}" AS t {where_clause} LIMIT 1"#);

	debug!(sql = %sql, "Executing forward relation query");

	let client = pool.get().await?;
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = client.query(&sql, &pg_refs).await?;
	if rows.is_empty() {
		return Ok(None);
	}

	let mut result = row_to_json(&rows[0]);
	// Propagate blockHeight so further nested relations can inherit it.
	if foreign_is_historical {
		if let (Value::Object(map), Some(bh)) = (&mut result, &inherited_height) {
			map.insert("_block_height".to_string(), json!(bh));
		}
	}

	Ok(Some(result))
}

/// Resolve a backward relation (parent → child connection).
/// Returns a plain `serde_json::Value` for nested field resolution.
///
/// When `child_is_historical` is true the child rows are filtered by
/// `_block_range`, using the `_block_height` embedded in the parent entity JSON
/// (set by the connection resolver when a `blockHeight` argument is provided).
/// If no inherited blockHeight is present, only the latest versions are returned
/// (`upper_inf(_block_range)`).
pub async fn resolve_backward_relation(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	child_table: &str,
	fk_column: &str,
	child_is_historical: bool,
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
	let parent_id = match parent.get("id") {
		Some(v) if !v.is_null() => v.clone(),
		_ =>
			return Ok(Some(json!({
				"nodes": [],
				"edges": [],
				"pageInfo": { "hasNextPage": false, "hasPreviousPage": false },
				"totalCount": 0,
			}))),
	};

	// Inherit blockHeight from the parent entity (set by the connection resolver).
	let inherited_height: Option<String> =
		parent.get("_block_height").and_then(|v| v.as_str()).map(str::to_string);

	let schema = ctx
		.data::<String>()
		.map_err(|_| async_graphql::Error::new("Missing schema name in context"))?
		.clone();

	let first = ctx.args.get("first").and_then(|v| v.i64().ok()).unwrap_or(100) as usize;
	let offset = ctx.args.get("offset").and_then(|v| v.i64().ok()).unwrap_or(0) as usize;

	let id_str = match &parent_id {
		Value::String(s) => s.clone(),
		v => v.to_string(),
	};

	let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(id_str)];
	let mut where_parts = vec![format!("t.\"{fk_column}\" = $1")];

	if child_is_historical {
		if let Some(ref bh) = inherited_height {
			let bh_int: i64 = bh.parse().unwrap_or(i64::MAX);
			params.push(Box::new(bh_int));
			where_parts.push(format!("t._block_range @> ${}::bigint", params.len()));
		} else {
			where_parts.push("upper_inf(t._block_range)".to_string());
		}
	}

	let where_clause = format!("WHERE {}", where_parts.join(" AND "));
	let sql = format!(
		r#"SELECT t.* FROM "{schema}"."{child_table}" AS t {where_clause} ORDER BY t.id ASC LIMIT {first} OFFSET {offset}"#
	);
	let count_sql =
		format!(r#"SELECT COUNT(*) AS total FROM "{schema}"."{child_table}" AS t {where_clause}"#);

	debug!(sql = %sql, "Executing backward relation query");

	let client = pool.get().await?;
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = client.query(&sql, &pg_refs).await?;
	let count_row = client.query_one(&count_sql, &pg_refs).await?;
	let total: i64 = count_row.get("total");

	let mut nodes = vec![];
	let mut edges = vec![];
	for row in &rows {
		let mut node = row_to_json(row);
		// Propagate blockHeight so further nested relations can inherit it.
		if child_is_historical {
			if let (Value::Object(map), Some(bh)) = (&mut node, &inherited_height) {
				map.insert("_block_height".to_string(), json!(bh));
			}
		}
		let cursor = crate::schema::cursor::encode_cursor(&[(
			"id",
			node.get("id").cloned().unwrap_or(json!(null)),
		)]);
		edges.push(json!({ "cursor": cursor, "node": node.clone() }));
		nodes.push(node);
	}

	Ok(Some(json!({
		"nodes": nodes,
		"edges": edges,
		"pageInfo": {
			"hasNextPage": (offset + first) < total as usize,
			"hasPreviousPage": offset > 0,
			"startCursor": edges.first().and_then(|e| e.get("cursor")),
			"endCursor": edges.last().and_then(|e| e.get("cursor")),
		},
		"totalCount": total,
	})))
}
