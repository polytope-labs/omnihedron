//! List query resolver — the primary GraphQL connection field handler.
//!
//! [`resolve_connection`] handles all `{entities}(...)` root query fields,
//! translating the full set of GraphQL arguments (filter, orderBy, first/last,
//! after/before, offset, blockHeight, distinct) into a single parameterized
//! SQL query, executing it, and assembling the `{Entity}Connection` response
//! (nodes, edges, pageInfo, totalCount).

use async_graphql::dynamic::ResolverContext;
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tokio_postgres::types::ToSql;
use tracing::debug;

use crate::{
	config::Config,
	schema::cursor::encode_cursor,
	sql::{
		filter::build_filter_sql,
		pagination::{PaginationArgs, resolve_pagination},
	},
};

/// Resolve a connection (list) query for the given table.
/// Returns a plain `serde_json::Value` so nested field resolvers can use
/// `ctx.parent_value.try_downcast_ref::<serde_json::Value>()`.
pub async fn resolve_connection(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	table: &str,
	cfg: &Config,
	is_historical: bool,
) -> async_graphql::Result<Option<Value>> {
	// ── Extract arguments ─────────────────────────────────────────────────────
	let first = ctx.args.get("first").and_then(|v| v.i64().ok()).map(|n| n as usize);
	let last = ctx.args.get("last").and_then(|v| v.i64().ok()).map(|n| n as usize);
	let after = ctx.args.get("after").and_then(|v| v.string().ok()).map(str::to_string);
	let before = ctx.args.get("before").and_then(|v| v.string().ok()).map(str::to_string);
	let offset = ctx.args.get("offset").and_then(|v| v.i64().ok()).map(|n| n as usize);
	let block_height: Option<String> =
		ctx.args.get("blockHeight").and_then(|v| v.string().ok()).map(str::to_string);

	let filter_val: Option<Value> = ctx
		.args
		.get("filter")
		.map(|v| serde_json::to_value(v.as_value()).unwrap_or(Value::Null));

	let order_by_gql = ctx.args.get("orderBy").map(|v| v.as_value().clone());
	let distinct_gql = ctx.args.get("distinct").map(|v| v.as_value().clone());

	let schema = &cfg.name;

	let requested = first.or(last).unwrap_or(cfg.query_limit);
	let limit = if cfg.unsafe_mode { requested } else { requested.min(cfg.query_limit) };

	// ── WHERE clauses ─────────────────────────────────────────────────────────
	let mut conditions: Vec<String> = vec![];
	let mut params: Vec<Value> = vec![];
	let mut param_offset: usize = 0;

	if let Some(height) = &block_height {
		param_offset += 1;
		conditions.push(format!("t._block_range @> ${param_offset}::bigint"));
		params.push(json!(height.parse::<i64>().unwrap_or(i64::MAX)));
	} else if is_historical {
		// No blockHeight specified for a historical table — return only the latest version.
		conditions.push("upper_inf(t._block_range)".to_string());
	}

	if let Some(f) = filter_val {
		let (filter_conds, filter_params) = build_filter_sql(&f, "t", &mut param_offset);
		conditions.extend(filter_conds);
		params.extend(filter_params);
	}

	// ── ORDER BY ──────────────────────────────────────────────────────────────
	let order_clauses = parse_orderby(order_by_gql.as_ref());
	let order_cols: Vec<String> = extract_order_cols(&order_clauses);

	// ── PAGINATION ────────────────────────────────────────────────────────────
	let pagination = resolve_pagination(
		&PaginationArgs { first, last, after: after.clone(), before: before.clone(), offset },
		&order_cols,
		&mut param_offset,
		cfg.query_limit,
	)?;

	if let Some((cursor_cond, cursor_params)) = &pagination.cursor_condition {
		conditions.push(cursor_cond.clone());
		params.extend(cursor_params.clone());
	}

	// ── DISTINCT ON ───────────────────────────────────────────────────────────
	let distinct_cols = parse_distinct(distinct_gql.as_ref());

	let where_clause = if conditions.is_empty() {
		String::new()
	} else {
		format!("WHERE {}", conditions.join(" AND "))
	};

	let distinct_clause = if distinct_cols.is_empty() {
		String::new()
	} else {
		format!(
			"DISTINCT ON ({}) ",
			distinct_cols.iter().map(|c| format!("t.{c}")).collect::<Vec<_>>().join(", ")
		)
	};

	let order_clause = if order_clauses.is_empty() {
		"ORDER BY t.id ASC".to_string()
	} else {
		format!("ORDER BY {}", order_clauses.join(", "))
	};

	let sql = format!(
		r#"SELECT {distinct_clause}t.* FROM "{schema}"."{table}" AS t {where_clause} {order_clause} LIMIT {limit} OFFSET {}"#,
		pagination.offset
	);

	let count_sql =
		format!(r#"SELECT COUNT(*) AS total FROM "{schema}"."{table}" AS t {where_clause}"#);

	// ── Execute ───────────────────────────────────────────────────────────────
	let client = pool.get().await?;

	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	debug!(sql = %sql, "Executing connection query");
	let rows = client.query(&sql, &pg_refs).await?;
	let total_row = client.query_one(&count_sql, &pg_refs).await?;
	let total: i64 = total_row.get("total");

	// ── Build response ────────────────────────────────────────────────────────
	let mut nodes: Vec<Value> = vec![];
	let mut edges: Vec<Value> = vec![];

	for row in &rows {
		let node = row_to_json(row);
		let cursor_fields: Vec<(&str, Value)> =
			vec![("id", node.get("id").cloned().unwrap_or(json!(null)))];
		let cursor = encode_cursor(&cursor_fields);
		edges.push(json!({ "cursor": cursor, "node": node }));
		nodes.push(node);
	}

	let has_next = (pagination.offset + limit) < total as usize;
	let has_prev = pagination.offset > 0 || after.is_some();

	let start_cursor = edges.first().and_then(|e| e.get("cursor")).cloned();
	let end_cursor = edges.last().and_then(|e| e.get("cursor")).cloned();

	let result = json!({
		"nodes": nodes,
		"edges": edges,
		"pageInfo": {
			"hasNextPage": has_next,
			"hasPreviousPage": has_prev,
			"startCursor": start_cursor,
			"endCursor": end_cursor,
		},
		"totalCount": total,
		// Aggregate context is embedded so the aggregates field resolver can
		// lazily compute aggregates using the same WHERE clause as the connection.
		"_agg_ctx": {
			"schema": schema,
			"table": table,
			"where_clause": where_clause,
			"params": params,
		},
	});

	Ok(Some(result))
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn parse_orderby(val: Option<&async_graphql::Value>) -> Vec<String> {
	let arr = match val {
		Some(async_graphql::Value::List(list)) => list.clone(),
		_ => return vec![],
	};
	arr.iter()
		.filter_map(|v| {
			let s = match v {
				async_graphql::Value::Enum(name) => name.as_str().to_string(),
				async_graphql::Value::String(s) => s.clone(),
				_ => return None,
			};
			if s == "NATURAL" {
				return None;
			}
			let (col_upper, dir) = if s.ends_with("_ASC") {
				(&s[..s.len() - 4], "ASC")
			} else if s.ends_with("_DESC") {
				(&s[..s.len() - 5], "DESC")
			} else {
				return None;
			};
			let col = col_upper.to_lowercase();
			Some(format!("t.{col} {dir}"))
		})
		.collect()
}

fn extract_order_cols(clauses: &[String]) -> Vec<String> {
	clauses
		.iter()
		.filter_map(|c| c.trim_start_matches("t.").split_whitespace().next().map(str::to_string))
		.collect()
}

fn parse_distinct(val: Option<&async_graphql::Value>) -> Vec<String> {
	let arr = match val {
		Some(async_graphql::Value::List(list)) => list.clone(),
		_ => return vec![],
	};
	arr.iter()
		.filter_map(|v| {
			let s = match v {
				async_graphql::Value::Enum(name) => name.as_str().to_string(),
				async_graphql::Value::String(s) => s.clone(),
				_ => return None,
			};
			Some(s.to_lowercase())
		})
		.collect()
}

pub fn row_to_json(row: &tokio_postgres::Row) -> Value {
	let mut map = serde_json::Map::new();
	for (i, col) in row.columns().iter().enumerate() {
		let name = col.name().to_string();
		let val = pg_col_to_json(row, i, col.type_());
		map.insert(name, val);
	}
	Value::Object(map)
}

pub fn pg_col_to_json(
	row: &tokio_postgres::Row,
	idx: usize,
	ty: &tokio_postgres::types::Type,
) -> Value {
	use tokio_postgres::types::Type;
	match *ty {
		Type::BOOL => row
			.try_get::<_, Option<bool>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT2 | Type::INT4 => row
			.try_get::<_, Option<i32>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT8 => row
			.try_get::<_, Option<i64>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_string())),
		Type::FLOAT4 | Type::FLOAT8 => row
			.try_get::<_, Option<f64>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => row
			.try_get::<_, Option<String>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::JSON | Type::JSONB => row
			.try_get::<_, Option<serde_json::Value>>(idx)
			.ok()
			.flatten()
			.unwrap_or(Value::Null),
		Type::TIMESTAMPTZ | Type::TIMESTAMP => row
			.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_rfc3339())),
		Type::DATE => row
			.try_get::<_, Option<chrono::NaiveDate>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_string())),
		Type::UUID => row
			.try_get::<_, Option<uuid::Uuid>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_string())),
		Type::NUMERIC => {
			// Read as string to preserve precision
			row.try_get::<_, Option<String>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v))
		},
		Type::BYTEA => row
			.try_get::<_, Option<Vec<u8>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(hex::encode(v))),
		_ => row
			.try_get::<_, Option<String>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
	}
}

pub fn json_to_pg_params(params: &[Value]) -> Vec<Box<dyn ToSql + Sync + Send>> {
	params
		.iter()
		.map(|v| -> Box<dyn ToSql + Sync + Send> {
			match v {
				Value::Null => Box::new(Option::<String>::None),
				Value::Bool(b) => Box::new(*b),
				Value::Number(n) =>
					if let Some(i) = n.as_i64() {
						Box::new(i)
					} else if let Some(f) = n.as_f64() {
						Box::new(f)
					} else {
						Box::new(n.to_string())
					},
				Value::String(s) => Box::new(s.clone()),
				Value::Array(_) | Value::Object(_) => Box::new(v.to_string()),
			}
		})
		.collect()
}
