//! List query resolver — the primary GraphQL connection field handler.
//!
//! [`resolve_connection`] handles all `{entities}(...)` root query fields,
//! translating the full set of GraphQL arguments (filter, orderBy, first/last,
//! after/before, offset, blockHeight, distinct) into a single parameterized
//! SQL query, executing it, and assembling the `{Entity}Connection` response
//! (nodes, edges, pageInfo, totalCount).

use std::collections::HashSet;

use async_graphql::dynamic::ResolverContext;
use bytes::BytesMut;
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tokio_postgres::types::{Format, IsNull, ToSql, Type};
use tracing::debug;

use crate::{
	config::Config,
	schema::{cursor::encode_cursor, inflector::to_camel_case},
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
	columns: &[String],
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

	let count_sql =
		format!(r#"SELECT COUNT(*) AS total FROM "{schema}"."{table}" AS t {where_clause}"#);

	// ── Execute ───────────────────────────────────────────────────────────────
	let client = pool.get().await?;

	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	// If the client only requested totalCount/pageInfo with no node fields,
	// skip fetching rows entirely and run only the count query.
	let needs_rows = has_node_selection(ctx);

	let (rows, total) = if !needs_rows {
		debug!(count_sql = %count_sql, "Executing count-only query");
		let row = client.query_one(&count_sql, &pg_refs).await?;
		(vec![], row.get::<_, i64>("total"))
	} else {
		// Build a selective column list from the GraphQL lookahead.
		let select_cols =
			build_select_cols(ctx, columns, &order_cols, &distinct_cols, is_historical);

		// For non-DISTINCT queries embed COUNT(*) OVER() to get the total in a
		// single round-trip. For DISTINCT queries the window function fires before
		// deduplication and would overcount, so use the separate count query.
		let (sql, use_window_count) = if distinct_cols.is_empty() {
			(
				format!(
					r#"SELECT {select_cols}, COUNT(*) OVER() AS __total_count FROM "{schema}"."{table}" AS t {where_clause} {order_clause} LIMIT {limit} OFFSET {}"#,
					pagination.offset
				),
				true,
			)
		} else {
			(
				format!(
					r#"SELECT {distinct_clause}{select_cols} FROM "{schema}"."{table}" AS t {where_clause} {order_clause} LIMIT {limit} OFFSET {}"#,
					pagination.offset
				),
				false,
			)
		};

		debug!(sql = %sql, "Executing connection query");
		let rows = client.query(&sql, &pg_refs).await?;
		let total = if use_window_count {
			rows.first()
				.and_then(|r| r.try_get::<_, i64>("__total_count").ok())
				.unwrap_or(0)
		} else {
			let total_row = client.query_one(&count_sql, &pg_refs).await?;
			total_row.get("total")
		};
		(rows, total)
	};

	// ── Build response ────────────────────────────────────────────────────────
	let mut nodes: Vec<Value> = vec![];
	let mut edges: Vec<Value> = vec![];

	for row in &rows {
		let mut node = row_to_json(row);
		// Strip the synthetic window-count column — it must not appear in GraphQL output.
		if let Value::Object(ref mut map) = node {
			map.remove("__total_count");
		}
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

/// Returns true if the query requests any node/edge row data.
/// False means the client only wants totalCount/pageInfo — no rows needed.
///
/// Uses `ctx.look_ahead().field("nodes").exists()` which is the correct
/// async-graphql 7.x API for presence checks in dynamic schema — `.exists()`
/// walks into the current field's selection set looking for the named sub-field.
/// Note: `.selection_fields()` returns the *matched field node itself* (name =
/// "nodes"), not its children — use `.exists()` for boolean checks and
/// `ctx.field().selection_set()` to iterate children.
fn has_node_selection(ctx: &ResolverContext<'_>) -> bool {
	ctx.look_ahead().field("nodes").exists() || ctx.look_ahead().field("edges").exists()
}

/// Build a selective `SELECT` column list from the GraphQL selection.
///
/// Uses `ctx.field().selection_set()` to iterate the direct children of the
/// current connection field (e.g. `nodes`, `edges`, `totalCount`), then drills
/// into `nodes { ... }` and `edges { node { ... } }` to collect the entity
/// field names actually requested by the client.
///
/// Always includes `id` (cursor generation), any orderBy/distinct columns, and
/// `_block_range` for historical tables.
fn build_select_cols(
	ctx: &ResolverContext<'_>,
	columns: &[String],
	order_cols: &[String],
	distinct_cols: &[String],
	is_historical: bool,
) -> String {
	let mut requested: HashSet<String> = HashSet::new();

	// Iterate the direct children of the connection field (nodes, edges, totalCount …)
	for top in ctx.field().selection_set() {
		match top.name() {
			"nodes" => {
				// nodes { id chain blockNumber … }
				for child in top.selection_set() {
					requested.insert(child.name().to_string());
				}
			},
			"edges" => {
				// edges { node { id chain … } }
				for node_field in top.selection_set().filter(|f| f.name() == "node") {
					for child in node_field.selection_set() {
						requested.insert(child.name().to_string());
					}
				}
			},
			_ => {},
		}
	}

	filter_columns_by_request(&requested, columns, order_cols, distinct_cols, is_historical)
}

/// Pure column-selection logic: given a set of requested camelCase GraphQL field
/// names, returns a comma-separated `t."col"` SELECT list.
///
/// Rules (in priority order):
/// 1. `id` is always included (cursor generation).
/// 2. Any column whose camelCase name (or raw name) appears in `requested`.
/// 3. All `order_cols` and `distinct_cols` (needed for ORDER BY / DISTINCT ON).
/// 4. `_block_range` for historical tables (needed in WHERE clause).
///
/// If `requested` is empty (shouldn't happen in practice) only `t."id"` is returned.
pub fn filter_columns_by_request(
	requested: &HashSet<String>,
	columns: &[String],
	order_cols: &[String],
	distinct_cols: &[String],
	is_historical: bool,
) -> String {
	if requested.is_empty() {
		return "t.\"id\"".to_string();
	}

	let mut selected: Vec<String> = Vec::new();
	let mut included: HashSet<String> = HashSet::new();

	let mut add = |col: &str| {
		if included.insert(col.to_string()) {
			selected.push(format!("t.\"{}\"", col));
		}
	};

	add("id");

	for col in columns {
		if col == "id" {
			continue;
		}
		if requested.contains(&to_camel_case(col)) || requested.contains(col.as_str()) {
			add(col);
		}
	}

	for col in order_cols.iter().chain(distinct_cols.iter()) {
		add(col);
	}

	if is_historical {
		add("_block_range");
	}

	selected.join(", ")
}

#[cfg(test)]
mod tests {
	use std::collections::HashSet;

	use super::filter_columns_by_request;

	fn set(items: &[&str]) -> HashSet<String> {
		items.iter().map(|s| s.to_string()).collect()
	}

	fn cols(items: &[&str]) -> Vec<String> {
		items.iter().map(|s| s.to_string()).collect()
	}

	#[test]
	fn always_includes_id() {
		let result =
			filter_columns_by_request(&set(&["id"]), &cols(&["id", "name"]), &[], &[], false);
		assert!(result.contains("t.\"id\""), "id should always be first: {result}");
	}

	#[test]
	fn selects_requested_camel_case_columns() {
		// "blockNumber" → column "block_number"
		let result = filter_columns_by_request(
			&set(&["blockNumber"]),
			&cols(&["id", "block_number", "amount"]),
			&[],
			&[],
			false,
		);
		assert!(result.contains("t.\"block_number\""), "block_number not in: {result}");
		assert!(!result.contains("t.\"amount\""), "amount should not be in: {result}");
	}

	#[test]
	fn does_not_duplicate_id() {
		// id is in both required-always and requested set
		let result = filter_columns_by_request(
			&set(&["id", "amount"]),
			&cols(&["id", "amount"]),
			&[],
			&[],
			false,
		);
		let id_count = result.matches("t.\"id\"").count();
		assert_eq!(id_count, 1, "id should appear exactly once: {result}");
	}

	#[test]
	fn includes_order_cols_even_if_not_requested() {
		let result = filter_columns_by_request(
			&set(&["id"]),
			&cols(&["id", "created_at", "amount"]),
			&cols(&["created_at"]),
			&[],
			false,
		);
		assert!(result.contains("t.\"created_at\""), "order col should be included: {result}");
		assert!(!result.contains("t.\"amount\""), "unrequested col should be absent: {result}");
	}

	#[test]
	fn includes_distinct_cols_even_if_not_requested() {
		let result = filter_columns_by_request(
			&set(&["id"]),
			&cols(&["id", "category", "value"]),
			&[],
			&cols(&["category"]),
			false,
		);
		assert!(result.contains("t.\"category\""), "distinct col should be included: {result}");
		assert!(!result.contains("t.\"value\""), "unrequested col should be absent: {result}");
	}

	#[test]
	fn historical_table_includes_block_range() {
		let result = filter_columns_by_request(
			&set(&["id"]),
			&cols(&["id", "amount"]),
			&[],
			&[],
			true, // is_historical
		);
		assert!(result.contains("t.\"_block_range\""), "_block_range missing: {result}");
	}

	#[test]
	fn non_historical_table_excludes_block_range() {
		let result =
			filter_columns_by_request(&set(&["id"]), &cols(&["id", "amount"]), &[], &[], false);
		assert!(!result.contains("_block_range"), "_block_range should be absent: {result}");
	}

	#[test]
	fn empty_requested_returns_id_only() {
		let result =
			filter_columns_by_request(&set(&[]), &cols(&["id", "name", "amount"]), &[], &[], false);
		assert_eq!(result, "t.\"id\"");
	}

	#[test]
	fn raw_snake_case_name_also_matches() {
		// If the client sends the raw snake_case name (not camelCase), it should still match.
		let result = filter_columns_by_request(
			&set(&["block_number"]),
			&cols(&["id", "block_number"]),
			&[],
			&[],
			false,
		);
		assert!(result.contains("t.\"block_number\""), "snake_case match failed: {result}");
	}
}

fn parse_orderby(val: Option<&async_graphql::Value>) -> Vec<String> {
	// async-graphql may not coerce a single enum value into a list for dynamic
	// schema arguments, so accept both List and bare Enum/String forms.
	let arr: Vec<async_graphql::Value> = match val {
		Some(async_graphql::Value::List(list)) => list.clone(),
		Some(v @ (async_graphql::Value::Enum(_) | async_graphql::Value::String(_))) =>
			vec![v.clone()],
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
	let arr: Vec<async_graphql::Value> = match val {
		Some(async_graphql::Value::List(list)) => list.clone(),
		Some(v @ (async_graphql::Value::Enum(_) | async_graphql::Value::String(_))) =>
			vec![v.clone()],
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

/// A PostgreSQL parameter that sends its value as a text-encoded string and
/// accepts any server type.  PostgreSQL will apply its text input function to
/// coerce the value to whatever the column / expression expects (INT4, NUMERIC,
/// BIGINT, etc.).  This avoids the OID mismatch that occurs when the driver
/// sends a binary-encoded i64 for a column the server typed as INT4.
#[derive(Debug)]
struct TextParam(String);

impl ToSql for TextParam {
	fn to_sql(
		&self,
		_ty: &Type,
		buf: &mut BytesMut,
	) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
		buf.extend_from_slice(self.0.as_bytes());
		Ok(IsNull::No)
	}

	fn accepts(_ty: &Type) -> bool {
		true
	}

	fn encode_format(&self, _ty: &Type) -> Format {
		Format::Text
	}

	tokio_postgres::types::to_sql_checked!();
}

pub fn json_to_pg_params(params: &[Value]) -> Vec<Box<dyn ToSql + Sync + Send>> {
	params
		.iter()
		.map(|v| -> Box<dyn ToSql + Sync + Send> {
			match v {
				Value::Null => Box::new(Option::<String>::None),
				Value::Bool(b) => Box::new(*b),
				// Send numbers as text so PostgreSQL can coerce to any column type
				// (INT4, NUMERIC, BIGINT, etc.) without OID mismatch errors.
				Value::Number(n) => Box::new(TextParam(n.to_string())),
				Value::String(s) => Box::new(s.clone()),
				// Arrays / objects are serialised to JSON text (used by `in` filter
				// which casts $N::jsonb on the SQL side).  TextParam accepts any
				// server type (including JSONB) and sends bytes in text format.
				Value::Array(_) | Value::Object(_) => Box::new(TextParam(v.to_string())),
			}
		})
		.collect()
}
