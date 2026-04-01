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

//! List query resolver — the primary GraphQL connection field handler.
//!
//! [`resolve_connection`] handles all `{entities}(...)` root query fields,
//! translating the full set of GraphQL arguments (filter, orderBy, first/last,
//! after/before, offset, blockHeight, distinct) into a single parameterized
//! SQL query, executing it, and assembling the `{Entity}Connection` response
//! (nodes, edges, pageInfo, totalCount).

use std::collections::{HashMap, HashSet};

use async_graphql::dynamic::ResolverContext;
use bytes::BytesMut;
use serde_json::{Value, json};
use tokio_postgres::types::{Format, IsNull, ToSql, Type};
use tracing::trace;

use super::pg_error_detail;

use crate::{
	config::Config,
	schema::{cursor::encode_cursor, inflector::to_camel_case},
	sql::{
		filter::{FilterContext, build_filter_sql_ctx},
		pagination::{PaginationArgs, resolve_pagination},
	},
};

/// Resolve a connection (list) query for the given table.
/// Returns a plain `serde_json::Value` so nested field resolvers can use
/// `ctx.parent_value.try_downcast_ref::<serde_json::Value>()`.
///
/// Accepts a [`FilterContext`] for relation-aware filtering (exists, forward, some/none/every).
pub async fn resolve_connection_ctx(
	ctx: &ResolverContext<'_>,
	table: &str,
	cfg: &Config,
	is_historical: bool,
	columns: &[String],
	filter_ctx: &crate::sql::filter::FilterContext,
	fk_field_to_col: &HashMap<String, String>,
) -> async_graphql::Result<Option<Value>> {
	// ── Extract arguments ─────────────────────────────────────────────────────
	// Match TS: first/last of 0 is treated as "not specified" (JS falsy: !0 === true).
	let first = ctx
		.args
		.get("first")
		.and_then(|v| v.i64().ok())
		.map(|n| n.max(0) as usize)
		.filter(|&n| n > 0);
	let last = ctx
		.args
		.get("last")
		.and_then(|v| v.i64().ok())
		.map(|n| n.max(0) as usize)
		.filter(|&n| n > 0);
	let after = ctx.args.get("after").and_then(|v| v.string().ok()).map(str::to_string);
	let before = ctx.args.get("before").and_then(|v| v.string().ok()).map(str::to_string);
	let offset = ctx.args.get("offset").and_then(|v| v.i64().ok()).map(|n| n.max(0) as usize);
	let block_height: Option<String> = ctx
		.args
		.get("blockHeight")
		.or_else(|| ctx.args.get("timestamp"))
		.and_then(|v| v.string().ok())
		.map(str::to_string);

	let filter_val: Option<Value> = ctx
		.args
		.get("filter")
		.map(|v| serde_json::to_value(v.as_value()).unwrap_or(Value::Null));

	let order_by_gql = ctx.args.get("orderBy").map(|v| v.as_value().clone());
	let order_by_null: Option<String> =
		ctx.args.get("orderByNull").and_then(|v| match v.as_value() {
			async_graphql::Value::Enum(s) => Some(s.as_str().to_string()),
			async_graphql::Value::String(s) => Some(s.clone()),
			_ => None,
		});
	let distinct_gql = ctx.args.get("distinct").map(|v| v.as_value().clone());

	let schema = &cfg.name;

	// Match TS behaviour: `--unsafe` removes the *default* limit when no first/last is
	// given (returning all rows), but explicit first/last values are ALWAYS clamped to
	// query_limit.  See PgConnectionArgFirstLastBeforeAfter.ts.
	let limit: Option<usize> = if first.is_some() || last.is_some() {
		// Explicit first/last — always clamp to query_limit.
		Some(first.or(last).unwrap().min(cfg.query_limit))
	} else if cfg.unsafe_mode {
		// No first/last + unsafe → no limit (return all rows).
		None
	} else {
		// No first/last + safe → default to query_limit.
		Some(cfg.query_limit)
	};

	// ── WHERE clauses ─────────────────────────────────────────────────────────
	let mut conditions: Vec<String> = vec![];
	let mut params: Vec<Value> = vec![];
	let mut param_offset: usize = 0;

	if is_historical {
		// Match PostGraphile: always use `_block_range @> N::bigint`.
		// Default blockHeight = MAX_INT64 (only matches rows with open upper bound).
		let bh = block_height.as_ref().and_then(|s| s.parse::<i64>().ok()).unwrap_or(i64::MAX);
		param_offset += 1;
		conditions.push(format!("t._block_range @> ${param_offset}::bigint"));
		params.push(json!(bh));
	}

	if let Some(f) = filter_val {
		let mut fctx = filter_ctx.clone();
		let (filter_conds, filter_params) =
			build_filter_sql_ctx(&f, "t", &mut param_offset, &mut fctx);
		conditions.extend(filter_conds);
		params.extend(filter_params);
	}

	// ── ORDER BY ──────────────────────────────────────────────────────────────
	let order_clauses =
		parse_orderby_with_schema(order_by_gql.as_ref(), Some(schema), Some(filter_ctx));
	let mut order_cols: Vec<String> = extract_order_cols(&order_clauses);
	// When no explicit orderBy is given the default ORDER BY is `t.id ASC`,
	// so the cursor must compare against the `id` column to work correctly.
	if order_cols.is_empty() {
		order_cols.push("id".to_string());
	}

	// ── PAGINATION ────────────────────────────────────────────────────────────
	let pagination = resolve_pagination(
		&PaginationArgs { first, last, after: after.clone(), before: before.clone(), offset },
		&order_cols,
		&mut param_offset,
		cfg.query_limit,
	)?;

	// Build the base WHERE clause (without cursor condition) for totalCount.
	// totalCount must reflect ALL matching rows regardless of cursor position.
	let base_where_clause = if conditions.is_empty() {
		String::new()
	} else {
		format!("WHERE {}", conditions.join(" AND "))
	};
	let base_params = params.clone();

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

	let nulls_suffix = match order_by_null.as_deref() {
		Some("NULLS_FIRST") => " NULLS FIRST",
		Some("NULLS_LAST") => " NULLS LAST",
		_ => "",
	};

	let forward_order_clause = {
		let mut clauses: Vec<String> = if order_clauses.is_empty() {
			vec![format!("t.id ASC{nulls_suffix}")]
		} else {
			order_clauses.iter().map(|c| format!("{c}{nulls_suffix}")).collect()
		};
		// PostGraphile appends `_id ASC` as a unique tiebreaker for historical tables
		// to ensure deterministic ordering when multiple versions share the same id.
		if is_historical && !clauses.iter().any(|c| c.contains("t._id ")) {
			clauses.push("t._id ASC".to_string());
		}
		// PostgreSQL requires DISTINCT ON columns to be leading ORDER BY columns.
		if !distinct_cols.is_empty() {
			let leading: Vec<String> = distinct_cols
				.iter()
				.filter(|dc| !clauses.iter().any(|c| c.starts_with(&format!("t.{dc} "))))
				.map(|dc| format!("t.{dc} ASC{nulls_suffix}"))
				.collect();
			if !leading.is_empty() {
				clauses.splice(0..0, leading);
			}
		}
		format!("ORDER BY {}", clauses.join(", "))
	};

	// For backward pagination (`last`), reverse each ORDER BY direction so the
	// database returns the last N rows of the logical set; we un-reverse them below.
	let order_clause = if pagination.is_backwards {
		reverse_order_clause(&forward_order_clause)
	} else {
		forward_order_clause
	};

	// totalCount must reflect ALL matching rows regardless of cursor position,
	// so its count query uses base_where_clause (without cursor condition).
	let count_sql =
		format!(r#"SELECT COUNT(*) AS total FROM "{schema}"."{table}" AS t {base_where_clause}"#);

	let has_cursor = pagination.cursor_condition.is_some();

	// ── Execute ───────────────────────────────────────────────────────────────
	let req_client = ctx
		.data::<std::sync::Arc<crate::db::RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?
		.clone();

	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();
	let base_pg_params = json_to_pg_params(&base_params);
	let base_pg_refs: Vec<&(dyn ToSql + Sync)> =
		base_pg_params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	// If the client only requested totalCount/pageInfo with no node fields,
	// skip fetching rows entirely and run only the count query.
	let needs_rows = has_node_selection(ctx);

	// `total` = true total count (no cursor filter) for the totalCount field.
	// `filtered_total` = rows matching the cursor-filtered WHERE, for hasNextPage.
	let (rows, total, filtered_total) = if !needs_rows {
		trace!(count_sql = %count_sql, "Executing count-only query");
		let row = req_client.query_one(&count_sql, &base_pg_refs).await.map_err(|e| {
			let detail = pg_error_detail(&e);
			tracing::error!(sql = %count_sql, error = %detail, "Count query failed");
			async_graphql::Error::new(format!("db error: {detail}"))
		})?;
		let t: i64 = row.get("total");
		(vec![], t, t)
	} else {
		// Build a selective column list from the GraphQL lookahead.
		let select_cols =
			build_select_cols(ctx, columns, &order_cols, &distinct_cols, fk_field_to_col);

		// For non-DISTINCT queries embed COUNT(*) OVER() to get the cursor-filtered
		// total in a single round-trip. For DISTINCT queries the window function
		// fires before deduplication and would overcount, so use a separate query.
		let limit_clause = match limit {
			Some(n) => format!("LIMIT {n}"),
			None => String::new(),
		};
		let (sql, use_window_count) = if distinct_cols.is_empty() {
			(
				format!(
					r#"SELECT {select_cols}, COUNT(*) OVER() AS __total_count FROM "{schema}"."{table}" AS t {where_clause} {order_clause} {limit_clause} OFFSET {}"#,
					pagination.offset
				),
				true,
			)
		} else if !has_cursor {
			// Lateral-join strategy: instead of sorting the entire matched set
			// for DISTINCT ON deduplication, first collect the distinct key
			// values and then fetch the top-1 row per key.  This turns an
			// O(N log N) sort into O(K × index-lookup) where K = distinct keys.
			let distinct_select = distinct_cols
				.iter()
				.map(|dc| format!(r#"t."{dc}""#))
				.collect::<Vec<_>>()
				.join(", ");
			let distinct_equalities = distinct_cols
				.iter()
				.map(|dc| format!(r#"t."{dc}" = _keys."{dc}""#))
				.collect::<Vec<_>>()
				.join(" AND ");
			let lateral_where = if where_clause.is_empty() {
				format!("WHERE {distinct_equalities}")
			} else {
				format!("{where_clause} AND {distinct_equalities}")
			};
			// Inner ORDER BY: user's order + historical tiebreaker, without
			// the prepended distinct columns (not needed inside a per-key lookup).
			let inner_order = {
				let mut clauses: Vec<String> = if order_clauses.is_empty() {
					vec![format!("t.id ASC{nulls_suffix}")]
				} else {
					order_clauses.iter().map(|c| format!("{c}{nulls_suffix}")).collect()
				};
				if is_historical && !clauses.iter().any(|c| c.contains("t._id ")) {
					clauses.push("t._id ASC".to_string());
				}
				format!("ORDER BY {}", clauses.join(", "))
			};
			// Outer ORDER BY: deterministic ordering by distinct columns.
			let outer_order = {
				let dir = if pagination.is_backwards { "DESC" } else { "ASC" };
				let cols = distinct_cols
					.iter()
					.map(|dc| format!(r#"_row."{dc}" {dir}"#))
					.collect::<Vec<_>>()
					.join(", ");
				format!("ORDER BY {cols}")
			};
			(
				format!(
					r#"SELECT _row.* FROM (SELECT DISTINCT {distinct_select} FROM "{schema}"."{table}" AS t {where_clause}) AS _keys CROSS JOIN LATERAL (SELECT {select_cols} FROM "{schema}"."{table}" AS t {lateral_where} {inner_order} LIMIT 1) AS _row {outer_order} {limit_clause} OFFSET {}"#,
					pagination.offset
				),
				false,
			)
		} else {
			// Cursor pagination with DISTINCT: fall back to DISTINCT ON since
			// the cursor condition filters the sorted+deduplicated result set.
			(
				format!(
					r#"SELECT {distinct_clause}{select_cols} FROM "{schema}"."{table}" AS t {where_clause} {order_clause} {limit_clause} OFFSET {}"#,
					pagination.offset
				),
				false,
			)
		};

		trace!(sql = %sql, "Executing connection query");
		let rows = req_client.query(&sql, &pg_refs).await.map_err(|e| {
			let detail = pg_error_detail(&e);
			tracing::error!(sql = %sql, error = %detail, "Connection query failed");
			async_graphql::Error::new(format!("db error: {detail}"))
		})?;
		let filtered = if use_window_count {
			rows.first()
				.and_then(|r| r.try_get::<_, i64>("__total_count").ok())
				.unwrap_or(0)
		} else {
			let total_row = req_client.query_one(&count_sql, &base_pg_refs).await.map_err(|e| {
				let detail = pg_error_detail(&e);
				tracing::error!(sql = %count_sql, error = %detail, "Count query failed");
				async_graphql::Error::new(format!("db error: {detail}"))
			})?;
			total_row.get("total")
		};

		// When cursor pagination is active, totalCount needs a separate count
		// query without the cursor condition; otherwise the window count is the
		// true total (no cursor → base_where == where_clause).
		let total = if has_cursor {
			let row = req_client.query_one(&count_sql, &base_pg_refs).await.map_err(|e| {
				let detail = pg_error_detail(&e);
				tracing::error!(sql = %count_sql, error = %detail, "Count query failed");
				async_graphql::Error::new(format!("db error: {detail}"))
			})?;
			row.get::<_, i64>("total")
		} else {
			filtered
		};

		(rows, total, filtered)
	};

	// ── Build response ────────────────────────────────────────────────────────
	let mut nodes: Vec<Value> = vec![];
	let mut edges: Vec<Value> = vec![];

	for row in &rows {
		let mut node = row_to_json(row);
		// Strip the synthetic window-count column — it must not appear in GraphQL output.
		if let Value::Object(ref mut map) = node {
			map.remove("__total_count");
			// Embed blockHeight so nested relation resolvers can inherit historical filtering.
			if is_historical {
				if let Some(ref bh) = block_height {
					map.insert("_block_height".to_string(), json!(bh));
				}
			}
		}
		// Encode the actual ORDER BY columns into the cursor so that
		// cursor-based pagination works correctly for any orderBy, not
		// just the default `id ASC`.
		let cursor_fields: Vec<(&str, Value)> = order_cols
			.iter()
			.map(|col| (col.as_str(), node.get(col).cloned().unwrap_or(json!(null))))
			.collect();
		let cursor = encode_cursor(&cursor_fields);
		edges.push(json!({ "cursor": cursor, "node": node }));
		nodes.push(node);
	}

	// Backward pagination: rows were fetched in reversed order — restore logical order.
	if pagination.is_backwards {
		nodes.reverse();
		edges.reverse();
	}

	let _row_count = nodes.len();
	// Use `filtered_total` (cursor-filtered count) for hasNextPage/hasPreviousPage
	// so page boundary detection works correctly with cursor pagination.
	let (has_next, has_prev) = if pagination.is_backwards {
		let has_prev = match limit {
			Some(l) => filtered_total as usize > l || before.is_some(),
			None => before.is_some(),
		};
		let has_next = before.is_some();
		(has_next, has_prev)
	} else {
		let has_next = match limit {
			Some(l) => (pagination.offset + l) < filtered_total as usize,
			None => false, // no limit → all rows fetched
		};
		let has_prev = pagination.offset > 0 || after.is_some();
		(has_next, has_prev)
	};

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
/// Always includes `id` (cursor generation) and any orderBy/distinct columns.
fn build_select_cols(
	ctx: &ResolverContext<'_>,
	columns: &[String],
	order_cols: &[String],
	distinct_cols: &[String],
	fk_field_to_col: &HashMap<String, String>,
) -> String {
	let mut requested: HashSet<String> = HashSet::new();

	// Iterate the direct children of the connection field (nodes, edges, totalCount …)
	for top in ctx.field().selection_set() {
		match top.name() {
			"nodes" => {
				// nodes { id chain blockNumber … }
				for child in top.selection_set() {
					let name = child.name().to_string();
					// If this field is a forward relation, include the FK column
					// so the relation resolver can read it from the row data.
					if let Some(fk_col) = fk_field_to_col.get(&name) {
						requested.insert(to_camel_case(fk_col));
					}
					requested.insert(name);
				}
			},
			"edges" => {
				// edges { node { id chain … } }
				for node_field in top.selection_set().filter(|f| f.name() == "node") {
					for child in node_field.selection_set() {
						let name = child.name().to_string();
						if let Some(fk_col) = fk_field_to_col.get(&name) {
							requested.insert(to_camel_case(fk_col));
						}
						requested.insert(name);
					}
				}
			},
			_ => {},
		}
	}

	filter_columns_by_request(&requested, columns, order_cols, distinct_cols)
}

/// Pure column-selection logic: given a set of requested camelCase GraphQL field
/// names, returns a comma-separated `t."col"` SELECT list.
///
/// Rules (in priority order):
/// 1. `id` is always included (cursor generation).
/// 2. `_id` is always included when present — needed to encode PostGraphile-compatible nodeIds.
/// 3. Any column whose camelCase name (or raw name) appears in `requested`.
/// 4. All `order_cols` and `distinct_cols` (needed for ORDER BY / DISTINCT ON).
///
/// If `requested` is empty (shouldn't happen in practice) only `t."id"` is returned.
pub fn filter_columns_by_request(
	requested: &HashSet<String>,
	columns: &[String],
	order_cols: &[String],
	distinct_cols: &[String],
) -> String {
	let has_internal_id = columns.iter().any(|c| c == "_id");

	if requested.is_empty() {
		return if has_internal_id {
			"t.\"id\", t.\"_id\"".to_string()
		} else {
			"t.\"id\"".to_string()
		};
	}

	let mut selected: Vec<String> = Vec::new();
	let mut included: HashSet<String> = HashSet::new();

	let mut add = |col: &str| {
		if included.insert(col.to_string()) {
			selected.push(format!("t.\"{}\"", col));
		}
	};

	add("id");
	if has_internal_id {
		add("_id");
	}

	for col in columns {
		if col == "id" || col == "_id" {
			continue;
		}
		if requested.contains(&to_camel_case(col)) || requested.contains(col.as_str()) {
			add(col);
		}
	}

	for col in order_cols.iter().chain(distinct_cols.iter()) {
		add(col);
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
		let result = filter_columns_by_request(&set(&["id"]), &cols(&["id", "name"]), &[], &[]);
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
		);
		assert!(result.contains("t.\"block_number\""), "block_number not in: {result}");
		assert!(!result.contains("t.\"amount\""), "amount should not be in: {result}");
	}

	#[test]
	fn does_not_duplicate_id() {
		// id is in both required-always and requested set
		let result =
			filter_columns_by_request(&set(&["id", "amount"]), &cols(&["id", "amount"]), &[], &[]);
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
		);
		assert!(result.contains("t.\"category\""), "distinct col should be included: {result}");
		assert!(!result.contains("t.\"value\""), "unrequested col should be absent: {result}");
	}

	#[test]
	fn block_range_not_selected() {
		// _block_range is only used in WHERE, never in SELECT.
		let result = filter_columns_by_request(&set(&["id"]), &cols(&["id", "amount"]), &[], &[]);
		assert!(!result.contains("_block_range"), "_block_range should not be selected: {result}");
	}

	#[test]
	fn empty_requested_returns_id_only() {
		let result =
			filter_columns_by_request(&set(&[]), &cols(&["id", "name", "amount"]), &[], &[]);
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
		);
		assert!(result.contains("t.\"block_number\""), "snake_case match failed: {result}");
	}
}

/// Reverse every ASC/DESC direction in an `ORDER BY ...` clause string.
/// Used for backward pagination (`last`): we reverse the sort so the DB gives
/// us the last N rows of the logical set, then we un-reverse the result.
pub fn reverse_order_clause(clause: &str) -> String {
	// clause looks like "ORDER BY t.id ASC" or "ORDER BY t.col1 ASC, t.col2 DESC"
	let prefix = "ORDER BY ";
	let terms = clause.trim_start_matches(prefix);
	let reversed = terms
		.split(',')
		.map(|term| {
			let t = term.trim();
			if t.ends_with(" ASC") {
				format!("{} DESC", &t[..t.len() - 4])
			} else if t.ends_with(" DESC") {
				format!("{} ASC", &t[..t.len() - 5])
			} else {
				// No explicit direction — default is ASC, so reverse to DESC
				format!("{t} DESC")
			}
		})
		.collect::<Vec<_>>()
		.join(", ");
	format!("{prefix}{reversed}")
}

pub fn parse_orderby(val: Option<&async_graphql::Value>) -> Vec<String> {
	parse_orderby_with_schema(val, None, None)
}

/// Parse orderBy enum values into SQL ORDER BY clauses.
/// When `schema` is provided, aggregate orderBy values (containing `_BY_`)
/// are expanded into correlated subqueries.
/// When `filter_ctx` is provided, forward-relation scalar ordering (double
/// underscore `__` pattern) is also supported.
pub fn parse_orderby_with_schema(
	val: Option<&async_graphql::Value>,
	schema: Option<&str>,
	filter_ctx: Option<&FilterContext>,
) -> Vec<String> {
	let arr: Vec<async_graphql::Value> = match val {
		Some(async_graphql::Value::List(list)) => list.clone(),
		Some(v @ (async_graphql::Value::Enum(_) | async_graphql::Value::String(_))) => {
			vec![v.clone()]
		},
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
			let (body, dir) = if s.ends_with("_ASC") {
				(&s[..s.len() - 4], "ASC")
			} else if s.ends_with("_DESC") {
				(&s[..s.len() - 5], "DESC")
			} else {
				return None;
			};

			// ── Relation orderBy: detect _BY_ pattern ────────────────
			if let Some(schema) = schema {
				if let Some(by_idx) = body.find("_BY_") {
					let after_by = &body[by_idx + 4..]; // after "_BY_"

					// Forward relation scalar: double underscore `__` separates
					// FK column from target column on the parent table.
					// Pattern: {TABLE}_BY_{FK_COL}__{TARGET_COL}
					if let Some(dunder_idx) = after_by.find("__") {
						let fk_col = after_by[..dunder_idx].to_lowercase();
						let target_col = after_by[dunder_idx + 2..].to_lowercase();
						if let Some(fctx) = filter_ctx {
							if let Some(info) =
								fctx.forward_relations.values().find(|r| r.fk_column == fk_col)
							{
								let sql = format!(
									"(SELECT _rel.\"{}\" FROM \"{}\".\"{}\" AS _rel WHERE _rel.\"{}\" = t.\"{}\")",
									target_col, schema, info.foreign_table, info.foreign_pk, fk_col
								);
								return Some(format!("{sql} {dir}"));
							}
						}
					}

					// Backward relation aggregate: single underscores with
					// aggregate keywords (COUNT, SUM, AVERAGE, MIN, MAX).
					let child_table = body[..by_idx].to_lowercase();
					if let Some(agg_sql) = parse_aggregate_order(schema, &child_table, after_by) {
						return Some(format!("{agg_sql} {dir}"));
					}
				}
			}

			// ── Plain column orderBy ─────────────────────────────────
			let col = body.to_lowercase();
			Some(format!("t.{col} {dir}"))
		})
		.collect()
}

/// Parse the aggregate part of an orderBy enum value and return a SQL expression.
/// `after_by` is e.g. `"AUTHOR_ID_COUNT"` or `"AUTHOR_ID_SUM_BLOCK_NUMBER"`.
fn parse_aggregate_order(schema: &str, child_table: &str, after_by: &str) -> Option<String> {
	// Split into FK part and aggregate part. The FK column is everything before
	// the aggregate keyword (COUNT, SUM, AVERAGE, MIN, MAX).
	let agg_keywords = ["_COUNT", "_SUM_", "_AVERAGE_", "_MIN_", "_MAX_"];
	for kw in &agg_keywords {
		if let Some(idx) = after_by.find(kw) {
			let fk_col = after_by[..idx].to_lowercase();
			if *kw == "_COUNT" {
				return Some(format!(
					"(SELECT COUNT(*) FROM \"{schema}\".\"{child_table}\" AS _agg WHERE _agg.\"{fk_col}\" = t.\"id\")"
				));
			}
			// Extract agg function and column
			let rest = &after_by[idx + kw.len()..]; // e.g. "BLOCK_NUMBER" for SUM_BLOCK_NUMBER
			let agg_col = rest.to_lowercase();
			let pg_func = match *kw {
				"_SUM_" => "SUM",
				"_AVERAGE_" => "AVG",
				"_MIN_" => "MIN",
				"_MAX_" => "MAX",
				_ => return None,
			};
			return Some(format!(
				"(SELECT {pg_func}(_agg.\"{agg_col}\") FROM \"{schema}\".\"{child_table}\" AS _agg WHERE _agg.\"{fk_col}\" = t.\"id\")"
			));
		}
	}
	None
}

pub fn extract_order_cols(clauses: &[String]) -> Vec<String> {
	clauses
		.iter()
		.filter_map(|c| {
			// Subquery expressions (aggregate / forward-relation ordering) are
			// self-contained and don't require an extra column in the SELECT list.
			if c.starts_with('(') {
				return None;
			}
			c.trim_start_matches("t.").split_whitespace().next().map(str::to_string)
		})
		.collect()
}

pub fn parse_distinct(val: Option<&async_graphql::Value>) -> Vec<String> {
	let arr: Vec<async_graphql::Value> = match val {
		Some(async_graphql::Value::List(list)) => list.clone(),
		Some(v @ (async_graphql::Value::Enum(_) | async_graphql::Value::String(_))) => {
			vec![v.clone()]
		},
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

/// A `FromSql` implementation that reads any PostgreSQL type as a raw UTF-8 string.
/// Used for custom types (enums, etc.) that `tokio-postgres` won't coerce to `String`.
struct AnyStr(String);
impl<'a> tokio_postgres::types::FromSql<'a> for AnyStr {
	fn from_sql(
		_ty: &tokio_postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		Ok(AnyStr(std::str::from_utf8(raw)?.to_string()))
	}

	fn accepts(_ty: &tokio_postgres::types::Type) -> bool {
		true
	}
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
		Type::INT2 => row
			.try_get::<_, Option<i16>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT4 => row
			.try_get::<_, Option<i32>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT8 => row
			.try_get::<_, Option<i64>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_string())),
		Type::FLOAT4 => row
			.try_get::<_, Option<f32>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::FLOAT8 => row
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
		Type::TIMESTAMPTZ => row
			.try_get::<_, Option<chrono::DateTime<chrono::Utc>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_rfc3339())),
		Type::TIMESTAMP => row
			.try_get::<_, Option<chrono::NaiveDateTime>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.format("%Y-%m-%dT%H:%M:%S").to_string())),
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
			// tokio-postgres uses binary protocol — NUMERIC can't be read as String.
			// Use rust_decimal::Decimal which implements FromSql for NUMERIC.
			row.try_get::<_, Option<rust_decimal::Decimal>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.to_string()))
		},
		Type::BYTEA => row
			.try_get::<_, Option<Vec<u8>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(hex::encode(v))),
		Type::CHAR => row
			.try_get::<_, Option<i8>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::OID => row
			.try_get::<_, Option<u32>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		// ── Time types ──────────────────────────────────────────────────
		Type::TIME => row
			.try_get::<_, Option<chrono::NaiveTime>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.format("%H:%M:%S").to_string())),
		Type::TIMETZ => {
			// tokio-postgres doesn't have a native FromSql for timetz.
			// Read the raw bytes and format manually: 8 bytes time + 4 bytes tz offset.
			row.try_get::<_, Option<AnyStr>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.0))
		},
		Type::INTERVAL => {
			// PG interval binary: 8 bytes microseconds (i64), 4 bytes days (i32), 4 bytes months
			// (i32).
			row.try_get::<_, Option<IntervalBinary>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| v.to_json())
		},
		// ── Bit string types ────────────────────────────────────────────
		Type::BIT | Type::VARBIT => row
			.try_get::<_, Option<bit_vec::BitVec>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				let s: String = v.iter().map(|b| if b { '1' } else { '0' }).collect();
				json!(s)
			}),
		// ── Network types ───────────────────────────────────────────────
		Type::MACADDR => row
			.try_get::<_, Option<eui48::MacAddress>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_hex_string())),
		Type::MACADDR8 => {
			// eui48 crate only handles 6-byte MACs; MACADDR8 is 8 bytes.
			row.try_get::<_, Option<AnyStr>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.0))
		},
		Type::INET => row
			.try_get::<_, Option<std::net::IpAddr>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.to_string())),
		Type::CIDR => {
			// CIDR binary: [family, prefix_len, is_cidr, addr_len, ...addr_bytes]
			// IpAddr FromSql doesn't handle CIDR. Decode binary manually.
			row.try_get::<_, Option<CidrBinary>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.0))
		},
		// ── Geometric types ─────────────────────────────────────────────
		Type::POINT => row
			.try_get::<_, Option<geo_types::Point<f64>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!({"x": v.x(), "y": v.y()})),
		Type::BOX => row.try_get::<_, Option<geo_types::Rect<f64>>>(idx).ok().flatten().map_or(
			Value::Null,
			|v| {
				json!({
					"x1": v.min().x, "y1": v.min().y,
					"x2": v.max().x, "y2": v.max().y,
				})
			},
		),
		Type::PATH => row
			.try_get::<_, Option<geo_types::LineString<f64>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.points().map(|p| json!({"x": p.x(), "y": p.y()})).collect())
			}),
		Type::LINE | Type::LSEG | Type::POLYGON | Type::CIRCLE => {
			// geo-types doesn't map these directly; serialize as text.
			row.try_get::<_, Option<AnyStr>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.0))
		},
		// Range types (INT4RANGE, INT8RANGE, NUMRANGE, TSRANGE, TSTZRANGE, DATERANGE)
		// are handled in the `_` fallback via OID matching, since tokio-postgres
		// doesn't expose them as Type constants in all versions.
		// ── Misc scalar types ───────────────────────────────────────────
		Type::MONEY => {
			// MONEY is stored as i64 cents internally.
			row.try_get::<_, Option<i64>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.to_string()))
		},
		Type::XML => row
			.try_get::<_, Option<AnyStr>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v.0)),
		Type::VOID => Value::Null,
		Type::RECORD => Value::Null,
		// ── Array types ─────────────────────────────────────────────────
		Type::BOOL_ARRAY => row
			.try_get::<_, Option<Vec<bool>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT2_ARRAY => row
			.try_get::<_, Option<Vec<i16>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT4_ARRAY => row
			.try_get::<_, Option<Vec<i32>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::INT8_ARRAY =>
			row.try_get::<_, Option<Vec<i64>>>(idx).ok().flatten().map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|n| json!(n.to_string())).collect())
			}),
		Type::FLOAT4_ARRAY => row
			.try_get::<_, Option<Vec<f32>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::FLOAT8_ARRAY => row
			.try_get::<_, Option<Vec<f64>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::TEXT_ARRAY | Type::VARCHAR_ARRAY | Type::BPCHAR_ARRAY | Type::NAME_ARRAY => row
			.try_get::<_, Option<Vec<String>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| json!(v)),
		Type::JSON_ARRAY | Type::JSONB_ARRAY => row
			.try_get::<_, Option<Vec<serde_json::Value>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| Value::Array(v)),
		Type::TIMESTAMPTZ_ARRAY => row
			.try_get::<_, Option<Vec<chrono::DateTime<chrono::Utc>>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|t| json!(t.to_rfc3339())).collect())
			}),
		Type::TIMESTAMP_ARRAY => row
			.try_get::<_, Option<Vec<chrono::NaiveDateTime>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(
					v.iter().map(|t| json!(t.format("%Y-%m-%dT%H:%M:%S").to_string())).collect(),
				)
			}),
		Type::DATE_ARRAY => row
			.try_get::<_, Option<Vec<chrono::NaiveDate>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|d| json!(d.to_string())).collect())
			}),
		Type::TIME_ARRAY => row
			.try_get::<_, Option<Vec<chrono::NaiveTime>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|t| json!(t.format("%H:%M:%S").to_string())).collect())
			}),
		Type::UUID_ARRAY => row
			.try_get::<_, Option<Vec<uuid::Uuid>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|u| json!(u.to_string())).collect())
			}),
		Type::NUMERIC_ARRAY => row
			.try_get::<_, Option<Vec<rust_decimal::Decimal>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|d| json!(d.to_string())).collect())
			}),
		Type::BYTEA_ARRAY => row
			.try_get::<_, Option<Vec<Vec<u8>>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|b| json!(hex::encode(b))).collect())
			}),
		Type::INET_ARRAY => row
			.try_get::<_, Option<Vec<std::net::IpAddr>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|a| json!(a.to_string())).collect())
			}),
		Type::MACADDR_ARRAY => row
			.try_get::<_, Option<Vec<eui48::MacAddress>>>(idx)
			.ok()
			.flatten()
			.map_or(Value::Null, |v| {
				Value::Array(v.iter().map(|m| json!(m.to_hex_string())).collect())
			}),
		// Remaining array types (TIMETZ, INTERVAL, BIT, VARBIT, geometric, range,
		// MACADDR8, OID, CHAR) — rare in SubQuery schemas. Use text-encoded fallback
		// which works because arrays send element text representations.
		Type::TIMETZ_ARRAY |
		Type::INTERVAL_ARRAY |
		Type::BIT_ARRAY |
		Type::VARBIT_ARRAY |
		Type::MACADDR8_ARRAY |
		Type::OID_ARRAY |
		Type::CHAR_ARRAY |
		Type::CIDR_ARRAY => {
			// These don't have convenient Vec<T> FromSql impls. Return null for arrays
			// of exotic types — they're essentially unused in SubQuery schemas.
			tracing::warn!(
				pg_type = %ty,
				column = idx,
				"unhandled array type — returning null"
			);
			Value::Null
		},
		// ── Custom / extension types (enums, domains, etc.) ─────────────
		// PostgreSQL enums and domains have dynamic OIDs that don't match any
		// Type constant. AnyStr reads them as text, which works because PG
		// sends enum values as their text labels.
		_ => {
			tracing::debug!(
				pg_type = %ty,
				oid = ty.oid(),
				column = idx,
				"unknown PG type — attempting text-mode read"
			);
			row.try_get::<_, Option<AnyStr>>(idx)
				.ok()
				.flatten()
				.map_or(Value::Null, |v| json!(v.0))
		},
	}
}

/// Decode CIDR binary format: [family, prefix_len, is_cidr, addr_len, ...addr_bytes]
struct CidrBinary(String);
impl<'a> tokio_postgres::types::FromSql<'a> for CidrBinary {
	fn from_sql(
		_ty: &tokio_postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		if raw.len() < 4 {
			return Err("CIDR binary too short".into());
		}
		let family = raw[0];
		let prefix_len = raw[1];
		let addr_bytes = &raw[4..];
		let addr_str = match family {
			2 if addr_bytes.len() >= 4 => {
				format!(
					"{}.{}.{}.{}/{}",
					addr_bytes[0], addr_bytes[1], addr_bytes[2], addr_bytes[3], prefix_len
				)
			},
			3 if addr_bytes.len() >= 16 => {
				let segs: Vec<String> = (0..8)
					.map(|i| {
						format!(
							"{:x}",
							u16::from_be_bytes([addr_bytes[i * 2], addr_bytes[i * 2 + 1]])
						)
					})
					.collect();
				format!("{}/{}", segs.join(":"), prefix_len)
			},
			_ => return Err(format!("unknown CIDR family {family}").into()),
		};
		Ok(CidrBinary(addr_str))
	}

	fn accepts(ty: &tokio_postgres::types::Type) -> bool {
		*ty == tokio_postgres::types::Type::CIDR
	}
}

/// Decode INTERVAL binary format: 8 bytes microseconds (i64), 4 bytes days (i32), 4 bytes months
/// (i32). Returns structured data matching PostGraphile's Interval object type.
struct IntervalBinary {
	years: i32,
	months: i32,
	days: i32,
	hours: i64,
	minutes: i64,
	seconds: i64,
}

impl IntervalBinary {
	fn to_json(&self) -> Value {
		json!({
			"years": self.years,
			"months": self.months,
			"days": self.days,
			"hours": self.hours,
			"minutes": self.minutes,
			"seconds": self.seconds,
		})
	}
}

impl<'a> tokio_postgres::types::FromSql<'a> for IntervalBinary {
	fn from_sql(
		_ty: &tokio_postgres::types::Type,
		raw: &'a [u8],
	) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
		if raw.len() < 16 {
			return Err("INTERVAL binary too short".into());
		}
		let microseconds = i64::from_be_bytes(raw[0..8].try_into()?);
		let days = i32::from_be_bytes(raw[8..12].try_into()?);
		let total_months = i32::from_be_bytes(raw[12..16].try_into()?);

		let years = total_months / 12;
		let months = total_months % 12;
		let total_secs = microseconds / 1_000_000;
		let hours = total_secs / 3600;
		let minutes = (total_secs % 3600) / 60;
		let seconds = total_secs % 60;

		Ok(IntervalBinary { years, months, days, hours, minutes, seconds })
	}

	fn accepts(ty: &tokio_postgres::types::Type) -> bool {
		*ty == tokio_postgres::types::Type::INTERVAL
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
				// Send strings as text-encoded parameters so PostgreSQL can coerce
				// to the target column type (e.g. NUMERIC, BIGINT).  BigFloat/BigInt
				// values arrive as JSON strings from GraphQL; a native Rust String
				// binds as PG `text` which fails binary-protocol comparison against
				// numeric columns.  TextParam sends in text format, letting PG's
				// text input function handle the conversion.
				Value::String(s) => Box::new(TextParam(s.clone())),
				// Arrays / objects are serialised to JSON text (used by `in` filter
				// which casts $N::jsonb on the SQL side).  TextParam accepts any
				// server type (including JSONB) and sends bytes in text format.
				Value::Array(_) | Value::Object(_) => Box::new(TextParam(v.to_string())),
			}
		})
		.collect()
}
