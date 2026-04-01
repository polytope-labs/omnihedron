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

//! Relation resolvers — forward and backward FK traversal.
//!
//! - [`resolve_forward_relation`] — resolves a field added to an entity type for each outgoing
//!   foreign key (FK column on this table → single record on the referenced table).
//! - [`resolve_backward_relation`] — resolves a field added to an entity type for each incoming
//!   foreign key (records in another table that reference this one), returned as a
//!   `{Child}Connection`.

use async_graphql::{dataloader::DataLoader, dynamic::ResolverContext};
use serde_json::{Value, json};
use tokio_postgres::types::ToSql;
use tracing::trace;

use crate::{
	config::Config,
	resolvers::{
		connection::{
			extract_order_cols, json_to_pg_params, parse_distinct, parse_orderby,
			reverse_order_clause, row_to_json,
		},
		dataloader::{RelationKey, RelationLoader},
	},
	sql::{
		filter::build_filter_sql,
		pagination::{PaginationArgs, resolve_pagination},
	},
};

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
	foreign_table: &str,
	fk_column: &str,
	foreign_is_historical: bool,
	foreign_columns: &[String],
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

	let loader = ctx
		.data::<DataLoader<RelationLoader>>()
		.map_err(|_| async_graphql::Error::new("Missing DataLoader in context"))?;

	let id_str = match &fk_val {
		Value::String(s) => s.clone(),
		v => v.to_string(),
	};

	// Collect requested columns from the GraphQL selection set, ensuring `id` is always included.
	let mut columns: Vec<String> = ctx
		.field()
		.selection_set()
		.map(|field| field.name().to_string())
		.filter(|name| foreign_columns.contains(name))
		.collect();
	if !columns.contains(&"id".to_string()) {
		columns.push("id".to_string());
	}
	// Include _id for historical ordering and _block_range for historical filtering
	if foreign_is_historical {
		for col in &["_id", "_block_range"] {
			let s = col.to_string();
			if !columns.contains(&s) {
				columns.push(s);
			}
		}
	}
	columns.sort();

	let key = RelationKey {
		id: id_str,
		schema,
		table: foreign_table.to_string(),
		columns,
		is_historical: foreign_is_historical,
		block_height: inherited_height,
	};

	let result = loader.load_one(key).await?;
	Ok(result)
}

/// Resolve a backward one-to-one relation (unique FK → single child record).
pub async fn resolve_backward_single(
	ctx: &ResolverContext<'_>,
	child_table: &str,
	fk_column: &str,
	child_is_historical: bool,
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
	let parent_id = match parent.get("id") {
		Some(v) if !v.is_null() => v.clone(),
		_ => return Ok(None),
	};

	let inherited_height: Option<String> =
		parent.get("_block_height").and_then(|v| v.as_str()).map(str::to_string);

	let schema = ctx
		.data::<String>()
		.map_err(|_| async_graphql::Error::new("Missing schema name in context"))?
		.clone();

	let id_str = match &parent_id {
		Value::String(s) => s.clone(),
		v => v.to_string(),
	};

	let mut params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(id_str)];
	let mut where_parts = vec![format!("t.\"{fk_column}\" = $1")];

	if child_is_historical {
		let bh: i64 = inherited_height.as_ref().and_then(|s| s.parse().ok()).unwrap_or(i64::MAX);
		params.push(Box::new(bh));
		where_parts.push(format!("t._block_range @> ${}::bigint", params.len()));
	}

	let where_clause = format!("WHERE {}", where_parts.join(" AND "));
	let sql = format!(
		r#"SELECT * FROM "{schema}"."{child_table}" AS t {where_clause} ORDER BY t._id ASC LIMIT 1"#
	);

	let req_client = ctx
		.data::<std::sync::Arc<crate::db::RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = req_client.query(&sql, &pg_refs).await.map_err(super::pg_to_gql_error)?;
	match rows.first() {
		Some(row) => {
			let mut node = row_to_json(row);
			if child_is_historical {
				if let (Value::Object(map), Some(bh)) = (&mut node, &inherited_height) {
					map.insert("_block_height".to_string(), serde_json::json!(bh));
				}
			}
			Ok(Some(node))
		},
		None => Ok(None),
	}
}

/// Resolve a backward relation (parent → child connection).
/// Returns a plain `serde_json::Value` for nested field resolution.
///
/// Supports the full set of connection args: `first`, `last`, `after`, `before`,
/// `offset`, `filter`, `orderBy`, `orderByNull`, `distinct`.
///
/// When `child_is_historical` is true the child rows are filtered by
/// `_block_range`, using the `_block_height` embedded in the parent entity JSON.
pub async fn resolve_backward_relation(
	ctx: &ResolverContext<'_>,
	child_table: &str,
	fk_column: &str,
	child_is_historical: bool,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
	let parent_id = match parent.get("id") {
		Some(v) if !v.is_null() => v.clone(),
		_ => {
			return Ok(Some(json!({
				"nodes": [],
				"edges": [],
				"pageInfo": { "hasNextPage": false, "hasPreviousPage": false },
				"totalCount": 0,
			})));
		},
	};

	let inherited_height: Option<String> =
		parent.get("_block_height").and_then(|v| v.as_str()).map(str::to_string);

	let schema = ctx
		.data::<String>()
		.map_err(|_| async_graphql::Error::new("Missing schema name in context"))?
		.clone();

	// ── Parse arguments ──────────────────────────────────────────────────────
	let first: Option<usize> =
		ctx.args.get("first").and_then(|v| v.i64().ok()).map(|n| n.max(0) as usize);
	let last: Option<usize> =
		ctx.args.get("last").and_then(|v| v.i64().ok()).map(|n| n.max(0) as usize);
	let after: Option<String> =
		ctx.args.get("after").and_then(|v| v.string().ok()).map(str::to_string);
	let before: Option<String> =
		ctx.args.get("before").and_then(|v| v.string().ok()).map(str::to_string);
	let offset: Option<usize> =
		ctx.args.get("offset").and_then(|v| v.i64().ok()).map(|n| n.max(0) as usize);

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

	let requested = first.or(last).unwrap_or(cfg.query_limit);
	let limit = if cfg.unsafe_mode { requested } else { requested.min(cfg.query_limit) };

	// ── WHERE clauses ────────────────────────────────────────────────────────
	let mut conditions: Vec<String> = vec![];
	let mut params: Vec<Value> = vec![];
	let mut param_offset: usize = 0;

	let id_str = match &parent_id {
		Value::String(s) => s.clone(),
		v => v.to_string(),
	};
	param_offset += 1;
	conditions.push(format!("t.\"{fk_column}\" = ${param_offset}"));
	params.push(json!(id_str));

	if child_is_historical {
		let bh: i64 = inherited_height.as_ref().and_then(|s| s.parse().ok()).unwrap_or(i64::MAX);
		param_offset += 1;
		conditions.push(format!("t._block_range @> ${param_offset}::bigint"));
		params.push(json!(bh));
	}

	if let Some(f) = filter_val {
		let (filter_conds, filter_params) = build_filter_sql(&f, "t", &mut param_offset);
		conditions.extend(filter_conds);
		params.extend(filter_params);
	}

	// ── ORDER BY ─────────────────────────────────────────────────────────────
	let order_clauses = parse_orderby(order_by_gql.as_ref());
	let order_cols: Vec<String> = extract_order_cols(&order_clauses);

	// ── PAGINATION ───────────────────────────────────────────────────────────
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

	// ── DISTINCT ON ──────────────────────────────────────────────────────────
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

	let forward_order_clause = if order_clauses.is_empty() {
		format!("ORDER BY t.id ASC{nulls_suffix}")
	} else {
		let clauses_with_nulls: Vec<String> =
			order_clauses.iter().map(|c| format!("{c}{nulls_suffix}")).collect();
		format!("ORDER BY {}", clauses_with_nulls.join(", "))
	};

	let order_clause = if pagination.is_backwards {
		reverse_order_clause(&forward_order_clause)
	} else {
		forward_order_clause
	};

	let use_window_count = distinct_cols.is_empty();

	let count_col = if use_window_count { ", COUNT(*) OVER() AS __total_count" } else { "" };
	let sql = format!(
		r#"SELECT {distinct_clause}t.*{count_col} FROM "{schema}"."{child_table}" AS t {where_clause} {order_clause} LIMIT {limit} OFFSET {}"#,
		pagination.offset
	);

	trace!(sql = %sql, "Executing backward relation query");

	let req_client = ctx
		.data::<std::sync::Arc<crate::db::RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;
	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();

	let rows = req_client.query(&sql, &pg_refs).await.map_err(super::pg_to_gql_error)?;

	let total: i64 = if use_window_count {
		// Extract total from the window function in the first row
		rows.first()
			.and_then(|r| r.try_get::<_, i64>("__total_count").ok())
			.unwrap_or(0)
	} else {
		// DISTINCT queries need a separate count query (window fires before dedup)
		let count_sql = format!(
			r#"SELECT COUNT(*) AS total FROM "{schema}"."{child_table}" AS t {where_clause}"#
		);
		let count_row = req_client
			.query_one(&count_sql, &pg_refs)
			.await
			.map_err(super::pg_to_gql_error)?;
		count_row.get("total")
	};

	let mut nodes = vec![];
	let mut edges = vec![];
	for row in &rows {
		let mut node = row_to_json(row);
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

	// Backward pagination: un-reverse the rows.
	if pagination.is_backwards {
		nodes.reverse();
		edges.reverse();
	}

	let has_next = if pagination.is_backwards {
		before.is_some()
	} else {
		(pagination.offset + limit) < total as usize
	};
	let has_prev = if pagination.is_backwards {
		total as usize > limit || before.is_some()
	} else {
		pagination.offset > 0 || after.is_some()
	};

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

/// Resolve a many-to-many relation via a junction table.
///
/// For entity A, junction table J has FK1→A and FK2→B. This resolver
/// returns a `{B}Connection` by JOINing through J.
pub async fn resolve_many_to_many(
	ctx: &ResolverContext<'_>,
	junction_table: &str,
	fk_to_source: &str, // FK column in junction pointing to parent
	fk_to_target: &str, // FK column in junction pointing to target
	target_table: &str,
	cfg: &Config,
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;
	let parent_id = match parent.get("id") {
		Some(v) if !v.is_null() => v.clone(),
		_ => {
			return Ok(Some(json!({
				"nodes": [],
				"edges": [],
				"pageInfo": { "hasNextPage": false, "hasPreviousPage": false },
				"totalCount": 0,
			})));
		},
	};

	let schema = ctx
		.data::<String>()
		.map_err(|_| async_graphql::Error::new("Missing schema name in context"))?
		.clone();

	let first = ctx
		.args
		.get("first")
		.and_then(|v| v.i64().ok())
		.map(|n| n.max(0))
		.unwrap_or(100) as usize;
	let offset =
		ctx.args.get("offset").and_then(|v| v.i64().ok()).map(|n| n.max(0)).unwrap_or(0) as usize;

	let id_str = match &parent_id {
		Value::String(s) => s.clone(),
		v => v.to_string(),
	};

	let sql = format!(
		r#"SELECT b.*, COUNT(*) OVER() AS __total_count FROM "{schema}"."{target_table}" AS b
		   JOIN "{schema}"."{junction_table}" AS j ON j."{fk_to_target}" = b."id"
		   WHERE j."{fk_to_source}" = $1
		   ORDER BY b.id ASC LIMIT {first} OFFSET {offset}"#
	);

	trace!(sql = %sql, "Executing many-to-many relation query");

	let req_client = ctx
		.data::<std::sync::Arc<crate::db::RequestClient>>()
		.map_err(|_| async_graphql::Error::new("Missing RequestClient in context"))?;
	let params: Vec<&(dyn ToSql + Sync)> = vec![&id_str];

	let rows = req_client.query(&sql, &params).await.map_err(super::pg_to_gql_error)?;
	let total: i64 = rows
		.first()
		.and_then(|r| r.try_get::<_, i64>("__total_count").ok())
		.unwrap_or(0);

	let mut nodes = vec![];
	let mut edges = vec![];
	for row in &rows {
		let node = row_to_json(row);
		let cursor = crate::schema::cursor::encode_cursor(&[(
			"id",
			node.get("id").cloned().unwrap_or(json!(null)),
		)]);
		edges.push(json!({ "cursor": cursor, "node": node.clone() }));
		nodes.push(node);
	}

	let _ = cfg; // reserved for future use (query_limit, unsafe_mode)
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
