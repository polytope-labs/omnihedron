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

//! Aggregate resolver — count, sum, min, max, avg, stddev, variance.
//!
//! [`resolve_aggregates`] executes a single `SELECT COUNT(*), SUM(col), ...`
//! query against the current connection's filter context and returns the
//! results as a `serde_json::Value` that the `{Entity}Aggregates` schema type
//! can resolve field-by-field.
//!
//! Serialisation rules (matching PostGraphile):
//!   - count / distinctCount  → BigInt string
//!   - sum / average / stddev / variance  → BigFloat string (CAST … AS TEXT via numeric)
//!   - min / max on Int/Float columns  → native JSON number
//!   - min / max on BigInt/BigFloat columns → BigFloat string

use async_graphql::{self, dynamic::ResolverContext};
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tracing::trace;

use crate::resolvers::connection::{json_to_pg_params, row_to_json};

fn is_native_number(gql_type: &str) -> bool {
	matches!(gql_type, "Int" | "Float")
}

/// Resolve the aggregates object for a connection query.
///
/// `numeric_cols` is `&[(snake_case_name, graphql_type)]` captured from introspection.
/// The graphql_type determines whether min/max results are returned as native
/// numbers (Int/Float) or BigFloat strings.
pub async fn resolve_aggregates(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	numeric_cols: &[(String, String)],
	all_cols: &[String],
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;

	let agg_ctx = match parent.get("_agg_ctx") {
		Some(v) => v,
		None => return Ok(None),
	};

	let schema = agg_ctx.get("schema").and_then(|v| v.as_str()).unwrap_or("");
	let table = agg_ctx.get("table").and_then(|v| v.as_str()).unwrap_or("");
	let where_clause = agg_ctx.get("where_clause").and_then(|v| v.as_str()).unwrap_or("");
	let params: Vec<Value> =
		agg_ctx.get("params").and_then(|v| v.as_array()).cloned().unwrap_or_default();

	// ── Inspect selection set to determine which aggregates are needed ────
	let requested: std::collections::HashSet<String> =
		ctx.field().selection_set().map(|f| f.name().to_string()).collect();

	let wants_count = requested.contains("count");
	let wants_distinct = requested.contains("distinctCount");
	let wants_sum = requested.contains("sum");
	let wants_min = requested.contains("min");
	let wants_max = requested.contains("max");
	let wants_avg = requested.contains("average");
	let wants_stddev_samp = requested.contains("stddevSample");
	let wants_stddev_pop = requested.contains("stddevPopulation");
	let wants_var_samp = requested.contains("varianceSample");
	let wants_var_pop = requested.contains("variancePopulation");

	let mut select_parts: Vec<String> = vec![];

	if wants_count {
		select_parts.push("COUNT(*) AS \"_count\"".to_string());
	}

	if wants_distinct {
		for col in all_cols {
			select_parts.push(format!("COUNT(DISTINCT t.\"{col}\") AS \"_dc_{col}\""));
		}
	}

	for (col, gql_type) in numeric_cols {
		let native = is_native_number(gql_type);

		if wants_sum {
			select_parts.push(format!("CAST(SUM(t.\"{col}\") AS TEXT) AS \"_sum_{col}\""));
		}
		if wants_avg {
			select_parts.push(format!("CAST(AVG(t.\"{col}\") AS TEXT) AS \"_avg_{col}\""));
		}
		if wants_stddev_samp {
			select_parts
				.push(format!("CAST(STDDEV_SAMP(t.\"{col}\") AS TEXT) AS \"_stddev_samp_{col}\""));
		}
		if wants_stddev_pop {
			select_parts
				.push(format!("CAST(STDDEV_POP(t.\"{col}\") AS TEXT) AS \"_stddev_pop_{col}\""));
		}
		if wants_var_samp {
			select_parts.push(format!(
				"CAST(VAR_SAMP(t.\"{col}\")::numeric AS TEXT) AS \"_var_samp_{col}\""
			));
		}
		if wants_var_pop {
			select_parts
				.push(format!("CAST(VAR_POP(t.\"{col}\")::numeric AS TEXT) AS \"_var_pop_{col}\""));
		}
		if wants_min {
			if native {
				select_parts.push(format!("MIN(t.\"{col}\") AS \"_min_{col}\""));
			} else {
				select_parts.push(format!("CAST(MIN(t.\"{col}\") AS TEXT) AS \"_min_{col}\""));
			}
		}
		if wants_max {
			if native {
				select_parts.push(format!("MAX(t.\"{col}\") AS \"_max_{col}\""));
			} else {
				select_parts.push(format!("CAST(MAX(t.\"{col}\") AS TEXT) AS \"_max_{col}\""));
			}
		}
	}

	// Fallback: if nothing specific requested, at least return count
	if select_parts.is_empty() {
		select_parts.push("COUNT(*) AS \"_count\"".to_string());
	}

	let select_clause = select_parts.join(", ");
	let sql = format!(r#"SELECT {select_clause} FROM "{schema}"."{table}" AS t {where_clause}"#);

	trace!(sql = %sql, "Executing aggregates query");

	let client = pool.get().await?;
	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as _).collect();

	let row = client.query_one(&sql, &pg_refs).await?;

	let count: i64 = row.try_get("_count").unwrap_or(0);

	let mut sum_map = serde_json::Map::new();
	let mut min_map = serde_json::Map::new();
	let mut max_map = serde_json::Map::new();
	let mut avg_map = serde_json::Map::new();
	let mut stddev_samp_map = serde_json::Map::new();
	let mut stddev_pop_map = serde_json::Map::new();
	let mut var_samp_map = serde_json::Map::new();
	let mut var_pop_map = serde_json::Map::new();
	let mut distinct_map = serde_json::Map::new();

	// distinctCount for ALL columns (always BigInt string)
	for col in all_cols {
		let field_name = crate::schema::inflector::to_camel_case(col);
		let dc_key = format!("_dc_{col}");
		let distinct_val: i64 = row.try_get(dc_key.as_str()).unwrap_or(0);
		distinct_map.insert(field_name, json!(distinct_val.to_string()));
	}

	for (col, gql_type) in numeric_cols {
		let field_name = crate::schema::inflector::to_camel_case(col);
		let native = is_native_number(gql_type);

		macro_rules! get_str {
			($prefix:expr) => {{
				let key = format!("{}_{}", $prefix, col);
				row.try_get::<_, Option<String>>(key.as_str())
					.ok()
					.flatten()
					.map(|s| Value::String(s))
					.unwrap_or(Value::Null)
			}};
		}

		sum_map.insert(field_name.clone(), get_str!("_sum"));
		avg_map.insert(field_name.clone(), get_str!("_avg"));
		stddev_samp_map.insert(field_name.clone(), get_str!("_stddev_samp"));
		stddev_pop_map.insert(field_name.clone(), get_str!("_stddev_pop"));
		var_samp_map.insert(field_name.clone(), get_str!("_var_samp"));
		var_pop_map.insert(field_name.clone(), get_str!("_var_pop"));

		// min / max: native number for Int/Float, string for BigFloat/BigInt.
		// INT4 columns must use i32 (not i64) — tokio-postgres rejects type mismatches.
		let (min_val, max_val) = if native {
			let min_key = format!("_min_{col}");
			let max_key = format!("_max_{col}");
			let try_native = |key: &str| -> Value {
				// Try i32 first (INT2/INT4), then i64 (INT8), then f64 (FLOAT)
				if let Ok(Some(n)) = row.try_get::<_, Option<i32>>(key) {
					return json!(n);
				}
				if let Ok(Some(n)) = row.try_get::<_, Option<i64>>(key) {
					return json!(n);
				}
				if let Ok(Some(f)) = row.try_get::<_, Option<f64>>(key) {
					return json!(f);
				}
				Value::Null
			};
			(try_native(&min_key), try_native(&max_key))
		} else {
			(get_str!("_min"), get_str!("_max"))
		};
		min_map.insert(field_name.clone(), min_val);
		max_map.insert(field_name.clone(), max_val);
	}

	Ok(Some(json!({
		"count": count.to_string(),
		"sum": Value::Object(sum_map),
		"min": Value::Object(min_map),
		"max": Value::Object(max_map),
		"average": Value::Object(avg_map),
		"stddevSample": Value::Object(stddev_samp_map),
		"stddevPopulation": Value::Object(stddev_pop_map),
		"varianceSample": Value::Object(var_samp_map),
		"variancePopulation": Value::Object(var_pop_map),
		"distinctCount": Value::Object(distinct_map),
	})))
}

/// A parsed `groupBy` enum item, distinguishing plain column references from
/// time-truncated ones (`{COL}_TRUNCATED_TO_HOUR` / `{COL}_TRUNCATED_TO_DAY`).
struct GroupByParsed {
	/// The actual table column name (snake_case).
	col: String,
	/// SQL expression for the SELECT list (includes alias when truncated).
	select_expr: String,
	/// SQL expression for the GROUP BY / ORDER BY clause.
	group_expr: String,
}

impl GroupByParsed {
	fn from_raw(raw: &str) -> Self {
		if let Some(col) = raw.strip_suffix("_truncated_to_hour") {
			Self {
				col: col.to_string(),
				select_expr: format!("date_trunc('hour', t.\"{col}\") AS \"{col}\""),
				group_expr: format!("date_trunc('hour', t.\"{col}\")"),
			}
		} else if let Some(col) = raw.strip_suffix("_truncated_to_day") {
			Self {
				col: col.to_string(),
				select_expr: format!("date_trunc('day', t.\"{col}\") AS \"{col}\""),
				group_expr: format!("date_trunc('day', t.\"{col}\")"),
			}
		} else {
			Self {
				col: raw.to_string(),
				select_expr: format!("t.\"{raw}\""),
				group_expr: format!("t.\"{raw}\""),
			}
		}
	}
}

/// Resolve the `groupedAggregates` field on a connection type.
///
/// Executes a GROUP BY query using the `groupBy` argument and returns one
/// aggregate object per group (or a single aggregate object when `groupBy` is empty).
///
/// Supports time-truncation groupBy variants: `{COL}_TRUNCATED_TO_HOUR` and
/// `{COL}_TRUNCATED_TO_DAY` emit `date_trunc('hour'/'day', t."col")` expressions.
pub async fn resolve_grouped_aggregates(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	numeric_cols: &[(String, String)],
	all_cols: &[String],
) -> async_graphql::Result<Option<Value>> {
	let parent = ctx.parent_value.try_downcast_ref::<Value>()?;

	let agg_ctx = match parent.get("_agg_ctx") {
		Some(v) => v,
		None => return Ok(None),
	};

	let schema = agg_ctx.get("schema").and_then(|v| v.as_str()).unwrap_or("");
	let table = agg_ctx.get("table").and_then(|v| v.as_str()).unwrap_or("");
	let where_clause = agg_ctx.get("where_clause").and_then(|v| v.as_str()).unwrap_or("");
	let params: Vec<Value> =
		agg_ctx.get("params").and_then(|v| v.as_array()).cloned().unwrap_or_default();

	// Parse `groupBy` argument: list of enum values → GroupByParsed items.
	let parsed_items: Vec<GroupByParsed> =
		match ctx.args.get("groupBy").map(|v| v.as_value().clone()) {
			Some(async_graphql::Value::List(items)) => items
				.iter()
				.filter_map(|item| match item {
					async_graphql::Value::Enum(name) =>
						Some(GroupByParsed::from_raw(&name.as_str().to_lowercase())),
					async_graphql::Value::String(s) =>
						Some(GroupByParsed::from_raw(&s.to_lowercase())),
					_ => None,
				})
				.collect(),
			_ => vec![],
		};

	// Build SELECT list.
	let mut select_parts: Vec<String> = Vec::new();

	// Grouped columns first (using the pre-computed SQL expressions).
	for item in &parsed_items {
		select_parts.push(item.select_expr.clone());
	}

	// ── Inspect selection set to only compute requested aggregates ────────
	let requested: std::collections::HashSet<String> =
		ctx.field().selection_set().map(|f| f.name().to_string()).collect();

	let wants_distinct = requested.contains("distinctCount");
	let wants_sum = requested.contains("sum");
	let wants_min = requested.contains("min");
	let wants_max = requested.contains("max");
	let wants_avg = requested.contains("average");
	let wants_stddev_samp = requested.contains("stddevSample");
	let wants_stddev_pop = requested.contains("stddevPopulation");
	let wants_var_samp = requested.contains("varianceSample");
	let wants_var_pop = requested.contains("variancePopulation");

	select_parts.push("COUNT(*) AS \"_count\"".to_string());

	if wants_distinct {
		for col in all_cols {
			select_parts.push(format!("COUNT(DISTINCT t.\"{col}\") AS \"_dc_{col}\""));
		}
	}

	for (col, gql_type) in numeric_cols {
		let native = is_native_number(gql_type);

		if wants_sum {
			select_parts.push(format!("CAST(SUM(t.\"{col}\") AS TEXT) AS \"_sum_{col}\""));
		}
		if wants_avg {
			select_parts.push(format!("CAST(AVG(t.\"{col}\") AS TEXT) AS \"_avg_{col}\""));
		}
		if wants_stddev_samp {
			select_parts
				.push(format!("CAST(STDDEV_SAMP(t.\"{col}\") AS TEXT) AS \"_stddev_samp_{col}\""));
		}
		if wants_stddev_pop {
			select_parts
				.push(format!("CAST(STDDEV_POP(t.\"{col}\") AS TEXT) AS \"_stddev_pop_{col}\""));
		}
		if wants_var_samp {
			select_parts.push(format!(
				"CAST(VAR_SAMP(t.\"{col}\")::numeric AS TEXT) AS \"_var_samp_{col}\""
			));
		}
		if wants_var_pop {
			select_parts
				.push(format!("CAST(VAR_POP(t.\"{col}\")::numeric AS TEXT) AS \"_var_pop_{col}\""));
		}
		if wants_min {
			if native {
				select_parts.push(format!("MIN(t.\"{col}\") AS \"_min_{col}\""));
			} else {
				select_parts.push(format!("CAST(MIN(t.\"{col}\") AS TEXT) AS \"_min_{col}\""));
			}
		}
		if wants_max {
			if native {
				select_parts.push(format!("MAX(t.\"{col}\") AS \"_max_{col}\""));
			} else {
				select_parts.push(format!("CAST(MAX(t.\"{col}\") AS TEXT) AS \"_max_{col}\""));
			}
		}
	}

	let select_clause = select_parts.join(", ");

	// GROUP BY / ORDER BY clauses (omitted when groupBy is empty).
	let group_order_clause = if parsed_items.is_empty() {
		String::new()
	} else {
		let grp_exprs: Vec<String> = parsed_items.iter().map(|i| i.group_expr.clone()).collect();
		let joined = grp_exprs.join(", ");
		format!("GROUP BY {joined} ORDER BY {joined}")
	};

	let sql = format!(
		r#"SELECT {select_clause} FROM "{schema}"."{table}" AS t {where_clause} {group_order_clause}"#
	);

	trace!(sql = %sql, "Executing groupedAggregates query");

	let client = pool.get().await?;
	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as _).collect();

	let rows = client.query(&sql, &pg_refs).await?;

	let mut groups: Vec<Value> = Vec::with_capacity(rows.len());

	for row in &rows {
		// Build `keys` as an array of string values (matches PostGraphile's `[String!]` type).
		let keys: Value = if parsed_items.is_empty() {
			Value::Null
		} else {
			let raw = row_to_json(row);
			let arr: Vec<Value> = parsed_items
				.iter()
				.map(|item| {
					let v = raw.get(item.col.as_str()).cloned().unwrap_or(Value::Null);
					match v {
						Value::String(s) => Value::String(s),
						Value::Number(n) => Value::String(n.to_string()),
						Value::Bool(b) => Value::String(b.to_string()),
						Value::Null => Value::String(String::new()),
						other => Value::String(other.to_string()),
					}
				})
				.collect();
			Value::Array(arr)
		};

		// distinctCount
		let mut distinct_map = serde_json::Map::new();
		for col in all_cols {
			let field_name = crate::schema::inflector::to_camel_case(col);
			let dc_key = format!("_dc_{col}");
			let distinct_val: i64 = row.try_get(dc_key.as_str()).unwrap_or(0);
			distinct_map.insert(field_name, json!(distinct_val.to_string()));
		}

		// Numeric aggregates.
		let mut sum_map = serde_json::Map::new();
		let mut min_map = serde_json::Map::new();
		let mut max_map = serde_json::Map::new();
		let mut avg_map = serde_json::Map::new();
		let mut stddev_samp_map = serde_json::Map::new();
		let mut stddev_pop_map = serde_json::Map::new();
		let mut var_samp_map = serde_json::Map::new();
		let mut var_pop_map = serde_json::Map::new();

		for (col, gql_type) in numeric_cols {
			let field_name = crate::schema::inflector::to_camel_case(col);
			let native = is_native_number(gql_type);

			macro_rules! get_str {
				($prefix:expr) => {{
					let key = format!("{}_{}", $prefix, col);
					row.try_get::<_, Option<String>>(key.as_str())
						.ok()
						.flatten()
						.map(|s| Value::String(s))
						.unwrap_or(Value::Null)
				}};
			}

			sum_map.insert(field_name.clone(), get_str!("_sum"));
			avg_map.insert(field_name.clone(), get_str!("_avg"));
			stddev_samp_map.insert(field_name.clone(), get_str!("_stddev_samp"));
			stddev_pop_map.insert(field_name.clone(), get_str!("_stddev_pop"));
			var_samp_map.insert(field_name.clone(), get_str!("_var_samp"));
			var_pop_map.insert(field_name.clone(), get_str!("_var_pop"));

			let (min_val, max_val) = if native {
				let min_key = format!("_min_{col}");
				let max_key = format!("_max_{col}");
				let try_native = |key: &str| -> Value {
					if let Ok(Some(n)) = row.try_get::<_, Option<i32>>(key) {
						return json!(n);
					}
					if let Ok(Some(n)) = row.try_get::<_, Option<i64>>(key) {
						return json!(n);
					}
					if let Ok(Some(f)) = row.try_get::<_, Option<f64>>(key) {
						return json!(f);
					}
					Value::Null
				};
				(try_native(&min_key), try_native(&max_key))
			} else {
				(get_str!("_min"), get_str!("_max"))
			};
			min_map.insert(field_name.clone(), min_val);
			max_map.insert(field_name.clone(), max_val);
		}

		groups.push(json!({
			"keys": keys,
			"sum": Value::Object(sum_map),
			"min": Value::Object(min_map),
			"max": Value::Object(max_map),
			"average": Value::Object(avg_map),
			"stddevSample": Value::Object(stddev_samp_map),
			"stddevPopulation": Value::Object(stddev_pop_map),
			"varianceSample": Value::Object(var_samp_map),
			"variancePopulation": Value::Object(var_pop_map),
			"distinctCount": Value::Object(distinct_map),
		}));
	}

	Ok(Some(Value::Array(groups)))
}
