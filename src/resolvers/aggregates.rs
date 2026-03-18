//! Aggregate resolver — count, sum, min, max, avg, stddev, variance.
//!
//! [`resolve_aggregates`] executes a single `SELECT COUNT(*), SUM(col), ...`
//! query against the current connection's filter context and returns the
//! results as a `serde_json::Value` that the `{Entity}Aggregates` schema type
//! can resolve field-by-field.
//!
//! All numeric results are cast to `TEXT` in SQL to avoid floating-point
//! precision loss when serialising to JSON.

use async_graphql::dynamic::ResolverContext;
use deadpool_postgres::Pool;
use serde_json::{Value, json};
use tracing::debug;

use crate::resolvers::connection::json_to_pg_params;

/// Resolve the aggregates object for a connection query.
///
/// The connection result embeds `_agg_ctx` with the schema, table, WHERE clause,
/// and params so this resolver can run the aggregate SQL with the same filter.
///
/// `numeric_cols` is the list of numeric column names (snake_case) for this table,
/// captured from introspection at schema-build time.
pub async fn resolve_aggregates(
	ctx: &ResolverContext<'_>,
	pool: &Pool,
	numeric_cols: &[String],
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

	// Build SELECT list:
	//   COUNT(*) as _count
	//   COUNT(DISTINCT col) as _distinct_count_{col}
	//   CAST(SUM(col) AS TEXT) as _sum_{col}
	//   ... for each numeric column
	let mut select_parts = vec!["COUNT(*) AS \"_count\"".to_string()];

	for col in numeric_cols {
		let q = format!(
			"CAST(SUM(t.\"{col}\") AS TEXT) AS \"_sum_{col}\",\
             CAST(MIN(t.\"{col}\") AS TEXT) AS \"_min_{col}\",\
             CAST(MAX(t.\"{col}\") AS TEXT) AS \"_max_{col}\",\
             CAST(AVG(t.\"{col}\") AS TEXT) AS \"_avg_{col}\",\
             CAST(STDDEV_SAMP(t.\"{col}\") AS TEXT) AS \"_stddev_samp_{col}\",\
             CAST(STDDEV_POP(t.\"{col}\") AS TEXT)  AS \"_stddev_pop_{col}\",\
             CAST(VAR_SAMP(t.\"{col}\") AS TEXT)    AS \"_var_samp_{col}\",\
             CAST(VAR_POP(t.\"{col}\") AS TEXT)     AS \"_var_pop_{col}\",\
             COUNT(DISTINCT t.\"{col}\") AS \"_distinct_{col}\""
		);
		select_parts.push(q);
	}

	// distinctCount for all columns (including non-numeric)
	// These are already captured in numeric_cols; if we need all cols we'd need
	// to capture them too. For now distinctCount covers numeric cols.

	let select_clause = select_parts.join(", ");
	let sql = format!(r#"SELECT {select_clause} FROM "{schema}"."{table}" AS t {where_clause}"#);

	debug!(sql = %sql, "Executing aggregates query");

	let client = pool.get().await?;
	let pg_params = json_to_pg_params(&params);
	let pg_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> =
		pg_params.iter().map(|p| p.as_ref() as _).collect();

	let row = client.query_one(&sql, &pg_refs).await?;

	// Extract count
	let count: i64 = row.try_get("_count").unwrap_or(0);

	// Build sub-aggregate objects
	let mut sum_map = serde_json::Map::new();
	let mut min_map = serde_json::Map::new();
	let mut max_map = serde_json::Map::new();
	let mut avg_map = serde_json::Map::new();
	let mut stddev_samp_map = serde_json::Map::new();
	let mut stddev_pop_map = serde_json::Map::new();
	let mut var_samp_map = serde_json::Map::new();
	let mut var_pop_map = serde_json::Map::new();
	let mut distinct_map = serde_json::Map::new();

	for col in numeric_cols {
		let field_name = crate::schema::inflector::to_camel_case(col);

		macro_rules! get_str {
			($prefix:expr) => {
				row.try_get::<_, Option<String>>(&format!("{}_{}", $prefix, col) as &str)
					.ok()
					.flatten()
					.map(Value::String)
					.unwrap_or(Value::Null)
			};
		}

		sum_map.insert(field_name.clone(), get_str!("_sum"));
		min_map.insert(field_name.clone(), get_str!("_min"));
		max_map.insert(field_name.clone(), get_str!("_max"));
		avg_map.insert(field_name.clone(), get_str!("_avg"));
		stddev_samp_map.insert(field_name.clone(), get_str!("_stddev_samp"));
		stddev_pop_map.insert(field_name.clone(), get_str!("_stddev_pop"));
		var_samp_map.insert(field_name.clone(), get_str!("_var_samp"));
		var_pop_map.insert(field_name.clone(), get_str!("_var_pop"));

		let distinct_val: i64 = row.try_get(&format!("_distinct_{col}") as &str).unwrap_or(0);
		distinct_map.insert(field_name, json!(distinct_val.to_string()));
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
