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

//! GraphQL filter input → SQL `WHERE` clause translation.
//!
//! [`build_filter_sql`] recursively walks the nested filter object supplied
//! by the client (including logical `and`/`or`/`not` combinators and
//! per-column scalar operators) and emits the corresponding SQL fragment with
//! `$N` parameterized values.
//!
//! Column names are always sourced from schema introspection and are never
//! derived from user input.

use std::collections::HashMap;

/// Metadata for a forward relation filter (FK on this table → foreign table).
#[derive(Clone, Debug)]
pub struct ForwardRelInfo {
	pub schema: String,
	pub foreign_table: String,
	pub fk_column: String,
	pub foreign_pk: String, // usually "id"
	pub is_historical: bool,
}

/// Metadata for a backward relation filter (FK on child table → this table).
#[derive(Clone, Debug)]
pub struct BackwardRelInfo {
	pub schema: String,
	pub child_table: String,
	pub fk_column: String, // column in child table referencing parent
	pub is_historical: bool,
}

/// Context for relation-aware filter SQL generation.
#[derive(Clone, Debug, Default)]
pub struct FilterContext {
	/// Maps `"{camelName}Exists"` → FK column (snake_case) for existence checks.
	pub exists_fields: HashMap<String, String>,
	/// Maps camelCase field name → forward relation metadata.
	pub forward_relations: HashMap<String, ForwardRelInfo>,
	/// Maps camelCase backward relation field name → backward relation metadata.
	pub backward_relations: HashMap<String, BackwardRelInfo>,
	/// Counter for generating unique sub-query aliases.
	pub sub_alias_counter: usize,
}

/// Translate a GraphQL filter input object into a SQL WHERE clause fragment
/// and a list of bound parameters.
///
/// Returns `(conditions, params)` where `conditions` are SQL fragments using
/// `$N` placeholders starting from `param_offset + 1`.
pub fn build_filter_sql(
	filter: &serde_json::Value,
	table_alias: &str,
	param_offset: &mut usize,
) -> (Vec<String>, Vec<serde_json::Value>) {
	build_filter_sql_ctx(filter, table_alias, param_offset, &mut FilterContext::default())
}

/// Like [`build_filter_sql`] but with a [`FilterContext`] for relation-aware filtering.
pub fn build_filter_sql_ctx(
	filter: &serde_json::Value,
	table_alias: &str,
	param_offset: &mut usize,
	ctx: &mut FilterContext,
) -> (Vec<String>, Vec<serde_json::Value>) {
	let mut conditions = vec![];
	let mut params = vec![];

	let obj = match filter.as_object() {
		Some(o) => o,
		None => return (conditions, params),
	};

	for (key, value) in obj {
		match key.as_str() {
			"and" =>
				if let Some(arr) = value.as_array() {
					let mut sub_parts = vec![];
					for sub in arr {
						let (sub_conds, sub_params) =
							build_filter_sql_ctx(sub, table_alias, param_offset, ctx);
						if !sub_conds.is_empty() {
							sub_parts.push(format!("({})", sub_conds.join(" AND ")));
						}
						params.extend(sub_params);
					}
					if !sub_parts.is_empty() {
						conditions.push(format!("({})", sub_parts.join(" AND ")));
					}
				},
			"or" =>
				if let Some(arr) = value.as_array() {
					let mut sub_parts = vec![];
					for sub in arr {
						let (sub_conds, sub_params) =
							build_filter_sql_ctx(sub, table_alias, param_offset, ctx);
						if !sub_conds.is_empty() {
							sub_parts.push(format!("({})", sub_conds.join(" AND ")));
						}
						params.extend(sub_params);
					}
					if !sub_parts.is_empty() {
						conditions.push(format!("({})", sub_parts.join(" OR ")));
					}
				},
			"not" => {
				let (sub_conds, sub_params) =
					build_filter_sql_ctx(value, table_alias, param_offset, ctx);
				if !sub_conds.is_empty() {
					conditions.push(format!("NOT ({})", sub_conds.join(" AND ")));
				}
				params.extend(sub_params);
			},
			field_name => {
				// ── {relation}Exists boolean filter ───────────────────────
				if let Some(fk_col) = ctx.exists_fields.get(field_name).cloned() {
					if let Some(b) = value.as_bool() {
						if b {
							conditions.push(format!("{table_alias}.\"{fk_col}\" IS NOT NULL"));
						} else {
							conditions.push(format!("{table_alias}.\"{fk_col}\" IS NULL"));
						}
					}
					continue;
				}

				// ── Forward relation filter ───────────────────────────────
				if let Some(rel) = ctx.forward_relations.get(field_name).cloned() {
					ctx.sub_alias_counter += 1;
					let sub_alias = format!("sub{}", ctx.sub_alias_counter);
					let (sub_conds, sub_params) =
						build_filter_sql_ctx(value, &sub_alias, param_offset, ctx);
					if !sub_conds.is_empty() {
						let hist_cond = if rel.is_historical {
							format!(" AND {sub_alias}._block_range @> {}::bigint", i64::MAX)
						} else {
							String::new()
						};
						conditions.push(format!(
							"EXISTS (SELECT 1 FROM \"{}\".\"{}\" AS {sub_alias} WHERE {sub_alias}.\"{}\" = {table_alias}.\"{}\" AND {}{hist_cond})",
							rel.schema, rel.foreign_table, rel.foreign_pk, rel.fk_column,
							sub_conds.join(" AND ")
						));
					}
					params.extend(sub_params);
					continue;
				}

				// ── Backward relation filter (some/none/every) ───────────
				if let Some(rel) = ctx.backward_relations.get(field_name).cloned() {
					let hist_cond = if rel.is_historical {
						format!(" AND {{alias}}._block_range @> {}::bigint", i64::MAX)
					} else {
						String::new()
					};
					if let Some(obj) = value.as_object() {
						for (quantifier, sub_filter) in obj {
							ctx.sub_alias_counter += 1;
							let sub_alias = format!("sub{}", ctx.sub_alias_counter);
							let (sub_conds, sub_params) =
								build_filter_sql_ctx(sub_filter, &sub_alias, param_offset, ctx);
							if sub_conds.is_empty() {
								continue;
							}
							let joined = sub_conds.join(" AND ");
							let hc = hist_cond.replace("{alias}", &sub_alias);
							let sql = match quantifier.as_str() {
								"some" => format!(
									"EXISTS (SELECT 1 FROM \"{}\".\"{}\" AS {sub_alias} WHERE {sub_alias}.\"{}\" = {table_alias}.\"id\"{hc} AND {joined})",
									rel.schema, rel.child_table, rel.fk_column
								),
								"none" => format!(
									"NOT EXISTS (SELECT 1 FROM \"{}\".\"{}\" AS {sub_alias} WHERE {sub_alias}.\"{}\" = {table_alias}.\"id\"{hc} AND {joined})",
									rel.schema, rel.child_table, rel.fk_column
								),
								"every" => format!(
									"NOT EXISTS (SELECT 1 FROM \"{}\".\"{}\" AS {sub_alias} WHERE {sub_alias}.\"{}\" = {table_alias}.\"id\"{hc} AND NOT ({joined}))",
									rel.schema, rel.child_table, rel.fk_column
								),
								_ => continue,
							};
							conditions.push(sql);
							params.extend(sub_params);
						}
					}
					continue;
				}

				// ── Scalar column filter ──────────────────────────────────
				let col = camel_to_snake(field_name);
				if let Some(ops) = value.as_object() {
					for (op, op_val) in ops {
						if let Some((cond, bound)) =
							build_op_condition(&col, table_alias, op, op_val, param_offset)
						{
							conditions.push(cond);
							if let Some(v) = bound {
								params.push(v);
							}
						}
					}
				}
			},
		}
	}

	(conditions, params)
}

fn build_op_condition(
	col: &str,
	alias: &str,
	op: &str,
	value: &serde_json::Value,
	param_offset: &mut usize,
) -> Option<(String, Option<serde_json::Value>)> {
	// Null-valued operators mean "no constraint" — skip them.
	// Without this, a null value is sent as `Option::<String>::None` (PG text NULL)
	// and PostgreSQL fails to resolve the operator (e.g., `>(timestamptz, text)`).
	// The `isNull` operator is the only one where a non-bool null is ignored anyway.
	if value.is_null() && op != "isNull" {
		return None;
	}

	let qualified = format!("{alias}.{col}");

	match op {
		"isNull" => {
			let is_null = value.as_bool().unwrap_or(true);
			let fragment = if is_null {
				format!("{qualified} IS NULL")
			} else {
				format!("{qualified} IS NOT NULL")
			};
			Some((fragment, None))
		},
		"equalTo" => {
			*param_offset += 1;
			Some((format!("{qualified} = ${}", param_offset), Some(value.clone())))
		},
		"notEqualTo" => {
			*param_offset += 1;
			Some((format!("{qualified} != ${}", param_offset), Some(value.clone())))
		},
		"lessThan" => {
			*param_offset += 1;
			Some((format!("{qualified} < ${}", param_offset), Some(value.clone())))
		},
		"lessThanOrEqualTo" => {
			*param_offset += 1;
			Some((format!("{qualified} <= ${}", param_offset), Some(value.clone())))
		},
		"greaterThan" => {
			*param_offset += 1;
			Some((format!("{qualified} > ${}", param_offset), Some(value.clone())))
		},
		"greaterThanOrEqualTo" => {
			*param_offset += 1;
			Some((format!("{qualified} >= ${}", param_offset), Some(value.clone())))
		},
		"like" => {
			*param_offset += 1;
			Some((format!("{qualified} LIKE ${}", param_offset), Some(value.clone())))
		},
		"notLike" => {
			*param_offset += 1;
			Some((format!("{qualified} NOT LIKE ${}", param_offset), Some(value.clone())))
		},
		"likeInsensitive" => {
			*param_offset += 1;
			Some((format!("{qualified} ILIKE ${}", param_offset), Some(value.clone())))
		},
		"notLikeInsensitive" => {
			*param_offset += 1;
			Some((format!("{qualified} NOT ILIKE ${}", param_offset), Some(value.clone())))
		},
		"startsWith" => {
			*param_offset += 1;
			let prefix = value.as_str().unwrap_or("").to_string() + "%";
			Some((
				format!("{qualified} LIKE ${}", param_offset),
				Some(serde_json::Value::String(prefix)),
			))
		},
		"notStartsWith" => {
			*param_offset += 1;
			let prefix = value.as_str().unwrap_or("").to_string() + "%";
			Some((
				format!("{qualified} NOT LIKE ${}", param_offset),
				Some(serde_json::Value::String(prefix)),
			))
		},
		"startsWithInsensitive" => {
			*param_offset += 1;
			let prefix = value.as_str().unwrap_or("").to_string() + "%";
			Some((
				format!("{qualified} ILIKE ${}", param_offset),
				Some(serde_json::Value::String(prefix)),
			))
		},
		"notStartsWithInsensitive" => {
			*param_offset += 1;
			let prefix = value.as_str().unwrap_or("").to_string() + "%";
			Some((
				format!("{qualified} NOT ILIKE ${}", param_offset),
				Some(serde_json::Value::String(prefix)),
			))
		},
		"endsWith" => {
			*param_offset += 1;
			let suffix = "%".to_string() + value.as_str().unwrap_or("");
			Some((
				format!("{qualified} LIKE ${}", param_offset),
				Some(serde_json::Value::String(suffix)),
			))
		},
		"notEndsWith" => {
			*param_offset += 1;
			let suffix = "%".to_string() + value.as_str().unwrap_or("");
			Some((
				format!("{qualified} NOT LIKE ${}", param_offset),
				Some(serde_json::Value::String(suffix)),
			))
		},
		"endsWithInsensitive" => {
			*param_offset += 1;
			let suffix = "%".to_string() + value.as_str().unwrap_or("");
			Some((
				format!("{qualified} ILIKE ${}", param_offset),
				Some(serde_json::Value::String(suffix)),
			))
		},
		"notEndsWithInsensitive" => {
			*param_offset += 1;
			let suffix = "%".to_string() + value.as_str().unwrap_or("");
			Some((
				format!("{qualified} NOT ILIKE ${}", param_offset),
				Some(serde_json::Value::String(suffix)),
			))
		},
		"includes" => {
			*param_offset += 1;
			let pattern = "%".to_string() + value.as_str().unwrap_or("") + "%";
			Some((
				format!("{qualified} LIKE ${}", param_offset),
				Some(serde_json::Value::String(pattern)),
			))
		},
		"notIncludes" => {
			*param_offset += 1;
			let pattern = "%".to_string() + value.as_str().unwrap_or("") + "%";
			Some((
				format!("{qualified} NOT LIKE ${}", param_offset),
				Some(serde_json::Value::String(pattern)),
			))
		},
		"includesInsensitive" => {
			*param_offset += 1;
			let pattern = "%".to_string() + value.as_str().unwrap_or("") + "%";
			Some((
				format!("{qualified} ILIKE ${}", param_offset),
				Some(serde_json::Value::String(pattern)),
			))
		},
		"notIncludesInsensitive" => {
			*param_offset += 1;
			let pattern = "%".to_string() + value.as_str().unwrap_or("") + "%";
			Some((
				format!("{qualified} NOT ILIKE ${}", param_offset),
				Some(serde_json::Value::String(pattern)),
			))
		},
		"equalToInsensitive" => {
			*param_offset += 1;
			Some((format!("lower({qualified}) = lower(${})", param_offset), Some(value.clone())))
		},
		"notEqualToInsensitive" => {
			*param_offset += 1;
			Some((format!("lower({qualified}) != lower(${})", param_offset), Some(value.clone())))
		},
		"distinctFromInsensitive" => {
			*param_offset += 1;
			Some((
				format!("lower({qualified}) IS DISTINCT FROM lower(${})", param_offset),
				Some(value.clone()),
			))
		},
		"notDistinctFromInsensitive" => {
			*param_offset += 1;
			Some((
				format!("lower({qualified}) IS NOT DISTINCT FROM lower(${})", param_offset),
				Some(value.clone()),
			))
		},
		"lessThanInsensitive" => {
			*param_offset += 1;
			Some((format!("lower({qualified}) < lower(${})", param_offset), Some(value.clone())))
		},
		"lessThanOrEqualToInsensitive" => {
			*param_offset += 1;
			Some((format!("lower({qualified}) <= lower(${})", param_offset), Some(value.clone())))
		},
		"greaterThanInsensitive" => {
			*param_offset += 1;
			Some((format!("lower({qualified}) > lower(${})", param_offset), Some(value.clone())))
		},
		"greaterThanOrEqualToInsensitive" => {
			*param_offset += 1;
			Some((format!("lower({qualified}) >= lower(${})", param_offset), Some(value.clone())))
		},
		"inInsensitive" =>
			if let Some(arr) = value.as_array() {
				if arr.is_empty() {
					return Some(("FALSE".to_string(), None));
				}
				*param_offset += 1;
				Some((
					format!(
						"lower({qualified}) = ANY(ARRAY(SELECT lower(x) FROM jsonb_array_elements_text(${}::jsonb) AS x))",
						param_offset
					),
					Some(value.clone()),
				))
			} else {
				None
			},
		"notInInsensitive" =>
			if let Some(arr) = value.as_array() {
				if arr.is_empty() {
					return None;
				}
				*param_offset += 1;
				Some((
					format!(
						"lower({qualified}) != ALL(ARRAY(SELECT lower(x) FROM jsonb_array_elements_text(${}::jsonb) AS x))",
						param_offset
					),
					Some(value.clone()),
				))
			} else {
				None
			},
		"distinctFrom" => {
			*param_offset += 1;
			Some((format!("{qualified} IS DISTINCT FROM ${}", param_offset), Some(value.clone())))
		},
		"notDistinctFrom" => {
			*param_offset += 1;
			Some((
				format!("{qualified} IS NOT DISTINCT FROM ${}", param_offset),
				Some(value.clone()),
			))
		},
		"in" => {
			if let Some(arr) = value.as_array() {
				if arr.is_empty() {
					return Some(("FALSE".to_string(), None));
				}
				// Each array element becomes its own $N parameter.
				// We can't return multiple params from this function directly,
				// so we encode as a single JSON array and use = ANY($N::jsonb).
				// The connection resolver wraps JSON arrays via `json_to_pg_params`.
				// Cast the column to text so the comparison works for all column
				// types (especially PostgreSQL enums, where `=(enum, text)` fails
				// in the = ANY() context).
				*param_offset += 1;
				Some((
					format!(
						"{qualified}::text = ANY(ARRAY(SELECT jsonb_array_elements_text(${}::jsonb)))",
						param_offset
					),
					Some(value.clone()),
				))
			} else {
				None
			}
		},
		"notIn" => {
			if let Some(arr) = value.as_array() {
				if arr.is_empty() {
					return None; // NOT IN () is always true, skip
				}
				*param_offset += 1;
				Some((
					format!(
						"{qualified}::text != ALL(ARRAY(SELECT jsonb_array_elements_text(${}::jsonb)))",
						param_offset
					),
					Some(value.clone()),
				))
			} else {
				None
			}
		},
		_ => None,
	}
}

/// Convert a camelCase field name back to snake_case for SQL.
fn camel_to_snake(s: &str) -> String {
	let mut result = String::with_capacity(s.len() + 4);
	for (i, ch) in s.chars().enumerate() {
		if ch.is_uppercase() && i > 0 {
			result.push('_');
		}
		result.extend(ch.to_lowercase());
	}
	result
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn snake_conversion() {
		assert_eq!(camel_to_snake("blockNumber"), "block_number");
		assert_eq!(camel_to_snake("id"), "id");
		assert_eq!(camel_to_snake("createdAt"), "created_at");
	}

	#[test]
	fn null_filter_values_skipped() {
		// Filter with null-valued operators should produce no conditions,
		// not generate broken SQL with text-typed NULL parameters.
		let filter = serde_json::json!({
			"createdAt": { "greaterThan": null, "lessThan": null },
			"status": { "equalTo": null },
		});
		let mut offset = 0;
		let (conditions, params) = build_filter_sql(&filter, "t", &mut offset);
		assert!(
			conditions.is_empty(),
			"null operators should produce no conditions: {conditions:?}"
		);
		assert!(params.is_empty(), "null operators should produce no params: {params:?}");
		assert_eq!(offset, 0, "null operators should not advance param_offset");
	}

	#[test]
	fn null_filter_mixed_with_real_values() {
		// Mix of null and real values — only real values should generate conditions.
		let filter = serde_json::json!({
			"blockNumber": { "greaterThan": 100, "lessThan": null },
		});
		let mut offset = 0;
		let (conditions, params) = build_filter_sql(&filter, "t", &mut offset);
		assert_eq!(conditions.len(), 1, "only non-null operator should produce a condition");
		assert_eq!(params.len(), 1);
		assert_eq!(offset, 1);
		assert!(conditions[0].contains("> $1"), "condition: {}", conditions[0]);
	}
}
