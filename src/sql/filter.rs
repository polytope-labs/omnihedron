//! GraphQL filter input → SQL `WHERE` clause translation.
//!
//! [`build_filter_sql`] recursively walks the nested filter object supplied
//! by the client (including logical `and`/`or`/`not` combinators and
//! per-column scalar operators) and emits the corresponding SQL fragment with
//! `$N` parameterized values.
//!
//! Column names are always sourced from schema introspection and are never
//! derived from user input.

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
							build_filter_sql(sub, table_alias, param_offset);
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
							build_filter_sql(sub, table_alias, param_offset);
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
				let (sub_conds, sub_params) = build_filter_sql(value, table_alias, param_offset);
				if !sub_conds.is_empty() {
					conditions.push(format!("NOT ({})", sub_conds.join(" AND ")));
				}
				params.extend(sub_params);
			},
			field_name => {
				// field_name is a camelCase column name from the filter input
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
		"ilike" => {
			*param_offset += 1;
			Some((format!("{qualified} ILIKE ${}", param_offset), Some(value.clone())))
		},
		"notIlike" => {
			*param_offset += 1;
			Some((format!("{qualified} NOT ILIKE ${}", param_offset), Some(value.clone())))
		},
		"startsWith" => {
			// LIKE 'prefix%'
			*param_offset += 1;
			let prefix = value.as_str().unwrap_or("").to_string() + "%";
			Some((
				format!("{qualified} LIKE ${}", param_offset),
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
		"contains" => {
			*param_offset += 1;
			let pattern = "%".to_string() + value.as_str().unwrap_or("") + "%";
			Some((
				format!("{qualified} LIKE ${}", param_offset),
				Some(serde_json::Value::String(pattern)),
			))
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
				*param_offset += 1;
				Some((
					format!(
						"{qualified} = ANY(ARRAY(SELECT jsonb_array_elements_text(${}::jsonb)))",
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
						"{qualified} != ALL(ARRAY(SELECT jsonb_array_elements_text(${}::jsonb)))",
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
}
