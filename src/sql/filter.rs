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
//! by the client (including logical `and`/`or`/`not` combinators, per-column
//! scalar operators and relation filters) and emits the corresponding SQL
//! fragment with `$N` parameterized values.
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

/// Metadata for an array column, which takes the `{Scalar}ListFilter` operators.
#[derive(Clone, Debug, Default)]
pub struct ArrayColumn {
	/// The elements are `json`/`jsonb`, so each operand is sent as JSON text.
	pub json_elements: bool,
}

/// Filter metadata for one table: how the keys of its `{Entity}Filter` input
/// that are not plain scalar columns map onto SQL.
#[derive(Clone, Debug, Default)]
pub struct TableFilterInfo {
	/// Maps `"{camelName}Exists"` → FK column (snake_case) for existence checks.
	pub exists_fields: HashMap<String, String>,
	/// Maps camelCase field name → forward relation metadata.
	pub forward_relations: HashMap<String, ForwardRelInfo>,
	/// Maps camelCase backward relation field name → backward relation metadata.
	pub backward_relations: HashMap<String, BackwardRelInfo>,
	/// Maps camelCase field name → array column metadata.
	pub array_columns: HashMap<String, ArrayColumn>,
}

/// Array-column operators that compare the column with a whole array, as
/// `(GraphQL operator, SQL operator)`. Each becomes `column <op> $n`.
///
/// This and [`ARRAY_ANY_OPERATORS`] are the single definition of the
/// `{Scalar}ListFilter` operators: the schema registers its fields from them, in
/// PostGraphile's order, and the SQL builder translates from them.
pub const ARRAY_OPERATORS: [(&str, &str); 11] = [
	("equalTo", "="),
	("notEqualTo", "<>"),
	("distinctFrom", "IS DISTINCT FROM"),
	("notDistinctFrom", "IS NOT DISTINCT FROM"),
	("lessThan", "<"),
	("lessThanOrEqualTo", "<="),
	("greaterThan", ">"),
	("greaterThanOrEqualTo", ">="),
	("contains", "@>"),
	("containedBy", "<@"),
	("overlaps", "&&"),
];

/// Array-column operators that compare one value with the column's elements, as
/// `(GraphQL operator, SQL operator)`. Each becomes `$n <op> ANY (column)`, so the
/// SQL operator reads from the value's side: "any element less than the value"
/// is `$n > ANY (column)`.
pub const ARRAY_ANY_OPERATORS: [(&str, &str); 6] = [
	("anyEqualTo", "="),
	("anyNotEqualTo", "<>"),
	("anyLessThan", ">"),
	("anyLessThanOrEqualTo", ">="),
	("anyGreaterThan", "<"),
	("anyGreaterThanOrEqualTo", "<="),
];

/// Filter metadata for every table in the schema, keyed by table name.
///
/// A filter is resolved against the entry of the table it applies to. Descending
/// into a relation's sub-filter switches to the related table's entry, so the keys
/// of a nested filter are looked up among the relations of the table they belong
/// to rather than those of the root table.
#[derive(Clone, Debug, Default)]
pub struct FilterContext {
	pub tables: HashMap<String, TableFilterInfo>,
}

/// Translate a GraphQL filter input object on `table` into a SQL WHERE clause
/// fragment and a list of bound parameters.
///
/// Returns `(conditions, params)` where `conditions` are SQL fragments using
/// `$N` placeholders starting from `param_offset + 1`. Relation filters are
/// resolved through `ctx` at any nesting depth.
///
/// Fails when the filter holds an operator this builder cannot translate. The
/// schema has already admitted the filter by then, so dropping the condition
/// would silently widen the result.
pub fn build_filter_sql(
	filter: &serde_json::Value,
	table: &str,
	table_alias: &str,
	param_offset: &mut usize,
	ctx: &FilterContext,
) -> anyhow::Result<(Vec<String>, Vec<serde_json::Value>)> {
	FilterBuilder { ctx, param_offset, sub_alias_counter: 0 }.build(filter, table, table_alias)
}

struct FilterBuilder<'a> {
	ctx: &'a FilterContext,
	param_offset: &'a mut usize,
	/// Shared by the whole filter so every sub-query alias in it is unique.
	sub_alias_counter: usize,
}

impl FilterBuilder<'_> {
	fn next_sub_alias(&mut self) -> String {
		self.sub_alias_counter += 1;
		format!("sub{}", self.sub_alias_counter)
	}

	/// Build the conditions for `filter`, an `{Entity}Filter` of `table`, whose
	/// rows are aliased `table_alias`.
	fn build(
		&mut self,
		filter: &serde_json::Value,
		table: &str,
		table_alias: &str,
	) -> anyhow::Result<(Vec<String>, Vec<serde_json::Value>)> {
		let mut conditions = vec![];
		let mut params = vec![];

		let obj = match filter.as_object() {
			Some(o) => o,
			None => return Ok((conditions, params)),
		};

		let ctx = self.ctx;
		let info = ctx.tables.get(table);

		for (key, value) in obj {
			match key.as_str() {
				"and" =>
					if let Some(arr) = value.as_array() {
						let mut sub_parts = vec![];
						for sub in arr {
							let (sub_conds, sub_params) = self.build(sub, table, table_alias)?;
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
							let (sub_conds, sub_params) = self.build(sub, table, table_alias)?;
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
					let (sub_conds, sub_params) = self.build(value, table, table_alias)?;
					if !sub_conds.is_empty() {
						conditions.push(format!("NOT ({})", sub_conds.join(" AND ")));
					}
					params.extend(sub_params);
				},
				field_name => {
					// ── {relation}Exists boolean filter ───────────────────────
					if let Some(fk_col) = info.and_then(|i| i.exists_fields.get(field_name)) {
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
					if let Some(rel) = info.and_then(|i| i.forward_relations.get(field_name)) {
						let sub_alias = self.next_sub_alias();
						// The sub-filter is a filter of the foreign table.
						let (sub_conds, sub_params) =
							self.build(value, &rel.foreign_table, &sub_alias)?;
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
					if let Some(rel) = info.and_then(|i| i.backward_relations.get(field_name)) {
						let hist_cond = if rel.is_historical {
							format!(" AND {{alias}}._block_range @> {}::bigint", i64::MAX)
						} else {
							String::new()
						};
						if let Some(obj) = value.as_object() {
							for (quantifier, sub_filter) in obj {
								let sub_alias = self.next_sub_alias();
								// The sub-filter is a filter of the child table.
								let (sub_conds, sub_params) =
									self.build(sub_filter, &rel.child_table, &sub_alias)?;
								if sub_conds.is_empty() {
									continue;
								}
								// An empty `in: []` produces a bare `FALSE` with no bound
								// params.  For `none`/`every` that yields a vacuously-true
								// NOT EXISTS(... AND FALSE) - skip it.  For `some` it is
								// always false, so push FALSE directly.
								if sub_conds.iter().all(|c| c == "FALSE") && sub_params.is_empty() {
									match quantifier.as_str() {
										"none" | "every" => continue,
										"some" => {
											conditions.push("FALSE".to_string());
											continue;
										},
										_ => {},
									}
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

					// ── Column filter (scalar or array) ───────────────────────
					let col = camel_to_snake(field_name);
					let array = info.and_then(|i| i.array_columns.get(field_name));
					if let Some(ops) = value.as_object() {
						for (op, op_val) in ops {
							let built = match array {
								Some(array) => build_array_op_condition(
									&col,
									table_alias,
									op,
									op_val,
									array,
									self.param_offset,
								)?,
								None => build_op_condition(
									&col,
									table_alias,
									op,
									op_val,
									self.param_offset,
								)?,
							};
							if let Some((cond, bound)) = built {
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

		Ok((conditions, params))
	}
}

/// Build the SQL condition for one `op` on `col`.
///
/// `Ok(None)` means the operator places no constraint (a null operand, an empty
/// `notIn`). An operator this builder does not know is an error.
fn build_op_condition(
	col: &str,
	alias: &str,
	op: &str,
	value: &serde_json::Value,
	param_offset: &mut usize,
) -> anyhow::Result<Option<(String, Option<serde_json::Value>)>> {
	// Null-valued operators mean "no constraint" — skip them.
	// Without this, a null value is sent as `Option::<String>::None` (PG text NULL)
	// and PostgreSQL fails to resolve the operator (e.g., `>(timestamptz, text)`).
	// The `isNull` operator is the only one where a non-bool null is ignored anyway.
	if value.is_null() && op != "isNull" {
		return Ok(None);
	}

	let qualified = format!("{alias}.{col}");

	let condition = match op {
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
					return Ok(Some(("FALSE".to_string(), None)));
				}
				*param_offset += 1;
				Some((
					format!(
						"lower({qualified}::text) = ANY(ARRAY(SELECT lower(x) FROM jsonb_array_elements_text(${}::jsonb) AS x))",
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
					return Ok(None);
				}
				*param_offset += 1;
				Some((
					format!(
						"lower({qualified}::text) != ALL(ARRAY(SELECT lower(x) FROM jsonb_array_elements_text(${}::jsonb) AS x))",
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
					return Ok(Some(("FALSE".to_string(), None)));
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
					return Ok(None); // NOT IN () is always true, skip
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
		// The schema admitted this operator, so the two disagree. Dropping the
		// condition would return rows the client asked to exclude.
		_ => anyhow::bail!("Unsupported filter operator `{op}` on `{col}`"),
	};

	Ok(condition)
}

/// Build the SQL condition for one `op` of a `{Scalar}ListFilter` on the array
/// column `col`.
///
/// Operands are bound as text and carry no cast: PostgreSQL infers the column's
/// array type (or its element type, for the `any…` operators) from the other
/// side of the operator, so one code path serves every element type.
fn build_array_op_condition(
	col: &str,
	alias: &str,
	op: &str,
	value: &serde_json::Value,
	array: &ArrayColumn,
	param_offset: &mut usize,
) -> anyhow::Result<Option<(String, Option<serde_json::Value>)>> {
	let qualified = format!("{alias}.{col}");

	if op == "isNull" {
		let fragment = if value.as_bool().unwrap_or(true) {
			format!("{qualified} IS NULL")
		} else {
			format!("{qualified} IS NOT NULL")
		};
		return Ok(Some((fragment, None)));
	}

	// A null operand means "no constraint", as for scalar columns.
	if value.is_null() {
		return Ok(None);
	}

	if let Some((_, sql_op)) = ARRAY_OPERATORS.iter().find(|(name, _)| *name == op) {
		// GraphQL coerces a single value to a one-element list; the dynamic
		// schema hands it over uncoerced.
		let elements = match value {
			serde_json::Value::Array(elements) => elements.as_slice(),
			single => std::slice::from_ref(single),
		};
		*param_offset += 1;
		let literal = pg_array_literal(elements, array.json_elements);
		return Ok(Some((
			format!("{qualified} {sql_op} ${param_offset}"),
			Some(serde_json::Value::String(literal)),
		)));
	}

	if let Some((_, sql_op)) = ARRAY_ANY_OPERATORS.iter().find(|(name, _)| *name == op) {
		*param_offset += 1;
		let operand = if array.json_elements {
			serde_json::Value::String(value.to_string())
		} else {
			value.clone()
		};
		return Ok(Some((format!("${param_offset} {sql_op} ANY ({qualified})"), Some(operand))));
	}

	anyhow::bail!("Unsupported filter operator `{op}` on `{col}`")
}

/// Encode `elements` as a PostgreSQL array literal (`{"a","b",NULL}`).
///
/// Every non-null element is double-quoted, with `"` and `\` backslash-escaped,
/// which is valid for any element type and leaves no input able to end the
/// element early. With `json_elements` each element is its JSON text, so a
/// string keeps its quotes and parses as a JSON string.
fn pg_array_literal(elements: &[serde_json::Value], json_elements: bool) -> String {
	let mut literal = String::from("{");
	for (i, element) in elements.iter().enumerate() {
		if i > 0 {
			literal.push(',');
		}
		let text = match element {
			serde_json::Value::Null => {
				literal.push_str("NULL");
				continue;
			},
			serde_json::Value::String(s) if !json_elements => s.clone(),
			other => other.to_string(),
		};
		literal.push('"');
		for ch in text.chars() {
			if matches!(ch, '"' | '\\') {
				literal.push('\\');
			}
			literal.push(ch);
		}
		literal.push('"');
	}
	literal.push('}');
	literal
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
		let (conditions, params) =
			build_filter_sql(&filter, "orders", "t", &mut offset, &FilterContext::default())
				.unwrap();
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
		let (conditions, params) =
			build_filter_sql(&filter, "orders", "t", &mut offset, &FilterContext::default())
				.unwrap();
		assert_eq!(conditions.len(), 1, "only non-null operator should produce a condition");
		assert_eq!(params.len(), 1);
		assert_eq!(offset, 1);
		assert!(conditions[0].contains("> $1"), "condition: {}", conditions[0]);
	}

	/// `fill_assets` → `fills` → `orders`, with `order_assets` → `orders`. Both
	/// `fills` and `orders` expose a backward relation named `assets`, each to its
	/// own child table.
	fn relation_ctx() -> FilterContext {
		fn forward(foreign_table: &str, fk_column: &str) -> ForwardRelInfo {
			ForwardRelInfo {
				schema: "app".to_string(),
				foreign_table: foreign_table.to_string(),
				fk_column: fk_column.to_string(),
				foreign_pk: "id".to_string(),
				is_historical: false,
			}
		}
		fn backward(child_table: &str, fk_column: &str) -> BackwardRelInfo {
			BackwardRelInfo {
				schema: "app".to_string(),
				child_table: child_table.to_string(),
				fk_column: fk_column.to_string(),
				is_historical: false,
			}
		}

		let mut orders = TableFilterInfo::default();
		orders
			.backward_relations
			.insert("fills".to_string(), backward("fills", "order_id"));
		orders
			.backward_relations
			.insert("assets".to_string(), backward("order_assets", "order_id"));

		let mut fills = TableFilterInfo::default();
		fills
			.forward_relations
			.insert("order".to_string(), forward("orders", "order_id"));
		fills.exists_fields.insert("orderExists".to_string(), "order_id".to_string());
		fills
			.backward_relations
			.insert("assets".to_string(), backward("fill_assets", "fill_id"));

		let mut fill_assets = TableFilterInfo::default();
		fill_assets
			.forward_relations
			.insert("fill".to_string(), forward("fills", "fill_id"));

		let mut ctx = FilterContext::default();
		ctx.tables.insert("orders".to_string(), orders);
		ctx.tables.insert("fills".to_string(), fills);
		ctx.tables.insert("fill_assets".to_string(), fill_assets);
		ctx.tables.insert("order_assets".to_string(), TableFilterInfo::default());
		ctx
	}

	fn build(table: &str, filter: serde_json::Value) -> (Vec<String>, Vec<serde_json::Value>) {
		build_filter_sql(&filter, table, "t", &mut 0, &relation_ctx()).unwrap()
	}

	#[test]
	fn forward_relation_nested_in_forward_relation() {
		// `order` is a relation of `fills`, not of the root table `fill_assets`.
		let (conditions, params) = build(
			"fill_assets",
			serde_json::json!({ "fill": { "order": { "chain": { "equalTo": "EVM-1" } } } }),
		);
		assert_eq!(
			conditions,
			vec![
				r#"EXISTS (SELECT 1 FROM "app"."fills" AS sub1 WHERE sub1."id" = t."fill_id" AND EXISTS (SELECT 1 FROM "app"."orders" AS sub2 WHERE sub2."id" = sub1."order_id" AND sub2.chain = $1))"#
			]
		);
		assert_eq!(params, vec![serde_json::json!("EVM-1")]);
	}

	#[test]
	fn forward_relation_nested_in_backward_relation() {
		let (conditions, _) = build(
			"orders",
			serde_json::json!({
				"fills": { "some": { "order": { "chain": { "equalTo": "EVM-1" } }, "orderExists": true } }
			}),
		);
		assert_eq!(
			conditions,
			vec![
				r#"EXISTS (SELECT 1 FROM "app"."fills" AS sub1 WHERE sub1."order_id" = t."id" AND EXISTS (SELECT 1 FROM "app"."orders" AS sub2 WHERE sub2."id" = sub1."order_id" AND sub2.chain = $1) AND sub1."order_id" IS NOT NULL)"#
			]
		);
	}

	#[test]
	fn nested_relation_sharing_a_name_with_the_root_table() {
		let assets = serde_json::json!({ "some": { "token": { "equalTo": "0xabc" } } });

		// On the root table `fills`, `assets` is the fill's assets.
		let (root, _) = build("fills", serde_json::json!({ "assets": assets }));
		assert_eq!(
			root,
			vec![
				r#"EXISTS (SELECT 1 FROM "app"."fill_assets" AS sub1 WHERE sub1."fill_id" = t."id" AND sub1.token = $1)"#
			]
		);

		// Inside `order` it is the order's assets, joined on the order alias.
		let (nested, _) = build("fills", serde_json::json!({ "order": { "assets": assets } }));
		assert_eq!(
			nested,
			vec![
				r#"EXISTS (SELECT 1 FROM "app"."orders" AS sub1 WHERE sub1."id" = t."order_id" AND EXISTS (SELECT 1 FROM "app"."order_assets" AS sub2 WHERE sub2."order_id" = sub1."id" AND sub2.token = $1))"#
			]
		);
	}

	fn array_ctx() -> FilterContext {
		let mut info = TableFilterInfo::default();
		info.array_columns.insert("tags".to_string(), ArrayColumn::default());
		info.array_columns
			.insert("payloads".to_string(), ArrayColumn { json_elements: true });
		let mut ctx = FilterContext::default();
		ctx.tables.insert("posts".to_string(), info);
		ctx
	}

	fn build_array(filter: serde_json::Value) -> (Vec<String>, Vec<serde_json::Value>) {
		build_filter_sql(&filter, "posts", "t", &mut 0, &array_ctx()).unwrap()
	}

	#[test]
	fn array_operators_compare_with_an_array_literal() {
		let (conditions, params) =
			build_array(serde_json::json!({ "tags": { "contains": ["a", "b"] } }));
		assert_eq!(conditions, vec!["t.tags @> $1"]);
		assert_eq!(params, vec![serde_json::json!(r#"{"a","b"}"#)]);

		let (conditions, params) = build_array(serde_json::json!({ "tags": { "equalTo": [] } }));
		assert_eq!(conditions, vec!["t.tags = $1"]);
		assert_eq!(params, vec![serde_json::json!("{}")]);

		// A bare value is a one-element list.
		let (conditions, params) = build_array(serde_json::json!({ "tags": { "overlaps": 7 } }));
		assert_eq!(conditions, vec!["t.tags && $1"]);
		assert_eq!(params, vec![serde_json::json!(r#"{"7"}"#)]);
	}

	#[test]
	fn array_any_operators_compare_from_the_value_side() {
		let (conditions, params) =
			build_array(serde_json::json!({ "tags": { "anyLessThan": 10 } }));
		assert_eq!(conditions, vec!["$1 > ANY (t.tags)"]);
		assert_eq!(params, vec![serde_json::json!(10)]);

		// A JSON element is bound as JSON text, so a string keeps its quotes.
		let (conditions, params) =
			build_array(serde_json::json!({ "payloads": { "anyEqualTo": "a" } }));
		assert_eq!(conditions, vec!["$1 = ANY (t.payloads)"]);
		assert_eq!(params, vec![serde_json::json!(r#""a""#)]);
	}

	#[test]
	fn array_is_null_and_null_operands() {
		let (conditions, params) = build_array(
			serde_json::json!({ "tags": { "isNull": false, "contains": null, "anyEqualTo": null } }),
		);
		assert_eq!(conditions, vec!["t.tags IS NOT NULL"]);
		assert!(params.is_empty());
	}

	#[test]
	fn array_literal_quotes_and_escapes_elements() {
		let elements = [
			serde_json::json!("plain"),
			serde_json::json!(r#"quote " brace } comma , slash \"#),
			serde_json::json!(null),
			serde_json::json!("NULL"),
			serde_json::json!(1.5),
			serde_json::json!(true),
		];
		assert_eq!(
			pg_array_literal(&elements, false),
			r#"{"plain","quote \" brace } comma , slash \\",NULL,"NULL","1.5","true"}"#
		);

		let json =
			[serde_json::json!({ "a": "x\"y" }), serde_json::json!("s"), serde_json::json!(null)];
		assert_eq!(pg_array_literal(&json, true), r#"{"{\"a\":\"x\\\"y\"}","\"s\"",NULL}"#);
	}

	#[test]
	fn scalar_operator_on_array_column_is_an_error() {
		let filter = serde_json::json!({ "tags": { "startsWith": "a" } });
		let err = build_filter_sql(&filter, "posts", "t", &mut 0, &array_ctx()).unwrap_err();
		assert_eq!(err.to_string(), "Unsupported filter operator `startsWith` on `tags`");
	}

	#[test]
	fn unknown_operator_is_an_error() {
		let filter = serde_json::json!({ "chain": { "equalTo": "EVM-1", "soundsLike": "evm" } });
		let err = build_filter_sql(&filter, "orders", "t", &mut 0, &relation_ctx()).unwrap_err();
		assert_eq!(err.to_string(), "Unsupported filter operator `soundsLike` on `chain`");
	}
}
