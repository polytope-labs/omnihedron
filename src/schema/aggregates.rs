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

//! Aggregate type registration for the `async-graphql` dynamic schema.
//!
//! [`register_aggregate_types`] generates the `{Entity}Aggregates` object type
//! containing count, sum, min, max, average, and statistical aggregate fields
//! for a given table.  Only columns with numeric PostgreSQL types receive
//! sum/avg/stddev/variance fields.

use async_graphql::{Value as GqlValue, dynamic::*};

use crate::{
	introspection::{TableInfo, types::pg_type_to_graphql},
	schema::inflector::{singularize, to_camel_case, to_pascal_case, to_screaming_snake},
};

/// Whether a GraphQL type should be returned as a native JSON number (Int/Float)
/// or as a BigFloat string.  PostGraphile preserves the source column type for
/// min/max but always returns BigFloat for sum/average/stddev/variance.
fn is_native_number(gql_type: &str) -> bool {
	matches!(gql_type, "Int" | "Float")
}

/// Register all aggregate types for a given entity table.
/// Returns `(builder, aggregate_type_name, numeric_col_info, all_public_col_names)`.
///
/// `numeric_col_info` is `Vec<(snake_case_name, graphql_type)>` — the graphql_type
/// is used by the aggregate resolver to decide whether min/max should be returned as
/// a native number or a BigFloat string.
pub fn register_aggregate_types(
	table: &TableInfo,
	mut builder: SchemaBuilder,
) -> (SchemaBuilder, String, Vec<(String, String)>, Vec<String>) {
	let type_name = to_pascal_case(&singularize(&table.name));
	let agg_type_name = format!("{type_name}Aggregates");

	let all_cols: Vec<String> = table.public_columns().map(|c| c.name.clone()).collect();
	let numeric_cols: Vec<(String, String)> = table
		.public_columns()
		.filter(|c| pg_type_to_graphql(&c.pg_type, &c.udt_name).1)
		.map(|c| {
			let gql_type = pg_type_to_graphql(&c.pg_type, &c.udt_name).0.to_string();
			(c.name.clone(), gql_type)
		})
		.collect();

	// ── Numeric sub-aggregate types ────────────────────────────────────────────
	let has_numeric = !numeric_cols.is_empty();
	if has_numeric {
		// Sum / Average / Stddev / Variance — always BigFloat strings.
		for op_name in &[
			"Sum",
			"Average",
			"StddevSample",
			"StddevPopulation",
			"VarianceSample",
			"VariancePopulation",
		] {
			let sub_type_name = format!("{type_name}{op_name}Aggregates");
			let mut obj = Object::new(&sub_type_name);
			for (col, _) in &numeric_cols {
				let field_name = to_camel_case(col);
				let key = field_name.clone();
				obj = obj.field(Field::new(field_name, TypeRef::named("BigFloat"), move |ctx| {
					let key = key.clone();
					FieldFuture::new(async move {
						let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
						Ok(parent
							.get(&key)
							.and_then(|v| v.as_str())
							.map(|s| GqlValue::String(s.to_string())))
					})
				}));
			}
			builder = builder.register(obj);
		}

		// Min / Max — preserve the source column type (Int/Float → native number;
		// BigInt/BigFloat → BigFloat string).  This matches PostGraphile behaviour.
		for op_name in &["Min", "Max"] {
			let sub_type_name = format!("{type_name}{op_name}Aggregates");
			let mut obj = Object::new(&sub_type_name);
			for (col, gql_type) in &numeric_cols {
				let field_name = to_camel_case(col);
				let key = field_name.clone();
				let native = is_native_number(gql_type);
				let type_ref = match gql_type.as_str() {
					"Int" => TypeRef::named(TypeRef::INT),
					"Float" => TypeRef::named(TypeRef::FLOAT),
					"BigInt" => TypeRef::named("BigInt"),
					_ => TypeRef::named("BigFloat"),
				};
				obj = obj.field(Field::new(field_name, type_ref, move |ctx| {
					let key = key.clone();
					FieldFuture::new(async move {
						let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
						let v = parent.get(&key);
						if native {
							Ok(v.and_then(|v| v.as_i64()).map(|n| GqlValue::Number(n.into())))
						} else {
							Ok(v.and_then(|v| v.as_str()).map(|s| GqlValue::String(s.to_string())))
						}
					})
				}));
			}
			builder = builder.register(obj);
		}
	}

	// ── DistinctCount: one BigInt field per public column ─────────────────────
	{
		let sub_type_name = format!("{type_name}DistinctCountAggregates");
		let mut obj = Object::new(&sub_type_name);
		for col in table.public_columns() {
			let field_name = to_camel_case(&col.name);
			let key = field_name.clone();
			obj = obj.field(Field::new(field_name, TypeRef::named("BigInt"), move |ctx| {
				let key = key.clone();
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					Ok(parent
						.get(&key)
						.and_then(|v| v.as_str())
						.map(|s| GqlValue::String(s.to_string())))
				})
			}));
		}
		builder = builder.register(obj);
	}

	// ── Top-level {Entity}Aggregates type ─────────────────────────────────────
	let make_sub_field = |name: &'static str, ty_suffix: &str| -> Field {
		let sub_type = format!("{type_name}{ty_suffix}Aggregates");
		Field::new(name, TypeRef::named(&sub_type), move |ctx| {
			let name = name;
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent.get(name).cloned().map(FieldValue::owned_any))
			})
		})
	};

	let mut agg_obj = Object::new(&agg_type_name)
		.field(Field::new("count", TypeRef::named_nn("BigInt"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent
					.get("count")
					.and_then(|v| v.as_str())
					.map(|s| GqlValue::String(s.to_string())))
			})
		}))
		.field(make_sub_field("distinctCount", "DistinctCount"));

	if has_numeric {
		agg_obj = agg_obj
			.field(make_sub_field("sum", "Sum"))
			.field(make_sub_field("min", "Min"))
			.field(make_sub_field("max", "Max"))
			.field(make_sub_field("average", "Average"))
			.field(make_sub_field("stddevSample", "StddevSample"))
			.field(make_sub_field("stddevPopulation", "StddevPopulation"))
			.field(make_sub_field("varianceSample", "VarianceSample"))
			.field(make_sub_field("variancePopulation", "VariancePopulation"));
	}

	builder = builder.register(agg_obj);
	(builder, agg_type_name, numeric_cols, all_cols)
}

/// Returns `true` if the PostgreSQL column type represents a timestamp — eligible
/// for time-truncation `groupBy` variants (`_TRUNCATED_TO_HOUR`, `_TRUNCATED_TO_DAY`).
fn is_timestamp_col(pg_type: &str, udt_name: &str) -> bool {
	matches!(pg_type, "timestamp without time zone" | "timestamp with time zone")
		|| matches!(udt_name, "timestamp" | "timestamptz")
}

/// Register the `{TypeName}GroupBy` enum and `{TypeName}AggregateGroup` object type
/// for `groupedAggregates` support.
///
/// For timestamp/timestamptz columns, additional time-truncation enum values are added:
/// `{COL}_TRUNCATED_TO_HOUR` and `{COL}_TRUNCATED_TO_DAY`.
///
/// Returns `(builder, agg_group_type_name, group_by_enum_name)`.
pub fn register_grouped_aggregate_types(
	table: &TableInfo,
	mut builder: SchemaBuilder,
) -> (SchemaBuilder, String, String) {
	let type_name = to_pascal_case(&singularize(&table.name));
	let group_by_enum_name = format!("{type_name}GroupBy");
	let agg_group_type_name = format!("{type_name}AggregateGroup");

	let has_numeric = table.public_columns().any(|c| pg_type_to_graphql(&c.pg_type, &c.udt_name).1);

	// ── {TypeName}GroupBy enum ─────────────────────────────────────────────
	let mut group_by_enum = Enum::new(&group_by_enum_name);
	for col in table.public_columns() {
		let upper = to_screaming_snake(&col.name);
		group_by_enum = group_by_enum.item(EnumItem::new(upper.clone()));
		// For timestamp columns, add time-truncation variants.
		if is_timestamp_col(&col.pg_type, &col.udt_name) {
			group_by_enum = group_by_enum.item(EnumItem::new(format!("{upper}_TRUNCATED_TO_HOUR")));
			group_by_enum = group_by_enum.item(EnumItem::new(format!("{upper}_TRUNCATED_TO_DAY")));
		}
	}
	builder = builder.register(group_by_enum);

	// ── {TypeName}AggregateGroup object type ──────────────────────────────
	let make_agg_sub_field = |name: &'static str, ty_suffix: &str| -> Field {
		let sub_type = format!("{type_name}{ty_suffix}Aggregates");
		Field::new(name, TypeRef::named(&sub_type), move |ctx| {
			let name = name;
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent.get(name).cloned().map(FieldValue::owned_any))
			})
		})
	};

	let mut agg_group_obj = Object::new(&agg_group_type_name).field(Field::new(
		"keys",
		TypeRef::named_nn_list_nn(TypeRef::STRING),
		|ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				match parent.get("keys") {
					Some(serde_json::Value::Array(arr)) => {
						let list: Vec<GqlValue> =
							arr.iter().map(|v| GqlValue::String(json_val_to_string(v))).collect();
						Ok(Some(FieldValue::list(list.into_iter().map(FieldValue::value))))
					},
					_ => Ok(Some(FieldValue::list(std::iter::empty::<FieldValue>()))),
				}
			})
		},
	));

	agg_group_obj = agg_group_obj.field(make_agg_sub_field("distinctCount", "DistinctCount"));

	if has_numeric {
		agg_group_obj = agg_group_obj
			.field(make_agg_sub_field("sum", "Sum"))
			.field(make_agg_sub_field("min", "Min"))
			.field(make_agg_sub_field("max", "Max"))
			.field(make_agg_sub_field("average", "Average"))
			.field(make_agg_sub_field("stddevSample", "StddevSample"))
			.field(make_agg_sub_field("stddevPopulation", "StddevPopulation"))
			.field(make_agg_sub_field("varianceSample", "VarianceSample"))
			.field(make_agg_sub_field("variancePopulation", "VariancePopulation"));
	}

	builder = builder.register(agg_group_obj);
	(builder, agg_group_type_name, group_by_enum_name)
}

/// Convert a serde_json::Value to a String for `keys` output.
fn json_val_to_string(v: &serde_json::Value) -> String {
	match v {
		serde_json::Value::String(s) => s.clone(),
		serde_json::Value::Number(n) => n.to_string(),
		serde_json::Value::Bool(b) => b.to_string(),
		serde_json::Value::Null => String::new(),
		other => other.to_string(),
	}
}
