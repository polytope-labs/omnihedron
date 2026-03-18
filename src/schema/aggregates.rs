//! Aggregate type registration for the `async-graphql` dynamic schema.
//!
//! [`register_aggregate_types`] generates the `{Entity}Aggregates` object type
//! containing count, sum, min, max, average, and statistical aggregate fields
//! for a given table.  Only columns with numeric PostgreSQL types receive
//! sum/avg/stddev/variance fields.

use async_graphql::{Value as GqlValue, dynamic::*};

use crate::{
	introspection::{TableInfo, types::pg_type_to_graphql},
	schema::inflector::{singularize, to_camel_case, to_pascal_case},
};

/// Register all aggregate types for a given entity table.
/// Returns `(builder, aggregate_type_name, numeric_col_names_snake_case)`.
pub fn register_aggregate_types(
	table: &TableInfo,
	mut builder: SchemaBuilder,
) -> (SchemaBuilder, String, Vec<String>) {
	let type_name = to_pascal_case(&singularize(&table.name));
	let agg_type_name = format!("{type_name}Aggregates");

	let numeric_cols: Vec<String> = table
		.public_columns()
		.filter(|c| pg_type_to_graphql(&c.pg_type, &c.udt_name).1)
		.map(|c| c.name.clone())
		.collect();

	// ── Numeric sub-aggregate types (Sum, Min, Max, Average, stddev, variance) ─
	// Only register these types when the table actually has numeric columns;
	// async-graphql rejects Object types with zero fields.
	let has_numeric = !numeric_cols.is_empty();
	if has_numeric {
		for op_name in &[
			"Sum",
			"Min",
			"Max",
			"Average",
			"StddevSample",
			"StddevPopulation",
			"VarianceSample",
			"VariancePopulation",
		] {
			let sub_type_name = format!("{type_name}{op_name}Aggregates");
			let mut obj = Object::new(&sub_type_name);
			for col in &numeric_cols {
				let field_name = to_camel_case(col);
				let key = field_name.clone();
				obj = obj.field(Field::new(
					field_name,
					TypeRef::named(TypeRef::STRING),
					move |ctx| {
						let key = key.clone();
						FieldFuture::new(async move {
							let parent =
								ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
							Ok(parent
								.get(&key)
								.and_then(|v| v.as_str())
								.map(|s| GqlValue::String(s.to_string())))
						})
					},
				));
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
	// Fields forward their sub-object to the sub-aggregate type resolver.
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

	// Only add numeric aggregate fields when the table has numeric columns
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
	(builder, agg_type_name, numeric_cols)
}
