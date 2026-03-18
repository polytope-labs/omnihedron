//! `_Metadata` and `_Metadatas` GraphQL type registration.
//!
//! Registers the bespoke SubQuery metadata types that are always present in
//! the schema regardless of the indexed project's table structure.  These types
//! are not derived from introspection — their shape is fixed by the SubQuery
//! protocol.

use async_graphql::{Value as GqlValue, dynamic::*};

fn str_field(name: &'static str) -> Field {
	Field::new(name, TypeRef::named(TypeRef::STRING), move |ctx| {
		FieldFuture::new(async move {
			let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
			Ok(parent
				.get(name)
				.and_then(|v| v.as_str())
				.map(|s| GqlValue::String(s.to_string())))
		})
	})
}

fn int_field(name: &'static str) -> Field {
	Field::new(name, TypeRef::named(TypeRef::INT), move |ctx| {
		FieldFuture::new(async move {
			let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
			Ok(parent.get(name).and_then(|v| v.as_i64()).map(|n| GqlValue::Number(n.into())))
		})
	})
}

fn bool_field(name: &'static str) -> Field {
	Field::new(name, TypeRef::named(TypeRef::BOOLEAN), move |ctx| {
		FieldFuture::new(async move {
			let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
			Ok(parent.get(name).and_then(|v| v.as_bool()).map(GqlValue::Boolean))
		})
	})
}

fn json_field(name: &'static str, ty: impl Into<String>) -> Field {
	let ty = ty.into();
	Field::new(name, TypeRef::named(ty), move |ctx| {
		let name = name;
		FieldFuture::new(async move {
			let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
			Ok(parent.get(name).cloned().and_then(|v| GqlValue::from_json(v).ok()))
		})
	})
}

/// Register the `_Metadata` type and `_Metadatas` type into the schema builder.
pub fn register_metadata_types(builder: SchemaBuilder) -> SchemaBuilder {
	let table_estimate = Object::new("TableEstimate")
		.field(Field::new("table", TypeRef::named_nn(TypeRef::STRING), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent
					.get("table")
					.and_then(|v| v.as_str())
					.map(|s| GqlValue::String(s.to_string())))
			})
		}))
		.field(Field::new("estimate", TypeRef::named_nn(TypeRef::INT), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent
					.get("estimate")
					.and_then(|v| v.as_i64())
					.map(|n| GqlValue::Number(n.into())))
			})
		}));

	let metadata = Object::new("_Metadata")
		.field(int_field("lastProcessedHeight"))
		.field(str_field("lastProcessedTimestamp"))
		.field(int_field("targetHeight"))
		.field(int_field("startHeight"))
		.field(str_field("chain"))
		.field(str_field("specName"))
		.field(str_field("genesisHash"))
		.field(str_field("evmChainId"))
		.field(int_field("lastFinalizedVerifiedHeight"))
		.field(str_field("unfinalizedBlocks"))
		.field(int_field("lastCreatedPoiHeight"))
		.field(int_field("latestSyncedPoiHeight"))
		.field(bool_field("indexerHealthy"))
		.field(str_field("indexerNodeVersion"))
		.field(str_field("queryNodeVersion"))
		.field(Field::new("rowCountEstimate", TypeRef::named_nn_list("TableEstimate"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				let arr: Vec<serde_json::Value> = parent
					.get("rowCountEstimate")
					.and_then(|v| v.as_array())
					.cloned()
					.unwrap_or_default();
				Ok(Some(FieldValue::list(arr.into_iter().map(FieldValue::owned_any))))
			})
		}))
		.field(str_field("dbSize"))
		.field(json_field("dynamicDatasources", "JSON"))
		.field(json_field("deployments", "JSON"))
		.field(str_field("historicalStateEnabled"));

	let metadatas = Object::new("_Metadatas")
		.field(Field::new("totalCount", TypeRef::named_nn(TypeRef::INT), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				let n = parent.get("totalCount").and_then(|v| v.as_i64()).unwrap_or(0);
				Ok(Some(GqlValue::Number(n.into())))
			})
		}))
		.field(Field::new("nodes", TypeRef::named_nn_list("_Metadata"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				let nodes: Vec<serde_json::Value> =
					parent.get("nodes").and_then(|v| v.as_array()).cloned().unwrap_or_default();
				Ok(Some(FieldValue::list(nodes.into_iter().map(FieldValue::owned_any))))
			})
		}));

	builder.register(table_estimate).register(metadata).register(metadatas)
}
