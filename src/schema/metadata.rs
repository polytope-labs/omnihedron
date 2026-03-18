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

	let metadatas_edge = Object::new("_MetadatasEdge")
		.field(Field::new("cursor", TypeRef::named_nn("Cursor"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent
					.get("cursor")
					.and_then(|v| v.as_str())
					.map(|s| GqlValue::String(s.to_string())))
			})
		}))
		.field(Field::new("node", TypeRef::named("_Metadata"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent.get("node").cloned().map(|v| FieldValue::owned_any(v)))
			})
		}));

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
		}))
		.field(Field::new("edges", TypeRef::named_nn_list("_MetadatasEdge"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				let edges: Vec<serde_json::Value> =
					parent.get("edges").and_then(|v| v.as_array()).cloned().unwrap_or_default();
				Ok(Some(FieldValue::list(edges.into_iter().map(FieldValue::owned_any))))
			})
		}));

	builder
		.register(table_estimate)
		.register(metadata)
		.register(metadatas_edge)
		.register(metadatas)
}
