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

use std::sync::Arc;

use crate::{
	config::Config,
	introspection::{
		model::{EnumInfo, TableInfo},
		types::pg_type_to_graphql,
	},
	resolvers,
	schema::{
		aggregates::{register_aggregate_types, register_grouped_aggregate_types},
		cursor::encode_node_id,
		filters::{filter_type_for as scalar_filter_for, register_scalar_filters},
		inflector::{
			backward_relation_field, forward_relation_field, table_to_connection_field,
			table_to_plural_type_name, table_to_single_field, table_to_type_name, to_camel_case,
			to_screaming_snake,
		},
		metadata::register_metadata_types,
		subscriptions::register_subscriptions,
	},
};
use async_graphql::{Name, Value as GqlValue, dynamic::*};
use deadpool_postgres::Pool;

/// Build the complete dynamic GraphQL schema from an introspected set of tables.
///
/// `historical_arg_name` controls whether historical tables expose a `blockHeight`
/// or `timestamp` argument. TS reads `historicalStateEnabled` from `_metadata`;
/// if `"timestamp"`, this should be `"timestamp"`, otherwise `"blockHeight"`.
pub fn build_schema(
	tables: &[TableInfo],
	enums: &[EnumInfo],
	pool: Arc<Pool>,
	cfg: Arc<Config>,
	historical_arg_name: &str,
) -> anyhow::Result<Schema> {
	// ── Register scalars ─────────────────────────────────────────────────────
	let subscription_root = if cfg.subscription { Some("Subscription") } else { None };
	let mut builder = Schema::build("Query", None, subscription_root)
		.register(Scalar::new("BigInt"))
		.register(Scalar::new("BigFloat"))
		.register(Scalar::new("Cursor"))
		.register(Scalar::new("Date"))
		.register(Scalar::new("Datetime"))
		.register(Scalar::new("JSON"));

	// ── Shared types ─────────────────────────────────────────────────────────
	builder = register_scalar_filters(builder);
	builder = register_metadata_types(builder);
	builder = register_page_info(builder);
	builder = register_mutation_type_enum(builder);
	builder = register_null_order(builder);
	builder = register_node_interface(builder);

	// ── Enum types and their filter input types ───────────────────────────────
	for enum_info in enums {
		builder = register_enum_type(enum_info, builder);
	}

	// ── Per-entity types ─────────────────────────────────────────────────────
	for table in tables {
		builder = register_table_types(table, tables, builder, pool.clone(), cfg.clone());
	}

	// ── Root Query type ───────────────────────────────────────────────────────
	// Build a table_name → type_name map for the node() resolver.
	// nodeId encodes [table_name, _id_uuid], so we decode table_name and map to the
	// GraphQL type name for FieldValue::with_type().
	let table_to_type: std::collections::HashMap<String, String> = tables
		.iter()
		.filter(|t| t.columns.iter().any(|c| c.name == "_id"))
		.map(|t| (t.name.clone(), table_to_type_name(&t.name)))
		.collect();
	let table_to_type = Arc::new(table_to_type);

	let mut query = Object::new("Query");
	for table in tables {
		let type_name = table_to_type_name(&table.name);
		let plural_type_name = table_to_plural_type_name(&table.name);
		let connection_field = table_to_connection_field(&table.name);
		let single_field = table_to_single_field(&table.name);
		let connection_type = format!("{plural_type_name}Connection");
		let filter_type = format!("{type_name}Filter");
		let orderby_enum = format!("{plural_type_name}OrderBy");
		let distinct_enum = format!("{}_distinct_enum", &table.name);
		let pool_clone = pool.clone();
		let cfg_clone = cfg.clone();
		let is_historical = table.is_historical;
		let col_names: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();

		// Build filter context for relation-aware filtering
		let filter_ctx = build_filter_context(table, tables, &cfg.name);
		let filter_ctx = Arc::new(filter_ctx);

		// Connection (list) query
		let mut conn_field = Field::new(&connection_field, TypeRef::named_nn(&connection_type), {
			let pool = pool_clone.clone();
			let table_name = table.name.clone();
			let cfg = cfg_clone.clone();
			let col_names = col_names.clone();
			let filter_ctx = filter_ctx.clone();
			move |ctx| {
				let pool = pool.clone();
				let table_name = table_name.clone();
				let cfg = cfg.clone();
				let col_names = col_names.clone();
				let filter_ctx = filter_ctx.clone();
				FieldFuture::new(async move {
					let maybe = resolvers::connection::resolve_connection_ctx(
						&ctx,
						&pool,
						&table_name,
						&cfg,
						is_historical,
						&col_names,
						&filter_ctx,
					)
					.await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			}
		})
		.argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
		.argument(InputValue::new("last", TypeRef::named(TypeRef::INT)))
		.argument(InputValue::new("after", TypeRef::named("Cursor")))
		.argument(InputValue::new("before", TypeRef::named("Cursor")))
		.argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)))
		.argument(InputValue::new("orderBy", TypeRef::named_nn_list(&orderby_enum)))
		.argument(InputValue::new("filter", TypeRef::named(&filter_type)))
		.argument(InputValue::new("distinct", TypeRef::named_nn_list(&distinct_enum)))
		.argument(InputValue::new("orderByNull", TypeRef::named("NullOrder")));

		if table.is_historical {
			conn_field = conn_field
				.argument(InputValue::new(historical_arg_name, TypeRef::named(TypeRef::STRING)));
		}

		query = query.field(conn_field);

		// Single-record query
		let pool_clone2 = pool.clone();
		let table_name2 = table.name.clone();
		let cfg_clone2 = cfg.clone();
		query = query.field(
			Field::new(&single_field, TypeRef::named(&type_name), move |ctx| {
				let pool = pool_clone2.clone();
				let table_name = table_name2.clone();
				let cfg = cfg_clone2.clone();
				FieldFuture::new(async move {
					let maybe =
						resolvers::single::resolve_single(&ctx, &pool, &table_name, &cfg).await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			})
			.argument(InputValue::new("id", TypeRef::named_nn(TypeRef::ID))),
		);

		// {entity}ByNodeId query
		let by_node_id_field = format!("{single_field}ByNodeId");
		let pool_clone3 = pool.clone();
		let table_name3 = table.name.clone();
		let cfg_clone3 = cfg.clone();
		query = query.field(
			Field::new(&by_node_id_field, TypeRef::named(&type_name), move |ctx| {
				let pool = pool_clone3.clone();
				let table_name = table_name3.clone();
				let cfg = cfg_clone3.clone();
				FieldFuture::new(async move {
					let maybe =
						resolvers::single::resolve_by_node_id(&ctx, &pool, &table_name, &cfg)
							.await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			})
			.argument(InputValue::new("nodeId", TypeRef::named_nn(TypeRef::ID))),
		);
	}

	// ── PostGraphile root compatibility fields ────────────────────────────────
	// `query: Query` — PostGraphile exposes the root Query as a self-referential
	// field for compatibility with some GraphQL clients.
	query = query.field(Field::new("query", TypeRef::named_nn("Query"), |_ctx| {
		FieldFuture::new(async move { Ok(Some(FieldValue::owned_any(serde_json::Value::Null))) })
	}));

	// `nodeId: ID` — PostGraphile exposes a root nodeId field (returns null at root).
	query = query.field(Field::new("nodeId", TypeRef::named(TypeRef::ID), |_ctx| {
		FieldFuture::new(async move { Ok(None::<FieldValue>) })
	}));

	// ── Anchor scalar filter types that have no column references ─────────────
	// async-graphql prunes registered input types that are not reachable from
	// the schema root.  PostGraphile always includes all scalar filter types;
	// reference BigIntFilter via an internal field argument so it survives.
	query = query.field(
		Field::new("_bigIntFilters", TypeRef::named(TypeRef::BOOLEAN), |_ctx| {
			FieldFuture::new(async move { Ok(None::<FieldValue>) })
		})
		.argument(InputValue::new("filter", TypeRef::named("BigIntFilter"))),
	);

	// ── node(nodeId: ID!): Node root query ───────────────────────────────────
	// Decodes the PostGraphile-compatible nodeId [table_name, _id_uuid], maps
	// table_name → GraphQL TypeName, fetches by _id, and returns with the
	// concrete type so inline fragments (`... on AssetTeleported { id }`) work.
	{
		let pool_node = pool.clone();
		let cfg_node = cfg.clone();
		let table_to_type_node = table_to_type.clone();
		query = query.field(
			Field::new("node", TypeRef::named("Node"), move |ctx| {
				let pool = pool_node.clone();
				let cfg = cfg_node.clone();
				let table_to_type = table_to_type_node.clone();
				FieldFuture::new(async move {
					use crate::schema::cursor::decode_node_id;
					let node_id_str = ctx
						.args
						.get("nodeId")
						.and_then(|v| v.string().ok())
						.map(str::to_string)
						.ok_or_else(|| {
							async_graphql::Error::new("Missing required argument: nodeId")
						})?;
					let (table_name, pk_val) = decode_node_id(&node_id_str)
						.map_err(|e| async_graphql::Error::new(format!("Invalid nodeId: {e}")))?;
					let type_name = table_to_type.get(&table_name).cloned().ok_or_else(|| {
						async_graphql::Error::new(format!("Unknown table: {table_name}"))
					})?;
					let pk_str = match &pk_val {
						serde_json::Value::String(s) => s.clone(),
						serde_json::Value::Number(n) => n.to_string(),
						other => other.to_string(),
					};
					let schema = &cfg.name;
					let sql = format!(
						r#"SELECT * FROM "{schema}"."{table_name}" AS t WHERE t."_id"::text = $1 LIMIT 1"#
					);
					let client = pool.get().await?;
					use tokio_postgres::types::ToSql;
					let params: Vec<Box<dyn ToSql + Sync + Send>> = vec![Box::new(pk_str)];
					let pg_refs: Vec<&(dyn ToSql + Sync)> =
						params.iter().map(|p| p.as_ref() as &(dyn ToSql + Sync)).collect();
					let rows = client.query(&sql, &pg_refs).await?;
					if rows.is_empty() {
						return Ok(None);
					}
					use crate::resolvers::connection::row_to_json;
					let row_val = row_to_json(&rows[0]);
					Ok(Some(FieldValue::with_type(FieldValue::owned_any(row_val), type_name)))
				})
			})
			.argument(InputValue::new("nodeId", TypeRef::named_nn(TypeRef::ID))),
		);
	}

	// ── _metadata queries ─────────────────────────────────────────────────────
	{
		let pool_m = pool.clone();
		let cfg_m = cfg.clone();
		query = query.field(
			Field::new("_metadata", TypeRef::named("_Metadata"), move |ctx| {
				let pool = pool_m.clone();
				let cfg = cfg_m.clone();
				FieldFuture::new(async move {
					let maybe = resolvers::metadata::resolve_metadata(&ctx, &pool, &cfg).await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			})
			.argument(InputValue::new("chainId", TypeRef::named(TypeRef::STRING))),
		);

		let pool_m2 = pool.clone();
		let cfg_m2 = cfg.clone();
		query = query.field(
			Field::new("_metadatas", TypeRef::named_nn("_Metadatas"), move |ctx| {
				let pool = pool_m2.clone();
				let cfg = cfg_m2.clone();
				FieldFuture::new(async move {
					let maybe = resolvers::metadata::resolve_metadatas(&ctx, &pool, &cfg).await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			})
			.argument(InputValue::new("after", TypeRef::named("Cursor")))
			.argument(InputValue::new("before", TypeRef::named("Cursor"))),
		);
	}

	builder = builder.register(query);

	// ── Subscriptions (optional) ──────────────────────────────────────────────
	if cfg.subscription {
		builder = register_subscriptions(tables, pool.clone(), cfg.clone(), builder);
	}

	builder.finish().map_err(|e| anyhow::anyhow!("Schema build error: {e}"))
}

// ── Helper: PageInfo ─────────────────────────────────────────────────────────

fn register_page_info(builder: SchemaBuilder) -> SchemaBuilder {
	builder.register(
		Object::new("PageInfo")
			.field(Field::new("startCursor", TypeRef::named("Cursor"), |ctx| {
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					Ok(parent
						.get("startCursor")
						.and_then(|v| v.as_str().map(|s| GqlValue::String(s.to_string()))))
				})
			}))
			.field(Field::new("endCursor", TypeRef::named("Cursor"), |ctx| {
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					Ok(parent
						.get("endCursor")
						.and_then(|v| v.as_str().map(|s| GqlValue::String(s.to_string()))))
				})
			}))
			.field(Field::new("hasNextPage", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					let v = parent.get("hasNextPage").and_then(|v| v.as_bool()).unwrap_or(false);
					Ok(Some(GqlValue::Boolean(v)))
				})
			}))
			.field(Field::new("hasPreviousPage", TypeRef::named_nn(TypeRef::BOOLEAN), |ctx| {
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					let v =
						parent.get("hasPreviousPage").and_then(|v| v.as_bool()).unwrap_or(false);
					Ok(Some(GqlValue::Boolean(v)))
				})
			})),
	)
}

fn register_mutation_type_enum(builder: SchemaBuilder) -> SchemaBuilder {
	builder.register(
		Enum::new("MutationType")
			.item(EnumItem::new("INSERT"))
			.item(EnumItem::new("UPDATE"))
			.item(EnumItem::new("DELETE")),
	)
}

fn register_null_order(builder: SchemaBuilder) -> SchemaBuilder {
	builder.register(
		Enum::new("NullOrder")
			.item(EnumItem::new("NULLS_FIRST"))
			.item(EnumItem::new("NULLS_LAST")),
	)
}

fn register_node_interface(builder: SchemaBuilder) -> SchemaBuilder {
	// PostGraphile's Node interface uses `id: ID!` where `id` is the base64 nodeId.
	// In omnihedron, this is exposed as `nodeId: ID!` on every entity type.
	builder.register(
		Interface::new("Node").field(InterfaceField::new("nodeId", TypeRef::named_nn(TypeRef::ID))),
	)
}

/// Register a dynamic enum type and its corresponding `{Name}Filter` input type.
fn register_enum_type(enum_info: &EnumInfo, mut builder: SchemaBuilder) -> SchemaBuilder {
	// ── Enum type ────────────────────────────────────────────────────────────
	let mut gql_enum = Enum::new(&enum_info.display_name);
	for value in &enum_info.values {
		gql_enum = gql_enum.item(EnumItem::new(value));
	}
	builder = builder.register(gql_enum);

	// ── Filter input type ────────────────────────────────────────────────────
	let filter_name = format!("{}Filter", enum_info.display_name);
	let enum_type_name = enum_info.display_name.clone();
	let filter_obj = InputObject::new(&filter_name)
		.field(InputValue::new("equalTo", TypeRef::named(&enum_type_name)))
		.field(InputValue::new("notEqualTo", TypeRef::named(&enum_type_name)))
		.field(InputValue::new("isNull", TypeRef::named(TypeRef::BOOLEAN)))
		.field(InputValue::new("in", TypeRef::named_nn_list(&enum_type_name)))
		.field(InputValue::new("notIn", TypeRef::named_nn_list(&enum_type_name)));
	builder = builder.register(filter_obj);

	builder
}

// ── Per-entity type registration ─────────────────────────────────────────────

fn register_table_types(
	table: &TableInfo,
	all_tables: &[TableInfo],
	mut builder: SchemaBuilder,
	pool: Arc<Pool>,
	cfg: Arc<Config>,
) -> SchemaBuilder {
	let type_name = table_to_type_name(&table.name);
	let plural_type_name = table_to_plural_type_name(&table.name);
	let filter_type_name = format!("{type_name}Filter");
	let orderby_enum_name = format!("{plural_type_name}OrderBy");
	let distinct_enum_name = format!("{}_distinct_enum", &table.name);
	let connection_type_name = format!("{plural_type_name}Connection");
	let edge_type_name = format!("{plural_type_name}Edge");

	// ── Entity type ────────────────────────────────────────────────────────
	// Only implement the Node interface for entities that have an `id` column —
	// the interface requires `id: ID!` which tables like `_global` (no id column) lack.
	let has_id_col = table.public_columns().any(|c| c.name == "id");
	let mut entity_obj = if has_id_col {
		Object::new(&type_name).implement("Node")
	} else {
		Object::new(&type_name)
	};

	// nodeId: PostGraphile-compatible computed field.
	// Format: base64(["table_name", _id_uuid]) — matches PostGraphile exactly.
	// `_id` is always fetched in the SELECT (see filter_columns_by_request rule 2).
	// Falls back to base64(["table_name", id_string]) for tables without `_id`.
	{
		let has_internal_id = table.columns.iter().any(|c| c.name == "_id");
		let table_name_for_node = table.name.clone();
		let fallback_pk = if has_id_col {
			"id".to_string()
		} else {
			table.primary_keys.first().cloned().unwrap_or_else(|| "id".to_string())
		};
		entity_obj =
			entity_obj.field(Field::new("nodeId", TypeRef::named_nn(TypeRef::ID), move |ctx| {
				let table_name = table_name_for_node.clone();
				let fallback = fallback_pk.clone();
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					let pk_val = if has_internal_id {
						parent.get("_id").cloned().unwrap_or(serde_json::Value::Null)
					} else {
						parent.get(&fallback).cloned().unwrap_or(serde_json::Value::Null)
					};
					let node_id = encode_node_id(&table_name, &pk_val);
					Ok(Some(GqlValue::String(node_id)))
				})
			}));
	}

	for col in table.public_columns() {
		let (base_gql_type, _) = pg_type_to_graphql(&col.pg_type, &col.udt_name);
		// For enum columns use the resolved display name; fall back to scalar mapping.
		let gql_type: &str = col.enum_display_name.as_deref().unwrap_or(base_gql_type);
		let is_enum = col.enum_display_name.is_some();
		let field_name = to_camel_case(&col.name);
		let type_ref =
			if col.is_nullable { TypeRef::named(gql_type) } else { TypeRef::named_nn(gql_type) };
		let col_name = col.name.clone();
		entity_obj = entity_obj.field(Field::new(field_name, type_ref, move |ctx| {
			let col = col_name.clone();
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				if is_enum {
					// async-graphql requires GqlValue::Enum for enum fields.
					Ok(parent
						.get(&col)
						.and_then(|v| v.as_str())
						.map(|s| GqlValue::Enum(Name::new(s))))
				} else {
					Ok(json_field_to_gql_value(parent, &col))
				}
			})
		}));
	}

	// ── Forward relation fields (FK → single related record) ───────────────
	for fk in &table.foreign_keys {
		let related_type = table_to_type_name(&fk.foreign_table);
		let field_name = forward_relation_field(&fk.column); // e.g. author (from author_id)
		let fk_col = fk.column.clone();
		let foreign_table = fk.foreign_table.clone();
		let pool_clone = pool.clone();
		// Determine whether the related table is historical so the resolver can
		// apply `_block_range` filtering when a blockHeight is inherited.
		let foreign_is_historical = all_tables
			.iter()
			.find(|t| t.name == fk.foreign_table)
			.map(|t| t.is_historical)
			.unwrap_or(false);

		entity_obj =
			entity_obj.field(Field::new(field_name, TypeRef::named(&related_type), move |ctx| {
				let pool = pool_clone.clone();
				let fk_col = fk_col.clone();
				let foreign_table = foreign_table.clone();
				FieldFuture::new(async move {
					let maybe = resolvers::relations::resolve_forward_relation(
						&ctx,
						&pool,
						&foreign_table,
						&fk_col,
						foreign_is_historical,
					)
					.await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			}));
	}

	// ── Backward relation fields (child → parent connection or single) ────
	for other_table in all_tables {
		for fk in &other_table.foreign_keys {
			if fk.foreign_table == table.name {
				let child_type_name = table_to_type_name(&other_table.name);
				let child_plural_type_name = table_to_plural_type_name(&other_table.name);
				let field_name = backward_relation_field(&other_table.name, &fk.column);
				let child_table = other_table.name.clone();
				let fk_col = fk.column.clone();
				let pool_clone = pool.clone();
				let cfg_clone = cfg.clone();
				let child_is_historical = other_table.is_historical;

				// One-to-one: FK column has a unique constraint → single record field
				let is_unique = other_table.is_column_unique(&fk.column);

				if is_unique {
					// Single record backward relation (one-to-one)
					entity_obj = entity_obj.field(Field::new(
						field_name,
						TypeRef::named(&child_type_name),
						move |ctx| {
							let pool = pool_clone.clone();
							let child_table = child_table.clone();
							let fk_col = fk_col.clone();
							FieldFuture::new(async move {
								let maybe = resolvers::relations::resolve_backward_single(
									&ctx,
									&pool,
									&child_table,
									&fk_col,
									child_is_historical,
								)
								.await?;
								Ok(maybe.map(FieldValue::owned_any))
							})
						},
					));
				} else {
					// Many backward relation (one-to-many) → connection
					let child_conn_type = format!("{child_plural_type_name}Connection");
					let child_filter_type = format!("{child_type_name}Filter");
					let child_orderby_enum = format!("{child_plural_type_name}OrderBy");
					let child_distinct_enum = format!("{}_distinct_enum", &other_table.name);

					entity_obj = entity_obj.field(
						Field::new(field_name, TypeRef::named_nn(&child_conn_type), move |ctx| {
							let pool = pool_clone.clone();
							let child_table = child_table.clone();
							let fk_col = fk_col.clone();
							let cfg = cfg_clone.clone();
							FieldFuture::new(async move {
								let maybe = resolvers::relations::resolve_backward_relation(
									&ctx,
									&pool,
									&child_table,
									&fk_col,
									child_is_historical,
									&cfg,
								)
								.await?;
								Ok(maybe.map(FieldValue::owned_any))
							})
						})
						.argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
						.argument(InputValue::new("last", TypeRef::named(TypeRef::INT)))
						.argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)))
						.argument(InputValue::new("after", TypeRef::named("Cursor")))
						.argument(InputValue::new("before", TypeRef::named("Cursor")))
						.argument(InputValue::new(
							"orderBy",
							TypeRef::named_nn_list(&child_orderby_enum),
						))
						.argument(InputValue::new("filter", TypeRef::named(&child_filter_type)))
						.argument(InputValue::new(
							"distinct",
							TypeRef::named_nn_list(&child_distinct_enum),
						))
						.argument(InputValue::new("orderByNull", TypeRef::named("NullOrder"))),
					);
				}
			}
		}
	}

	// ── Many-to-many relation fields (via junction tables) ───────────────
	for junction in all_tables {
		if !junction.is_junction_table() {
			continue;
		}
		let fks = &junction.foreign_keys;
		// For each pair of FKs in the junction table, if one points to this table,
		// the other is the target. Register a shortcut field on this entity.
		for (i, fk) in fks.iter().enumerate() {
			if fk.foreign_table != table.name {
				continue;
			}
			let other_fk = &fks[1 - i]; // the other FK
			let target_plural = table_to_plural_type_name(&other_fk.foreign_table);
			let target_conn = format!("{target_plural}Connection");
			let field_name = table_to_connection_field(&other_fk.foreign_table);
			let junction_name = junction.name.clone();
			let fk_to_source = fk.column.clone();
			let fk_to_target = other_fk.column.clone();
			let target_table = other_fk.foreign_table.clone();
			let pool_clone = pool.clone();
			let cfg_clone = cfg.clone();

			entity_obj = entity_obj.field(
				Field::new(&field_name, TypeRef::named_nn(&target_conn), move |ctx| {
					let pool = pool_clone.clone();
					let junction = junction_name.clone();
					let fk_src = fk_to_source.clone();
					let fk_tgt = fk_to_target.clone();
					let target = target_table.clone();
					let cfg = cfg_clone.clone();
					FieldFuture::new(async move {
						let maybe = resolvers::relations::resolve_many_to_many(
							&ctx, &pool, &junction, &fk_src, &fk_tgt, &target, &cfg,
						)
						.await?;
						Ok(maybe.map(FieldValue::owned_any))
					})
				})
				.argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
				.argument(InputValue::new("last", TypeRef::named(TypeRef::INT)))
				.argument(InputValue::new("offset", TypeRef::named(TypeRef::INT))),
			);
		}
	}

	builder = builder.register(entity_obj);

	// ── Edge type ──────────────────────────────────────────────────────────
	let edge_obj = Object::new(&edge_type_name)
		.field(Field::new("cursor", TypeRef::named_nn("Cursor"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent
					.get("cursor")
					.and_then(|v| v.as_str())
					.map(|s| GqlValue::String(s.to_string())))
			})
		}))
		.field(Field::new("node", TypeRef::named_nn(&type_name), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent.get("node").cloned().map(FieldValue::owned_any))
			})
		}));
	builder = builder.register(edge_obj);

	// ── Connection type ────────────────────────────────────────────────────
	// The connection resolver returns serde_json::Value; nested fields use
	// try_downcast_ref::<serde_json::Value>().  For list fields we use
	// FieldValue::list() so each element is individually accessible.
	let mut conn_obj = Object::new(&connection_type_name)
		.field(Field::new("nodes", TypeRef::named_nn_list_nn(&type_name), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				let nodes: Vec<serde_json::Value> =
					parent.get("nodes").and_then(|v| v.as_array()).cloned().unwrap_or_default();
				Ok(Some(FieldValue::list(nodes.into_iter().map(FieldValue::owned_any))))
			})
		}))
		.field(Field::new("edges", TypeRef::named_nn_list_nn(&edge_type_name), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				let edges: Vec<serde_json::Value> =
					parent.get("edges").and_then(|v| v.as_array()).cloned().unwrap_or_default();
				Ok(Some(FieldValue::list(edges.into_iter().map(FieldValue::owned_any))))
			})
		}))
		.field(Field::new("pageInfo", TypeRef::named_nn("PageInfo"), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent.get("pageInfo").cloned().map(FieldValue::owned_any))
			})
		}))
		.field(Field::new("totalCount", TypeRef::named_nn(TypeRef::INT), |ctx| {
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(parent
					.get("totalCount")
					.and_then(|v| v.as_i64())
					.map(|n| GqlValue::Number(n.into())))
			})
		}));

	if cfg.aggregate {
		let (new_builder, agg_type_name, numeric_cols, all_cols) =
			register_aggregate_types(table, builder);
		builder = new_builder;

		// Clone before moving into closures so grouped aggregates can share them.
		let numeric_cols_gagg = numeric_cols.clone();
		let all_cols_gagg = all_cols.clone();

		let pool_agg = pool.clone();
		conn_obj =
			conn_obj.field(Field::new("aggregates", TypeRef::named(&agg_type_name), move |ctx| {
				let pool = pool_agg.clone();
				let num_cols = numeric_cols.clone();
				let all = all_cols.clone();
				FieldFuture::new(async move {
					let maybe =
						resolvers::aggregates::resolve_aggregates(&ctx, &pool, &num_cols, &all)
							.await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			}));

		let (new_builder, agg_group_type_name, group_by_enum_name) =
			register_grouped_aggregate_types(table, builder);
		builder = new_builder;

		let pool_gagg = pool.clone();
		conn_obj = conn_obj.field(
			Field::new(
				"groupedAggregates",
				TypeRef::named_nn_list(&agg_group_type_name),
				move |ctx| {
					let pool = pool_gagg.clone();
					let num_cols = numeric_cols_gagg.clone();
					let all = all_cols_gagg.clone();
					FieldFuture::new(async move {
						let maybe = resolvers::aggregates::resolve_grouped_aggregates(
							&ctx, &pool, &num_cols, &all,
						)
						.await?;
						match maybe {
							Some(serde_json::Value::Array(groups)) => Ok(Some(FieldValue::list(
								groups.into_iter().map(FieldValue::owned_any),
							))),
							_ => Ok(Some(FieldValue::list(std::iter::empty::<FieldValue>()))),
						}
					})
				},
			)
			.argument(InputValue::new("groupBy", TypeRef::named_nn_list(&group_by_enum_name))),
		);
	}

	builder = builder.register(conn_obj);

	// ── Filter input type ──────────────────────────────────────────────────
	let mut filter_obj = InputObject::new(&filter_type_name)
		.field(InputValue::new("and", TypeRef::named_nn_list(&filter_type_name)))
		.field(InputValue::new("or", TypeRef::named_nn_list(&filter_type_name)))
		.field(InputValue::new("not", TypeRef::named(&filter_type_name)));

	for col in table.public_columns() {
		let (base_gql_type, _) = pg_type_to_graphql(&col.pg_type, &col.udt_name);
		let field_name = to_camel_case(&col.name);
		let field_filter: String = if let Some(display_name) = &col.enum_display_name {
			format!("{display_name}Filter")
		} else {
			scalar_filter_for(base_gql_type).to_string()
		};
		filter_obj = filter_obj.field(InputValue::new(field_name, TypeRef::named(field_filter)));
	}

	// ── Forward relation filters + Exists filters ─────────────────────────
	for fk in &table.foreign_keys {
		let foreign_type = table_to_type_name(&fk.foreign_table);
		let fk_field_name = forward_relation_field(&fk.column);
		// Forward filter: e.g. `testAuthors: TestAuthorFilter`
		let foreign_filter = format!("{foreign_type}Filter");
		filter_obj =
			filter_obj.field(InputValue::new(&fk_field_name, TypeRef::named(&foreign_filter)));
		// Exists filter: e.g. `testAuthorsExists: Boolean`
		let exists_field = format!("{fk_field_name}Exists");
		filter_obj =
			filter_obj.field(InputValue::new(&exists_field, TypeRef::named(TypeRef::BOOLEAN)));
	}

	// ── Backward relation (ToMany) filters ────────────────────────────────
	for other_table in all_tables {
		for fk in &other_table.foreign_keys {
			if fk.foreign_table == table.name {
				let child_type = table_to_type_name(&other_table.name);
				let child_filter = format!("{child_type}Filter");
				let to_many_filter_name = format!("{type_name}ToMany{child_type}Filter");
				let rel_field_name = backward_relation_field(&other_table.name, &fk.column);

				// Register the ToMany filter input type (some/none/every)
				let to_many_obj = InputObject::new(&to_many_filter_name)
					.field(InputValue::new("some", TypeRef::named(&child_filter)))
					.field(InputValue::new("none", TypeRef::named(&child_filter)))
					.field(InputValue::new("every", TypeRef::named(&child_filter)));
				builder = builder.register(to_many_obj);

				filter_obj = filter_obj
					.field(InputValue::new(&rel_field_name, TypeRef::named(&to_many_filter_name)));
			}
		}
	}

	builder = builder.register(filter_obj);

	// ── OrderBy enum ───────────────────────────────────────────────────────
	let mut orderby = Enum::new(&orderby_enum_name).item(EnumItem::new("NATURAL"));
	for col in table.public_columns() {
		let upper = to_screaming_snake(&col.name);
		orderby = orderby
			.item(EnumItem::new(format!("{upper}_ASC")))
			.item(EnumItem::new(format!("{upper}_DESC")));
	}

	// ── Aggregate orderBy enum values (PgOrderByAggregatesPlugin) ─────────
	// For each backward relation, add COUNT and per-numeric-column aggregate variants.
	for other_table in all_tables {
		for fk in &other_table.foreign_keys {
			if fk.foreign_table == table.name {
				let child_upper = to_screaming_snake(&other_table.name);
				let fk_upper = to_screaming_snake(&fk.column);
				let prefix = format!("{child_upper}_BY_{fk_upper}");

				// Count aggregate
				orderby = orderby
					.item(EnumItem::new(format!("{prefix}_COUNT_ASC")))
					.item(EnumItem::new(format!("{prefix}_COUNT_DESC")));

				// Per-numeric-column aggregates
				for col in other_table.public_columns() {
					let (_, is_numeric) = pg_type_to_graphql(&col.pg_type, &col.udt_name);
					if is_numeric {
						let col_upper = to_screaming_snake(&col.name);
						for agg in &["SUM", "AVERAGE", "MIN", "MAX"] {
							orderby = orderby
								.item(EnumItem::new(format!("{prefix}_{agg}_{col_upper}_ASC")))
								.item(EnumItem::new(format!("{prefix}_{agg}_{col_upper}_DESC")));
						}
					}
				}
			}
		}
	}

	builder = builder.register(orderby);

	// ── Distinct enum ──────────────────────────────────────────────────────
	let mut distinct = Enum::new(&distinct_enum_name);
	for col in table.public_columns() {
		let upper = to_screaming_snake(&col.name);
		distinct = distinct.item(EnumItem::new(upper));
	}
	builder = builder.register(distinct);

	builder
}

// ── Filter context builder ────────────────────────────────────────────────────

fn build_filter_context(
	table: &TableInfo,
	all_tables: &[TableInfo],
	schema: &str,
) -> crate::sql::filter::FilterContext {
	use crate::sql::filter::{BackwardRelInfo, FilterContext, ForwardRelInfo};

	let mut ctx = FilterContext::default();

	// Forward relations + Exists
	for fk in &table.foreign_keys {
		let field_name = forward_relation_field(&fk.column);
		let foreign_is_historical = all_tables
			.iter()
			.find(|t| t.name == fk.foreign_table)
			.map(|t| t.is_historical)
			.unwrap_or(false);
		ctx.forward_relations.insert(
			field_name.clone(),
			ForwardRelInfo {
				schema: schema.to_string(),
				foreign_table: fk.foreign_table.clone(),
				fk_column: fk.column.clone(),
				foreign_pk: "id".to_string(),
				is_historical: foreign_is_historical,
			},
		);
		ctx.exists_fields.insert(format!("{field_name}Exists"), fk.column.clone());
	}

	// Backward relations
	for other in all_tables {
		for fk in &other.foreign_keys {
			if fk.foreign_table == table.name {
				let rel_name = backward_relation_field(&other.name, &fk.column);
				ctx.backward_relations.insert(
					rel_name,
					BackwardRelInfo {
						schema: schema.to_string(),
						child_table: other.name.clone(),
						fk_column: fk.column.clone(),
						is_historical: other.is_historical,
					},
				);
			}
		}
	}

	ctx
}

// ── Value helpers ─────────────────────────────────────────────────────────────

fn json_field_to_gql_value(row: &serde_json::Value, field: &str) -> Option<GqlValue> {
	let v = row.get(field)?.clone();
	GqlValue::from_json(v).ok()
}
