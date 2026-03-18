use std::sync::Arc;

use async_graphql::{Value as GqlValue, dynamic::*};
use deadpool_postgres::Pool;
use tracing::info;

use crate::{
	config::Config,
	introspection::{
		model::{EnumInfo, TableInfo},
		types::pg_type_to_graphql,
	},
	resolvers,
	schema::{
		aggregates::register_aggregate_types,
		cursor::encode_node_id,
		filters::{filter_type_for as scalar_filter_for, register_scalar_filters},
		inflector::{
			backward_relation_field, table_to_connection_field, table_to_plural_type_name,
			table_to_single_field, table_to_type_name, to_camel_case, to_screaming_snake,
		},
		metadata::register_metadata_types,
		subscriptions::register_subscriptions,
	},
};

/// Build the complete dynamic GraphQL schema from an introspected set of tables.
pub fn build_schema(
	tables: &[TableInfo],
	enums: &[EnumInfo],
	pool: Arc<Pool>,
	cfg: Arc<Config>,
) -> anyhow::Result<Schema> {
	info!(table_count = tables.len(), "Building GraphQL schema");

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

		// Connection (list) query
		let mut conn_field = Field::new(&connection_field, TypeRef::named_nn(&connection_type), {
			let pool = pool_clone.clone();
			let table_name = table.name.clone();
			let cfg = cfg_clone.clone();
			let col_names = col_names.clone();
			move |ctx| {
				let pool = pool.clone();
				let table_name = table_name.clone();
				let cfg = cfg.clone();
				let col_names = col_names.clone();
				FieldFuture::new(async move {
					let maybe = resolvers::connection::resolve_connection(
						&ctx,
						&pool,
						&table_name,
						&cfg,
						is_historical,
						&col_names,
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
				.argument(InputValue::new("blockHeight", TypeRef::named(TypeRef::STRING)));
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
		let by_node_id_field = format!("{type_name}ByNodeId");
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

	// ── node(id) root query — anchors the Node interface in the schema ────────
	// PostGraphile exposes a `node(nodeId: ID!): Node` root field so that the
	// Node interface is always reachable from the schema root.  Our resolver
	// always returns null (we rely on per-entity single-record queries instead).
	query = query.field(
		Field::new("node", TypeRef::named("Node"), |_ctx| {
			FieldFuture::new(async move { Ok(None::<FieldValue>) })
		})
		.argument(InputValue::new("nodeId", TypeRef::named_nn(TypeRef::ID))),
	);

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
	builder.register(
		Interface::new("Node").field(InterfaceField::new("id", TypeRef::named_nn(TypeRef::ID))),
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
	let mut entity_obj = Object::new(&type_name);

	// nodeId: PostGraphile computed field — base64(["TypeName", pkValue])
	// Uses the first primary key column, falling back to "id".
	{
		let pk_col = table.primary_keys.first().cloned().unwrap_or_else(|| "id".to_string());
		let type_name_for_node = type_name.clone();
		entity_obj =
			entity_obj.field(Field::new("nodeId", TypeRef::named_nn(TypeRef::ID), move |ctx| {
				let pk = pk_col.clone();
				let type_name = type_name_for_node.clone();
				FieldFuture::new(async move {
					let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
					let pk_val = parent.get(&pk).cloned().unwrap_or(serde_json::Value::Null);
					let node_id = encode_node_id(&type_name, &pk_val);
					Ok(Some(GqlValue::String(node_id)))
				})
			}));
	}

	for col in table.public_columns() {
		let (base_gql_type, _) = pg_type_to_graphql(&col.pg_type, &col.udt_name);
		// For enum columns use the resolved display name; fall back to scalar mapping.
		let gql_type: &str = col.enum_display_name.as_deref().unwrap_or(base_gql_type);
		let field_name = to_camel_case(&col.name);
		let type_ref =
			if col.is_nullable { TypeRef::named(gql_type) } else { TypeRef::named_nn(gql_type) };
		let col_name = col.name.clone();
		entity_obj = entity_obj.field(Field::new(field_name, type_ref, move |ctx| {
			let col = col_name.clone();
			FieldFuture::new(async move {
				let parent = ctx.parent_value.try_downcast_ref::<serde_json::Value>()?;
				Ok(json_field_to_gql_value(parent, &col))
			})
		}));
	}

	// ── Forward relation fields (FK → single related record) ───────────────
	for fk in &table.foreign_keys {
		let related_type = table_to_type_name(&fk.foreign_table);
		let field_name = to_camel_case(&fk.foreign_table); // e.g. account
		let fk_col = fk.column.clone();
		let foreign_table = fk.foreign_table.clone();
		let pool_clone = pool.clone();

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
					)
					.await?;
					Ok(maybe.map(FieldValue::owned_any))
				})
			}));
	}

	// ── Backward relation fields (child → parent connection) ───────────────
	for other_table in all_tables {
		for fk in &other_table.foreign_keys {
			if fk.foreign_table == table.name {
				let child_plural_type_name = table_to_plural_type_name(&other_table.name);
				let child_conn_type = format!("{child_plural_type_name}Connection");
				let field_name = backward_relation_field(&other_table.name, &fk.column);
				let child_table = other_table.name.clone();
				let fk_col = fk.column.clone();
				let pool_clone = pool.clone();

				entity_obj = entity_obj.field(
					Field::new(field_name, TypeRef::named_nn(&child_conn_type), move |ctx| {
						let pool = pool_clone.clone();
						let child_table = child_table.clone();
						let fk_col = fk_col.clone();
						FieldFuture::new(async move {
							let maybe = resolvers::relations::resolve_backward_relation(
								&ctx,
								&pool,
								&child_table,
								&fk_col,
							)
							.await?;
							Ok(maybe.map(FieldValue::owned_any))
						})
					})
					.argument(InputValue::new("first", TypeRef::named(TypeRef::INT)))
					.argument(InputValue::new("last", TypeRef::named(TypeRef::INT)))
					.argument(InputValue::new("offset", TypeRef::named(TypeRef::INT)))
					.argument(InputValue::new("after", TypeRef::named("Cursor")))
					.argument(InputValue::new("before", TypeRef::named("Cursor"))),
				);
			}
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
		// For enum columns, the filter type is `{EnumDisplayName}Filter`.
		let field_filter: String = if let Some(display_name) = &col.enum_display_name {
			format!("{display_name}Filter")
		} else {
			scalar_filter_for(base_gql_type).to_string()
		};
		filter_obj = filter_obj.field(InputValue::new(field_name, TypeRef::named(field_filter)));
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

// ── Value helpers ─────────────────────────────────────────────────────────────

fn json_field_to_gql_value(row: &serde_json::Value, field: &str) -> Option<GqlValue> {
	let v = row.get(field)?.clone();
	GqlValue::from_json(v).ok()
}
