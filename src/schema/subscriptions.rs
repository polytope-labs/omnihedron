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

//! GraphQL subscription field registration and resolution.
//!
//! [`register_subscriptions`] adds one subscription field per entity table.
//! Each field listens on a per-table PostgreSQL `NOTIFY` channel (whose name
//! is derived from the SubQuery `hashName` function) and streams
//! `{Entity}SubscriptionPayload` events to connected clients.
//!
//! Clients may filter by `id` and/or `mutation_type` (`INSERT`, `UPDATE`,
//! `DELETE`).

use std::sync::Arc;

use async_graphql::{Value as GqlValue, dynamic::*};
use deadpool_postgres::Pool;
use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio_postgres::AsyncMessage;
use tracing::error;

use crate::{
	config::Config,
	hot_reload::listener::hash_name,
	introspection::TableInfo,
	resolvers::connection::row_to_json,
	schema::inflector::{table_to_connection_field, table_to_type_name},
};

/// Register subscription fields for every table.
/// Returns the updated builder with a "Subscription" root type added.
pub fn register_subscriptions(
	tables: &[TableInfo],
	pool: Arc<Pool>,
	cfg: Arc<Config>,
	mut builder: SchemaBuilder,
) -> SchemaBuilder {
	let mut subscription_obj = Subscription::new("Subscription");

	for table in tables {
		let type_name = table_to_type_name(&table.name);
		let field_name = table_to_connection_field(&table.name); // e.g. "transfers"
		let payload_type = format!("{type_name}SubscriptionPayload");

		// Register the payload type: { id, mutation_type, _entity }
		let entity_type = type_name.clone();
		let payload_obj = Object::new(&payload_type)
			.field(Field::new("id", TypeRef::named_nn(TypeRef::ID), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<Value>()?;
					Ok(p.get("id")
						.and_then(|v| v.as_str())
						.map(|s| GqlValue::String(s.to_string())))
				})
			}))
			.field(Field::new("mutation_type", TypeRef::named_nn("MutationType"), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<Value>()?;
					Ok(p.get("mutation_type")
						.and_then(|v| v.as_str())
						.map(|s| GqlValue::Enum(async_graphql::Name::new(s))))
				})
			}))
			.field(Field::new("_entity", TypeRef::named(&entity_type), |ctx| {
				FieldFuture::new(async move {
					let p = ctx.parent_value.try_downcast_ref::<Value>()?;
					Ok(p.get("_entity").cloned().map(FieldValue::owned_any))
				})
			}));
		builder = builder.register(payload_obj);

		// Register the subscription field
		let pool_clone = pool.clone();
		let cfg_clone = cfg.clone();
		let table_name = table.name.clone();
		let schema_name = cfg.name.clone();

		let sub_field =
			SubscriptionField::new(&field_name, TypeRef::named(&payload_type), move |ctx| {
				let pool = pool_clone.clone();
				let cfg = cfg_clone.clone();
				let table = table_name.clone();
				let schema = schema_name.clone();

				// Move ctx into the async block so the future's lifetime is bound to 'a,
				// satisfying SubscriptionFieldFuture<'a> invariance.
				SubscriptionFieldFuture::new(async move {
					// Extract filter args inside async block (ctx moved here)
					let filter_ids: Vec<String> = ctx
						.args
						.get("id")
						.and_then(|v| {
							if let GqlValue::List(list) = v.as_value() {
								Some(
									list.iter()
										.filter_map(|v| match v {
											GqlValue::String(s) => Some(s.clone()),
											_ => None,
										})
										.collect(),
								)
							} else {
								None
							}
						})
						.unwrap_or_default();

					let filter_mutations: Vec<String> = ctx
						.args
						.get("mutation")
						.and_then(|v| {
							if let GqlValue::List(list) = v.as_value() {
								Some(
									list.iter()
										.filter_map(|v| match v {
											GqlValue::Enum(name) => Some(name.as_str().to_string()),
											GqlValue::String(s) => Some(s.clone()),
											_ => None,
										})
										.collect(),
								)
							} else {
								None
							}
						})
						.unwrap_or_default();

					let channel = hash_name(&schema, "notify_channel", &table);

					let raw_stream = create_subscription_stream(
						pool,
						cfg,
						schema,
						table,
						channel,
						filter_ids,
						filter_mutations,
					)
					.await?;

					// Map serde_json::Value → FieldValue inside the 'a-scoped async block
					// so the FieldValue lifetime matches 'a (not 'static), satisfying
					// SubscriptionFieldFuture<'a>'s invariance requirement.
					let stream = raw_stream.map(|item| item.map(FieldValue::owned_any));

					Ok(stream)
				})
			})
			.argument(InputValue::new("id", TypeRef::named_nn_list(TypeRef::ID)))
			.argument(InputValue::new("mutation", TypeRef::named_nn_list("MutationType")));

		subscription_obj = subscription_obj.field(sub_field);
	}

	builder.register(subscription_obj)
}

/// Create a stream of serde_json::Value payloads from a PostgreSQL LISTEN channel.
/// The caller wraps each item in FieldValue::owned_any inside the 'a-scoped async block.
async fn create_subscription_stream(
	pool: Arc<Pool>,
	cfg: Arc<Config>,
	schema: String,
	table: String,
	channel: String,
	filter_ids: Vec<String>,
	filter_mutations: Vec<String>,
) -> anyhow::Result<impl futures_util::Stream<Item = async_graphql::Result<Value>> + Send + 'static>
{
	use crate::{config::DbConfig, db::pool::build_tls_connector};

	let db = DbConfig::from_env()?;
	let tls = build_tls_connector(&cfg)?;

	let mut pg_cfg = tokio_postgres::Config::new();
	pg_cfg
		.host(&db.host)
		.port(db.port)
		.user(&db.user)
		.password(db.password.as_str())
		.dbname(&db.database);

	let (client, mut connection) = pg_cfg.connect(tls).await?;

	// Channel for PG notifications
	let (tx, rx) = tokio::sync::mpsc::channel::<tokio_postgres::Notification>(64);

	tokio::spawn(async move {
		loop {
			let msg = futures_util::future::poll_fn(|cx| connection.poll_message(cx)).await;
			match msg {
				Some(Ok(AsyncMessage::Notification(n))) => {
					if tx.send(n).await.is_err() {
						break;
					}
				},
				Some(Ok(_)) => {},
				Some(Err(e)) => {
					error!(error = %e, "Subscription PG connection error");
					break;
				},
				None => break,
			}
		}
	});

	client.execute(&format!(r#"LISTEN "{}""#, channel), &[]).await?;

	// Convert the mpsc receiver into a stream
	let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

	let schema_clone = schema.clone();
	let table_clone = table.clone();

	let mapped = stream.filter_map(move |notif| {
		let pool = pool.clone();
		let schema = schema_clone.clone();
		let table = table_clone.clone();
		let filter_ids = filter_ids.clone();
		let filter_mutations = filter_mutations.clone();

		async move {
			handle_notification(notif, pool, schema, table, filter_ids, filter_mutations).await
		}
	});

	Ok(mapped)
}

async fn handle_notification(
	notif: tokio_postgres::Notification,
	pool: Arc<Pool>,
	schema: String,
	table: String,
	filter_ids: Vec<String>,
	filter_mutations: Vec<String>,
) -> Option<async_graphql::Result<Value>> {
	let payload: Value = match serde_json::from_str(notif.payload()) {
		Ok(v) => v,
		Err(e) => {
			error!(error = %e, "Failed to parse subscription notification payload");
			return None;
		},
	};

	let id = payload.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
	let mutation_type =
		payload.get("mutation_type").and_then(|v| v.as_str()).unwrap_or("").to_string();

	// Apply filters
	if !filter_ids.is_empty() && !filter_ids.contains(&id) {
		return None;
	}
	if !filter_mutations.is_empty() && !filter_mutations.contains(&mutation_type) {
		return None;
	}

	// Fetch current entity state (or use _entity from payload if present)
	let entity = if let Some(entity_val) = payload.get("_entity").filter(|v| !v.is_null()) {
		entity_val.clone()
	} else if mutation_type != "DELETE" {
		// Fetch from DB
		match fetch_entity(&pool, &schema, &table, &id).await {
			Ok(Some(row)) => row,
			Ok(None) => Value::Null,
			Err(e) => {
				error!(error = %e, "Failed to fetch entity for subscription");
				Value::Null
			},
		}
	} else {
		Value::Null
	};

	let result = json!({
		"id": id,
		"mutation_type": mutation_type,
		"_entity": entity,
	});

	Some(Ok(result))
}

async fn fetch_entity(
	pool: &Pool,
	schema: &str,
	table: &str,
	id: &str,
) -> anyhow::Result<Option<Value>> {
	let client = pool.get().await?;
	let sql = format!(r#"SELECT * FROM "{schema}"."{table}" WHERE id = $1 LIMIT 1"#);
	let rows = client.query(&sql, &[&id]).await?;
	Ok(rows.first().map(row_to_json))
}
