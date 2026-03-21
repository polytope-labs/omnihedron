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

//! Schema change listener task.
//!
//! [`start_schema_listener`] spawns a long-running Tokio task that:
//! 1. Acquires a dedicated PostgreSQL connection (separate from the query pool).
//! 2. `LISTEN`s on the SubQuery schema-change channel.
//! 3. On receiving a `schema_updated` notification, re-runs full introspection and atomically
//!    replaces the live schema via the [`server::SharedSchema`] `Arc<RwLock<Schema>>`.
//! 4. Sends a keep-alive `SELECT 1` every `sl_keep_alive_interval` ms to prevent idle connection
//!    termination.

use std::{sync::Arc, time::Duration};

use async_graphql::dynamic::Schema;
use blake2::{Blake2b, Digest, digest::consts::U64};
use deadpool_postgres::Pool;
use tokio::sync::RwLock;
use tokio_postgres::AsyncMessage;
use tracing::{error, info, warn};

use crate::{
	config::Config,
	db::pool::build_tls_connector,
	introspection::{introspect_enums, introspect_schema, introspect_search_functions},
	schema::build_schema,
	server::SchemaState,
};

/// Start the schema change listener in a background task.
pub async fn start_schema_listener(
	pool: Arc<Pool>,
	schema: Arc<RwLock<Schema>>,
	cfg: Arc<Config>,
	schema_state: Arc<SchemaState>,
) {
	if cfg.disable_hot_schema {
		info!("Hot schema reload is disabled");
		return;
	}

	let channel = hash_name(&cfg.name, "schema_channel", "_metadata");

	tokio::spawn(async move {
		loop {
			match listen_for_changes(
				pool.clone(),
				schema.clone(),
				cfg.clone(),
				&channel,
				schema_state.clone(),
			)
			.await
			{
				Ok(_) => {},
				Err(e) => {
					error!(error = %e, "Schema listener error, restarting in 5s");
					tokio::time::sleep(Duration::from_secs(5)).await;
				},
			}
		}
	});
}

async fn listen_for_changes(
	pool: Arc<Pool>,
	schema: Arc<RwLock<Schema>>,
	cfg: Arc<Config>,
	channel: &str,
	schema_state: Arc<SchemaState>,
) -> anyhow::Result<()> {
	let db = crate::config::DbConfig::from_env()?;
	let tls = build_tls_connector(&cfg)?;

	let mut pg_cfg = tokio_postgres::Config::new();
	pg_cfg
		.host(&db.host)
		.port(db.port)
		.user(&db.user)
		.password(db.password.as_str())
		.dbname(&db.database);

	let (client, mut connection) = pg_cfg.connect(tls).await?;

	// Channel for async notifications delivered via the connection driver
	let (tx, mut rx) = tokio::sync::mpsc::channel::<tokio_postgres::Notification>(16);

	// Drive the connection and extract notifications via poll_message
	tokio::spawn(async move {
		loop {
			let msg = futures::future::poll_fn(|cx| connection.poll_message(cx)).await;
			match msg {
				Some(Ok(AsyncMessage::Notification(n))) =>
					if let Err(e) = tx.send(n).await {
						warn!(error = %e, "Failed to forward schema change notification");
					},
				Some(Ok(_)) => {}, // Notice or other message
				Some(Err(_)) | None => break,
			}
		}
	});

	client.execute(&format!(r#"LISTEN "{channel}""#), &[]).await?;
	info!(channel = %channel, "Listening for schema changes");

	let keep_alive_ms = cfg.sl_keep_alive_interval;
	let ka_pool = pool.clone();
	tokio::spawn(async move {
		let mut ticker = tokio::time::interval(Duration::from_millis(keep_alive_ms));
		loop {
			ticker.tick().await;
			if let Ok(c) = ka_pool.get().await {
				if let Err(e) = c.execute("SELECT 1", &[]).await {
					warn!(error = %e, "Schema listener keep-alive failed");
				}
			}
		}
	});

	loop {
		match rx.recv().await {
			Some(notif) if notif.payload() == "schema_updated" => {
				rebuild_schema(pool.clone(), schema.clone(), cfg.clone(), schema_state.clone())
					.await;
			},
			Some(_) => {}, // Different payload, ignore
			None => return Err(anyhow::anyhow!("Notification channel closed")),
		}
	}
}

async fn try_build_schema(
	pool: &Arc<Pool>,
	cfg: &Arc<Config>,
) -> anyhow::Result<async_graphql::dynamic::Schema> {
	let tables = introspect_schema(pool, &cfg.name).await?;
	let enums = introspect_enums(pool, &cfg.name).await?;
	let search_fns = introspect_search_functions(pool, &cfg.name).await?;
	// Detect historical mode on every rebuild (it may have changed).
	let hist_arg = detect_historical_mode(pool, &cfg.name).await;
	build_schema(&tables, &enums, pool.clone(), cfg.clone(), &hist_arg, &search_fns)
}

/// Query `_metadata` for `historicalStateEnabled`.
async fn detect_historical_mode(pool: &Pool, schema: &str) -> String {
	let client = match pool.get().await {
		Ok(c) => c,
		Err(_) => return "blockHeight".to_string(),
	};
	let sql = format!(
		r#"SELECT value FROM "{schema}"."_metadata" WHERE key = 'historicalStateEnabled' LIMIT 1"#,
	);
	if let Ok(Some(row)) = client.query_opt(&sql, &[]).await {
		if let Ok(val) = row.try_get::<_, serde_json::Value>(0) {
			if val.as_str() == Some("timestamp") {
				return "timestamp".to_string();
			}
		}
	}
	"blockHeight".to_string()
}

async fn rebuild_schema(
	pool: Arc<Pool>,
	schema_lock: Arc<RwLock<Schema>>,
	cfg: Arc<Config>,
	schema_state: Arc<SchemaState>,
) {
	let start = std::time::Instant::now();
	const MAX_RETRIES: u32 = 5;
	for attempt in 1..=MAX_RETRIES {
		match try_build_schema(&pool, &cfg).await {
			Ok(new_schema) => {
				*schema_lock.write().await = new_schema;
				let duration = start.elapsed().as_secs_f64();

				// Update schema state for health checks
				let table_count =
					introspect_schema(&pool, &cfg.name).await.map(|t| t.len()).unwrap_or(0);
				schema_state.update(table_count).await;

				info!(duration_ms = (duration * 1000.0) as u64, "Schema successfully rebuilt");
				return;
			},
			Err(e) => {
				error!(error = %e, attempt, "Failed to rebuild schema");
				if attempt < MAX_RETRIES {
					tokio::time::sleep(Duration::from_secs(10)).await;
				}
			},
		}
	}
	error!("Giving up schema rebuild after {MAX_RETRIES} attempts");
}

/// Compute the PostgreSQL LISTEN channel name.
///
/// Matches SubQuery's `hashName(schema, type, tableName)` TypeScript function exactly:
///   blake2AsHex(`${schema}_${tableName}_${type}`, 64).substring(0, 63)
/// where the hash is BLAKE2b-512 (64 bytes = 512 bits), output is lowercase hex.
pub fn hash_name(schema: &str, channel_type: &str, table: &str) -> String {
	let input = format!("{schema}_{table}_{channel_type}");
	let mut hasher = Blake2b::<U64>::new();
	hasher.update(input.as_bytes());
	let result = hasher.finalize();
	let hex = hex::encode(result);
	hex[..63.min(hex.len())].to_string()
}
