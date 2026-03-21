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

mod config;
mod db;
mod hot_reload;
mod introspection;
mod metrics;
mod resolvers;
mod schema;
mod server;
mod sql;
mod validation;

use std::sync::{
	Arc,
	atomic::{AtomicBool, AtomicUsize, Ordering},
};

use clap::Parser;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use config::{Config, DbConfig};
use db::{create_pool, discover_schema};
use hot_reload::start_schema_listener;
use introspection::{introspect_enums, introspect_schema, introspect_search_functions};
use schema::build_schema;
use server::{AppState, SchemaState, SharedSchema, build_router};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let cfg = Arc::new(Config::parse());
	setup_logging(&cfg);

	info!(version = env!("CARGO_PKG_VERSION"), "Starting omnihedron");

	// ── Database ──────────────────────────────────────────────────────────────
	// Match TS: use DB_HOST_READ for queries ONLY when subscriptions are disabled.
	// When subscriptions are enabled, all connections go to the primary (LISTEN/NOTIFY
	// only works on the primary).
	let db_cfg = DbConfig::from_env()?;
	let for_subscription = cfg.subscription;
	let pool = Arc::new(create_pool(&db_cfg, &cfg, for_subscription)?);
	if !for_subscription && db_cfg.host_read.is_some() {
		info!(
			host = %db_cfg.host_read.as_deref().unwrap_or(""),
			"Using read replica for queries"
		);
	}

	// ── Schema discovery ──────────────────────────────────────────────────────
	let schema_name = discover_schema(&pool, &cfg.name).await?;
	info!(schema = %schema_name, "Resolved PostgreSQL schema");

	// ── Introspection ─────────────────────────────────────────────────────────
	let tables = introspect_schema(&pool, &schema_name).await?;
	let enums = introspect_enums(&pool, &schema_name).await?;
	let search_fns = introspect_search_functions(&pool, &schema_name).await?;

	// ── Detect historical mode ────────────────────────────────────────────────
	let historical_arg = detect_historical_mode(&pool, &schema_name).await;
	info!(historical_arg = %historical_arg, "Historical argument name");

	// ── Build GraphQL schema ──────────────────────────────────────────────────
	let gql_schema =
		build_schema(&tables, &enums, pool.clone(), cfg.clone(), &historical_arg, &search_fns)?;
	let shared_schema: SharedSchema = Arc::new(RwLock::new(gql_schema));
	info!(
		tables = tables.len(),
		enums = enums.len(),
		search_functions = search_fns.len(),
		"GraphQL schema built"
	);

	// ── Schema state (for health checks) ─────────────────────────────────────
	let schema_state = Arc::new(SchemaState::new(schema_name.clone(), tables.len()));

	// ── Hot reload ────────────────────────────────────────────────────────────
	start_schema_listener(pool.clone(), shared_schema.clone(), cfg.clone(), schema_state.clone())
		.await;

	// ── HTTP server ───────────────────────────────────────────────────────────
	let port = cfg.port.unwrap_or(3000);
	let addr = format!("0.0.0.0:{port}");
	let listener = tokio::net::TcpListener::bind(&addr).await?;
	let actual_port = listener.local_addr()?.port();

	info!(port = actual_port, "Server listening");

	if cfg.playground {
		info!(url = %format!("http://localhost:{actual_port}/"), "GraphiQL playground available");
	}

	// ── Metrics ──────────────────────────────────────────────────────────────
	let metrics_handle = if cfg.metrics {
		let handle = metrics::init_recorder();
		metrics::start_runtime_sampler();
		// Seed pool max_size so the gauge is available before any requests arrive
		let pool_status = pool.status();
		metrics::set_db_pool_metrics(
			pool_status.size as f64,
			pool_status.available as f64,
			pool_status.max_size as f64,
		);
		info!("Prometheus metrics enabled at /metrics");
		Some(handle)
	} else {
		None
	};

	// ── Shutdown state ───────────────────────────────────────────────────────
	let shutting_down = Arc::new(AtomicBool::new(false));
	let in_flight = Arc::new(AtomicUsize::new(0));
	let shutdown_timeout = cfg.shutdown_timeout;

	let app = build_router(AppState {
		schema: shared_schema,
		cfg,
		pool: pool.clone(),
		metrics_handle,
		schema_state,
		shutting_down: shutting_down.clone(),
		in_flight: in_flight.clone(),
	});

	let shutdown_signal = {
		let shutting_down = shutting_down.clone();
		let in_flight = in_flight.clone();
		async move {
			// Wait for SIGINT or SIGTERM
			let signal_name = {
				let ctrl_c = tokio::signal::ctrl_c();
				let mut sigterm =
					tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
						.expect("failed to register SIGTERM handler");
				tokio::select! {
					_ = ctrl_c => "SIGINT",
					_ = sigterm.recv() => "SIGTERM",
				}
			};

			info!("Received {signal_name}, starting graceful shutdown");

			// Mark as shutting down — health endpoint will return 503
			shutting_down.store(true, Ordering::SeqCst);

			// Drain in-flight requests
			let current = in_flight.load(Ordering::Relaxed);
			if current > 0 {
				info!(
					in_flight = current,
					timeout_secs = shutdown_timeout,
					"Waiting for in-flight requests to complete"
				);

				let deadline =
					tokio::time::Instant::now() + std::time::Duration::from_secs(shutdown_timeout);

				loop {
					let remaining = in_flight.load(Ordering::Relaxed);
					if remaining == 0 {
						info!("All in-flight requests completed");
						break;
					}
					if tokio::time::Instant::now() >= deadline {
						tracing::warn!(
							remaining,
							"Shutdown timeout reached, {remaining} requests forcefully terminated"
						);
						break;
					}
					tokio::time::sleep(std::time::Duration::from_millis(100)).await;
				}
			}
		}
	};

	axum::serve(listener, app).with_graceful_shutdown(shutdown_signal).await?;

	info!("Server shut down");
	Ok(())
}

/// Query `_metadata` for `historicalStateEnabled`. Returns `"timestamp"` if the
/// project uses timestamp-based history, otherwise `"blockHeight"`.
async fn detect_historical_mode(pool: &deadpool_postgres::Pool, schema: &str) -> String {
	let client = match pool.get().await {
		Ok(c) => c,
		Err(_) => return "blockHeight".to_string(),
	};
	// Try all metadata tables (plain _metadata or _metadata_0x*)
	let sql = format!(
		r#"SELECT value FROM "{schema}"."_metadata" WHERE key = 'historicalStateEnabled' LIMIT 1"#,
	);
	if let Ok(row) = client.query_opt(&sql, &[]).await {
		if let Some(row) = row {
			if let Ok(val) = row.try_get::<_, serde_json::Value>(0) {
				if val.as_str() == Some("timestamp") {
					return "timestamp".to_string();
				}
			}
		}
	}
	"blockHeight".to_string()
}

fn setup_logging(cfg: &Config) {
	let level = match cfg.log_level.as_str() {
		"fatal" | "error" => "error",
		"warn" => "warn",
		"info" => "info",
		"debug" => "debug",
		"trace" => "trace",
		"silent" => "off",
		_ => "info",
	};

	// RUST_LOG takes precedence; fallback: omnihedron + tower_http at configured level
	let filter = EnvFilter::try_from_default_env()
		.unwrap_or_else(|_| EnvFilter::new(format!("omnihedron={level},tower_http={level}")));

	if cfg.output_fmt == "json" {
		tracing_subscriber::registry()
			.with(filter)
			.with(fmt::layer().json().with_file(false).with_line_number(false).with_target(false))
			.init();
	} else {
		tracing_subscriber::registry()
			.with(filter)
			.with(
				fmt::layer()
					.compact()
					.with_file(false)
					.with_line_number(false)
					.with_target(false),
			)
			.init();
	}
}
