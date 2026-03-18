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
mod resolvers;
mod schema;
mod server;
mod sql;
mod validation;

use std::sync::Arc;

use clap::Parser;
use tokio::sync::RwLock;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use config::{Config, DbConfig};
use db::{create_pool, discover_schema};
use hot_reload::start_schema_listener;
use introspection::{introspect_enums, introspect_schema};
use schema::build_schema;
use server::{AppState, SharedSchema, build_router};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let cfg = Arc::new(Config::parse());
	setup_logging(&cfg);

	info!(version = env!("CARGO_PKG_VERSION"), "Starting omnihedron");

	// ── Database ──────────────────────────────────────────────────────────────
	let db_cfg = DbConfig::from_env()?;
	let pool = Arc::new(create_pool(&db_cfg, &cfg, false)?);

	// ── Schema discovery ──────────────────────────────────────────────────────
	let schema_name = discover_schema(&pool, &cfg.name).await?;
	info!(schema = %schema_name, "Resolved PostgreSQL schema");

	// ── Introspection ─────────────────────────────────────────────────────────
	info!("Introspecting database schema...");
	let tables = introspect_schema(&pool, &schema_name).await?;
	let enums = introspect_enums(&pool, &schema_name).await?;
	info!(tables = tables.len(), enums = enums.len(), "Introspection complete");

	// ── Detect historical mode ────────────────────────────────────────────────
	let historical_arg = detect_historical_mode(&pool, &schema_name).await;
	info!(historical_arg = %historical_arg, "Historical argument name");

	// ── Build GraphQL schema ──────────────────────────────────────────────────
	info!("Building GraphQL schema...");
	let gql_schema = build_schema(&tables, &enums, pool.clone(), cfg.clone(), &historical_arg)?;
	let shared_schema: SharedSchema = Arc::new(RwLock::new(gql_schema));
	info!("GraphQL schema built successfully");

	// ── Hot reload ────────────────────────────────────────────────────────────
	start_schema_listener(pool.clone(), shared_schema.clone(), cfg.clone()).await;

	// ── HTTP server ───────────────────────────────────────────────────────────
	let port = cfg.port.unwrap_or(3000);
	let addr = format!("0.0.0.0:{port}");
	let listener = tokio::net::TcpListener::bind(&addr).await?;
	let actual_port = listener.local_addr()?.port();

	info!(port = actual_port, "Server listening");

	if cfg.playground {
		info!(url = %format!("http://localhost:{actual_port}/"), "GraphiQL playground available");
	}

	let app = build_router(AppState { schema: shared_schema, cfg });

	axum::serve(listener, app).await?;
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
