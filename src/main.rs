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

	// ── Build GraphQL schema ──────────────────────────────────────────────────
	info!("Building GraphQL schema...");
	let gql_schema = build_schema(&tables, &enums, pool.clone(), cfg.clone())?;
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

	let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

	if cfg.output_fmt == "json" {
		tracing_subscriber::registry().with(filter).with(fmt::layer().json()).init();
	} else {
		tracing_subscriber::registry().with(filter).with(fmt::layer().pretty()).init();
	}
}
