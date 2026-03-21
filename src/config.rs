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

use clap::Parser;

/// Omnihedron — high-performance GraphQL query service for SubQuery indexers
#[derive(Parser, Debug, Clone)]
#[command(
	name = "omnihedron",
	version,
	about = "High-performance GraphQL query service for SubQuery indexers"
)]
pub struct Config {
	// ── Project ────────────────────────────────────────────────────────────────
	/// PostgreSQL schema name (project name)
	#[arg(short = 'n', long, env = "OMNIHEDRON_NAME")]
	pub name: String,

	// ── Server ─────────────────────────────────────────────────────────────────
	/// Port to listen on
	#[arg(short = 'p', long, env = "OMNIHEDRON_PORT")]
	pub port: Option<u16>,

	/// Enable GraphiQL playground
	#[arg(long, env = "OMNIHEDRON_PLAYGROUND", default_value_t = false)]
	pub playground: bool,

	/// Enable GraphQL subscriptions
	#[arg(long, env = "OMNIHEDRON_SUBSCRIPTION", default_value_t = false)]
	pub subscription: bool,

	// ── Query limits ───────────────────────────────────────────────────────────
	/// Max records returned per entity query (default: 100)
	#[arg(long, env = "OMNIHEDRON_QUERY_LIMIT", default_value_t = 100)]
	pub query_limit: usize,

	/// Max queries allowed in a single batch request
	#[arg(long, env = "OMNIHEDRON_QUERY_BATCH_LIMIT")]
	pub query_batch_limit: Option<usize>,

	/// Max query nesting depth
	#[arg(long, env = "OMNIHEDRON_QUERY_DEPTH_LIMIT")]
	pub query_depth_limit: Option<usize>,

	/// Max field aliases per query
	#[arg(long, env = "OMNIHEDRON_QUERY_ALIAS_LIMIT")]
	pub query_alias_limit: Option<usize>,

	/// Max query complexity score
	#[arg(long, env = "OMNIHEDRON_QUERY_COMPLEXITY")]
	pub query_complexity: Option<usize>,

	/// Query execution timeout in milliseconds (default: 10000)
	#[arg(long, env = "OMNIHEDRON_QUERY_TIMEOUT", default_value_t = 10_000)]
	pub query_timeout: u64,

	/// Disable all query limit protections
	#[arg(long, env = "OMNIHEDRON_UNSAFE", default_value_t = false)]
	pub unsafe_mode: bool,

	// ── Features ───────────────────────────────────────────────────────────────
	/// Enable aggregation queries (default: true)
	#[arg(long, env = "OMNIHEDRON_AGGREGATE", default_value_t = true)]
	pub aggregate: bool,

	/// Enable dictionary optimisation for distinct queries
	#[arg(long, env = "OMNIHEDRON_DICTIONARY_OPTIMISATION", default_value_t = false)]
	pub dictionary_optimisation: bool,

	/// Log SQL EXPLAIN output for each query
	#[arg(long, env = "OMNIHEDRON_QUERY_EXPLAIN", default_value_t = false)]
	pub query_explain: bool,

	/// URL of the indexer service for metadata fallback
	#[arg(long, env = "OMNIHEDRON_INDEXER")]
	pub indexer: Option<String>,

	// ── Observability ─────────────────────────────────────────────────────────
	/// Enable Prometheus metrics endpoint at /metrics
	#[arg(long, env = "OMNIHEDRON_METRICS", default_value_t = true)]
	pub metrics: bool,

	/// Graceful shutdown timeout in seconds (default: 30)
	#[arg(long, env = "OMNIHEDRON_SHUTDOWN_TIMEOUT", default_value_t = 30)]
	pub shutdown_timeout: u64,

	// ── Schema reload ──────────────────────────────────────────────────────────
	/// Disable hot schema reload via PostgreSQL LISTEN/NOTIFY
	#[arg(long, env = "OMNIHEDRON_DISABLE_HOT_SCHEMA", default_value_t = false)]
	pub disable_hot_schema: bool,

	/// Schema listener keep-alive interval in milliseconds (default: 180000)
	#[arg(long, env = "OMNIHEDRON_SL_KEEP_ALIVE_INTERVAL", default_value_t = 180_000)]
	pub sl_keep_alive_interval: u64,

	// ── Database ───────────────────────────────────────────────────────────────
	/// PostgreSQL max connection pool size (default: 10)
	#[arg(long, env = "OMNIHEDRON_MAX_CONNECTION", default_value_t = 10)]
	pub max_connection: usize,

	/// Path to PostgreSQL CA certificate
	#[arg(long, env = "OMNIHEDRON_PG_CA")]
	pub pg_ca: Option<String>,

	/// Path to PostgreSQL client key
	#[arg(long, env = "OMNIHEDRON_PG_KEY")]
	pub pg_key: Option<String>,

	/// Path to PostgreSQL client certificate
	#[arg(long, env = "OMNIHEDRON_PG_CERT")]
	pub pg_cert: Option<String>,

	// ── Logging ────────────────────────────────────────────────────────────────
	/// Log level: fatal|error|warn|info|debug|trace (default: info)
	#[arg(long, env = "OMNIHEDRON_LOG_LEVEL", default_value = "info")]
	pub log_level: String,

	/// Output format: json|colored (default: colored)
	#[arg(long, env = "OMNIHEDRON_OUTPUT_FMT", default_value = "colored")]
	pub output_fmt: String,

	/// Path to log file
	#[arg(long, env = "OMNIHEDRON_LOG_PATH")]
	pub log_path: Option<String>,

	/// Enable log file rotation
	#[arg(long, env = "OMNIHEDRON_LOG_ROTATE", default_value_t = false)]
	pub log_rotate: bool,
}

/// Database connection settings — read from environment variables.
#[derive(Debug, Clone)]
pub struct DbConfig {
	pub host: String,
	/// Optional read-replica host (not used for subscription connections)
	pub host_read: Option<String>,
	pub port: u16,
	pub user: String,
	pub password: String,
	pub database: String,
}

impl DbConfig {
	pub fn from_env() -> anyhow::Result<Self> {
		Ok(Self {
			host: std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into()),
			host_read: std::env::var("DB_HOST_READ").ok(),
			port: std::env::var("DB_PORT").unwrap_or_else(|_| "5432".into()).parse()?,
			user: std::env::var("DB_USER").unwrap_or_else(|_| "postgres".into()),
			password: std::env::var("DB_PASS").unwrap_or_default(),
			database: std::env::var("DB_DATABASE").unwrap_or_else(|_| "postgres".into()),
		})
	}
}
