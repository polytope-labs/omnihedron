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

use clap::{Args, CommandFactory, FromArgMatches, Parser, Subcommand};

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

	/// Disable historical block-range filtering (_block_range @> N).
	/// Use this when all tables are append-only and never updated, which
	/// avoids a redundant per-row range check on every query.
	#[arg(long, env = "OMNIHEDRON_NO_HISTORICAL", default_value_t = false)]
	pub no_historical: bool,

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

/// Arguments for `omnihedron healthcheck`.
#[derive(Args, Debug, Clone)]
pub struct HealthcheckArgs {
	/// Full URL to probe. Overrides --port. Defaults to http://127.0.0.1:<port>/health
	#[arg(long, env = "OMNIHEDRON_HEALTHCHECK_URL")]
	pub url: Option<String>,

	/// Port the local omnihedron server listens on (used when --url is not set)
	#[arg(short = 'p', long, env = "OMNIHEDRON_PORT", default_value_t = 3000)]
	pub port: u16,

	/// Request timeout in seconds
	#[arg(
		long,
		env = "OMNIHEDRON_HEALTHCHECK_TIMEOUT",
		default_value_t = 3,
		value_parser = clap::value_parser!(u64).range(1..)
	)]
	pub timeout: u64,
}

impl HealthcheckArgs {
	/// The URL to probe: `--url` if given, else `http://127.0.0.1:{port}/health`.
	pub fn target_url(&self) -> String {
		self.url
			.clone()
			.unwrap_or_else(|| format!("http://127.0.0.1:{}/health", self.port))
	}
}

// NOTE: clap's `Subcommand` derive lifts the enum's doc comment onto the parent
// command as its `about`, so this enum deliberately has no doc comment.
#[derive(Subcommand, Debug, Clone)]
pub enum Command {
	/// Probe a running omnihedron's /health endpoint; exits 0 on a 2xx response, 1 otherwise
	Healthcheck(HealthcheckArgs),
}

/// Parsed command line: either run the server (default, no subcommand) or a subcommand.
#[derive(Debug)]
pub enum Cli {
	Serve(Box<Config>),
	Command(Command),
}

impl Cli {
	/// The full clap command tree: the server args plus every subcommand.
	pub fn command() -> clap::Command {
		Command::augment_subcommands(Config::command())
			.subcommand_negates_reqs(true)
			.args_conflicts_with_subcommands(true)
			.disable_help_subcommand(true)
	}

	pub fn try_parse_from<I, T>(itr: I) -> Result<Self, clap::Error>
	where
		I: IntoIterator<Item = T>,
		T: Into<std::ffi::OsString> + Clone,
	{
		let mut cmd = Self::command();
		let mut matches = cmd.try_get_matches_from_mut(itr)?;
		let parsed = if matches.subcommand().is_some() {
			Command::from_arg_matches_mut(&mut matches).map(Cli::Command)
		} else {
			Config::from_arg_matches_mut(&mut matches).map(|cfg| Cli::Serve(Box::new(cfg)))
		};
		parsed.map_err(|e| e.format(&mut cmd))
	}

	pub fn parse() -> Self {
		Self::try_parse_from(std::env::args_os()).unwrap_or_else(|e| e.exit())
	}
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

#[cfg(test)]
mod tests {
	use clap::error::ErrorKind;

	use super::*;

	#[test]
	fn cli_definition_is_valid() {
		Cli::command().debug_assert();
	}

	#[test]
	fn no_subcommand_serves() {
		let cli = Cli::try_parse_from(["omnihedron", "--name", "app", "--port", "3000"]).unwrap();
		match cli {
			Cli::Serve(cfg) => {
				assert_eq!(cfg.name, "app");
				assert_eq!(cfg.port, Some(3000));
			},
			other => panic!("expected serve, got {other:?}"),
		}
	}

	#[test]
	fn healthcheck_does_not_need_a_name() {
		let cli = Cli::try_parse_from(["omnihedron", "healthcheck"]).unwrap();
		assert!(matches!(cli, Cli::Command(Command::Healthcheck(_))));
	}

	#[test]
	fn healthcheck_url_overrides_port() {
		let cli = Cli::try_parse_from([
			"omnihedron",
			"healthcheck",
			"--url",
			"http://10.0.0.1:9/health",
			"--port",
			"4000",
		])
		.unwrap();
		match cli {
			Cli::Command(Command::Healthcheck(args)) => {
				assert_eq!(args.target_url(), "http://10.0.0.1:9/health");
			},
			other => panic!("expected healthcheck, got {other:?}"),
		}
	}

	#[test]
	fn healthcheck_port_builds_url() {
		let cli = Cli::try_parse_from(["omnihedron", "healthcheck", "--port", "4000"]).unwrap();
		match cli {
			Cli::Command(Command::Healthcheck(args)) => {
				assert_eq!(args.target_url(), "http://127.0.0.1:4000/health");
			},
			other => panic!("expected healthcheck, got {other:?}"),
		}
	}

	#[test]
	fn healthcheck_timeout_must_be_positive() {
		let err = Cli::try_parse_from(["omnihedron", "healthcheck", "--timeout", "0"]).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::ValueValidation);
	}

	#[test]
	fn root_port_cannot_be_combined_with_healthcheck() {
		let err = Cli::try_parse_from(["omnihedron", "-p", "4000", "healthcheck"]).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
	}

	#[test]
	fn root_name_cannot_be_combined_with_healthcheck() {
		let err = Cli::try_parse_from(["omnihedron", "--name", "app", "healthcheck"]).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::ArgumentConflict);
	}

	#[test]
	fn version_is_displayed() {
		let err = Cli::try_parse_from(["omnihedron", "--version"]).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::DisplayVersion);
	}

	#[test]
	fn subcommands_do_not_change_the_about_text() {
		// The `Subcommand` derive lifts the enum's doc comment onto the parent;
		// make sure the program description still comes from `Config`.
		assert_eq!(Cli::command().get_about(), Config::command().get_about());
	}

	#[test]
	fn missing_name_is_an_error() {
		// The environment can satisfy `--name`; only assert when it is unset.
		if std::env::var_os("OMNIHEDRON_NAME").is_some() {
			return;
		}

		let err = Cli::try_parse_from(["omnihedron"]).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::MissingRequiredArgument);
	}
}
