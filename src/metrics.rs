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

//! Optional Prometheus metrics.
//!
//! When enabled via `--metrics`, initialises a Prometheus recorder and exposes
//! the following metrics:
//!
//! - `omnihedron_http_requests_total` (counter, labels: method, path, status)
//! - `omnihedron_http_request_duration_seconds` (histogram, labels: method, path)
//! - `omnihedron_graphql_queries_total` (counter, labels: operation, type)
//! - `omnihedron_graphql_query_duration_seconds` (histogram, labels: type)
//! - `omnihedron_graphql_errors_total` (counter)
//! - `omnihedron_db_pool_size` (gauge)
//! - `omnihedron_db_pool_available` (gauge)
//! - `omnihedron_db_pool_max_size` (gauge)
//! - `omnihedron_db_pool_wait_duration_seconds` (histogram)
//! - `omnihedron_stmt_cache_hits_request_total` (counter)
//! - `omnihedron_stmt_cache_hits_connection_total` (counter)
//! - `omnihedron_stmt_cache_misses_total` (counter)
//! - `omnihedron_graphql_response_size_bytes` (histogram)
//! - `omnihedron_sql_queries_total` (counter, labels: type)
//! - `omnihedron_sql_query_duration_seconds` (histogram, labels: type)
//! - `omnihedron_connection_checkout_total` (counter, labels: result)
//! - `omnihedron_in_flight_requests` (gauge)
//! - `omnihedron_query_complexity_score` (histogram)
//! - `omnihedron_websocket_connections_active` (gauge)
//! - `omnihedron_dataloader_batch_size` (histogram)
//! - `omnihedron_process_resident_memory_bytes` (gauge)
//! - `omnihedron_process_virtual_memory_bytes` (gauge)
//! - `omnihedron_tokio_alive_tasks` (gauge)
//! - `omnihedron_tokio_num_workers` (gauge)
//! - `omnihedron_tokio_global_queue_depth` (gauge)

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};

// ── Metric name constants ────────────────────────────────────────────────────

/// Total HTTP requests served.
pub const HTTP_REQUESTS_TOTAL: &str = "omnihedron_http_requests_total";

/// HTTP request duration in seconds.
pub const HTTP_REQUEST_DURATION_SECONDS: &str = "omnihedron_http_request_duration_seconds";

/// Total GraphQL queries executed (labels: operation, type).
pub const GRAPHQL_QUERIES_TOTAL: &str = "omnihedron_graphql_queries_total";

/// GraphQL query duration in seconds (labels: type).
pub const GRAPHQL_QUERY_DURATION_SECONDS: &str = "omnihedron_graphql_query_duration_seconds";

/// Total GraphQL errors returned.
pub const GRAPHQL_ERRORS_TOTAL: &str = "omnihedron_graphql_errors_total";

/// Current database pool size.
pub const DB_POOL_SIZE: &str = "omnihedron_db_pool_size";

/// Available (idle) database pool connections.
pub const DB_POOL_AVAILABLE: &str = "omnihedron_db_pool_available";

/// Maximum configured pool size.
pub const DB_POOL_MAX_SIZE: &str = "omnihedron_db_pool_max_size";

/// Time spent waiting to acquire a pool connection.
pub const DB_POOL_WAIT_DURATION: &str = "omnihedron_db_pool_wait_duration_seconds";

/// Prepared statement cache hits (request-level: same SQL reused within one request).
pub const STMT_CACHE_HITS_REQUEST: &str = "omnihedron_stmt_cache_hits_request_total";

/// Prepared statement cache hits (connection-level: SQL already prepared on this PG connection).
pub const STMT_CACHE_HITS_CONNECTION: &str = "omnihedron_stmt_cache_hits_connection_total";

/// Prepared statement cache misses (true PG miss: statement sent to PostgreSQL for parse+plan).
pub const STMT_CACHE_MISSES: &str = "omnihedron_stmt_cache_misses_total";

/// GraphQL response body size in bytes.
pub const GRAPHQL_RESPONSE_SIZE_BYTES: &str = "omnihedron_graphql_response_size_bytes";

/// Total raw SQL queries executed (labels: type=select/count).
pub const SQL_QUERIES_TOTAL: &str = "omnihedron_sql_queries_total";

/// SQL query execution duration in seconds.
pub const SQL_QUERY_DURATION_SECONDS: &str = "omnihedron_sql_query_duration_seconds";

/// Total pool checkouts (labels: result=success/timeout).
pub const CONNECTION_CHECKOUT_TOTAL: &str = "omnihedron_connection_checkout_total";

/// Currently active in-flight requests.
pub const IN_FLIGHT_REQUESTS: &str = "omnihedron_in_flight_requests";

/// Computed query complexity scores.
pub const QUERY_COMPLEXITY_SCORE: &str = "omnihedron_query_complexity_score";

/// Active WebSocket subscription connections.
pub const WEBSOCKET_CONNECTIONS_ACTIVE: &str = "omnihedron_websocket_connections_active";

/// Dataloader batch sizes.
pub const DATALOADER_BATCH_SIZE: &str = "omnihedron_dataloader_batch_size";

/// Process resident set size in bytes.
pub const PROCESS_RESIDENT_MEMORY_BYTES: &str = "omnihedron_process_resident_memory_bytes";

/// Process virtual memory size in bytes.
pub const PROCESS_VIRTUAL_MEMORY_BYTES: &str = "omnihedron_process_virtual_memory_bytes";

/// Number of alive async tasks in the Tokio runtime.
pub const TOKIO_ALIVE_TASKS: &str = "omnihedron_tokio_alive_tasks";

/// Number of Tokio worker threads.
pub const TOKIO_NUM_WORKERS: &str = "omnihedron_tokio_num_workers";

/// Depth of the Tokio global task queue.
pub const TOKIO_GLOBAL_QUEUE_DEPTH: &str = "omnihedron_tokio_global_queue_depth";

/// Initialise the Prometheus exporter recorder and return a handle for
/// rendering metrics on the `/metrics` endpoint.
///
/// Also initialises counters that should always be present (even at 0)
/// so that Prometheus rate() expressions don't return NaN.
///
/// # Panics
///
/// Panics if a global recorder has already been installed (should only be
/// called once).
pub fn init_recorder() -> PrometheusHandle {
	let handle = PrometheusBuilder::new()
		.install_recorder()
		.expect("failed to install Prometheus recorder");

	// Pre-initialise counters so they appear in /metrics from the start.
	metrics::counter!(GRAPHQL_ERRORS_TOTAL).absolute(0);

	handle
}

/// Start a background task that periodically samples process memory and
/// Tokio runtime metrics. Called once after the recorder is installed.
pub fn start_runtime_sampler() {
	let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as u64;

	tokio::spawn(async move {
		let handle = tokio::runtime::Handle::current();
		let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
		loop {
			interval.tick().await;

			// ── Process memory from /proc/self/statm ──────────────────────
			if let Ok(statm) = std::fs::read_to_string("/proc/self/statm") {
				let parts: Vec<&str> = statm.split_whitespace().collect();
				if parts.len() >= 2 {
					if let (Ok(vsize), Ok(rss)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
					{
						metrics::gauge!(PROCESS_RESIDENT_MEMORY_BYTES)
							.set((rss * page_size) as f64);
						metrics::gauge!(PROCESS_VIRTUAL_MEMORY_BYTES)
							.set((vsize * page_size) as f64);
					}
				}
			}

			// ── Tokio runtime metrics ─────────────────────────────────────
			let rt = handle.metrics();
			metrics::gauge!(TOKIO_ALIVE_TASKS).set(rt.num_alive_tasks() as f64);
			metrics::gauge!(TOKIO_NUM_WORKERS).set(rt.num_workers() as f64);
			metrics::gauge!(TOKIO_GLOBAL_QUEUE_DEPTH).set(rt.global_queue_depth() as f64);
		}
	});
}

/// Record a completed GraphQL query with operation name and query type.
pub fn record_graphql_query(operation: &str, query_type: &str) {
	metrics::counter!(
		GRAPHQL_QUERIES_TOTAL,
		"operation" => operation.to_string(),
		"type" => query_type.to_string(),
	)
	.increment(1);
}

/// Record GraphQL query duration by type.
pub fn record_graphql_duration(query_type: &str, duration_secs: f64) {
	metrics::histogram!(
		GRAPHQL_QUERY_DURATION_SECONDS,
		"type" => query_type.to_string(),
	)
	.record(duration_secs);
}

/// Record a GraphQL error.
pub fn record_graphql_error() {
	metrics::counter!(GRAPHQL_ERRORS_TOTAL).increment(1);
}

/// Update database pool connection gauges.
pub fn set_db_pool_metrics(size: f64, available: f64, max_size: f64) {
	metrics::gauge!(DB_POOL_SIZE).set(size);
	metrics::gauge!(DB_POOL_AVAILABLE).set(available);
	metrics::gauge!(DB_POOL_MAX_SIZE).set(max_size);
}

/// Record time spent waiting for a pool connection.
pub fn record_pool_wait(duration_secs: f64) {
	metrics::histogram!(DB_POOL_WAIT_DURATION).record(duration_secs);
}

/// Record a prepared statement cache hit at the request level
/// (same SQL reused within one GraphQL request).
pub fn record_stmt_cache_hit_request() {
	metrics::counter!(STMT_CACHE_HITS_REQUEST).increment(1);
}

/// Record a prepared statement cache hit at the connection level
/// (SQL already prepared on this pooled PG connection from a prior request).
pub fn record_stmt_cache_hit_connection() {
	metrics::counter!(STMT_CACHE_HITS_CONNECTION).increment(1);
}

/// Record a prepared statement cache miss
/// (statement sent to PostgreSQL for parse+plan for the first time on this connection).
pub fn record_stmt_cache_miss() {
	metrics::counter!(STMT_CACHE_MISSES).increment(1);
}

/// Record GraphQL response body size.
pub fn record_response_size(size_bytes: usize) {
	metrics::histogram!(GRAPHQL_RESPONSE_SIZE_BYTES).record(size_bytes as f64);
}

/// Record a raw SQL query execution.
pub fn record_sql_query(query_type: &str, duration_secs: f64) {
	metrics::counter!(SQL_QUERIES_TOTAL, "type" => query_type.to_string()).increment(1);
	metrics::histogram!(SQL_QUERY_DURATION_SECONDS, "type" => query_type.to_string())
		.record(duration_secs);
}

/// Record a pool checkout attempt.
pub fn record_connection_checkout(result: &str) {
	metrics::counter!(CONNECTION_CHECKOUT_TOTAL, "result" => result.to_string()).increment(1);
}

/// Set the current in-flight request count.
pub fn set_in_flight_requests(count: usize) {
	metrics::gauge!(IN_FLIGHT_REQUESTS).set(count as f64);
}

/// Record a computed query complexity score.
pub fn record_query_complexity(score: usize) {
	metrics::histogram!(QUERY_COMPLEXITY_SCORE).record(score as f64);
}

/// Record a dataloader batch size.
pub fn record_dataloader_batch_size(size: usize) {
	metrics::histogram!(DATALOADER_BATCH_SIZE).record(size as f64);
}
