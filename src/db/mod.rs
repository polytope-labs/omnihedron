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

//! PostgreSQL connection pool and schema discovery.
//!
//! - [`pool::create_pool`]              — builds a `deadpool-postgres` connection pool, optionally
//!   with TLS using the certificates from [`Config`].
//! - [`schema_discovery::discover_schema`] — resolves the target PostgreSQL schema name from the
//!   project name supplied via `--name`.

pub mod pool;
pub mod schema_discovery;
pub mod stmt_cache;

pub use pool::create_pool;
pub use schema_discovery::discover_schema;
pub use stmt_cache::StmtCache;

/// Checkout a connection from the pool with metrics instrumentation.
///
/// Records `omnihedron_db_pool_wait_duration_seconds` (time spent waiting for a
/// connection) and `omnihedron_connection_checkout_total` (success/timeout counter).
pub async fn checkout(
	pool: &deadpool_postgres::Pool,
) -> Result<deadpool_postgres::Client, deadpool_postgres::PoolError> {
	let start = std::time::Instant::now();
	let result = pool.get().await;
	crate::metrics::record_pool_wait(start.elapsed().as_secs_f64());
	match &result {
		Ok(_) => crate::metrics::record_connection_checkout("success"),
		Err(_) => crate::metrics::record_connection_checkout("timeout"),
	}
	result
}

/// A per-request database client that shares a single connection and prepared
/// statement cache across all resolvers in a GraphQL request.
///
/// This avoids the N+1 connection checkout problem and enables prepared
/// statement reuse across resolvers, improving cache hit rates.
pub struct RequestClient {
	client: deadpool_postgres::Client,
	cache: tokio::sync::Mutex<StmtCache>,
}

impl RequestClient {
	/// Checkout a connection from the pool and create a per-request client.
	pub async fn new(pool: &deadpool_postgres::Pool) -> Result<Self, deadpool_postgres::PoolError> {
		let client = checkout(pool).await?;
		Ok(Self { client, cache: tokio::sync::Mutex::new(StmtCache::new()) })
	}

	/// Execute a query using the shared connection and statement cache.
	pub async fn query(
		&self,
		sql: &str,
		params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
	) -> Result<Vec<tokio_postgres::Row>, tokio_postgres::Error> {
		self.cache.lock().await.query(&self.client, sql, params).await
	}

	/// Execute a query expecting exactly one row.
	pub async fn query_one(
		&self,
		sql: &str,
		params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
	) -> Result<tokio_postgres::Row, tokio_postgres::Error> {
		self.cache.lock().await.query_one(&self.client, sql, params).await
	}

	/// Execute a query expecting zero or one row.
	pub async fn query_opt(
		&self,
		sql: &str,
		params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
	) -> Result<Option<tokio_postgres::Row>, tokio_postgres::Error> {
		self.cache.lock().await.query_opt(&self.client, sql, params).await
	}
}
