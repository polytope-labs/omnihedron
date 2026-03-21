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

//! Prepared statement cache leveraging deadpool-postgres's built-in per-connection cache.
//!
//! Uses `client.prepare_cached()` which persists prepared statements across
//! pool checkouts on the same underlying PostgreSQL connection. This means
//! a statement prepared in request N is reusable by request N+1 if they
//! happen to get the same connection from the pool.
//!
//! Additionally maintains a per-request `HashMap` to avoid repeated lookups
//! into the deadpool cache (which takes a read lock) for statements already
//! used within the current request.
//!
//! Metrics are split into three tiers:
//! - **request hit**: same SQL reused within one GraphQL request
//! - **connection hit**: SQL already prepared on this pooled PG connection
//! - **miss**: statement sent to PostgreSQL for parse+plan

use std::collections::HashMap;
use tokio_postgres::{Row, Statement, types::ToSql};

/// A prepared statement cache that combines per-request fast lookup with
/// deadpool's per-connection persistent cache.
pub struct StmtCache {
	/// Per-request cache: avoids repeated deadpool cache lookups for
	/// statements used multiple times within the same request.
	request_stmts: HashMap<String, Statement>,
}

impl Default for StmtCache {
	fn default() -> Self {
		Self::new()
	}
}

impl StmtCache {
	pub fn new() -> Self {
		Self { request_stmts: HashMap::with_capacity(32) }
	}

	/// Execute a query, preparing the statement on first use and caching it.
	pub async fn query(
		&mut self,
		client: &deadpool_postgres::Client,
		sql: &str,
		params: &[&(dyn ToSql + Sync)],
	) -> Result<Vec<Row>, tokio_postgres::Error> {
		let stmt = self.get_or_prepare(client, sql).await?;
		let start = std::time::Instant::now();
		let result = client.query(&stmt, params).await;
		crate::metrics::record_sql_query("select", start.elapsed().as_secs_f64());
		result
	}

	/// Execute a query expecting exactly one row.
	pub async fn query_one(
		&mut self,
		client: &deadpool_postgres::Client,
		sql: &str,
		params: &[&(dyn ToSql + Sync)],
	) -> Result<Row, tokio_postgres::Error> {
		let stmt = self.get_or_prepare(client, sql).await?;
		let start = std::time::Instant::now();
		let result = client.query_one(&stmt, params).await;
		crate::metrics::record_sql_query("select", start.elapsed().as_secs_f64());
		result
	}

	/// Execute a query expecting zero or one row.
	pub async fn query_opt(
		&mut self,
		client: &deadpool_postgres::Client,
		sql: &str,
		params: &[&(dyn ToSql + Sync)],
	) -> Result<Option<Row>, tokio_postgres::Error> {
		let stmt = self.get_or_prepare(client, sql).await?;
		let start = std::time::Instant::now();
		let result = client.query_opt(&stmt, params).await;
		crate::metrics::record_sql_query("select", start.elapsed().as_secs_f64());
		result
	}

	async fn get_or_prepare(
		&mut self,
		client: &deadpool_postgres::Client,
		sql: &str,
	) -> Result<Statement, tokio_postgres::Error> {
		// Fast path: already used in this request
		if let Some(stmt) = self.request_stmts.get(sql) {
			crate::metrics::record_stmt_cache_hit_request();
			return Ok(stmt.clone());
		}

		// Check deadpool's per-connection cache size before and after to
		// distinguish connection-level hits from true PG misses.
		let size_before = client.statement_cache.size();
		let stmt = client.prepare_cached(sql).await?;
		let size_after = client.statement_cache.size();

		if size_after > size_before {
			// Cache grew — this was a true miss, PG parsed+planned the statement
			crate::metrics::record_stmt_cache_miss();
		} else {
			// Cache didn't grow — statement was already prepared on this connection
			crate::metrics::record_stmt_cache_hit_connection();
		}

		self.request_stmts.insert(sql.to_string(), stmt.clone());
		Ok(stmt)
	}
}
