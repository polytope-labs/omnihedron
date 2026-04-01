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

//! GraphQL field resolvers.
//!
//! Each sub-module handles one category of resolver:
//! - [`connection`]  — list queries with filtering, ordering, and pagination
//! - [`single`]      — single-record lookup by primary key or nodeId
//! - [`relations`]   — forward (FK → parent) and backward (reverse FK → children) relations
//! - [`aggregates`]  — aggregate functions (count, sum, min, max, avg, stddev, variance)
//! - [`metadata`]    — `_metadata` and `_metadatas` queries

pub mod aggregates;
pub mod connection;
pub mod dataloader;
pub mod metadata;
pub mod relations;
pub mod search;
pub mod single;

/// Extract the actual PostgreSQL error message from a `tokio_postgres::Error`.
///
/// `tokio_postgres::Error`'s `Display` just shows "db error" for server-side
/// errors.  The real message (e.g. "canceling statement due to statement timeout")
/// is inside the `DbError` accessible via `.as_db_error()`.
pub fn pg_error_detail(e: &tokio_postgres::Error) -> String {
	if let Some(db) = e.as_db_error() {
		let mut msg = db.message().to_string();
		if let Some(detail) = db.detail() {
			msg.push_str(": ");
			msg.push_str(detail);
		}
		msg
	} else {
		e.to_string()
	}
}

/// Convert a `tokio_postgres::Error` into an `async_graphql::Error` with a
/// human-readable message extracted from the underlying `DbError`.
pub fn pg_to_gql_error(e: tokio_postgres::Error) -> async_graphql::Error {
	let detail = pg_error_detail(&e);
	async_graphql::Error::new(format!("db error: {detail}"))
}
