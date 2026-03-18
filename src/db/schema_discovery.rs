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

//! SubQuery project schema discovery.
//!
//! [`discover_schema`] queries `pg_namespace` to find the PostgreSQL schema
//! that corresponds to the project name supplied via `--name`.  SubQuery
//! indexers create one schema per project; this lookup maps the human-readable
//! project name to the actual schema identifier used in subsequent queries.

use anyhow::{Result, bail};
use deadpool_postgres::Pool;
use tracing::debug;

/// Resolve the PostgreSQL schema name for the given project.
///
/// Mirrors `ProjectService.getProjectSchema()` from the TypeScript implementation:
/// 1. Look for an existing schema in `information_schema.schemata`.
/// 2. Fall back to the legacy `public.subqueries` table.
pub async fn discover_schema(pool: &Pool, project_name: &str) -> Result<String> {
	let client = pool.get().await?;

	// ── Primary: information_schema ─────────────────────────────────────────
	let rows = client.query("SELECT schema_name FROM information_schema.schemata", &[]).await?;

	let schemas: Vec<String> = rows.iter().map(|r| r.get::<_, String>("schema_name")).collect();

	debug!(schema_count = schemas.len(), "Discovered PostgreSQL schemas");

	if schemas.iter().any(|s| s == project_name) {
		return Ok(project_name.to_string());
	}

	// ── Fallback: legacy public.subqueries table ────────────────────────────
	let legacy = client
		.query("SELECT db_schema FROM public.subqueries WHERE name = $1", &[&project_name])
		.await;

	match legacy {
		Ok(rows) if !rows.is_empty() => {
			let schema: String = rows[0].get("db_schema");
			Ok(schema)
		},
		_ => bail!("Unknown project name: '{project_name}'"),
	}
}
